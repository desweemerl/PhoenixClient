package phoenixclient

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import phoenixclient.engine.OkHttpEngine


const val VSN = "2.0.0"

const val DEFAULT_HEARTBEAT_INTERVAL = 30000L // 30 secs
const val DEFAULT_TIMEOUT = 1000L // 1 sec
const val DEFAULT_RETRY = 5000L // 5 secs


enum class ConnectionState {
    CONNECTED,
    CONNECTING,
    DISCONNECTED,
}

fun Flow<ConnectionState>.isConnected() = this.filter { it == ConnectionState.CONNECTED }

interface Client {
    val state: StateFlow<ConnectionState>
    val messages: SharedFlow<IncomingMessage>

    fun connect(params: Map<String, String>)
    fun disconnect()

    suspend fun join(topic: String, payload: Map<String, Any?> = mapOf()): Result<Channel>
    suspend fun join(topic: String, payload: Map<String, Any?>, timeout: Long): Result<Channel>
}

fun refGenerator(): () -> Long {
    var ref = 0L

    return {
        ref = if (ref == Long.MAX_VALUE) {
            0
        } else {
            ++ref
        }
        ref
    }
}

private class ClientImpl(
    val host: String = DEFAULT_WS_HOST,
    val port: Int = DEFAULT_WS_PORT,
    val path: String = DEFAULT_WS_PATH,
    val ssl: Boolean = DEFAULT_WS_SSL,
    val untrustedCertificate: Boolean = DEFAULT_UNTRUSTED_CERTIFICATE,
    val retry: Long = DEFAULT_RETRY,
    val heartbeatInterval: Long = DEFAULT_HEARTBEAT_INTERVAL,
    private val defaultTimeout: Long = DEFAULT_TIMEOUT,
    private val webSocketEngine: WebSocketEngine = OkHttpEngine(),
    val serializer: (message: OutgoingMessage) -> String = {it.toJson()},
) : Client {
    private val logger = KotlinLogging.logger {}
    private var activated = false

    private var scope = CoroutineScope(Dispatchers.Default)
    private var heatBeatJob: Job? = null

    // Incoming and outgoing messages
    private val incomingFlow = MutableSharedFlow<IncomingMessage>()
    private val outgoingFlow = MutableSharedFlow<OutgoingMessage>()

    // Manage message ref
    private val messageRef = refGenerator()

    // Channels storage
    private val channels = mutableMapOf<String, ChannelImpl>()

    private val _state = MutableStateFlow(ConnectionState.DISCONNECTED)
    override val state = _state.asStateFlow()
    override val messages = incomingFlow.asSharedFlow()

    init {
        if (heartbeatInterval < defaultTimeout) {
            throw ConfigurationException(
                "heartbeatInterval $heartbeatInterval must be greater or equal to defaultTimeout $defaultTimeout"
            )
        }
    }

    private suspend fun send(
        topic: String,
        event: String,
        payload: Map<String, Any?>,
        timeout: Long? = null,
        joinRef: String? = null,
    ) : Result<IncomingMessage?> {
        val ref = messageRef().toString()

        val message = OutgoingMessage(topic, event, payload, ref, joinRef)
        webSocketEngine.send(serializer(message))

        if (timeout == null) {
            return Result.success(null)
        }

        return try {
            if (timeout > 0) {
                withTimeout(timeout) {
                    incomingFlow.filterRef(ref).toResult()
                }
            } else {
                incomingFlow.filterRef(ref).toResult()
            }
        } catch (ex: TimeoutCancellationException) {
            Result.failure(
                TimeoutException("Response for message with ref '$ref' didn't come after $timeout ms")
            )
        } catch (ex: Exception) {
            Result.failure(ex)
        }
    }

    fun dirtyCloseChannels() {
        channels.values.forEach { it.dirtyClose() }
    }

    suspend fun flushChannels() {
        if (_state.value != ConnectionState.CONNECTED) {
            return
        }

        channels.values.forEach {
            it.flushPushBuffer()
        }
    }

    override suspend fun join(topic: String, payload: Map<String, Any?>): Result<Channel> =
        join(topic, payload, defaultTimeout)

    override suspend fun join(topic: String, payload: Map<String, Any?>, timeout: Long): Result<Channel> {
        val channel = channels.getOrPut(topic) {
            var channelImpl: ChannelImpl? = null

            val incomingChannelMessage = messages
                .filter { it.topic == topic }

            val errorChannelJob = scope.launch {
                incomingChannelMessage
                    .filter { it.isError() }
                    .collect {
                        logger.info("Trying to reconnect to channel with topic '$topic'")
                        channelImpl?.let { ci ->
                            ci.dirtyClose()
                            ci.rejoin(timeout).onFailure {
                                logger.error(
                                    "Failed to rejoin channel with topic '$topic': " +
                                        it.stackTraceToString()
                                )
                            }
                        }
                    }
            }

            val sendToSocket: suspend (String, Map<String, Any?>, Long?, String?) -> Result<IncomingMessage?> =
                { event, payload, channelTimeout, joinRef ->
                    send(topic, event, payload, channelTimeout, joinRef)
                }

            val disposeFromSocket: suspend (topic: String) -> Unit = {
                channels[topic]?.let {
                    it.close()
                    errorChannelJob.cancel()
                    channels.remove(topic)
                }
            }

            channelImpl = ChannelImpl(topic, incomingChannelMessage, sendToSocket, disposeFromSocket)

            return@getOrPut channelImpl
        }

        return if (
            state.value == ConnectionState.CONNECTED
            && channel.state.value == ChannelState.CLOSE
        ) {
            logger.debug("Joining channel with topic '$topic'")
            channel.join(payload, timeout)
        } else {
            try {
                withTimeout(timeout) {
                    logger.info("Waiting for channel with topic '$topic' to be connected")
                    state.filter { it == ConnectionState.CONNECTED }.first()
                    channel.join(payload, 0)
                }
            } catch (ex: TimeoutCancellationException) {
                Result.failure(
                    TimeoutException("Failed to join channel with topic '$topic' after $timeout ms")
                )
            }
        }
    }

    suspend fun launchWebSocket(params: Map<String, String>)  {
        if (_state.value != ConnectionState.DISCONNECTED) {
            return
        }

        logger.info("Launching webSocket")
        _state.update { ConnectionState.CONNECTING }

        webSocketEngine.connect(
            host = host,
            port = port,
            path = path,
            params = params,
            ssl = ssl,
            untrustedCertificate = untrustedCertificate,
        )
            .takeWhile {
                activated &&  _state.value != ConnectionState.DISCONNECTED
            }
            .collect { incomingMessage ->
                incomingMessage.message?.let {
                    logger.debug("Receiving message from engine: ${incomingMessage.message}")
                    incomingFlow.emit(it)

                    if (it == Forbidden) {
                        activated = false
                    }
                }

                incomingMessage.state?.let { newState ->
                    logger.debug("Receiving new state '$newState' from webSocket engine")

                    if (newState == ConnectionState.CONNECTED) {
                        launchHeartbeat()
                    } else if (newState == ConnectionState.DISCONNECTED) {
                        heatBeatJob?.cancel()
                        dirtyCloseChannels()
                    }

                    _state.update { newState }
                }
            }
    }

    private fun launchHeartbeat() {
        if (heatBeatJob?.isActive == true) {
            return
        }

        heatBeatJob = scope.launch {
            while (activated) {
                delay(heartbeatInterval)
                if (_state.value == ConnectionState.CONNECTED
                    && send("phoenix", "heartbeat", mapOf(), defaultTimeout).getOrNull() == null
                ) {
                    webSocketEngine.close()
                }
            }
        }
    }

    override fun connect(params: Map<String, String>) {
        if (activated) {
            throw Exception("WebSocket is already activated")
        }

        activated = true

        scope.launch { launchWebSocket(params) }

        // Retry after being disconnected
        scope.launch {
            state
                .takeWhile { activated }
                .filter { it == ConnectionState.DISCONNECTED }
                .debounce(retry)
                .collect { scope.launch { launchWebSocket(params) } }
        }
    }

    override fun disconnect() {
        activated = false
        webSocketEngine.close()
    }
}

fun okHttpPhoenixClient(
    host: String = DEFAULT_WS_HOST,
    port: Int = DEFAULT_WS_PORT,
    path: String = DEFAULT_WS_PATH,
    ssl: Boolean = DEFAULT_WS_SSL,
    untrustedCertificate: Boolean = DEFAULT_UNTRUSTED_CERTIFICATE,
    retry: Long = DEFAULT_RETRY,
    heartbeatInterval: Long = DEFAULT_HEARTBEAT_INTERVAL,
    defaultTimeout: Long = DEFAULT_TIMEOUT,
) : Result<Client> =
    try {
        Result.success(
            ClientImpl(
                host, port, path, ssl, untrustedCertificate, retry, heartbeatInterval, defaultTimeout, OkHttpEngine()
            )
        )
    } catch (ex: Exception) {
        Result.failure(ex)
    }