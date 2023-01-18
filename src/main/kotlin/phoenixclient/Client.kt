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
    DISCONNECTING,
    DISCONNECTED,
    RECONNECTING,
}

interface Client {
    val state: StateFlow<ConnectionState>
    val messages: SharedFlow<IncomingMessage>

    suspend fun connect(params: Map<String, String>)
    suspend fun <T> onConnected(block: suspend (Client) -> T): T
    suspend fun disconnect()

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

    private var wsJob: Job? = null
    private var retryJob: Job? = null
    private var reconnectJob: Job? = null

    private val _state = MutableStateFlow(ConnectionState.DISCONNECTED)

    // Incoming and outgoing messages
    private val incomingFlow = MutableSharedFlow<IncomingMessage>()
    private val outgoingFlow = MutableSharedFlow<OutgoingMessage>()

    // Manage message ref
    private val messageRef = refGenerator().toString()

    // Channels storage
    private val channels: MutableMap<String, ChannelImpl> = mutableMapOf()

    override val state = _state.asStateFlow()
    override val messages = incomingFlow.asSharedFlow()

    init {
        if (heartbeatInterval < defaultTimeout) {
            ConfigurationException(
                "heartbeatInterval $heartbeatInterval must be greater or equal to defaultTimeout"
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

        val messageRef = refGenerator().toString()

        val message = OutgoingMessage(topic, event, payload, messageRef, joinRef)
        logger.debug("Emitting message to webSocket: $message")
        outgoingFlow.emit(message)

        if (timeout == null) {
            return Result.success(null)
        }

        return try {
            if (timeout > 0) {
                withTimeout(timeout) {
                    incomingFlow.filterRef(messageRef).toResult()
                }
            } else {
                incomingFlow.filterRef(messageRef).toResult()
            }
        } catch (ex: TimeoutCancellationException) {
            Result.failure(
                TimeoutException("Response for message $messageRef didn't come after $timeout ms")
            )
        } catch (ex: Exception) {
            Result.failure(ex)
        }
    }

    suspend fun heartbeat(): Boolean {
        logger.info("Sending heartbeat")
        return send("phoenix", "heartbeat", mapOf(), defaultTimeout).getOrNull() != null
    }

    suspend fun flushChannels() {
        if (_state.value != ConnectionState.CONNECTED) {
            return
        }

        channels.values.forEach {
            it.flushPushBuffer()
            }
        }

    override suspend fun <T> onConnected(block: suspend (Client) -> T): T {
        state.filter { it == ConnectionState.CONNECTED }.first()
        return block(this)
    }

    override suspend fun join(topic: String, payload: Map<String, Any?>): Result<Channel> =
        join(topic, payload, defaultTimeout)

    override suspend fun join(topic: String, payload: Map<String, Any?>, timeout: Long): Result<Channel> {
        val channel = channels.getOrPut(topic) {
            val incomingChannelMessage = messages
                .filter { it.topic == topic }

            val sendToSocket: suspend (String, Map<String, Any?>, Long?, String?) -> Result<IncomingMessage?> =
                { event, payload, channelTimeout, joinRef ->
                    send(topic, event, payload, channelTimeout, joinRef)
                }

            val disposeFromSocket: suspend (topic: String) -> Unit = {
                channels[topic]?.let {
                    it.close()
                    channels.remove(topic)
                }
            }

            ChannelImpl(topic, incomingChannelMessage, sendToSocket, disposeFromSocket)
        }

        return if (
            state.value == ConnectionState.CONNECTED
            && channel.state.value == ChannelState.CLOSE
        ) {
            channel.join(payload, timeout)
        } else {
            try {
                withTimeout(timeout) {
                    state.filter { it == ConnectionState.CONNECTED }.first()
                    channel.join(payload, 0)
                }
            } catch (ex: TimeoutCancellationException) {
                Result.failure(
                    TimeoutException("Failed to join channel with topic $topic after $timeout ms")
                )
            }
        }
    }

    suspend fun launchWebSocket(params: Map<String, String>) = coroutineScope {
        wsJob = launch {
            launch {
                webSocketEngine.connect(
                    host = host,
                    port = port,
                    path = path,
                    params = params,
                    ssl = ssl,
                    untrustedCertificate = untrustedCertificate,
                )
                    .collect { incomingMessage ->
                        incomingMessage.message?.let {
                            logger.info("Receiving message from engine: ${incomingMessage.message}")

                            incomingFlow.emit(it)

                            if (it == Forbidden) {
                                activated = false
                            }
                        }

                        incomingMessage.state?.let { newState ->
                            if (
                                (_state.value == ConnectionState.RECONNECTING
                                        && newState == ConnectionState.CONNECTED)
                                || _state.value != ConnectionState.RECONNECTING
                            ) {
                                _state.update { newState }
                            }
                        }
                    }
            }

            launch {
                _state
                    .filter { it == ConnectionState.CONNECTED }
                    .first()

                launch {
                    logger.info("Connection is established, waiting for outgoing messages")

                    outgoingFlow
                        .takeWhile { _state.value == ConnectionState.CONNECTED }
                        .collect {
                            logger.info("Sending message to engine: $it")
                            webSocketEngine.send(serializer(it))
                        }
                }

                launch {
                    logger.info("Setting up heartbeat")

                    while (true) {
                        delay(heartbeatInterval)

                        if (_state.value != ConnectionState.CONNECTED) {
                            break
                        }

                        if (!heartbeat()) {
                            webSocketEngine.close()
                            _state.update { ConnectionState.DISCONNECTED }
                            wsJob?.cancel()
                        }
                    }
                }

                launch {
                    flushChannels()
                }
            }
        }
    }

    private fun closeWebSocket() {
        webSocketEngine.close()
        wsJob?.cancel()
    }

    suspend fun reconnect(params: Map<String, String>) {
        while (true) {
            if (_state.value == ConnectionState.CONNECTED) {
                break
            }

            logger.info("WebSocket disconnected, trying to reconnect")

            _state.update { ConnectionState.RECONNECTING }

            closeWebSocket()
            wsJob = scope.launch {
                launchWebSocket(params)
            }

            delay(retry)
        }
    }

    override suspend fun connect(params: Map<String, String>) {
        if (state.value == ConnectionState.CONNECTED) {
            throw Exception("WebSocket is already connected")
        }

        activated = true

        scope.launch {
            reconnectJob = launch {
                state
                    .takeWhile { activated }
                    .debounce(retry)
                    .filter {
                        it != ConnectionState.RECONNECTING
                                && it != ConnectionState.CONNECTED
                                && retryJob?.isActive != true
                    }
                    .collect {
                        retryJob = launch {
                            reconnect(params)
                        }
                    }
            }

            wsJob = launch {
                launchWebSocket(params)
            }
        }
    }

    override suspend fun disconnect() {
        activated = false

        reconnectJob?.cancel()
        closeWebSocket()

        _state.update { ConnectionState.DISCONNECTED }
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