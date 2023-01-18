package phoenixclient

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import phoenixclient.engine.OkHttpEngine


enum class ConnectionState {
    CONNECTED,
    CONNECTING,
    DISCONNECTED,
}

fun Flow<ConnectionState>.isConnected() = this.filter { it == ConnectionState.CONNECTED }.map { true }

interface Client {
    val state: StateFlow<ConnectionState>
    val messages: SharedFlow<IncomingMessage>

    fun connect(params: Map<String, String>)
    suspend fun disconnect()

    suspend fun join(topic: String, payload: Map<String, Any?> = mapOf()): Result<Channel>
    suspend fun join(topic: String, payload: Map<String, Any?>, timeout: DynamicTimeout): Result<Channel>
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
    val retry: DynamicTimeout = DEFAULT_RETRY,
    val heartbeatInterval: Long = DEFAULT_HEARTBEAT_INTERVAL,
    val heartbeatTimeout: Long = DEFAULT_HEARTBEAT_TIMEOUT,
    private val defaultTimeout: Long = DEFAULT_TIMEOUT,
    private val webSocketEngine: WebSocketEngine = OkHttpEngine(),
    val serializer: (message: OutgoingMessage) -> String = {it.toJson()},
) : Client {
    private val logger = KotlinLogging.logger {}
    private var active = false

    private var scope = CoroutineScope(Dispatchers.Default)
    private var heatBeatJob: Job? = null
    private var connectJob: Job? = null

    // Incoming and outgoing messages
    private val incomingFlow = MutableSharedFlow<IncomingMessage>()
    private val incomingBuffer = mutableMapOf<String, IncomingMessage>()

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
        timeout: DynamicTimeout,
        joinRef: String? = null,
        noReply: Boolean = false
    ) : Result<IncomingMessage?> {
        val channel = channels[topic]

        channel?.let {
            if (!it.isJoinedOnce && event != "phx_join") {
                return Result.failure(
                    BadActionException(
                        "Channel with topic '$topic' was never joined. " +
                                "Join the channel before pushing message"
                    )
                )
            }
        }

        val sendTimer = timer(timeout) {
            if (channel != null && event != "phx_join" && channel.state.value != ChannelState.JOINED) {
                logger.debug(
                    "Waiting for channel with topic '${channel.topic}' to join before sending "
                            + "message with event='$event' and payload='$payload'"
                )
                waitUntil(1) {
                    channel.state.value == ChannelState.JOINED
                }
            }

            val ref = messageRef().toString()
            val message = OutgoingMessage(topic, event, payload, ref, joinRef)
            webSocketEngine.send(serializer(message))

            if (noReply) {
                null
            } else {
                waitUntil(1) {
                    incomingBuffer.containsKey(ref)
                }

                incomingBuffer.remove(ref)
            }
        }

        sendTimer.start()

        return sendTimer.lastResult!!
    }

    fun dirtyCloseChannels() {
        channels.values.forEach { it.dirtyClose() }
    }

    fun rejoinChannels() {
        scope.launch {
            channels.values.filter { it.isJoinedOnce }.forEach{ it.rejoin() }
        }
    }

    override suspend fun join(topic: String, payload: Map<String, Any?>): Result<Channel> =
        join(topic, payload, defaultTimeout.toDynamicTimeout(true))

    override suspend fun join(topic: String, payload: Map<String, Any?>, timeout: DynamicTimeout): Result<Channel> {
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
                            ci.rejoin()
                        }
                    }
            }

            val sendToSocket: suspend (String, Map<String, Any?>, DynamicTimeout, String?) -> Result<IncomingMessage?> =
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

        return if (!active) {
            Result.failure(BadActionException("Socket is not active"))
        } else if (channel.isJoinedOnce) {
            if (channel.state.value != ChannelState.JOINED) {
                logger.debug("Socket is connected and channel with topic '$topic' will be joined automatically")
            }
            Result.success(channel)
        } else if (
            state.value == ConnectionState.CONNECTED
            && channel.state.value == ChannelState.CLOSE
        ) {
            logger.debug("Joining channel with topic '$topic'")
            channel.join(payload)
        } else {
            logger.debug("Waiting socket to be connected before joining channel with topic '$topic'")
            val joinTimer = timer(timeout) {
                waitWhile(10L) {
                    active && state.value != ConnectionState.CONNECTED
                }
                active
            }

            joinTimer.start()

            val result = joinTimer.lastResult!!

            if (result.isFailure) {
                Result.failure(BadActionException("Socket is not active"))
            } else {
                channel.join(payload)
            }
        }
    }

    suspend fun launchWebSocket(params: Map<String, String>)  {
        if (_state.value != ConnectionState.DISCONNECTED) {
            return
        }

        logger.info("Launching webSocket")
        _state.update { ConnectionState.CONNECTING }

        try {
            webSocketEngine.connect(
                host = host,
                port = port,
                path = path,
                params = params,
                ssl = ssl,
                untrustedCertificate = untrustedCertificate,
            )
                .takeWhile {
                    active && _state.value != ConnectionState.DISCONNECTED
                }
                .collect { incomingMessage ->
                    incomingMessage.message?.let {
                        logger.debug("Receiving message from engine: ${incomingMessage.message}")
                        incomingMessage.message.ref?.let { ref ->
                            incomingBuffer[ref] = incomingMessage.message
                        }

                        incomingFlow.emit(it)

                        // TODO: Check forbidden on both socket and channel
                        if (it == Forbidden) {
                            active = false
                        }
                    }

                    incomingMessage.state?.let { newState ->
                        logger.debug("Receiving new state '$newState' from webSocket engine")
                        _state.update { newState }
                    }
                }
        } finally {
            _state.update { ConnectionState.DISCONNECTED }
        }
    }

    private fun launchHeartbeat() {
        if (heatBeatJob?.isActive == true) {
            return
        }

        heatBeatJob = scope.launch {
            while (active) {
                delay(heartbeatInterval)

                if (_state.value == ConnectionState.CONNECTED
                    && send("phoenix", "heartbeat", mapOf(), heartbeatTimeout.toDynamicTimeout()).isFailure
                ) {
                    webSocketEngine.close()
                }
            }
        }
    }

    override fun connect(params: Map<String, String>) {
        if (active) {
            throw Exception("WebSocket is already active")
        }

        active = true

        // Retry after being disconnected
        scope.launch {
            val retryTimer = Timer(retry) {
                // Let the webSocket set the connection up
                if (connectJob?.isActive != true || state.value != ConnectionState.CONNECTING) {
                    connectJob?.cancel()
                    connectJob = scope.launch {
                        launchWebSocket(params)
                    }
                }

                waitWhile(1) {
                    active && state.value != ConnectionState.CONNECTED
                }
            }

            state
                .takeWhile { active }
                .collect {
                    when (it) {
                        ConnectionState.CONNECTED -> {
                            launchHeartbeat()
                            rejoinChannels()
                        }
                        ConnectionState.DISCONNECTED -> {
                            heatBeatJob?.cancel()
                            dirtyCloseChannels()

                            if (!retryTimer.active) {
                                scope.launch {
                                    retryTimer.reset()
                                    retryTimer.start()
                                }
                            }
                        }
                    }
                }
        }
    }

    override suspend fun disconnect() {
        active = false

        channels.values.forEach { it.leave() }
        channels.clear()

        webSocketEngine.close()

        incomingBuffer.clear()
        scope.cancel()
    }
}

fun okHttpPhoenixClient(
    host: String = DEFAULT_WS_HOST,
    port: Int = DEFAULT_WS_PORT,
    path: String = DEFAULT_WS_PATH,
    ssl: Boolean = DEFAULT_WS_SSL,
    untrustedCertificate: Boolean = DEFAULT_UNTRUSTED_CERTIFICATE,
    retry: DynamicTimeout = DEFAULT_RETRY,
    heartbeatInterval: Long = DEFAULT_HEARTBEAT_INTERVAL,
    heartbeatTimeout: Long = DEFAULT_HEARTBEAT_TIMEOUT,
    defaultTimeout: Long = DEFAULT_TIMEOUT,
) : Result<Client> =
    try {
        Result.success(
            ClientImpl(
                host, port, path, ssl, untrustedCertificate, retry, heartbeatInterval, heartbeatTimeout, defaultTimeout, OkHttpEngine()
            )
        )
    } catch (ex: Exception) {
        Result.failure(ex)
    }