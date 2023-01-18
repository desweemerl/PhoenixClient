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

typealias MessageCallback = (message: IncomingMessage) -> Unit

interface Client {
    val state: Flow<ConnectionState>
    val active: Boolean
    val messages: Flow<IncomingMessage>

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
    val serializer: (message: OutgoingMessage) -> String = { it.toJson() },
) : Client {
    private val logger = KotlinLogging.logger {}
    private var _active = false
    override val active
        get() = _active

    private var scope = CoroutineScope(Dispatchers.Default)
    private var heatBeatJob: Job? = null
    private var connectJob: Job? = null

    // Manage message ref
    private val messageRef = refGenerator()

    // Channels storage
    private val channels = mutableMapOf<String, ChannelImpl>()

    private val _state = MutableStateFlow(ConnectionState.DISCONNECTED)
    override val state = _state.asStateFlow()

    // Store incoming messages by ref
    private val messageBuffer = mutableMapOf<String, IncomingMessage>()

    // Incoming messages
    private val _messages = MutableSharedFlow<IncomingMessage>(extraBufferCapacity = 100)
    override val messages = _messages.asSharedFlow()

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
        noReply: Boolean = false,
    ): Result<IncomingMessage?> {
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
            if (channel != null && channel.state.value != ChannelState.JOINED) {
                if (event == "phx_join") {
                    if (state.value != ConnectionState.CONNECTED) {
                        throw BadActionException(
                            "Failed to join channel with topic '${channel.topic}' because WebSocket is not connected"
                        )
                    }
                } else {
                    logger.debug(
                        "Waiting for channel with topic '${channel.topic}' to join before sending "
                                + "message with event='$event' and payload='$payload'"
                    )

                    waitUntil(1) {
                        channel.state.value == ChannelState.JOINED
                    }
                }
            }

            val ref = messageRef().toString()
            val outgoingMessage = OutgoingMessage(topic, event, payload, ref, joinRef)
            webSocketEngine.send(serializer(outgoingMessage))

            if (noReply) {
                null
            } else {
                waitUntil(1) {
                    messageBuffer.containsKey(ref)
                }
                val message = messageBuffer.remove(ref)

                if (message?.isError() == true || message?.isReplyError() == true) {
                    rejoinChannel(topic)
                    throw ResponseException(
                        "Phoenix returned an error for message with ref '${ref}",
                        message
                    )
                }
                message
            }
        }

        sendTimer.start()

        return sendTimer.lastResult!!
    }

    private fun dirtyCloseChannels() {
        channels.values.forEach { it.dirtyClose() }
    }

    private suspend fun rejoinChannel(topic: String) = coroutineScope {
        channels[topic]?.let {
            launch {
                it.dirtyClose()
                it.rejoin()
            }
        }
    }

    private fun rejoinChannels() {
        scope.launch {
            channels.values.filter { it.isJoinedOnce }.forEach { it.rejoin() }
        }
    }

    override suspend fun join(topic: String, payload: Map<String, Any?>): Result<Channel> =
        join(topic, payload, defaultTimeout.toDynamicTimeout(true))

    override suspend fun join(topic: String, payload: Map<String, Any?>, timeout: DynamicTimeout): Result<Channel> {
        val channel = channels.getOrPut(topic) {
            val sendToSocket: suspend (String, Map<String, Any?>, DynamicTimeout, String?, Boolean)
            -> Result<IncomingMessage?> =
                { event, payload, channelTimeout, joinRef, noReply ->
                    send(topic, event, payload, channelTimeout, joinRef, noReply)
                }

            val disposeFromSocket: suspend (topic: String) -> Unit = {
                channels[topic]?.let {
                    it.close()
                    channels.remove(topic)
                }
            }

            val topicMessages = messages.filter {
                it.topic == topic
            }

            return@getOrPut ChannelImpl(topic, topicMessages, sendToSocket, disposeFromSocket)
        }

        return if (!_active) {
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
                    _active && state.value != ConnectionState.CONNECTED
                }
                _active
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

    private suspend fun launchWebSocket(params: Map<String, String>) = coroutineScope {
        if (_state.value != ConnectionState.DISCONNECTED) {
            return@coroutineScope
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
            ).takeWhile {
                _active && _state.value != ConnectionState.DISCONNECTED
            }
                .collect { event ->
                    event.message?.let { incomingMessage ->
                        logger.debug("Receiving message from engine: $incomingMessage")

                        launch {
                            val emitted = _messages.tryEmit(incomingMessage)
                            if (!emitted) {
                                logger.warn("Failed to emit message: $incomingMessage")
                            }
                        }

                        event.message.let { message ->
                            if (message.ref != null) {
                                messageBuffer[message.ref] = message
                            }
                        }

                        // TODO: Check forbidden on both socket and channel
                        if (incomingMessage == Forbidden) {
                            _active = false
                        }
                    }

                    event.state?.let { newState ->
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
            while (_active) {
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
        if (_active) {
            throw Exception("WebSocket is already active")
        }

        _active = true

        // Retry after being disconnected
        scope.launch {
            val retryTimer = Timer(retry) {
                if (!_active) {
                    throw BadActionException("WebSocket is not active")
                }
                // Let the webSocket set the connection up
                if (state.value == ConnectionState.DISCONNECTED) {
                    connectJob?.cancelAndJoin()
                    connectJob = scope.launch {
                        launchWebSocket(params)
                    }
                }

                waitWhile(1) {
                    _active && state.value != ConnectionState.CONNECTED
                }
            }

            state
                .takeWhile { _active }
                .collect {
                    if (it == ConnectionState.CONNECTED) {
                        launchHeartbeat()
                        rejoinChannels()
                    } else if (it == ConnectionState.DISCONNECTED) {
                        heatBeatJob?.cancelAndJoin()
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

    override suspend fun disconnect() {
        _active = false

        channels.values.forEach { it.leave() }
        channels.clear()

        webSocketEngine.close()
        messageBuffer.clear()

        scope.cancel()
    }
}

fun okHttpPhoenixClient(
    host: String = DEFAULT_WS_HOST,
    port: Int = DEFAULT_WS_PORT,
    path: String = DEFAULT_WS_PATH,
    ssl: Boolean = DEFAULT_WS_SSL,
    untrustedCertificate: Boolean = DEFAULT_UNTRUSTED_CERTIFICATE,
    retryTimeout: DynamicTimeout = DEFAULT_RETRY,
    heartbeatInterval: Long = DEFAULT_HEARTBEAT_INTERVAL,
    heartbeatTimeout: Long = DEFAULT_HEARTBEAT_TIMEOUT,
    defaultTimeout: Long = DEFAULT_TIMEOUT,
): Result<Client> =
    try {
        Result.success(
            ClientImpl(
                host,
                port,
                path,
                ssl,
                untrustedCertificate,
                retryTimeout,
                heartbeatInterval,
                heartbeatTimeout,
                defaultTimeout,
                OkHttpEngine(),
            )
        )
    } catch (ex: Exception) {
        Result.failure(ex)
    }