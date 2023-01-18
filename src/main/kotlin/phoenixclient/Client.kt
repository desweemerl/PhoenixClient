package phoenixclient

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import phoenixclient.engine.*


enum class ConnectionState {
    CONNECTED,
    CONNECTING,
    DISCONNECTED,
}

class ClientState(
    val active: Boolean = false,
    val connectionState: ConnectionState = ConnectionState.DISCONNECTED,
)

fun Flow<ClientState>.isConnected() = this.filter { it.connectionState == ConnectionState.CONNECTED }.map { true }
suspend fun Flow<ClientState>.waitConnected() = this.isConnected().first()

fun Flow<ClientState>.isConnecting() = this.filter { it.connectionState == ConnectionState.CONNECTING }.map { true }
suspend fun Flow<ClientState>.waitConnecting() = this.isConnecting().first()
fun Flow<ClientState>.isDisconnected() = this.filter { it.connectionState == ConnectionState.DISCONNECTED }.map { true }
suspend fun Flow<ClientState>.waitDisconnected() = this.isDisconnected().first()

fun Flow<ClientState>.isActive() = this.map { it.active }

interface Client {
    val state: Flow<ClientState>
    val messages: Flow<IncomingMessage>
    val active: Boolean

    fun connect(params: Map<String, String>)
    suspend fun disconnect()

    suspend fun join(topic: String, payload: Any = emptyPayload): Result<Channel>
    suspend fun join(topic: String, payload: Any, timeout: DynamicTimeout): Result<Channel>
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
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default),
) : Client {
    private val logger = KotlinLogging.logger {}

    // Manage message ref
    private val messageRef = refGenerator()

    // Channels storage
    private val channels = mutableMapOf<String, ChannelImpl>()

    private val _state = MutableStateFlow(ClientState())
    override val state = _state.asStateFlow()
    override val active: Boolean
        get() = _state.value.active

    // Incoming messages
    private val _messages = MutableSharedFlow<IncomingMessage>(extraBufferCapacity = 100)
    // Shared flow with replay of 1 to avoid loss of messages (collecting flow after receiving the message)
    private val refMessages = MutableSharedFlow<IncomingMessage>(replay = 1, extraBufferCapacity = 100)
    override val messages = _messages.asSharedFlow()

    private var webSocketJob: Job? = null

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
        payload: Any,
        timeout: DynamicTimeout,
        joinRef: String? = null,
        noReply: Boolean = false,
        crashRetries: Int = 3,
    ): Result<IncomingMessage?> {
        val channel = channels[topic]

        // No need to join a channel with the topic "phoenix" (heartbeat).
        if (
            (topic != "phoenix" && channel == null)
            || (channel?.isJoinedOnce == false && event != "phx_join")
        ) {
            return Result.failure(
                BadActionException(
                    "channel with topic '$topic' was never joined. " +
                            "Join the channel before pushing message"
                )
            )
        }

        var retry = 0
        var result: Result<IncomingMessage?>? = null

        outerLoop@ while (retry++ <= crashRetries) {
            var attempt = 0

            innerLoop@ while (true) {
                val currentTimeout: Long = timeout(attempt)
                    ?: return Result.failure(TimeoutException("number of attempt ($attempt) exceeds limit"))

                val ref = messageRef().toString()

                try {
                    withTimeout(currentTimeout) {
                        if (channel != null && channel.state.value != ChannelState.JOINED) {
                            if (event == "phx_join") {
                                if (state.value.connectionState != ConnectionState.CONNECTED) {
                                    result = Result.failure(
                                        BadActionException("failed to join channel with topic '${channel.topic}' because WebSocket is not connected")
                                    )
                                    return@withTimeout
                                }
                            } else {
                                logger.debug(
                                    "Waiting for channel with topic '${channel.topic}' to join before sending "
                                            + "message with event='$event' and payload='$payload'"
                                )

                                channel.state.filter { it == ChannelState.JOINED }.first()
                            }
                        }

                        val outgoingMessage = OutgoingMessage(topic, event, payload, ref, joinRef)

                        if (noReply) {
                            webSocketEngine.send(outgoingMessage)
                            result = Result.success(null)
                            return@withTimeout
                        }

                        val flows = arrayOf(
                            refMessages.filter { it.ref == ref }.map { Result.success(it) },
                            if (topic != "phoenix" && event != "phx_join" && channel != null) {
                                channel.state.filter { it != ChannelState.JOINED }.map {
                                    Result.failure(ChannelException("Channel with topic '$topic' was closed"))
                                }
                            } else {
                                null
                            }
                        )

                        val flow = merge(*flows.filterNotNull().toTypedArray())
                        webSocketEngine.send(outgoingMessage)
                        result = flow.first()

                        result?.getOrNull()?.let {
                            if (it.hasStatus("error")) {
                                logger.debug("Incoming message with ref '$ref' contains error: $it")
                                result = Result.failure(ResponseException("get an error in request with ref '$ref'", it))
                            }
                        }
                    }
                } catch (ex: TimeoutCancellationException) {
                    result = Result.failure(TimeoutException("request with ref '$ref' timed out"))
                } catch (ex: Exception) {
                    result = Result.failure(ex)
                }

                if (result?.exceptionOrNull() is ResponseException
                    || result?.exceptionOrNull() is ChannelException
                ) {
                    break@innerLoop
                } else if (result != null && result?.exceptionOrNull() !is TimeoutException) {
                    break@outerLoop
                }

                attempt++
            }
        }

        return result ?: Result.failure(ChannelException("no response found"))
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

    private suspend fun rejoinChannels() {
        channels.values.filter { it.isJoinedOnce }.forEach { it.rejoin() }
    }

    override suspend fun join(topic: String, payload: Any): Result<Channel> =
        join(topic, payload, defaultTimeout.toDynamicTimeout(true))

    override suspend fun join(topic: String, payload: Any, timeout: DynamicTimeout): Result<Channel> {
        val channel = channels.getOrPut(topic) {
            val sendToSocket: suspend (String, Any, DynamicTimeout, String?, Boolean)
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

        return if (!state.value.active) {
            Result.failure(BadActionException("Socket is not active"))
        } else if (channel.isJoinedOnce) {
            if (channel.state.value != ChannelState.JOINED) {
                logger.debug("WebSocket is connected and channel with topic '$topic' will be joined automatically")
            }
            Result.success(channel)
        } else if (
            state.value.connectionState == ConnectionState.CONNECTED
            && channel.state.value == ChannelState.CLOSE
        ) {
            logger.debug("Joining channel with topic '$topic'")
            channel.join(payload)
        } else {
            logger.debug("Waiting WebSocket to be connected before joining channel with topic '$topic'")

            try {
                dynamicTimer(timeout) {
                    state.filter { it.active && it.connectionState == ConnectionState.CONNECTED }.first()
                }
                channel.join(payload)
            } catch (ex: Exception) {
                Result.failure(BadActionException("WebSocket is not active"))
            }
        }
    }

    private suspend fun launchWebSocket(params: Map<String, String>) = coroutineScope {
        if (state.value.connectionState != ConnectionState.DISCONNECTED) {
            return@coroutineScope
        }

        logger.info("Launching webSocket")
        _state.update { ClientState(active = it.active, connectionState = ConnectionState.CONNECTING) }

        webSocketEngine.connect(
            host = host,
            port = port,
            path = path,
            params = params,
            ssl = ssl,
            untrustedCertificate = untrustedCertificate,
        ) { event ->
            val incomingMessage = event.message

            if (incomingMessage != null) {
                logger.debug("Receiving message from engine: $incomingMessage")

                if (!_messages.tryEmit(incomingMessage)) {
                    logger.warn("Failed to emit message: $incomingMessage")
                }

                if (!refMessages.tryEmit(incomingMessage)) {
                    logger.warn("Failed to emit message for internal processing: $incomingMessage")
                }

                // TODO: Check forbidden on both WebSocket and channel
                if (incomingMessage == Forbidden) {
                    logger.info("Received message 'forbidden'")
                    _state.update { ClientState(active = false, connectionState = it.connectionState) }
                } else if (incomingMessage.isError()) {
                    launch {
                        rejoinChannel(incomingMessage.topic)
                    }
                }
            }

            val newState = event.state

            if (newState != null) {
                logger.debug("Receiving new state '$newState' from WebSocket engine")
                _state.update {ClientState(active = it.active, connectionState = newState) }
            }
        }

        state.waitDisconnected()
    }

    private suspend fun launchHeartbeat() {
        while (state.value.active) {
            delay(heartbeatInterval)

            if (state.value.connectionState == ConnectionState.CONNECTED) {
                val result = send(
                    topic = "phoenix",
                    event = "heartbeat",
                    payload = emptyPayload,
                    timeout = heartbeatTimeout.toDynamicTimeout(),
                    crashRetries = 1
                )
                if (result.isFailure) {
                    logger.debug("Reconnecting WebSocket because heartbeat gave error: ${result.exceptionOrNull()?.message}")
                    webSocketEngine.close()
                }
            }
        }
    }

    private suspend fun monitorConnection(params: Map<String, String>) {
        dynamicTimer(retry) {
            if (!state.value.active) {
                throw BadActionException("WebSocket is not active")
            }

            // Let the webSocket set the connection up
            if (state.value.connectionState == ConnectionState.DISCONNECTED) {
                webSocketJob?.cancelAndJoin()
                webSocketJob = scope.launch {
                    launchWebSocket(params)
                }
            }

            state.waitConnected()
        }
    }

    override fun connect(params: Map<String, String>) {
        logger.info("Connect client")

        if (state.value.active) {
            throw Exception("WebSocket is already active")
        }

        _state.update { ClientState(active = true, connectionState = it.connectionState) }

        // Retry after being disconnected
        scope.launch {
            try {
                monitorConnection(params)
            } catch (ex: Exception) {
                logger.error("Failed to launch the WebSocket: ${ex.message}")
                return@launch
            }

            var heartbeatJob: Job? = null
            var joinChannelsJob: Job? = null

            state.collect {
                logger.debug("Client state updated: active='${it.active}' connectionState='${it.connectionState}'")
                try {
                    if (!it.active) {
                        webSocketJob?.cancelAndJoin()
                        cancel()
                    } else if (it.connectionState == ConnectionState.CONNECTED) {
                        if (heartbeatJob?.isActive != true) {
                            heartbeatJob = launch {
                                launchHeartbeat()
                            }
                        }

                        if (joinChannelsJob?.isActive != true) {
                            joinChannelsJob = launch {
                                rejoinChannels()
                            }
                        }
                    } else if (it.connectionState == ConnectionState.DISCONNECTED) {
                        heartbeatJob?.cancelAndJoin()
                        dirtyCloseChannels()
                        monitorConnection(params)
                    }
                } catch (ex: Exception) {
                    logger.error("ex: ${ex.message}")
                }
            }
        }
    }

    override suspend fun disconnect() {
        logger.info("Disconnect client")

        _state.update { ClientState(active = false, connectionState = it.connectionState) }

        channels.values.forEach { it.leave() }
        channels.clear()

        webSocketEngine.close()
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
    serializer: (message: OutgoingMessage) -> String = { it.toJson() },
    deserializer: (input: String) -> IncomingMessage = ::fromJson,
    scope: CoroutineScope = CoroutineScope(Dispatchers.Default),
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
                OkHttpEngine(
                    serializer = serializer,
                    deserializer = deserializer
                ),
                scope,
            )
        )
    } catch (ex: Exception) {
        Result.failure(ex)
    }