package phoenixclient

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import phoenixclient.engine.OkHttpEngine


const val VSN = "2.0.0"


class DefaultPhoenixClient(
    val host: String = DEFAULT_WS_HOST,
    val port: Int = DEFAULT_WS_PORT,
    val path: String = DEFAULT_WS_PATH,
    val ssl: Boolean = DEFAULT_WS_SSL,
    private val untrustedCertificate: Boolean = DEFAULT_UNTRUSTED_CERTIFICATE,
    private val retry: Long = DEFAULT_RETRY,
    private val heartbeatInterval: Long = DEFAULT_HEARTBEAT_INTERVAL,
    private val timeout: Long = DEFAULT_TIMEOUT,
    private val engine: WebSocketEngine = OkHttpEngine(),
) : PhoenixClient {
    init {
        if (heartbeatInterval < timeout) {
            throw Exception("heartbeatInterval must be greater or equal to timeout")
        }
    }

    private var _activated = false
    private val activated: Boolean
        get() {
            return _activated
        }

    private var wsJob: Job? = null
    private var retryJob: Job? = null
    private var reconnectJob: Job? = null

    // Logger
    private val logger = KotlinLogging.logger {}

    // Manage message ref
    private var _ref = 0L
    private val ref: Long
        get() {
            _ref = if (_ref == Long.MAX_VALUE) {
                0
            } else {
                ++_ref
            }
            return _ref
        }

    // Connection state monitoring
    private val _state = MutableStateFlow(ConnectionState.DISCONNECTED)
    override val state = _state.asStateFlow()

    private val outgoingFlow = MutableSharedFlow<OutgoingMessage>()

    // Incoming messages
    private val _incomingFlow = MutableSharedFlow<IncomingMessage>()
    override val messages = _incomingFlow.asSharedFlow()

    private suspend fun heartbeat(): Boolean {
        logger.info("Sending heartbeat")

        val result = send(
            topic = "phoenix",
            event = "heartbeat",
            payload = mapOf<String, Any?>(),
            timeout = timeout,
        )

        return result.getOrNull() != null
    }

    fun serialize(message: OutgoingMessage): String = message.toJson()

    override suspend fun send(topic: String, event: String, payload: Map<String, Any?>, timeout: Long)
            : Result<IncomingMessage?> {

        val messageRef = ref.toString()

        outgoingFlow.emit(
            OutgoingMessage(
                topic = topic,
                event = event,
                payload = payload,
                ref = messageRef,
            )
        )

        var response: IncomingMessage? = null

        if (timeout <= 0L) {
            return Result.success(response)
        }

        return try {
            withTimeout(timeout) {
                response = messages.filter { it.ref == messageRef }.first()
            }
            Result.success(response)
        } catch (ex: Exception) {
            Result.failure(ex)
        }
    }

    private suspend fun launchWebSocket(params: Map<String, String>) = coroutineScope {
        wsJob = launch {
            launch {
                engine.connect(
                    host = host,
                    port = port,
                    path = path,
                    params = params,
                    ssl = ssl,
                    untrustedCertificate = untrustedCertificate,
                )
                    .collect { incomingMessage ->
                        incomingMessage.message?.let {
                            _incomingFlow.emit(it)

                            if (it == Forbidden) {
                                _activated = false
                            }
                        }

                        incomingMessage.state?.let { newState ->
                            if (
                                (state.value == ConnectionState.RECONNECTING
                                        && newState == ConnectionState.CONNECTED)
                                || state.value != ConnectionState.RECONNECTING
                            ) {
                                _state.update { newState }
                            }
                        }
                    }
            }

            launch {
                state
                    .filter { it == ConnectionState.CONNECTED }
                    .first()

                launch {
                    logger.info("Connection is established, waiting for outgoing messages")

                    outgoingFlow
                        .takeWhile { state.value == ConnectionState.CONNECTED }
                        .collect {
                            engine.send(serialize(it))
                        }
                }

                launch {
                    logger.info("Setting up heartbeat")

                    while (true) {
                        delay(heartbeatInterval)

                        if (state.value != ConnectionState.CONNECTED) {
                            break
                        }

                        if (!heartbeat()) {
                            engine.close()
                            _state.update { ConnectionState.DISCONNECTED }
                            wsJob?.cancel()
                        }
                    }
                }
            }
        }
    }

    private fun cancelWebSocket() {
        engine.close()
        wsJob?.cancel()
    }

    private suspend fun reconnect(params: Map<String, String>) = supervisorScope {
        launch {
            while (true) {
                if (state.value == ConnectionState.CONNECTED) {
                    break
                }

                logger.info("WebSocket disconnected, trying to reconnect")

                _state.update { ConnectionState.RECONNECTING }

                cancelWebSocket()
                wsJob = launch {
                    launchWebSocket(params)
                }

                delay(retry)
            }
        }
    }

    override suspend fun connect(params: Map<String, String>) {
        if (state.value == ConnectionState.CONNECTED) {
            throw Exception("WebSocket is already connected")
        }

        _activated = true

        coroutineScope {
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
        _activated = false

        reconnectJob?.cancel()
        cancelWebSocket()

        _state.update { ConnectionState.DISCONNECTED }
    }
}