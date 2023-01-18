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
    private val heartbeatInterval: Long = DEFAULT_HEARTBEAT_INTERVAL,
    private val engine: WebSocketEngine = OkHttpEngine(),
) : PhoenixClient {
    private var _activated = false
    private val activated: Boolean
    get() {
         return _activated
    }

    private var wsJob: Job? = null
    private var heartBeatJob: Job? = null

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
            payload = mapOf(),
            timeout = 1000,
        )

        return result
            .onFailure {
                if (_activated) {
                    logger.error("Heartbeat timed out, reconnecting")
                    _state.update { ConnectionState.RECONNECTING }
                }
            }
            .getOrNull() != null
    }

    fun serialize(message: OutgoingMessage): String = message.toJson()

    override suspend fun send(topic: String, event: String, payload: Payload, timeout: Long)
            : Result<IncomingMessage?> {

        // Don't let ontgoingFlow grow with heartbeat messages.
        if (topic == "phoenix" && event == "heartbeat" && state.value != ConnectionState.CONNECTED) {
            return Result.failure(Exception("WebSocket is not connected"))
        }

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

                    incomingMessage.state?.let { state -> _state.update { state } }
                }

        }

        launch {
            state
                .filter { it == ConnectionState.CONNECTED }
                .first()

            logger.info("Connection is established, waiting for outgoing messages")

            launch {
                outgoingFlow
                    .takeWhile { state.value == ConnectionState.CONNECTED }
                    .collect {
                        engine.send(serialize(it))
                    }
            }
        }
    }

    override suspend fun connect(params: Map<String, String>) {
        if (state.value == ConnectionState.CONNECTED) {
            throw Exception("WebSocket is already connected")
        }

        _activated = true

        coroutineScope {
            heartBeatJob = launch {
                while (activated) {
                    // Process heartbeat
                    delay(heartbeatInterval)
                    if (!heartbeat()) {
                        engine.close()
                        wsJob?.cancel()
                        wsJob = launch {
                           launchWebSocket(params)
                        }
                   }

                    engine.close()
                    wsJob?.cancel()
                    cancel()
                }
            }

            wsJob = launch {
                launchWebSocket(params)
            }
        }
    }

    override suspend fun disconnect() {
        _activated = false

        heartBeatJob?.cancel()
        engine.close()
        wsJob?.cancel()

        _state.update { ConnectionState.DISCONNECTED }
    }
}