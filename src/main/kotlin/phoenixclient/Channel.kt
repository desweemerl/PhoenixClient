package phoenixclient

import kotlinx.coroutines.flow.*
import mu.KotlinLogging

enum class ChannelState {
    JOINING,
    JOINED,
    LEAVING,
    CLOSE,
}


interface Channel {
    val topic: String

    val state: Flow<ChannelState>
    val messages: Flow<IncomingMessage>

    suspend fun pushNoReply(event: String, payload: Map<String, Any?> = mapOf()): Result<Unit>
    suspend fun pushNoReply(event: String, payload: Map<String, Any?>, timeout: Long): Result<Unit>

    suspend fun push(event: String, payload: Map<String, Any?> = mapOf()): Result<Reply>
    suspend fun push(event: String, payload: Map<String, Any?>, timeout: Long): Result<Reply>

    suspend fun leave()
}

internal class ChannelImpl(
    override val topic: String,
    override val messages: Flow<IncomingMessage>,

    private val sendToSocket: suspend (
        event: String,
        payload: Map<String, Any?>,
        timeout: DynamicTimeout,
        joinRef: String?,
        noRepy: Boolean,
    )
        -> Result<IncomingMessage?>,
    private val disposeFromSocket: suspend (topic: String) -> Unit,
    private val defaultTimeout: Long = DEFAULT_TIMEOUT,
) : Channel {
    private val logger = KotlinLogging.logger {}

    private val _state = MutableStateFlow(ChannelState.CLOSE)
    override val state = _state.asStateFlow()

    private var joinPayload: Map<String, Any?>? = null
    private var joinRef: String? = null

    internal val isJoinedOnce: Boolean
        get() {
            return joinRef != null
        }

    override suspend fun pushNoReply(event: String, payload: Map<String, Any?>): Result<Unit> =
        pushNoReply(event, payload, defaultTimeout)

    override suspend fun pushNoReply(event: String, payload: Map<String, Any?>, timeout: Long): Result<Unit> =
        sendToSocket(event, payload, timeout.toDynamicTimeout(), joinRef, true).map { }

    override suspend fun push(event: String, payload: Map<String, Any?>) =
        push(event, payload, defaultTimeout)

    override suspend fun push(event: String, payload: Map<String, Any?>, timeout: Long)
            : Result<Reply> =
        sendToSocket(event, payload, timeout.toDynamicTimeout(), joinRef, false).fold(
            { it!!.toReply() }, { Result.failure(it) }
        )

    override suspend fun leave() {
        disposeFromSocket(topic)
    }

    internal fun dirtyClose() {
        logger.debug("Dirty closing channel with topic '$topic'")
        _state.update { ChannelState.CLOSE }
    }

    internal suspend fun close(timeout: Long = defaultTimeout) {
        if (state.value == ChannelState.LEAVING
            || state.value == ChannelState.CLOSE
        ) {
            return
        }

        _state.update { ChannelState.LEAVING }
        joinRef = null

        push("phx_leave", mapOf(), timeout)
            .getOrNull()?.let {
                // Don't care about the result.
                _state.update { ChannelState.CLOSE }
            }
    }

    internal suspend fun rejoin(timeout: DynamicTimeout = DEFAULT_REJOIN_TIMEOUT): Result<Channel> =
        if (joinRef == null) {
            Result.failure(BadActionException("Channel with topic '$topic' was never joined"))
        } else {
            join(joinPayload ?: mapOf(), timeout)
        }

    internal suspend fun join(
        payload: Map<String, Any?> = mapOf(),
        timeout: DynamicTimeout = DEFAULT_REJOIN_TIMEOUT,
    ): Result<Channel> =
        when (state.value) {
            ChannelState.LEAVING
            -> Result.failure(BadActionException("Channel with topic '$topic' is leaving"))

            ChannelState.JOINING
            -> Result.failure(BadActionException("Channel with topic '$topic' is already joining"))

            ChannelState.JOINED
            -> Result.failure(BadActionException("Channel with topic '$topic' is already joined"))

            else -> {
                _state.update { ChannelState.JOINING }

                joinPayload = payload

                sendToSocket("phx_join", payload, timeout, joinRef, false).map { it!! }
                    .onSuccess {
                        logger.debug("Channel with topic '$topic' was joined")
                        _state.update { ChannelState.JOINED }
                        joinRef = it.ref
                    }
                    .onFailure {
                        logger.error("Failed to join channel with '$topic': " + it.stackTraceToString())
                    }
                    .map { this }
            }
        }
}