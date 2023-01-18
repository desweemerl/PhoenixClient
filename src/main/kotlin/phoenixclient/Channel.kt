package phoenixclient

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.withTimeout

enum class ChannelState {
    JOINING,
    JOINED,
    LEAVING,
    CLOSE,
}

interface Channel {
    val topic: String

    val state: StateFlow<ChannelState>
    val message: Flow<IncomingMessage>

    suspend fun pushNoWait(event: String, payload: Map<String, Any?>): Result<Unit>

    suspend fun push(event: String, payload: Map<String, Any?>): Result<IncomingMessage>
    suspend fun push(event: String, payload: Map<String, Any?>, timeout: Long): Result<IncomingMessage>

    suspend fun dispose()
}

internal class ChannelImpl(
    override val topic: String,
    override val message: Flow<IncomingMessage>,
    private val sendToSocket: suspend (event: String, payload: Map<String, Any?>, timeout: Long?, joinRef: String?) -> Result<IncomingMessage?>,
    private val disposeFromSocket: suspend (topic: String) -> Unit,
    private val defaultTimeout: Long = 1000L
) : Channel {
    private data class PhoenixChannelMessage(
        val event: String,
        val payload: Map<String, Any?>,
    )

    private var joinedOnce = false
    private var pushBuffer = mutableListOf<PhoenixChannelMessage>()

    private val _state = MutableStateFlow(ChannelState.CLOSE)
    override val state = _state.asStateFlow()


    private var joinRef: String? = null

    internal suspend fun flushPushBuffer() {
        if (state.value != ChannelState.JOINED) {
            return
        }

        pushBuffer.forEach {
            pushNoWait(it.event, it.payload)
        }

        pushBuffer.clear()
    }

    override suspend fun pushNoWait(event: String, payload: Map<String, Any?>): Result<Unit> =

        if (!joinedOnce) {
            Result.failure(BadActionException("Channel $topic was never joined. Join the channel before pushing message"))
        } else if (state.value == ChannelState.JOINED) {
            sendToSocket(event, payload, null, joinRef).map { Unit }
        } else {
            pushBuffer.add(PhoenixChannelMessage(event, payload))
            Result.success(Unit)
        }

    override suspend fun push(event: String, payload: Map<String, Any?>) =
        push(event, payload, defaultTimeout)

    override suspend fun push(event: String, payload: Map<String, Any?>, timeout: Long)
            : Result<IncomingMessage> =

        if (state.value == ChannelState.JOINED
            || (state.value == ChannelState.JOINING && event == "phx_join")
        ) {
            sendToSocket(event, payload, timeout, joinRef).map { it!! }
        } else if (!joinedOnce) {
            Result.failure(BadActionException("Channel $topic was never joined. Join the channel before pushing message"))
        } else {
            try {
                withTimeout(timeout) {
                    state.filter { state.value == ChannelState.JOINED }.first()
                    sendToSocket(event, payload, 0, joinRef).map { it!! }
                }
            } catch (ex: TimeoutCancellationException) {
                Result.failure(
                    TimeoutException("Response didn't come after $timeout ms")
                )
            }
        }

    override suspend fun dispose() {
        disposeFromSocket(topic)
    }

    suspend fun close(timeout: Long = defaultTimeout) {
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

    suspend fun join(
        payload: Map<String, Any?> = mapOf(),
        timeout: Long = defaultTimeout
    ): Result<Channel> =
        when (state.value) {
            ChannelState.LEAVING
            -> Result.failure(BadActionException("Channel with topic $topic is leaving"))

            ChannelState.JOINING
            -> Result.failure(BadActionException("Channel with topic $topic is already joining"))

            ChannelState.JOINED
            -> Result.failure(BadActionException("Channel with topic $topic is already joined"))

            else -> {
                _state.update { ChannelState.JOINING }
                push("phx_join", payload, timeout)
                    .onSuccess {
                        joinedOnce = true
                        _state.update { ChannelState.JOINED }
                        joinRef = it.ref
                    }
                    .map { this }
            }
        }
}