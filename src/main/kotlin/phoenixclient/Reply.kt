package phoenixclient

import kotlin.reflect.KClass


enum class Status {
    OK,
    ERROR,
    UNKNOWN,
}

interface Reply {
    val status: Status

    fun <T : Any> convertTo(clazz: KClass<T>): Result<T>
}

fun String.toStatus() = when (this) {
    "ok" -> Status.OK
    "error" -> Status.ERROR
    else -> Status.UNKNOWN
}

class ReplyDeserializer(
    private val payload: Payload,
) : Reply {
    override val status: Status
        get() = payload.convertTo(String::class, "status").getOrThrow().toStatus()

    override fun <T : Any> convertTo(clazz: KClass<T>): Result<T> =
        payload.convertTo(clazz, "response")
}

fun IncomingMessage.toReply(): Result<Reply> =
    if (payload == null) {
        Result.failure(Exception("Incoming message has no payload"))
    } else if (event != "phx_reply") {
        Result.failure(Exception("Incoming message is not a reply"))
    } else {
        Result.success(ReplyDeserializer(payload))
    }

data class ReplyReason(
    val reason: String,
)

fun Reply.isError(reason: String? = null): Boolean =
    status == Status.ERROR
            && (reason == null || convertTo(ReplyReason::class).getOrNull()?.reason == reason)

fun Reply.isTopicClosed(): Boolean = isError("unmatched topic")
