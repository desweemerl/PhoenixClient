package phoenixclient

import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import java.lang.reflect.Type
import kotlin.reflect.KClass

enum class Status {
    OK,
    ERROR,
    UNKNOWN,
}

fun String.toStatus() = when (this) {
    "ok" -> Status.OK
    "error" -> Status.ERROR
    else -> Status.UNKNOWN
}

interface Reply {
    val status: Status?

    fun <T : Any> convertTo(clazz: KClass<T>): Result<T>
}

class ReplyDeserializer(
    private val jsonObject: JsonObject,
    private val context: JsonDeserializationContext,
) : Reply {
    override val status: Status?
        get() = jsonObject["status"]?.asString?.toStatus()

    override fun <T : Any> convertTo(clazz: KClass<T>): Result<T> =
        try {
            val response = jsonObject["response"]?.asJsonObject
            val deserializedResponse: T = context.deserialize(response, clazz.java)
            Result.success(deserializedResponse)
        } catch (ex: Exception) {
            Result.failure(ex)
        }
}

class ReplyInternalError(
    private val throwable: Throwable,
    private val message: String? = null,
) : Reply {
    override val status: Status = Status.ERROR

    override fun <T : Any> convertTo(clazz: KClass<T>): Result<T> =
        when (clazz) {
            ReplyReason::class -> {
                val output = ReplyFailure(
                    stackTrace = throwable.stackTraceToString(),
                    message = message ?: throwable.message,
                ) as T
                Result.success(output)
            }
            else -> Result.failure(Exception("ReplyInternalError can only be converted to ReplyReason"))
        }
}

data class IncomingMessage(
    val topic: String,
    val event: String,
    val reply: Reply? = null,
    val ref: String? = null,
    val joinRef: String? = null,
)

object IncomingMessageDeserializer : JsonDeserializer<IncomingMessage> {
    override fun deserialize(
        json: JsonElement?,
        typeOfT: Type?,
        context: JsonDeserializationContext?
    ): IncomingMessage {
        if (json == null) {
            throw Exception("json is null")
        }

        if (context == null) {
            throw Exception("context is null")
        }

        val values = json.asJsonArray ?: throw Exception("json must be an array")

        if (values.size() < 5) {
            throw Exception("values must have size of 5")
        }

        val joinRef =
            if (values[0].isJsonNull) null else values[0].asString ?: throw Exception("join_ref must be a string")
        val ref =
            if (values[1].isJsonNull) null else values[1].asString ?: throw Exception("ref must be a string")
        val topic = values[2]?.asString ?: throw Exception("topic must be a string")
        val event = values[3]?.asString ?: throw Exception("event must be a string")
        val reply =
            if (values[4].isJsonNull) null else values[4].asJsonObject ?: throw Exception("reply must be an object")

        return IncomingMessage(
            joinRef = joinRef,
            ref = ref,
            topic = topic,
            event = event,
            reply = reply?.let { ReplyDeserializer(it, context) },
        )
    }
}

fun fromJson(input: String): IncomingMessage = JsonProcessor.fromJson(input, IncomingMessage::class.java)

val Forbidden = IncomingMessage(topic = "phoenix", event = "forbidden")
val SocketClose = IncomingMessage(topic = "phoenix", event = "socket_close")

data class ReplyReason(
    val reason: String,
)

data class ReplyFailure(
    val stackTrace: String,
    val message: String? = null,
)

fun IncomingMessage.isError(): Boolean = event == "phx_error"

fun IncomingMessage.isReplyOK(targetTopic: String? = null): Boolean =
    event == "phx_reply"
            && reply?.status == Status.OK
            && (targetTopic == null || topic == targetTopic)

fun IncomingMessage.isReplyError(reason: String? = null): Boolean =
    event == "phx_reply"
            && reply?.status == Status.ERROR
            && (reason == null || reply?.convertTo(ReplyReason::class)?.getOrNull()?.reason == reason)