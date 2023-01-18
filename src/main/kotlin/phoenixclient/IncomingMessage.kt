package phoenixclient

import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import java.lang.reflect.Type

data class IncomingMessage(
    val topic: String,
    val event: String,
    val reply: Map<String, Any?>? = null,
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
            reply = context.deserialize(reply, Map::class.java),
        )
    }
}

fun fromJson(input: String): IncomingMessage = JsonProcessor.fromJson(input, IncomingMessage::class.java)

val Forbidden = IncomingMessage(topic = "phoenix", event = "forbidden")
val SocketClose = IncomingMessage(topic = "phoenix", event = "socket_close")

fun IncomingMessage.getResponse(): Map<String, Any?>? {
    val response = reply?.get("response") ?: return null

    return try {
        response as Map<String, Any?>
    } catch (ex: ClassCastException) {
        null
    }
}

fun IncomingMessage.isReplyOK(targetTopic: String? = null): Boolean =
    event == "phx_reply"
            && reply?.get("status") == "ok"
            && (targetTopic == null || topic == targetTopic)

fun IncomingMessage.isError(): Boolean = event == "phx_error"
fun IncomingMessage.isReplyError(reason: String? = null): Boolean =
    event == "phx_reply"
            && reply?.get("status") == "error"
            && (reason == null || getResponse()?.get("reason") == reason)

fun Flow<IncomingMessage>.isForbidden() = this.filter { it == Forbidden }
fun Flow<IncomingMessage>.isSocketClose() = this.filter { it == SocketClose }

suspend fun Flow<IncomingMessage>.filterRef(ref: String): IncomingMessage = this.filter { it.ref == ref }.first()

fun IncomingMessage.toResult(): Result<IncomingMessage> =
    if (this.isReplyError()) {
        Result.failure(ResponseException("Phoenix returned an error for message with ref '${this.ref}'"))
    } else {
        Result.success(this)
    }
