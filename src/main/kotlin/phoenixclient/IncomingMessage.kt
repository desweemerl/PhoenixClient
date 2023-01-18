package phoenixclient

import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import java.lang.reflect.Type

data class Reply(
    val response: Map<String, Any?>,
    val status: String,
)

data class IncomingMessage(
    val topic: String,
    val event: String,
    val reply: Reply? = null,
    // ref and joinRef could be null for internal messages like forbidden
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
        val ref = values[1]?.asString ?: throw Exception("ref must be a string")
        val topic = values[2]?.asString ?: throw Exception("topic must be a string")
        val event = values[3]?.asString ?: throw Exception("event must be a string")
        val reply = values[4]?.asJsonObject ?: throw Exception("reply must be an object")
        val response = reply.get("response").asJsonObject ?: throw Exception("response must be an object")
        val status = reply.get("status").asString ?: throw Exception("response must be a string")

        return IncomingMessage(
            joinRef = joinRef,
            ref = ref,
            topic = topic,
            event = event,
            reply = Reply(
                response = context.deserialize(response, Map::class.java),
                status = status
            )
        )
    }
}

fun fromJson(input: String): IncomingMessage = JsonProcessor.fromJson(input, IncomingMessage::class.java)

val Forbidden = IncomingMessage(topic = "phoenix", event = "forbidden")
val SocketClose = IncomingMessage(topic = "phoenix", event = "socket_close")

fun Flow<IncomingMessage>.isForbidden() = this.filter { it == Forbidden }
fun Flow<IncomingMessage>.isSocketClose() = this.filter { it == SocketClose }
