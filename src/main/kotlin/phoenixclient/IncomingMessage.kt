package phoenixclient

import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import java.lang.reflect.Type
import kotlin.reflect.KClass


interface Payload {
    fun <T : Any> convertTo(clazz: KClass<T>, key: String? = null): Result<T>
}

fun <T : Any> deserializeJsonObject(
    jsonObject: JsonObject,
    context: JsonDeserializationContext,
    clazz: KClass<T>,
    key: String?,
): Result<T> = try {
    val target = if (key == null) jsonObject else jsonObject[key]
    val deserializedTarget: T = context.deserialize(target, clazz.java)
    Result.success(deserializedTarget)
} catch (ex: Exception) {
    Result.failure(ex)
}

class PayloadDeserializer(
    private val jsonObject: JsonObject,
    private val context: JsonDeserializationContext,
) : Payload {
    override fun <T : Any> convertTo(clazz: KClass<T>, key: String?): Result<T> =
        deserializeJsonObject(jsonObject, context, clazz, key)

    override fun toString(): String = jsonObject.toString()
}

data class IncomingMessage(
    val topic: String,
    val event: String,
    val payload: Payload? = null,
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
        val payload =
            if (values[4].isJsonNull) null else values[4].asJsonObject ?: throw Exception("payload must be an object")

        return IncomingMessage(
            joinRef = joinRef,
            ref = ref,
            topic = topic,
            event = event,
            payload = payload?.let { PayloadDeserializer(it, context) },
        )
    }
}

fun fromJson(input: String): IncomingMessage = JsonProcessor.fromJson(input, IncomingMessage::class.java)

val Forbidden = IncomingMessage(topic = "phoenix", event = "forbidden")
val SocketClose = IncomingMessage(topic = "phoenix", event = "socket_close")
val Failure = IncomingMessage(topic = "phoenix", event = "failure")

data class Reason(
    val reason: String
)

fun IncomingMessage.isError(): Boolean = event == "phx_error"
fun IncomingMessage.hasStatus(status: String): Boolean =
    payload?.convertTo(String::class, "status")?.getOrNull() == status
fun IncomingMessage.hasReason(reason: String): Boolean =
    payload?.convertTo(Reason::class, "response")?.getOrNull()?.reason == reason

fun Flow<IncomingMessage>.filterTopic(topic: String) = filter { it.topic == topic }
fun Flow<IncomingMessage>.filterEvent(event: String) = filter { it.event == event }