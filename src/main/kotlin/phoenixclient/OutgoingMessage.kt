package phoenixclient

import com.google.gson.*
import java.lang.reflect.Type

typealias Payload = Map<String, Any>

data class OutgoingMessage (
    val topic: String,
    val event: String,
    val payload: Payload = mapOf(),
    val ref: String,
    val joinRef: String? = null,
)

object OutgoingMessageSerializer : JsonSerializer<OutgoingMessage> {
    override fun serialize(src: OutgoingMessage?, typeOfSrc: Type?, context: JsonSerializationContext?): JsonElement {
        if (src == null) {
            throw Exception("message is null")
        }

        if (context == null) {
            throw Exception("context is null")
        }

        val array = JsonArray()

        array.add(src.joinRef)
        array.add(src.ref)
        array.add(src.topic)
        array.add(src.event)
        array.add(context.serialize(src.payload))

        return array
    }
}
