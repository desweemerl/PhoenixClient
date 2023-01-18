package phoenixclient

import com.google.gson.Gson
import com.google.gson.GsonBuilder

val JsonProcessor: Gson =
    GsonBuilder()
        .registerTypeAdapter(OutgoingMessage::class.java, OutgoingMessageSerializer)
        .registerTypeAdapter(IncomingMessage::class.java, IncomingMessageDeserializer)
        .create()

fun OutgoingMessage.toJson(): String = JsonProcessor.toJson(this)