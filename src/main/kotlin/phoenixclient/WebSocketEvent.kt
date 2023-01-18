package phoenixclient

class WebSocketEvent(
    val state: ConnectionState? = null,
    val message: IncomingMessage? = null
)