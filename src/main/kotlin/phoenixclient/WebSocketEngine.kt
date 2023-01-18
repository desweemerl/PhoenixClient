package phoenixclient

import kotlinx.coroutines.flow.Flow

const val DEFAULT_WS_HOST = "localhost"
const val DEFAULT_WS_PORT = 443
const val DEFAULT_WS_PATH = "/socket"
const val DEFAULT_WS_SSL = true
const val DEFAULT_UNTRUSTED_CERTIFICATE = false

class WebSocketEvent(
    val state: ConnectionState? = null,
    val message: IncomingMessage? = null
)

interface WebSocketEngine {
    suspend fun connect(
        host: String = DEFAULT_WS_HOST,
        port: Int = DEFAULT_WS_PORT,
        path: String = DEFAULT_WS_PATH,
        params: Map<String, String> = mapOf(),
        ssl: Boolean = DEFAULT_WS_SSL,
        untrustedCertificate: Boolean = DEFAULT_UNTRUSTED_CERTIFICATE,
    ): Flow<WebSocketEvent>

    fun send(value: String)
    fun close()
}

