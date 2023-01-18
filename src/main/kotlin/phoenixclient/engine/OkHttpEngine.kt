package phoenixclient.engine

import mu.KotlinLogging
import okhttp3.*
import okhttp3.logging.HttpLoggingInterceptor
import phoenixclient.*
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.SSLContext
import javax.net.ssl.X509TrustManager

fun getHttpClient(): OkHttpClient = OkHttpClient.Builder().retryOnConnectionFailure(false).build()

fun getUntrustedOkHttpClient(): OkHttpClient {
    val x509UntrustManager = object : X509TrustManager {
        override fun checkClientTrusted(p0: Array<out X509Certificate>?, p1: String?) {
        }

        override fun checkServerTrusted(p0: Array<out X509Certificate>?, p1: String?) {
        }

        override fun getAcceptedIssuers(): Array<X509Certificate> {
            return arrayOf()
        }
    }

    val trustAllCerts = arrayOf(x509UntrustManager)
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(
        null,
        trustAllCerts,
        SecureRandom()
    )

    val sslSocketFactory = sslContext.socketFactory

    val logging = HttpLoggingInterceptor()
    logging.level = HttpLoggingInterceptor.Level.BODY

    return OkHttpClient.Builder()
        .sslSocketFactory(sslSocketFactory, trustAllCerts[0])
        .hostnameVerifier { _, _ -> true }
        .retryOnConnectionFailure(false)
        .build()
}

class OkHttpEngine(
    val serializer: (OutgoingMessage) -> String = OutgoingMessage::toJson,
    val deserializer: (String) -> IncomingMessage = ::fromJson,
) : WebSocketEngine {
    // Logger
    private val logger = KotlinLogging.logger {}
    private var ws: WebSocket? = null

    override suspend fun connect(
        host: String,
        port: Int,
        path: String,
        params: Map<String, String>,
        ssl: Boolean,
        untrustedCertificate: Boolean,
        receiver: (event: WebSocketEvent) -> Unit,
    ) {
        var closed = false
        val client = if (ssl && untrustedCertificate)
            getUntrustedOkHttpClient() else getHttpClient()

        val scheme = if (ssl) "wss" else "ws"
        val finalPath = "${path.trim('/')}/websocket?vsn=$VSN&" +
                params.entries.joinToString("&") { e -> "${e.key}=${e.value}" }

        val url = "$scheme://$host:$port/$finalPath"
        val request = Request.Builder().url(url).build()

        val listener = object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                if (!closed) {
                    receiver(WebSocketEvent(state = ConnectionState.CONNECTED))
                }
            }

            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                if (!closed) {
                    receiver(WebSocketEvent(state = ConnectionState.DISCONNECTED))
                    closed = true
                }
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                if (!closed) {
                    receiver(WebSocketEvent(state = ConnectionState.DISCONNECTED))
                    closed = true
                }
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                logger.error("Got a failure on WebSocket: " + t.message)

                if (!closed) {
                    val message = when (response?.message?.lowercase()) {
                        "forbidden" -> Forbidden
                        "socket close" -> SocketClose
                        else -> Failure
                    }

                    receiver(
                        WebSocketEvent(
                            state = ConnectionState.DISCONNECTED,
                            message = message,
                        )
                    )
                }
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                receiver(WebSocketEvent(message = deserializer(text)))
            }
        }

        try {
            ws = client.newWebSocket(request, listener)
        } catch (ex: Exception) {
            logger.error("Failed to setup WebSocket: " + ex.printStackTrace())
            throw ex
        }
    }

    override fun send(message: OutgoingMessage): Result<Unit> =
        try {
            logger.debug("Send message $message to WebSocket")
            ws!!.send(serializer(message))
            Result.success(Unit)
        } catch (ex: Exception) {
            logger.error("Failed to send message $message to WebSocket: ${ex.stackTraceToString()}")
            Result.failure(ex)
        }

    override fun close() {
        logger.info("Closing WebSocket")
        ws?.close(1001, "Closing WebSocket")
    }
}