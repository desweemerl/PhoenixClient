package phoenixclient.engine

import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import mu.KotlinLogging
import okhttp3.*
import okhttp3.logging.HttpLoggingInterceptor
import phoenixclient.*
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.SSLContext
import javax.net.ssl.X509TrustManager

fun getHttpClient(): OkHttpClient = OkHttpClient.Builder().build()

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
    ): Flow<WebSocketEvent> = callbackFlow {
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
                    trySendBlocking(WebSocketEvent(state = ConnectionState.CONNECTED))
                        .onFailure {
                            logger.error("Failed to send state CONNECTED")
                        }
                }
            }

            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                if (!closed) {
                    trySendBlocking(WebSocketEvent(state = ConnectionState.DISCONNECTED))
                        .onFailure {
                            logger.error("Failed to send state DISCONNECTED")
                        }
                }
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                if (!closed) {
                    trySendBlocking(WebSocketEvent(state = ConnectionState.DISCONNECTED))
                        .onFailure {
                            logger.error("Failed to send state DISCONNECTED")
                        }
                }
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                logger.error("Got a failure on webSocket: " + t.stackTraceToString())

                if (!closed) {
                    val message = when (response?.message?.lowercase()) {
                        "forbidden" -> Forbidden
                        "socket close" -> SocketClose
                        else -> IncomingMessage(
                            topic = "phoenix",
                            event = "failure",
                            reply = ReplyInternalError(
                                throwable = t,
                                message = response?.message,
                            ),
                        )
                    }

                    trySendBlocking(
                        WebSocketEvent(
                            state = ConnectionState.DISCONNECTED,
                            message = message,
                        )
                    )
                        .onFailure {
                            logger.error("Failed to send failure event: $message")
                        }
                }
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                trySendBlocking(WebSocketEvent(message = deserializer(text)))
                    .onFailure {
                        logger.error("Failed to receive message: ${it?.stackTraceToString()}")
                    }
            }
        }

        try {
            ws = client.newWebSocket(request, listener)
        } catch (ex: Exception) {
            logger.error("Failed to setup webSocket: " + ex.printStackTrace())
            throw ex
        }

        awaitClose {
            ws?.let {
                logger.info("Unregistering webSocket callbacks")
                it.close(1001, "Closing request")
                closed = true
            }
        }
    }

    override fun send(message: OutgoingMessage): Result<Unit> =
        try {
            logger.debug("Send message $message to web socket")
            ws!!.send(serializer(message))
            Result.success(Unit)
        } catch (ex: Exception) {
            logger.error("Send message $message to web socket: ${ex.stackTraceToString()}")
            Result.failure(ex)
        }

    override fun close() {
        logger.info("Closing webSocket")
        ws?.close(1001, "Closing webSocket")
    }
}