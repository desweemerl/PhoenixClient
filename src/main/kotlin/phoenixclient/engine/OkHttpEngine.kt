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

class OkHttpEngine() : WebSocketEngine {
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
        val client = if (ssl && untrustedCertificate)
            getUntrustedOkHttpClient() else getHttpClient()

        val scheme = if (ssl) "wss" else "ws"
        val finalPath = "${path.trim('/')}/websocket?vsn=$VSN&" +
                params.entries.joinToString("&") { e -> "${e.key}=${e.value}" }

        val url = "$scheme://$host:$port/$finalPath"
        val request = Request.Builder().url(url).build()

        val listener = object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                trySendBlocking(WebSocketEvent(state = ConnectionState.CONNECTED))
                    .onFailure {
                        logger.error("Failed to send state CONNECTED")
                    }
            }

            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                trySendBlocking(WebSocketEvent(state = ConnectionState.DISCONNECTED))
                    .onFailure {
                        logger.error("Failed to send state DISCONNECTED")
                    }
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                trySendBlocking(WebSocketEvent(state = ConnectionState.DISCONNECTED))
                    .onFailure {
                        logger.error("Failed to send state DISCONNECTED")
                    }
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                logger.error("Got a failure on webSocket: " + t.stackTraceToString())

                val message = when (response?.message?.lowercase()) {
                    "forbidden" -> Forbidden
                    "socket close" -> SocketClose
                    else -> IncomingMessage(
                        topic = "phoenix",
                        event = "failure",
                        reply = mapOf(
                            "status" to "error",
                            "response" to mapOf(
                                "message" to response?.message,
                                "exception" to t.message,
                            )
                        )
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

            override fun onMessage(webSocket: WebSocket, text: String) {
                trySendBlocking(WebSocketEvent(message = fromJson(text)))
                    .onFailure {
                        logger.error("Failed to send message: $text")
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
            }
        }
    }

    override fun send(value: String) {
        logger.debug("Send message $value to web socket")
        ws?.send(value)
    }

    override fun close() {
        logger.info("Closing webSocket")
        ws?.close(1001, "Closing webSocket")
    }
}