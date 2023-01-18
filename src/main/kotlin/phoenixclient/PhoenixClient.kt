package phoenixclient

import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow


const val DEFAULT_HEARTBEAT_INTERVAL = 30000L // 30 secs
const val DEFAULT_TIMEOUT = 1000L // 1 sec
const val DEFAULT_RETRY = 5000L // 5 secs


interface PhoenixClient {
    val state: StateFlow<ConnectionState>
    val messages: SharedFlow<IncomingMessage>

    suspend fun connect(params: Map<String, String>)
    suspend fun disconnect()

    suspend fun send(
        topic: String,
        event: String,
        payload: Map<String, Any?>,
        timeout: Long = 0L
    ): Result<IncomingMessage?>
}
