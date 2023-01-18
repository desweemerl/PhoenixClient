package phoenixclient

import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow


const val DEFAULT_HEARTBEAT_INTERVAL = 10000L // 10 secs


interface PhoenixClient {
    val state: StateFlow<ConnectionState>
    val messages: SharedFlow<IncomingMessage>

    suspend fun connect(params: Map<String, String>)
    suspend fun disconnect()

    suspend fun send(topic: String, event: String, payload: Payload, timeout: Long = -0L): Result<IncomingMessage?>
}
