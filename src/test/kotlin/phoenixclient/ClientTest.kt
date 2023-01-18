package phoenixclient

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test


class ClientTest {
    @Test
    fun testUnauthorizedConnection() {
        runBlocking {
            withTimeout(5000) {
                val client = getClient()
                client.connect(mapOf("token" to "wrongToken"))
                client.messages
                    .isForbidden()
                    .first()
                client.disconnect()

                assert(true)
            }
        }
    }

    @Test
    fun testAuthorizedConnection() {
        runBlocking {
            withTimeout(5000) {
                val client = getClient()
                client.connect(mapOf("token" to "user1234"))
                client.state.isConnected().first()
                client.disconnect()

                assert(true)
            }
        }
    }

    private fun getClient(): Client =
        okHttpPhoenixClient(
            port = 4000,
            ssl = false,
        ).getOrThrow()
}