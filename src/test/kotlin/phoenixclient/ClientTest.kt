package phoenixclient

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test


class ClientTest {
    @Test
    fun testUnauthorizedConnection() {
        runBlocking {

            withTimeout(1000) {
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
            withTimeout(1000) {
                val client = getClient()
                client.connect(mapOf("token" to "user1234"))
                val result = client.onConnected { true }
                client.disconnect()
                assert(result)
            }
        }
    }

    private fun getClient(): Client =
        okHttpPhoenixClient(
            port = 4000,
            ssl = false,
        ).getOrThrow()
}