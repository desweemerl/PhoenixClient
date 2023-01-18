package phoenixclient

import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test


class PhoenixClientImplTest {
    @Test
    fun testUnauthorizedConnection() {
        runBlocking {
            var client = getClient()
            var forbidden = false

            launch {
                client.connect(mapOf("token" to "wrongToken"))
            }

            withTimeout(1000) {
                client.messages
                    .isForbidden()
                    .first()

                forbidden = true
            }

            client.disconnect()
            assert(forbidden)
        }
    }

    @Test
    fun testAuthorizedConnection() {
        runBlocking {
            var client = getClient()
            var connected = false

            launch {
                client.connect(mapOf("token" to "user1234"))
            }

            withTimeout(1000) {
                client.state
                    .filter { it == ConnectionState.CONNECTED }
                    .first()

                connected = true
            }

            client.disconnect()
            assert(connected)
        }
    }

    fun getClient(): PhoenixClient {
        val client = DefaultPhoenixClient(
            port = 4000,
            ssl = false,
        )

        return client
    }
}