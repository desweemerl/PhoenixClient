package phoenixclient

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test

class ChannelTest {

    @Test
    fun testJoinChannel() {
        runBlocking {
            withTimeout(5000) {
                val client = getClient()
                client.connect(mapOf("token" to "user1234"))
                val response = client.onConnected {
                    it
                        .join("test:1").getOrThrow()
                        .push("hello", mapOf("name" to "toto")).getOrThrow().getResponse()
                }

                client.disconnect()
                assert(response!!.get("message") == "hello toto")
            }
        }
    }

    private fun getClient(): Client =
        okHttpPhoenixClient(
            port = 4000,
            ssl = false,
        ).getOrThrow()
}