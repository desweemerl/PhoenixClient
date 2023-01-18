package phoenixclient

import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test

class ChannelTest {

    @Test
    fun testJoinChannel() {
        runBlocking {
            withTimeout(5000) {
                val client = getClient()

                var refs = mutableListOf<String?>();
                var refsJob = launch {
                    client.messages.map { it.ref }.collect { refs.add(it) }
                }

                client.connect(mapOf("token" to "user1234"))
                val response = client.onConnected {
                    it
                        .join("test:1").getOrThrow()
                        .push("hello", mapOf("name" to "toto")).getOrThrow().getResponse()
                }

                client.disconnect()
                refsJob.cancel()

                assert(response!!.get("message") == "hello toto")
                assert(refs  == listOf("1", "2"))
            }
        }
    }

    private fun getClient(): Client =
        okHttpPhoenixClient(
            port = 4000,
            ssl = false,
        ).getOrThrow()
}