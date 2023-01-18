package phoenixclient

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.junit.jupiter.api.Test

class ChannelTest {

    @Test
    fun testChannelJoinAndRef() {
        runBlocking {
            withTimeout(5000) {
                val client = getClient()
                var ref = ""
                var refJob = launch {
                    client.messages
                        .filter { it.topic == "test:1" }
                        .map { it.ref }
                        .collect { ref = it!! }
                }
                client.connect(mapOf("token" to "user1234"))
                val message = client.state.isConnected().map {
                    client
                        .join("test:1").getOrThrow()
                        .push("hello", mapOf("name" to "toto")).getOrThrow()
                }.first()
                client.disconnect()
                refJob.cancel()

                assert(message.getResponse()?.get("message") == "hello toto")
                assert(message.ref == ref && Regex("""^\d+$""").matches(ref))
            }
        }
    }

    @Test
    fun testChannelDisconnection() {
        runBlocking {
            withTimeout(5000) {
                val client = getClient(retry = 10)
                var connection = 0
                var disconnection = 0

                val counterJob = launch {
                    client.state.filter { it == ConnectionState.DISCONNECTED }.collect { disconnection++ }
                }
                client.connect(mapOf("token" to "user1234"))
                client.state.isConnected()
                    .takeWhile { connection <= 3 }
                    .collect {
                        client.join("test:1").getOrThrow().pushNoReply("close_socket")
                        connection++
                    }
                counterJob.cancel()
                client.disconnect()

                assert(disconnection == connection)
            }
        }
    }
    /*
    @Test
    fun testChannelCrash() {
        runBlocking {
            withTimeout(50000) {
                val client = getClient()
                client.connect(mapOf("token" to "user1234"))
                client.state.isConnected()
                    .collect {
                        var channel = client.join("test:1").getOrThrow()
                        channel.pushNoReply("crash_channel")
                        println(channel.push("hello", mapOf("name" to "toto")))
                    }

            }
            delay(50000)

            assert(false)
        }
    }
    */
    private fun getClient(
        retry: Long = DEFAULT_RETRY,
        heartbeatInterval: Long = DEFAULT_HEARTBEAT_INTERVAL
    ): Client =
        okHttpPhoenixClient(
            port = 4000,
            ssl = false,
            retry = retry,
            heartbeatInterval = heartbeatInterval,
        ).getOrThrow()
}