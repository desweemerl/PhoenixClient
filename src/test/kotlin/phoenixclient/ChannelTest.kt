package phoenixclient

import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test

class ChannelTest {

    @Test
    fun testChannelJoinAndRef() = runTest {
        val client = getClient()
        var message: IncomingMessage? = null

        var job = launch {
            message = client.state.isConnected().map {
                client
                    .join("test:1").getOrThrow()
                    .push("hello", mapOf("name" to "toto")).getOrThrow()
            }.first()
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 5000L) {
            message == null
        }

        job.cancel()
        client.disconnect()

        assert(message?.getResponse()?.get("message") == "hello toto")
        assert(Regex("""^\d+$""").matches(message?.ref!!))
    }

    @Test
    fun testChannelDisconnection() = runTest {
        val client = getClient()
        var connection = 0

        client.connect(mapOf("token" to "user1234"))
        val channel = client.join("test:1").getOrThrow()

        val disconnectionJob = launch {
            client.state.isConnected()
                .takeWhile { connection < 3 }
                .collect {
                    connection++
                    launch {
                        channel.pushNoReply("close_socket")
                    }
                }
        }

        waitWhile(10, 5000L) {
            disconnectionJob.isActive
        }

        client.disconnect()

        assert(connection == 3)
    }

    @RepeatedTest(10)
    fun testChannelCrash() = runBlocking {
        val client = getClient()
        var exception: Throwable? = null

        val job = launch {
            client.state.isConnected()
                .collect {
                    var channel = client.join("test:1").getOrThrow()
                    channel.pushNoReply("crash_channel")
                    exception = channel.push("hello", mapOf("name" to "toto"), 1000L).exceptionOrNull()
                }
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 5000) {
            exception == null
        }

        job.cancel()
        client.disconnect()

        assert(exception is TimeoutException || exception is ResponseException)
    }


    @RepeatedTest(5)
    fun testChannelBatch() = runTest {
        val client = getClient()
        var counter = 0

        val job = launch {
            client.state.isConnected()
                .collect {
                    var channel = client.join("test:1").getOrThrow()
                    repeat(1000) {
                        val name = "toto$counter"
                        channel.push("hello", mapOf("name" to name), 100L).getOrThrow()
                        counter++
                    }
                }
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 10000) {
            counter < 1000
        }

        job.cancel()
        client.disconnect()

        assert(counter == 1000)
    }

    private fun getClient(
        retry: DynamicTimeout = DEFAULT_RETRY,
        heartbeatInterval: Long = DEFAULT_HEARTBEAT_INTERVAL
    ): Client =
        okHttpPhoenixClient(
            port = 4000,
            ssl = false,
            retryTimeout = retry,
            heartbeatInterval = heartbeatInterval,
        ).getOrThrow()
}