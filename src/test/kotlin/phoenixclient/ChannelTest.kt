package phoenixclient

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test

class ReplyMessage(val message: String)

class ChannelTest {

    @Test
    @ExperimentalCoroutinesApi
    fun testChannelJoinAndRef() = runTest {
        val client = getClient()
        var message1: Reply? = null
        var message2: IncomingMessage? = null

        val job1 = launch {
            message1 = client.state.isConnected().map {
                client
                    .join("test:1").getOrThrow()
                    .push("hello", TestPayload(name = "toto")).getOrThrow()
            }.first()
        }

        val job2 = launch {
            client.messages.collect {
                message2 = it
            }
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 5000L) {
            message1 == null || message2 == null
        }

        job1.cancel()
        job2.cancel()
        client.disconnect()

        assert(message1?.convertTo(ReplyMessage::class)?.getOrNull()?.message == "hello toto")
        assert(Regex("""^\d+$""").matches(message2?.ref!!))
    }

    @Test
    @ExperimentalCoroutinesApi
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
                    val channel = client.join("test:1").getOrThrow()
                    channel.pushNoReply("crash_channel")
                    exception = channel.push("hello", TestPayload(name = "toto"), 1000L).exceptionOrNull()
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

    @RepeatedTest(10)
    fun testChannelCrashRejoin() = runBlocking {
        val client = getClient()
        var countJoined = 0

        val job = launch {
            client.state.isConnected()
                .collect {
                    val channel = client.join("test:1").getOrThrow()

                    launch {
                        channel.state
                            .filter { it == ChannelState.JOINED }
                            .collect {
                                countJoined++
                            }
                    }

                    launch {
                        channel.pushNoReply("crash_channel")
                    }
                }
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 5000) {
            countJoined < 2
        }

        job.cancel()
        client.disconnect()

        assert(countJoined == 2)
    }

    @RepeatedTest(5)
    @ExperimentalCoroutinesApi
    fun testChannelBatch() = runTest {
        val client = getClient()
        var counter1 = 0
        var counter2 = 0

        val job1 = launch {
            client.state.isConnected()
                .collect {
                    val channel = client.join("test:1").getOrThrow()
                    repeat(1000) {
                        val name = "toto$counter1"
                        channel.push("hello", TestPayload(name = name), 100L).getOrThrow()
                        counter1++
                    }
                }
        }

        val job2 = launch {
            client.messages.collect {
                if (it.topic == "test:1" && it.joinRef != null) {
                    counter2++
                }
            }
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 10000) {
            counter1 < 1000 || counter2 < 1000
        }

        job1.cancel()
        job2.cancel()
        client.disconnect()

        assert(counter1 == 1000)
        assert(counter2 == 1000)
    }

    @Test
    @ExperimentalCoroutinesApi
    fun testChannelMessages() = runTest {
        val client = getClient()
        var message: IncomingMessage? = null

        val job = launch {
            client.state
                .isConnected()
                .map {
                    client.join("test:1").getOrThrow()
                }
                .collect { channel ->
                    launch {
                        channel.messages.collect {
                            message = it
                        }
                    }

                    channel.push("hello", TestPayload(name = "toto"), 100L).getOrThrow()
                }
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 10000) {
            message == null
        }

        job.cancel()
        client.disconnect()

        assert(message?.payload?.convertTo(ReplyMessage::class, "response")?.getOrNull()?.message == "hello toto")
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