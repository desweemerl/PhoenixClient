package phoenixclient

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test


class ClientTest {

    @Test
    @ExperimentalCoroutinesApi
    fun testUnauthorizedConnection() = runTest {
        withContext(Dispatchers.Default) {
            withTimeout(5000) {
                var forbidden = false
                val client = getClient()
                val job = launch {
                    forbidden = client.messages.filter { it == Forbidden }.map { true }.first()
                }

                client.connect(mapOf("token" to "wrongToken"))
                job.join()
                client.disconnect()

                assert(forbidden)
                assert(!client.active)
            }
        }
    }

    @Test
    @ExperimentalCoroutinesApi
    fun testAuthorizedConnection() = runTest {
        withContext(Dispatchers.Default) {
            withTimeout(5000) {
                val client = getClient()
                var isConnected = false
                val job = launch {
                    isConnected = client.state.waitConnected()
                }

                client.connect(mapOf("token" to "user1234"))
                job.join()
                client.disconnect()

                assert(isConnected)
            }
        }
    }

    @Test
    @ExperimentalCoroutinesApi
    fun testHeartbeat() = runTest {
        withContext(Dispatchers.Default) {
            val client = getClient(
                heartbeatTimeout = 1000L,
                heartbeatInterval = 1000L,
                defaultTimeout = 1000L,
            )

            var counter = 0

            val job = launch {
                client.messages
                    .filter { it.topic == "phoenix" && it.event == "phx_reply" }
                    .collect {
                        counter++
                    }
            }

            client.connect(mapOf("token" to "user1234"))
            client.state.waitConnected()

            delay(4000L)
            val refCounter = counter

            client.disconnect()
            client.state.waitDisconnected()

            delay(4000L)
            assert(counter > 0 && refCounter == counter)

            client.connect(mapOf("token" to "user1234"))
            client.state.waitConnected()

            delay(4000L)
            assert(refCounter != counter)

            client.disconnect()
            job.cancel()
        }
    }

    @Test
    @ExperimentalCoroutinesApi
    fun testHeadersConnection() = runTest {
        withContext(Dispatchers.Default) {
            withTimeout(5000) {
                val client = getClient()
                var reply: Reply? = null

                val job = launch {
                    client.state.waitConnected()
                    val channel = client.join("test:1").getOrThrow()
                    reply = channel.push("get_headers").getOrThrow()
                }

                client.connect(
                    params = mapOf("token" to "user1234"),
                    headers = mapOf("X-Header1" to "value1", "X-Header2" to "value2"),
                )
                job.join()
                client.disconnect()

                val values = reply?.convertTo(Map::class)?.getOrThrow()
                assert(values?.get("header1") == "value1")
                assert(values?.get("header2") == "value2")
            }
        }
    }

    private fun getClient(
        heartbeatInterval: Long = DEFAULT_HEARTBEAT_INTERVAL,
        heartbeatTimeout: Long = DEFAULT_HEARTBEAT_TIMEOUT,
        defaultTimeout: Long = DEFAULT_TIMEOUT,
    ): Client =
        okHttpPhoenixClient(
            port = 4000,
            ssl = false,
            heartbeatInterval = heartbeatInterval,
            heartbeatTimeout = heartbeatTimeout,
            defaultTimeout = defaultTimeout,
        ).getOrThrow()
}
