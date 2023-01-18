package phoenixclient

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test


class ClientTest {

    @Test
    @ExperimentalCoroutinesApi
    fun testUnauthorizedConnection() = runTest {
        var forbidden = false
        val client = getClient()
        val job = launch {
            client.messages.collect {
                if (it == Forbidden) {
                    forbidden = true
                }
            }

        }

        client.connect(mapOf("token" to "wrongToken"))

        waitWhile(1, 5000) {
            !forbidden
        }

        job.cancel()
        client.disconnect()

        assert(forbidden)
        assert(!client.active)
    }

    @Test
    @ExperimentalCoroutinesApi
    fun testAuthorizedConnection() = runTest {
        val client = getClient()
        var isConnected = false

        val job = launch {
            isConnected = client.state.isConnected().first()
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 5000) {
            job.isActive
        }

        job.cancel()
        client.disconnect()

        assert(isConnected)
    }

    private fun getClient(): Client =
        okHttpPhoenixClient(
            port = 4000,
            ssl = false,
        ).getOrThrow()
}