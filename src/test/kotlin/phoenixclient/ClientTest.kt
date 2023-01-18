package phoenixclient

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test


class ClientTest {

    @Test
    fun testUnauthorizedConnection() = runTest {
        val client = getClient()
        var forbidden = false

        val job = launch {
            forbidden = client.messages
                .isForbidden()
                .map { true }
                .first()
        }

        client.connect(mapOf("token" to "wrongToken"))

        waitWhile(1, 5000) {
            job.isActive
        }

        job.cancel()
        client.disconnect()

        assert(forbidden)
    }

    @Test
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