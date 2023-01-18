package phoenixclient

import com.google.gson.annotations.SerializedName
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class DeserializationTest {

    data class TestReplyObject(
        @SerializedName(value = "value_string")
        val valueString: String,
        @SerializedName(value = "value_number")
        val valueNumber: Float,
        @SerializedName(value = "value_boolean")
        val valueBoolean: Boolean,
    )

    data class TestListReplyObject(
        val list: ArrayList<TestReplyObject>
    )

    @Test
    @ExperimentalCoroutinesApi
    fun testDeserializeObject() = runTest {
        val client = getClient()
        var response: TestReplyObject? = null

        val job = launch {
            client.state.isConnected().map {
                val reply = client
                    .join("test:1").getOrThrow()
                    .push("deserialize_object")
                    .getOrThrow()

                response = reply.convertTo(TestReplyObject::class).getOrThrow()
            }.first()
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 5000L) {
            response == null
        }

        job.cancel()

        assert(
            response == TestReplyObject(
                valueString = "test1234",
                valueNumber = -1234.5678f,
                valueBoolean = true,
            )
        )
    }

    @Test
    @ExperimentalCoroutinesApi
    fun testDeserializeList() = runTest {
        val client = getClient()
        var response: TestListReplyObject? = null

        val job = launch {
            client.state.isConnected().map {
                val reply = client
                    .join("test:1").getOrThrow()
                    .push("deserialize_list")
                    .getOrThrow()

                response = reply.convertTo(TestListReplyObject::class).getOrThrow()
            }.first()
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 5000L) {
            response == null
        }

        job.cancel()

        val expected = listOf("_1", "_2").map {
            TestReplyObject(
                valueString = "test1234${it}",
                valueNumber = -1234.5678f,
                valueBoolean = true,
            )
        }
        assert(response?.list == expected)
    }

    @Test
    @ExperimentalCoroutinesApi
    fun testDeserializeListFailed() = runTest {
        val client = getClient()
        var response: Result<TestListReplyObject>? = null

        val job = launch {
            client.state.isConnected().map {
                val reply = client
                    .join("test:1").getOrThrow()
                    .push("deserialize_list_failed")
                    .getOrThrow()

                response = reply.convertTo(TestListReplyObject::class)
            }.first()
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 5000L) {
            response == null
        }

        job.cancel()

        assert(response!!.exceptionOrNull()!!.message!!
            .contains("Expected BEGIN_OBJECT but was BEGIN_ARRAY at path"))
    }

    @Test
    @ExperimentalCoroutinesApi
    fun testDeserializeEvent() = runTest {
        val client = getClient()
        var response: TestReplyObject? = null

        val job1 = launch {
            client.state.isConnected().map {
                client
                    .join("test:1").getOrThrow()
                    .pushNoReply("deserialize_event")
            }.first()
        }

        val job2 = launch {
            response = client.messages
                .filterEvent("test_event").first()
                .payload!!.convertTo(TestReplyObject::class).getOrThrow()
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 5000L) {
            response == null
        }

        job1.cancel()
        job2.cancel()

        assert(
            response == TestReplyObject(
                valueString = "test1234",
                valueNumber = -1234.5678f,
                valueBoolean = true,
            )
        )
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
