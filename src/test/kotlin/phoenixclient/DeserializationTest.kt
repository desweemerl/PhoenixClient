package phoenixclient

import com.google.gson.annotations.SerializedName
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import kotlin.reflect.KClass

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
    fun testDeserializeObject() = runTest {
        val client = getClient()
        var response: TestReplyObject? = null

        val job = launch {
            client.state.isConnected().map {
                response = client
                    .join("test:1").getOrThrow()
                    .push("deserialize_object")
                    .getOrThrow().reply
                    ?.convertTo(TestReplyObject::class)?.getOrThrow()
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
    fun testDeserializeList() = runTest {
        val client = getClient()
        var response: TestListReplyObject? = null

        val job = launch {
            client.state.isConnected().map {
                response = client
                    .join("test:1").getOrThrow()
                    .push("deserialize_list")
                    .getOrThrow().reply
                    ?.convertTo(TestListReplyObject::class)?.getOrThrow()
            }.first()
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 50000L) {
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
    fun testDeserializeListFailed() = runTest {
        val client = getClient()
        var response: Result<TestListReplyObject>? = null

        val job = launch {
            client.state.isConnected().map {
                response = client
                    .join("test:1").getOrThrow()
                    .push("deserialize_list_failed")
                    .getOrThrow().reply
                    ?.convertTo(TestListReplyObject::class)
            }.first()
        }

        client.connect(mapOf("token" to "user1234"))

        waitWhile(1, 50000L) {
            response == null
        }

        job.cancel()

        assert(response!!.exceptionOrNull()!!.message!!.contains("Not a JSON Object"))
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
