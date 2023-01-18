package phoenixclient

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

data class TestPayload(
    val name: String
)

class OutgoingMessageTest {

    @Test
    fun testMessageSerialization() {
        val message = OutgoingMessage(
            joinRef = "1",
            ref = "2",
            topic = "test",
            event = "myEvent",
            payload = mapOf("name" to "hello")
        )
        val output = message.toJson()
        val expected = """["1","2","test","myEvent",{"name":"hello"}]"""

        Assertions.assertEquals(output, expected)
    }

    @Test
    fun testMessageWithPayloadClassSerialization() {
        val message = OutgoingMessage(
            joinRef = "1",
            ref = "2",
            topic = "test",
            event = "myEvent",
            payload = TestPayload(name = "hello")
        )
        val output = message.toJson()
        val expected = """["1","2","test","myEvent",{"name":"hello"}]"""

        Assertions.assertEquals(output, expected)
    }

    @Test
    fun testMessageSerializationDefault() {
        val message = OutgoingMessage(
            ref = "2",
            topic = "test",
            event = "myEvent",
        )
        val output = message.toJson()
        val expected = """[null,"2","test","myEvent",{}]"""

        Assertions.assertEquals(output, expected)
    }
}