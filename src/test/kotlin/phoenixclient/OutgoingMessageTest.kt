package phoenixclient

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class OutgoingMessageTest {

    @Test
    fun testMessageSerialization() {
        val message = OutgoingMessage(
            joinRef = "1",
            ref = "2",
            topic = "test",
            event = "myEvent",
            payload = mapOf("toto" to "hello")
        )
        val output = message.toJson()
        val expected = """["1","2","test","myEvent",{"toto":"hello"}]"""

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