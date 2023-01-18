package phoenixclient

import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.assertThrows

class TimerTest {

    private val finiteTimeout: DynamicTimeout
        get() = {
            listOf(100L, 250L, 500L).getOrNull(it)
        }


    private val infiniteTimeout: DynamicTimeout
        get() = {
            listOf(100L, 250L).getOrNull(it) ?: 500L
        }

    @Test
    @ExperimentalCoroutinesApi
    fun testTimerTimeout() = runTest {
        var counter = 0
        val timer = timer(finiteTimeout) {
            withContext(Dispatchers.Default) {
                counter++
                delay(1000)
            }
        }

        timer.start()

        assert(counter == 3)
        assert(timer.lastResult?.exceptionOrNull() is TimeoutException)
    }

    @Test
    @ExperimentalCoroutinesApi
    fun testTimerSecondSucceeded() = runTest {
        var counter = 0
        val timer = timer(finiteTimeout) {
            withContext(Dispatchers.Default) {
                counter++
                delay(150L)
                true
            }
        }

        timer.start()

        assert(counter == 2)
        assert(timer.lastResult?.getOrNull() == true)
    }

    @Test
    @ExperimentalCoroutinesApi
    fun testTimerLastSucceeded() = runTest {
        var counter = 0

        val timer = timer(finiteTimeout) {
            withContext(Dispatchers.Default) {
                counter++
                delay(300L)
                true
            }
        }

        timer.start()

        assert(counter == 3)
        assert(timer.lastResult?.getOrNull() == true)
    }

    @Test
    @ExperimentalCoroutinesApi
    fun testTimerFlowSucceeded() = runTest {
        val message = MutableSharedFlow<String>()
        val timer = timer(infiniteTimeout) {
            message.first { it == "ok" }
        }

        launch {
            withContext(Dispatchers.Default) {
                delay(400)
                message.emit("ok")
            }
        }

        timer.start()

        assert(timer.lastResult?.getOrNull() == "ok")
    }


    @Test
    @ExperimentalCoroutinesApi
    fun testTimerFlowFailed() = runTest {
        val message = MutableSharedFlow<String>()
        val timer = timer(infiniteTimeout) {
            message.first { it == "ok" }
            throw Exception("it crashes")
        }

        launch {
            withContext(Dispatchers.Default) {
                delay(400)
                message.emit("ok")
            }
        }

        timer.start()

        assertThrows<Exception>("it crashes") {
            timer.lastResult?.getOrThrow()
        }
    }
}