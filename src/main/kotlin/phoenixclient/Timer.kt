package phoenixclient

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout

typealias DynamicTimeout = (tries: Int) -> Long?

fun Long.toDynamicTimeout(repeat: Boolean = false): DynamicTimeout = {
    if (repeat) {
        this
    } else {
        if (it > 0) null else this
    }
}

suspend fun dynamicTimer(dynamicTimeout: DynamicTimeout, block: suspend () -> Unit) = coroutineScope {
    var attempt = 0
    var active = true

    while (active) {
        val currentTimeout: Long = dynamicTimeout(attempt)
            ?: throw TimeoutException("number of attempts '$attempt' exceeds limit")

        try {
            withTimeout(currentTimeout) {
                block()
                active = false
            }
        } catch (_: TimeoutCancellationException) {
        }

        attempt++
    }
}