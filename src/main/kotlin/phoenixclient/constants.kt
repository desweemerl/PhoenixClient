package phoenixclient

// Keep default values defined in https://github.com/phoenixframework/phoenix/blob/master/assets/js/phoenix/constants.js
const val VSN = "2.0.0"

const val DEFAULT_HEARTBEAT_INTERVAL = 30000L // 30 secs
const val DEFAULT_HEARTBEAT_TIMEOUT = 1000L // 1 sec
const val DEFAULT_TIMEOUT = 10000L // 10 secs

val DEFAULT_RETRY: DynamicTimeout = {
    listOf(10L, 50L, 100L, 150L, 200L, 250L, 500L, 1000L, 2000L).getOrNull(it) ?: 5000
}

val DEFAULT_REJOIN_TIMEOUT: DynamicTimeout = {
    listOf(1000L, 2000L, 5000L).getOrNull(it) ?: 10000L
}