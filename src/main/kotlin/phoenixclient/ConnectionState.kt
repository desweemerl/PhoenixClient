package phoenixclient

enum class ConnectionState {
    CONNECTED,
    DISCONNECTING,
    DISCONNECTED,
    RECONNECTING,
}