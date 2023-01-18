package phoenixclient

class ConfigurationException(message: String) : Exception(message)

class ResponseException(message: String, val source: IncomingMessage) : Exception(message)

class BadActionException(message: String) : Exception(message)

class ChannelException(message: String) : Exception(message)

class TimeoutException(message: String) : Exception(message)
