package phoenixclient

class ConfigurationException(message: String) : Exception(message)

class ResponseException(message: String) : Exception(message)

class BadActionException(message: String) : Exception(message)

class TimeoutException(message: String) : Exception(message)
