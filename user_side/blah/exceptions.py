class CallbackNotFound(Exception):
    """Unable to find callback"""

    def __init__(self, event, callback):
        self.event = event
        self.callback = callback

    def __str__(self):
        return "Unable to location callback {} for event {}".format(self.callback, self.event)


class DataEngineError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        if self.message:
            return self.message
        else:
            return "There was an error while performing a data engine operation"


class EventNotFound(Exception):
    """Unable to find event"""

    def __init__(self, event):
        self.event = event

    def __str__(self):
        return "Unable to find event {}".format(self.event)


class EnvVarNotFound(Exception):
    def __init__(self, var):
        self.env_var = var

    def __str__(self):
        return "Environmnet variable, {}, is not set".format(self.env_var)


class Unauthorized(Exception):
    def __str__(self):
        return "Unable to locate credentials, please authenticate"


class InvalidRequestMethod(Exception):
    def __init__(self, method):
        self.method = method

    def __str__(self):
        return "HTTP Method {} is not supported".format(self.method)


class MissingRequiredParameters(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        if self.message:
            return self.message
        else:
            return "Missing Required Parameters"


class QueueError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message
