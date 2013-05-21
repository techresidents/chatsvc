import abc

class TwilioHandlerException(Exception):
    """Twilio handler exception class."""
    pass

class TwilioHandler(object):
    """Abstract base message handler class.
    
    Twilio handler's handle() method will be invoked each
    time a requeest is received whose path matches one
    of the types returned in the handled_request_paths()
    method.
    
    Each handler is repsonsible for validating the request.
    If the message is not valid a TwilioHandlerException
    should be raised. 
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, service_handler):
        self.service_handler = service_handler
    
    @abc.abstractmethod
    def handled_request_paths(self):
        """Return a list of handled request paths.

        Returns:
            list of requeset paths handled by this handler.
        """
        return

    @abc.abstractmethod
    def handle(self, request_context, chat, path, params):
        """Handle a request.

        Args:
            request_context: RequestContext object
            chat: Chat object
            path: http request path
            params: dict of http request params
        Returns:
            Twilio TwiML response
        Raises:
            TwilioHandlerException if the message is invalid
            and should be propagated.
        """
        return
