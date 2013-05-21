from twilio_handlers.base import TwilioHandler
from twilio_handlers.manager import TwilioHandlerManager

START_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say>Please record your message at the beep.</Say>
    <Record action="twilio_voice_end?chat_token=blahblah" method="GET" maxLength="30" />
</Response>
"""

SAY_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say>Please record your message at the beep.</Say>
    <Hangup/>
</Response>
"""

START_MULTI_TEMPLATE = """
"""

END_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say>Thank you.</Say>
    <Hangup/>
</Response>
"""



class VoiceHandler(TwilioHandler):
    """Twilio voice callback handler."""
    
    @staticmethod
    def create(service_handler):
        """Handler factory method."""
        return VoiceHandler(service_handler)

    def handled_request_paths(self):
        """Return a list of handled request paths.

        Returns:
            list of requeset paths handled by this handler.
        """
        return ["/chatsvc/twilio_voice", "/chatsvc/twilio_voice_end", "/chatsvc/twilio_status"]

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
        if path == "/chatsvc/twilio_voice":
            return self._handle_twilio_voice(
                    request_context, chat, path, params)
        elif path == "/chatsvc/twilio_voice_end":
            return self._handle_twilio_voice_end(
                    request_context, chat, path, params)
        elif path == "/chatsvc/twilio_status":
            return SAY_TEMPLATE

    def _handle_twilio_voice(self, request_context, chat, path, params):
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
        print 'called'
        print params
        return START_TEMPLATE

    def _handle_twilio_voice_end(self, request_context, chat, path, params):
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
        print params
        return END_TEMPLATE
    

#Register handler factory method with TwilioHandlerManager.
#Note that in order for this handler to be activated it must be 
#imported before TwilioHandlerManager is instantiated.
#To ensure that this happens this module should be imported
#in message_handlers.__init__.py.
TwilioHandlerManager.register_twilio_handler(VoiceHandler.create)
