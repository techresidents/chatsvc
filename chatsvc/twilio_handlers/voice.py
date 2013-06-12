import json
from string import Template

from twilio_handlers.base import TwilioHandler
from twilio_handlers.manager import TwilioHandlerManager

START_TEMPLATE = Template("""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Record action="twilio_voice_end?chat_token=$chat_token&amp;user_id=$user_id" method="GET" maxLength="$max_duration" />
</Response>
""")

MULTI_START_TEMPLATE = Template("""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Dial action="twilio_voice_end?chat_token=$chat_token" record="true">
        <Conference maxParticipants="$max_participants">$chat_token</Conference>
    </Dial>
</Response>
""")

END_TEMPLATE = Template("""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say>Thank you.</Say>
    <Hangup/>
</Response>
""")


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
        return ["/chatsvc/twilio_voice", "/chatsvc/twilio_voice_end"]

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
        user_id = params.get("user_id")
        call_sid = params.get("CallSid")
        
        context = {
            "chat_token": chat.state.token,
            "max_duration": chat.state.maxDuration + chat.expiration_threshold,
            "max_participants": chat.state.maxParticipants,
            "user_id": user_id
        }

        twilio_data = chat.state.session.get("twilio_data")
        if twilio_data:
            twilio_data = json.loads(twilio_data)
        else:
            twilio_data = {
                "users": {}
            }

        #add call to chat state and replicate state to
        #other chatsvc nodes
        if user_id not in twilio_data["users"]:
            twilio_data["users"][user_id] = {
                    "calls": {}
            }
        twilio_data["users"][user_id]["calls"][call_sid] = {
                "call_sid": call_sid
        }

        chat.state.session["twilio_data"] = json.dumps(twilio_data)
        self.service_handler.replicator.replicate(chat, [])
        
        if chat.state.maxParticipants == 1:
            result = START_TEMPLATE.substitute(context)
        else:
            result = MULTI_START_TEMPLATE.substitute(context)
        
        #zmp doesn't support unicode strings so convert to string
        return str(result)

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
        context = {}
        result = END_TEMPLATE.substitute(context)
        
        #zmp doesn't support unicode strings so convert to string
        return str(result)
    

#Register handler factory method with TwilioHandlerManager.
#Note that in order for this handler to be activated it must be 
#imported before TwilioHandlerManager is instantiated.
#To ensure that this happens this module should be imported
#in message_handlers.__init__.py.
TwilioHandlerManager.register_twilio_handler(VoiceHandler.create)
