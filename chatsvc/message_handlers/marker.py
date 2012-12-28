from trpycore.timezone import tz

from trchatsvc.gen.ttypes import MessageType, MarkerType
from message_handlers.base import MessageHandler
from message_handlers.manager import MessageHandlerManager

class ChatMarkerMessageHandler(MessageHandler):
    """Chat marker message handler.

    Message handler's handle() method will be invoked each
    time a message is received whose type matches one
    of the types returned in the handled_message_types()
    method.
    
    Each handler is repsonsible for validating the message.
    If the message is not valid a MessageHandlerException
    should be raised. Note that invalid messages will
    NOT be replicated or persisted.

    Following validation, a handler may choose to modify the
    message as needed, and optionally return a list of
    additional Message objects to be propagated accordingly.

    Note that additional messages will not be sent through
    registered handlers. This is mandated to limit complexity.
    This feature ideally exists for the simple case where
    a incoming message may result in additional outbound
    event messages (which should not need handlers).
    """
    
    @staticmethod
    def create(service_handler):
        """ChatMarkerMessageHandler factory method."""
        return ChatMarkerMessageHandler(service_handler)

    def _handle_connected_marker(self, request_context, chat_session, message):
        """CONNECTED_MARKER handler method."""
        if not chat_session.connect:
            chat_session.connect = tz.timestamp_to_utc(message.header.timestamp)
            chat_session.save()

    def _handle_publishing_marker(self, request_context, chat_session, message):
        """PUBLISHING_MARKER handler method."""
        if not chat_session.publish:
            chat_session.publish = tz.timestamp_to_utc(message.header.timestamp)
            chat_session.save()
    
    def _handle_started_marker(self, request_context, chat_session, message):
        """STARTED_MARKER handler method."""
        if not chat_session.started:
            chat_session.start = tz.timestamp_to_utc(message.header.timestamp)
            chat_session.save()

    def _handle_ended_marker(self, request_context, chat_session, message):
        """ENDED_MARKER handler method."""
        if not chat_session.completed:
            chat_session.end = tz.timestamp_to_utc(message.header.timestamp)
            chat_session.save()

    def _handle_skew_marker(self, request_context, chat_session, message):
        """SKEW_MARKER handler method."""
        msg = message.markerCreateMessage.marker.skewMarker
        msg.systemTimestamp = tz.timestamp()
        msg.skew = msg.userTimestamp - msg.systemTimestamp 

    def handled_message_types(self):
        """Return a list of handled message types.

        Returns:
            list of MessageType's handled by this handler.
        """
        return [MessageType.MARKER_CREATE]

    def handle(self, request_context, chat_session, message):
        """Handle a message.

        Args:
            request_context: RequestContext object
            chat_session: ChatSession object
            message: Message object
        Returns:
            list of additional Message objects to propagate.
            Note that the message passed to handle() should not
            be included in this list, and that any additional
            messages will not be passed through message handlers.
        Raises:
            MessageHandlerException if the message is invalid
            and should be propagated.
        """
        marker = message.markerCreateMessage.marker
        if marker.type == MarkerType.CONNECTED_MARKER:
            self._handle_connected_marker(request_context, chat_session, message)
        elif marker.type == MarkerType.PUBLISHING_MARKER:
            self._handle_publishing_marker(request_context, chat_session, message)
        elif marker.type == MarkerType.STARTED_MARKER:
            self._handle_started_marker(request_context, chat_session, message)
        elif marker.type == MarkerType.ENDED_MARKER:
            self._handle_ended_marker(request_context, chat_session, message)
        elif marker.type == MarkerType.SKEW_MARKER:
            self._handle_skew_marker(request_context, chat_session, message)

        return []

#Register message handler factory method with MessageHandlerManager.
#Note that in order for this handler to be activated it must be 
#imported before MessagehandlerManager is instantiated.
#To ensure that this happens this module should be imported
#in message_handlers.__init__.py.
MessageHandlerManager.register_message_handler(ChatMarkerMessageHandler.create)
