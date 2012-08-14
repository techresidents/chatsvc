from trpycore.timezone import tz

from trchatsvc.gen.ttypes import MessageType, MarkerType
from message_handlers.base import MessageHandler
from message_handlers.manager import MessageHandlerManager

class ChatMarkerMessageHandler(MessageHandler):
    
    @staticmethod
    def create(service_handler):
        return ChatMarkerMessageHandler(service_handler)
    
    def _handle_started_marker(self, request_context, chat_session, message):
        if not chat_session.started:
            chat_session.start = tz.timestamp_to_utc(message.header.timestamp)
            chat_session.save()

    def _handle_ended_marker(self, request_context, chat_session, message):
        if not chat_session.completed:
            chat_session.end = tz.timestamp_to_utc(message.header.timestamp)
            chat_session.save()

    def handled_message_types(self):
        return [MessageType.MARKER_CREATE]

    def handle(self, request_context, chat_session, message):
        marker = message.markerCreateMessage.marker
        if marker.type == MarkerType.STARTED_MARKER:
            self._handle_started_marker(request_context, chat_session, message)
        elif marker.type == MarkerType.ENDED_MARKER:
            self._handle_ended_marker(request_context, chat_session, message)

MessageHandlerManager.register_message_handler(ChatMarkerMessageHandler.create)
