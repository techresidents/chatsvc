import logging

from trchatsvc.gen.ttypes import MessageType
from message_handlers.base import MessageHandlerException

class MessageHandlerManager(object):
    handler_factories = []

    @classmethod
    def register_message_handler(cls, handler_factory):
        cls.handler_factories.append(handler_factory)


    def __init__(self, service_handler):
        self.service_handler = service_handler
        self.handlers = {}
        self.log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))

        for factory in self.handler_factories:
            try:
                handler = factory(self.service_handler)
                for message_type in handler.handled_message_types():
                    self._get_handlers(message_type).append(handler)
            except Exception as error:
                self.log.exception(error)
    
    def _get_handlers(self, message_type):
        if message_type not in self.handlers:
            self.handlers[message_type] = []
        return self.handlers[message_type]

    def _default_handle(self, request_context, chat_session, message):
        if message.header.type != MessageType.MARKER_CREATE:
            if not chat_session.active:
                raise MessageHandlerException("chat session closed.")

    def handle(self, request_context, chat_session, message):
        self._default_handle(request_context, chat_session, message)

        message_type = message.header.type
        handlers = self._get_handlers(message_type)
        
        if handlers:
            for handler in handlers:
                handler.handle(request_context, chat_session, message)
