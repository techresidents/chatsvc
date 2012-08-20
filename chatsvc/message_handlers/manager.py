import logging

from trchatsvc.gen.ttypes import MessageType
from message_handlers.base import MessageHandlerException

class MessageHandlerManager(object):
    """Message handler manager.

    This class is responsible for managing specific message handlers
    and delegating incoming messages to its registered handlers.

    Concrete message handlers must register a factory method
    with this manager using the register_message_handler() method.
    Handlers will be instantiated upon manager instantiation.
    """

    #list of registered handler factory methods
    handler_factories = []

    @classmethod
    def register_message_handler(cls, handler_factory):
        """Register a message handler factory.
        
        All handlers must be registered prior to MesageHandlerManager
        instantiation. It is recommended that handlers register a 
        factory method when their module is imported.

        Active handler modules should be imported in
        message_handler.__init__.py to faciliate proper registration.

        Args:
            cls: class object
            handler_factory: handler factory method taking
                a ChatServiceHandler object as its sole
                parameter.
        """
        cls.handler_factories.append(handler_factory)


    def __init__(self, service_handler):
        """MessageHandlerManager constructor.

        Args:
            service_handler: ChatServiceHandler object.
        """
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
        """Get handlers for the given message type.

        Args:
            message_type: MessageType enum
        Returns:
            list of Handler objects registered to handle the
            given message_type.
        """
        if message_type not in self.handlers:
            self.handlers[message_type] = []
        return self.handlers[message_type]

    def _default_handle(self, request_context, chat_session, message):
        """Default handle method which should be applied to all messages.

        Args:
            request_context: RequestContext object
            chat_session: ChatSession object
            message: Message object
        """
        if message.header.type != MessageType.MARKER_CREATE:
            if not chat_session.active:
                raise MessageHandlerException("chat session closed.")

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
        result = []
        
        try:
            self._default_handle(request_context, chat_session, message)

            message_type = message.header.type
            handlers = self._get_handlers(message_type)

            if handlers:
                for handler in handlers:
                    messages = handler.handle(request_context, chat_session, message)
                    if messages:
                        result.extend(messages)
        except MessageHandlerException:
            raise
        except Exception as error:
            self.log.error("unhandled message handler exception.")
            self.log.exception(error)
            raise MessageHandlerException(str(error))

        return result
