import logging
import uuid

from trpycore.timezone import tz
from trchatsvc.gen.ttypes import MessageType, MessageRoute, MessageRouteType, \
        UserStatus, UserStatusMessage, Message, MessageHeader
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

    def _default_handle(self, request_context, chat, message):
        """Default handle method which should be applied to all messages.

        Args:
            request_context: RequestContext object
            chat: Chat object
            message: Message object
        """
        #give messages a working header timestamp even
        #thoough it will be updated immediately before message
        #is sent out. If user provided a timestamp use it to calculate
        #the skew between the client and server clocks.
        now = tz.timestamp()
        if message.header.timestamp is None:
            message.header.skew = 0
        else:
            message.header.skew = message.header.timestamp - now
        message.header.timestamp = now

        if message.header.id is None:
            message.header.id = uuid.uuid4().hex

        if message.header.route is None:
            message.header.route = MessageRoute(
                    MessageRouteType.BROADCAST_ROUTE)

        message_types = [MessageType.USER_STATUS, MessageType.CHAT_STATUS]
        if message.header.type not in message_types:
            if not chat.active:
                raise MessageHandlerException("chat is not active.")

    def handle(self, request_context, chat, message):
        """Handle a message.

        Args:
            request_context: RequestContext object
            chat: Chat object
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
            self._default_handle(request_context, chat, message)

            message_type = message.header.type
            handlers = self._get_handlers(message_type)

            if handlers:
                for handler in handlers:
                    messages = handler.handle(request_context, chat, message)
                    if messages:
                        result.extend(messages)
        except MessageHandlerException:
            raise
        except Exception as error:
            self.log.error("unhandled message handler exception.")
            self.log.exception(error)
            raise MessageHandlerException(str(error))

        return result

    def handle_poll(self, request_context, chat):
        """Handle poll for messages.

        Args:
            request_context: RequestContext object
            chat: Chat object
        Returns:
            list of additional Message objects to propagate.
            Note that messages returned will be propagated through
            message handlers just like ordinary messages.
        """
        result = []
        now = tz.timestamp()
        
        #update the polling user's updateTimestamp. This timestamp
        #represents the last time the user communicated with us.
        #If it exceeds a threshold we'll change their status to
        #UNAVAILABLE.
        user_state = chat.state.users.get(request_context.userId)
        if user_state:
            user_state.updateTimestamp = now

        #find idle users and update status
        for user_state in chat.state.users.values():
            if user_state.status != UserStatus.UNAVAILABLE and \
               now - user_state.updateTimestamp > 20:
                msg = self._build_user_status_message(chat,
                        user_state.userId, UserStatus.UNAVAILABLE)
                result.append(msg)
        return result
    
    def _build_user_status_message(self, chat, user_id, status):
        header = MessageHeader(
            chatToken=chat.state.token,
            type=MessageType.USER_STATUS,
            userId=0,
            skew=0,
            route=MessageRoute(MessageRouteType.BROADCAST_ROUTE))
        user_status_message = UserStatusMessage(
            userId=user_id,
            status=status)
        message = Message(header=header, userStatusMessage=user_status_message)
        return message
