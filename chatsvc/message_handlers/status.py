from trchatsvc.gen.ttypes import MessageType, ChatStatus,\
        UserStatus, UserState
from message_handlers.base import MessageHandler
from message_handlers.manager import MessageHandlerManager

class StatusMessageHandler(MessageHandler):
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
        return StatusMessageHandler(service_handler)

    def _handle_chat_status(self, request_context, chat, message):
        """CHAT_STATUS handler method."""
        msg = message.chatStatusMessage
        if chat.state.status == ChatStatus.PENDING:
            if msg.status == ChatStatus.STARTED:
                chat.state.status = msg.status
                chat.state.startTimestamp = message.header.timestamp
                chat.save() 
        if chat.state.status == ChatStatus.STARTED:
            if msg.status == ChatStatus.ENDED:
                chat.state.status = msg.status
                chat.state.endTimestamp = message.header.timestamp
                chat.save() 

    def _handle_user_status(self, request_context, chat, message):
        """USER_STATUS handler method."""
        msg = message.userStatusMessage
        user_state = chat.state.users.get(msg.userId)

        if user_state is None:
            user_state = UserState(
                    userId=msg.userId,
                    status=UserStatus.DISCONNECTED,
                    updateTimestamp=message.header.timestamp)
            chat.state.users[request_context.userId] = user_state

        user_state.status = msg.status
        user_state.updateTimestamp = message.header.timestamp

    def handled_message_types(self):
        """Return a list of handled message types.

        Returns:
            list of MessageType's handled by this handler.
        """
        return [MessageType.CHAT_STATUS, MessageType.USER_STATUS]

    def handle(self, request_context, chat, message):
        """Handle a message.

        Args:
            request_context: RequestContext object
            chat: ChatSession object
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
        if message.header.type == MessageType.CHAT_STATUS:
            self._handle_chat_status(request_context, chat, message)
        elif message.header.type == MessageType.USER_STATUS:
            self._handle_user_status(request_context, chat, message)

        return []

#Register message handler factory method with MessageHandlerManager.
#Note that in order for this handler to be activated it must be 
#imported before MessagehandlerManager is instantiated.
#To ensure that this happens this module should be imported
#in message_handlers.__init__.py.
MessageHandlerManager.register_message_handler(StatusMessageHandler.create)
