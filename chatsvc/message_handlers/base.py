import abc

class MessageHandlerException(Exception):
    """Message handler exception class."""
    pass

class MessageHandler(object):
    """Abstract base message handler class.
    
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

    __metaclass__ = abc.ABCMeta

    def __init__(self, service_handler):
        self.service_handler = service_handler
    
    @abc.abstractmethod
    def handled_message_types(self):
        """Return a list of handled message types.

        Returns:
            list of MessageType's handled by this handler.
        """
        return

    @abc.abstractmethod
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
        return
