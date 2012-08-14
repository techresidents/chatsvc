import abc

class MessageHandlerException(Exception):
    """Message handler exception class."""
    pass

class MessageHandler(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, service_handler):
        self.service_handler = service_handler
    
    @abc.abstractmethod
    def handled_message_types(self):
        return

    @abc.abstractmethod
    def handle(self, request_context, chat_session, message):
        return
