import logging

from twilio_handlers.base import TwilioHandlerException

class TwilioHandlerManager(object):
    """Twilio request handler manager.

    This class is responsible for managing Twilio request handlers.

    Concrete request handlers must register a factory method
    with this manager using the register_twilio_handler() method.
    Handlers will be instantiated upon manager instantiation.
    """

    #list of registered handler factory methods
    handler_factories = []

    @classmethod
    def register_twilio_handler(cls, handler_factory):
        """Register a twilio handler factory.
        
        All handlers must be registered prior to TwilioHandlerManager
        instantiation. It is recommended that handlers register a 
        factory method when their module is imported.

        Active handler modules should be imported in
        twioio_handler.__init__.py to faciliate proper registration.

        Args:
            cls: class object
            handler_factory: handler factory method taking
                a ChatServiceHandler object as its sole
                parameter.
        """
        cls.handler_factories.append(handler_factory)


    def __init__(self, service_handler):
        """TwilioHandlerManager constructor.

        Args:
            service_handler: ChatServiceHandler object.
        """
        self.service_handler = service_handler
        self.handlers = {}
        self.log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))

        for factory in self.handler_factories:
            try:
                handler = factory(self.service_handler)
                for path in handler.handled_request_paths():
                    self._get_handlers(path).append(handler)
            except Exception as error:
                self.log.exception(error)
    
    def _get_handlers(self, path):
        """Get handlers for the given twilio request type.

        Args:
            path: Twilio http request path
        Returns:
            list of Handler objects registered to handle the
            given requeset_type.
        """
        if path not in self.handlers:
            self.handlers[path] = []
        return self.handlers[path]

    def _default_handle(self, request_context, chat, path, params):
        """Default handle method which should be applied to all messages.

        Args:
            request_context: RequestContext object
            chat: Chat object
            path: http request path,
            params: http request params
        Raises:
            TwilioandlerException if the request cannot be handled.
        """
        pass

    def handle(self, request_context, chat, path, params):
        """Handle a message.

        Args:
            request_context: RequestContext object
            chat: Chat object
            path: http request path,
            params: http request params
        Returns:
            Twilio TwiML response.
        Raises:
            TwilioandlerException if the request cannot be handled.
        """
        twiml = None
        
        try:
            self._default_handle(request_context, chat, path, params)

            handlers = self._get_handlers(path)

            if handlers:
                for handler in handlers:
                    twiml = handler.handle(request_context, chat, path, params)
                    if twiml:
                        break
        except TwilioHandlerException:
            raise
        except Exception as error:
            self.log.error("unhandled message handler exception.")
            self.log.exception(error)
            raise TwilioHandlerException(str(error))

        return twiml
