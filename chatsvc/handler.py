import logging

import gevent.queue

from tridlcore.gen.ttypes import RequestContext
from trpycore.greenlet.util import join
from trpycore.timezone import tz
from trpycore.zookeeper_gevent.util import expire_zookeeper_client_session
from trsvcscore.proxy.basic import BasicServiceProxy
from trsvcscore.service_gevent.handler.service import GServiceHandler
from trsvcscore.service_gevent.handler.mongrel2 import GMongrel2Handler
from trsvcscore.hashring.zoo import ZookeeperServiceHashring
from trchatsvc.gen import TChatService
from trchatsvc.gen.ttypes import HashringNode, UnavailableException, \
        InvalidChatException, InvalidMessageException

import settings
from chat import ChatManager
from message_handlers.base import MessageHandlerException
from message_handlers.manager import MessageHandlerManager
from twilio_handlers.base import TwilioHandlerException
from twilio_handlers.manager import TwilioHandlerManager
from replication import ReplicationException, GreenletPoolReplicator
from garbage import GarbageCollector, GarbageCollectionEvent

class ChatServiceHandler(TChatService.Iface, GServiceHandler):
    """Chat service handler."""

    def __init__(self, service):
        """ChatServiceHandler constructor.

        Args:
            service: Service object. Note that this will not be fully initialized
                until start() is called. It may be neccessary delay some handler
                instantiation until then.
        """
        super(ChatServiceHandler, self).__init__(
                service,
                zookeeper_hosts=settings.ZOOKEEPER_HOSTS,
                database_connection=settings.DATABASE_CONNECTION)
        

        self.chat_manager =  ChatManager(self)
        self.message_handler_manager = MessageHandlerManager(self)
        self.twilio_handler_manager = TwilioHandlerManager(self)
        self.deferred_init = False
        self.log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))
        
        #defer instantiation of the following until start()
        #since they require a fully initialized Service.
        self.service_info = None
        self.server_endpoint = None
        self.hashring = None
        self.replicator = None
        self.garbage_collector = None
        
    def _deferred_init(self):
        """Deferred initialization.

        This method contains all initialization which depends on
        a fully initialized self.service.
        """
        if not self.deferred_init:
            self.service_info = self.service.info()
            self.server_endpoint = self.service_info.default_endpoint()

            self.hashring = ZookeeperServiceHashring(
                    zookeeper_client=self.zookeeper_client,
                    service_name=settings.SERVICE,
                    service=self.service,
                    positions=[None, None, None],
                    position_data=None)

            self.replicator = GreenletPoolReplicator(
                    service=self.service,
                    hashring=self.hashring,
                    chat_manager=self.chat_manager,
                    size=settings.REPLICATION_POOL_SIZE,
                    N=settings.REPLICATION_N,
                    W=settings.REPLICATION_W,
                    max_connections_per_service=settings.REPLICATION_MAX_CONNECTIONS_PER_SERVICE,
                    allow_same_host_replications=settings.REPLICATION_ALLOW_SAME_HOST)
            
            self.garbage_collector = GarbageCollector(
                    service=self.service,
                    hashring=self.hashring,
                    chat_manager=self.chat_manager,
                    interval=60,
                    throttle=0.1)
            self.garbage_collector.add_observer(self._gc_observer)

            self.deferred_init = True


    def _is_remote_node(self, node):
        """Check if the given hashring node is remote.

        Args:
            node: ServiceHashringNode object
        Returns:
            True if node is not this node, False otherwise.
        """
        return node.service_info.key != self.service_info.key

    def _primary_node(self, chat_token):
        """Get the primary node for the chat_token.

        Returns:
            ServiceHashringNode responsible for the chat,
            or None if no nodes are available.
        """
        result = None
        preference_list = self.hashring.preference_list(chat_token)
        if preference_list:
            result = preference_list[0]
        return result

    def _service_proxy(self, node):
        """Get a service proxy to the given node.

        Returns:
            BasicServiceProxy object to the given node.
        """
        endpoint = node.service_info.default_endpoint()
        proxy = BasicServiceProxy(
                service_name=node.service_info.name,
                service_hostname=endpoint.address,
                service_port=endpoint.port,
                service_class=TChatService,
                is_gevent=True)
        return proxy

    def _convert_hashring_nodes(self, nodes):
        """Convert ServiceHashringNode's to HashringNode's.

        Returns:
            list of HashringNode object's.
        """
        result = []
        for node in nodes or []:
            endpoint = node.service_info.default_endpoint()
            hashring_node = HashringNode(
                    serviceName=node.service_info.name,
                    serviceAddress=endpoint.address,
                    servicePort=endpoint.port,
                    token="%032x" % node.token,
                    hostname=node.service_info.hostname,
                    fqdn=node.service_info.fqdn)
            result.append(hashring_node)
        return result

    def _gc_observer(self, event):
        """GarbageCollector observer method.

        Args:
            event: GarbageCollectionEvent object
        """
        if event.event_type == GarbageCollectionEvent.ZOMBIE_SESSION_EVENT:
            primary_node = self._primary_node(event.chat.token)
            if not self._is_remote_node(primary_node):
                pass

    def start(self):
        """Start handler."""
        self._deferred_init()

        super(ChatServiceHandler, self).start()
        self.replicator.start()
        self.hashring.start()
        self.garbage_collector.start()
    
    def stop(self):
        """Stop handler."""
        #Stop the hashring which will remove all of our positions.
        #Wait for the hashring to be stopped before stopping our parent,
        #since this will stop the zookeeper client which is required
        #to stop the hashring.
        self.garbage_collector.stop()
        self.hashring.stop()
        self.hashring.join()
        self.replicator.stop()

        super(ChatServiceHandler, self).stop()

        #Trigger messages which will cause all open
        #getMessage requests to return.
        self.chat_manager.trigger_messages()

    def join(self, timeout=None):
        """Join service handler.

        Join the handler, waiting for the completion of all threads 
        or greenlets.

        Args:
            timeout: Optional timeout in seconds to observe before returning.
                If timeout is specified, the status() method must be called
                to determine if the handler is still running.
        """
        greenlets = [
                self.garbage_collector,
                self.hashring,
                self.replicator,
                super(ChatServiceHandler, self)
                ]
        join(greenlets, timeout)
    
    def getHashring(self, requestContext):
        """Return hashring as ordered list of HashringNode's.
        
        Hashring is represented as an ordered list of HashringNode's.
        The list is ordered by hashring position (HashringNode.token).
        
        Args:
            requestContext: RequestContext object.
        Returns:
            Ordered list of HashringNode's.
        """
        return self._convert_hashring_nodes(self.hashring.hashring())

    def getPreferenceList(self, requestContext, chatToken):
        """Return a preference list of HashringNode's for chatToken.
        
        Generates an ordered list of HashringNode's responsible for
        the data. The list is ordered by node preference, where the
        first node in the list is the most preferred node to process
        the data. Upon failure, lower preference nodes in the list
        should be tried.

        Note that each service (unique service_key) will only appear
        once in the perference list. For each service, The
        most preferred ServiceHashringNode will be returned.
        Removing duplicate service nodes make the preference
        list makes it easier to use for failure retries, and
        replication.

        Args:
            requestContext: RequestContext object.
            chatToken: chat token
        Returns:
            Ordered list of HashringNode's.
        """
        merge_nodes = not settings.REPLICATION_ALLOW_SAME_HOST
        preference_list = self.hashring.preference_list(
                chatToken,
                merge_nodes=merge_nodes)
        return self._convert_hashring_nodes(preference_list)

    def getMessages(self, requestContext, chatToken, asOf, block, timeout):
        """Long poll for new chat messages.

        Args:
            requestContext: RequestContext object.
            chatToken: chat token
            asOf: unix timestamp after which messages should
                be returned.
            block: boolean indicating if this method should block.
            timeout: if block is True, how long to wait for
                new messages before timing out.
        Returns:
            list of Message objects.
        Raises:
            UnavailableException if no nodes are available.
        """
        primary_node = self._primary_node(chatToken)
        if primary_node is None:
            raise UnavailableException("no nodes available")

        if self._is_remote_node(primary_node):
            proxy = self._service_proxy(primary_node)
            return proxy.getMessages(requestContext, chatToken, asOf, block, timeout)
        
        try:
            chat = self.chat_manager.get(chatToken)
            messages = chat.get_messages(asOf, block, timeout, requestContext.userId)
            return messages
        except KeyError:
            raise InvalidChatException("invalid chat token: %s" % chatToken)
        except Exception as error:
            self.log.exception(error)
            raise UnavailableException(str(error))

    def sendMessage(self, requestContext, message, N, W):
        """Send message to a chat.

        Args:
            requestContext: RequestContext object.
            message: Message object.
            N: number of nodes that message should
                be replicated to.
            W: number of nodes that message needs to be
                written to before the write can be considered
                successful.
        Returns:
            Updated message.
        Raises:
            UnavailableException if no nodes are available or W
                cannot be satisified.
        """
        primary_node = self._primary_node(message.header.chatToken)
        if primary_node is None:
            raise UnavailableException("no nodes available")

        if self._is_remote_node(primary_node):
            proxy = self._service_proxy(primary_node)
            return proxy.sendMessage(requestContext, message, N, W)

        try:
            chat = self.chat_manager.get(message.header.chatToken)
            additional_messages = self.message_handler_manager.handle(
                    requestContext,
                    chat,
                    message)

            #create message list, including additional messages
            #returned by handler.
            messages = [message]
            messages.extend(additional_messages)

            #send messages to waiting users.
            chat.send_messages(messages)
            
            #replicate messages
            async_result = self.replicator.replicate(chat, messages, N, W)
            async_result.get(block=True, timeout=settings.REPLICATION_TIMEOUT)

            #return updated message
            return message
        
        except KeyError:
            raise InvalidChatException("invalid chat token: %s" %
                    message.header.chatToken)
        except MessageHandlerException as error:
            self.log.exception(error)
            raise InvalidMessageException(str(error))
        except ReplicationException as error:
            self.log.exception(error)
            raise UnavailableException(str(error))
        except gevent.Timeout:
            message = "timeout: (%ss)" % settings.REPLICATION_TIMEOUT
            self.log.error(message)
            raise UnavailableException(message)
        except Exception as error:
            self.log.exception(error)
            raise UnavailableException(str(error))

    def twilioRequest(self, requestContext, path, params):
        """Twilio callback request

        Args:
            requestContext: RequestContext object.
            path: http request path
            params: Dict of http request params
        Returns:
            Twiml string
        Raises:
            UnavailableException if no nodes are available.
        """
        chat_token = params.get("chat_token")
        print chat_token
        primary_node = self._primary_node(chat_token)
        if primary_node is None:
            raise UnavailableException("no nodes available")

        if self._is_remote_node(primary_node):
            proxy = self._service_proxy(primary_node)
            return proxy.twilioRequest(requestContext, path, params)
        
        try:
            chat = self.chat_manager.get(chat_token)
            twiml = self.twilio_handler_manager.handle(
                    requestContext, chat, path, params)
            return twiml
        except (TwilioHandlerException, KeyError):
            raise InvalidChatException("invalid chat token: %s" % chat_token)
        except Exception as error:
            self.log.exception(error)
            raise UnavailableException(str(error))

    def replicate(self, requestContext, replicationSnapshot):
        """Store a replication snapshot from another node.

        Args:
            requestContext: RequestContext object
            replicationSnapshot: ReplicationSnapshot object
        """
        chat_snapshot = replicationSnapshot.chatSnapshot
        chat = self.chat_manager.get(chat_snapshot.token)

        #update start timestamp
        if chat_snapshot.startTimestamp > 0:
            chat.start = tz.timestamp_to_utc(chat_snapshot.startTimestamp)
        
        #update end timestamp
        if chat_snapshot.endTimestamp > 0:
            chat.end = tz.timestamp_to_utc(chat_snapshot.endTimestamp)
        
        #store replicated messages.
        chat.store_replicated_messages(chat_snapshot.messages)

    def expireZookeeperSession(self, requestContext, timeout):
        result = False
        if settings.ENV == "default" or \
                settings.ENV == "test":
            result = expire_zookeeper_client_session(self.zookeeper_client, timeout)
        return result


class ChatMongrel2Handler(GMongrel2Handler):
    """Chat mongrel2 handler."""

    URL_HANDLERS = [
        (r'^/chatsvc/twilio_.*$', 'handle_twilio_request'),
    ]

    def __init__(self, service_handler):
        """ChatMongrel2Handler constructor.

        Args:
            service_handler: ChatServiceHandler object.
        """
        super(ChatMongrel2Handler, self).__init__(
                url_handlers=self.URL_HANDLERS)

        self.service_handler = service_handler
        self.log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))

    def handle_twilio_request(self, request):
        request_context = RequestContext()
        print request.req.path
        print request.params()
        
        twiml = self.service_handler.twilioRequest(
                request_context,
                request.req.path,
                request.params())
        return self.Response(twiml, headers = {
            "Content-type": "text/xml"
        })
