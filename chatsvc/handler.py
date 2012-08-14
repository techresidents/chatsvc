import json
import logging

import gevent.queue

from trpycore import riak_gevent
from trpycore.greenlet.util import join
from trpycore.riak_common.factory import RiakClientFactory
from trpycore.timezone import tz
from trpycore.zookeeper_gevent.util import expire_zookeeper_client_session
from trsvcscore.mongrel2.decorator import session_required
from trsvcscore.proxy.basic import BasicServiceProxy
from trsvcscore.service_gevent.handler.service import GServiceHandler
from trsvcscore.service_gevent.handler.mongrel2 import GMongrel2Handler
from trsvcscore.session.riak import RiakSessionStorePool
from trsvcscore.hashring.zoo import ZookeeperServiceHashring
from trsvcscore.http.error import HttpError
from tridlcore.gen.ttypes import RequestContext
from trchatsvc.gen import TChatService
from trchatsvc.gen.ttypes import HashringNode, UnavailableException, InvalidMessageException


import settings
from session import ChatSessionsManager
from message import MessageFactory, MessageEncoder
from message_handlers.base import MessageHandlerException
from message_handlers.manager import MessageHandlerManager
from replication import ReplicationException, GreenletPoolReplicator
from persistence import GreenletPoolPersister

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
        

        self.chat_sessions_manager =  ChatSessionsManager(self)
        self.message_handler_manager = MessageHandlerManager(self)
        self.deferred_init = False
        self.log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))
        
        #defer instantiation of the following until start()
        #since they require a fully initialized Service.
        self.service_info = None
        self.server_endpoint = None
        self.hashring = None
        self.replicator = None
        self.persister = None

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
                    chat_sessions_manager=self.chat_sessions_manager,
                    size=settings.REPLICATION_POOL_SIZE,
                    N=settings.REPLICATION_N,
                    W=settings.REPLICATION_W,
                    max_connections_per_service=settings.REPLICATION_MAX_CONNECTIONS_PER_SERVICE,
                    allow_same_host_replications=settings.REPLICATION_ALLOW_SAME_HOST)
            
            self.persister = GreenletPoolPersister(
                    service_handler=self,
                    chat_sessions_manager=self.chat_sessions_manager,
                    size=4)

            self.deferred_init = True


    def _is_remote_node(self, node):
        """Check if the given hashring node is remote.

        Args:
            node: ServiceHashringNode object
        Returns:
            True if node is not this node, False otherwise.
        """
        return node.service_info.key != self.service_info.key

    def _primary_node(self, chat_session_token):
        """Get the primary node for the chat_session_token.

        Returns:
            ServiceHashringNode responsible for the chat session,
            or None if no nodes are available.
        """
        result = None
        preference_list = self.hashring.preference_list(chat_session_token)
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


    def start(self):
        """Start handler."""
        self._deferred_init()

        super(ChatServiceHandler, self).start()
        self.persister.start()
        self.replicator.start()
        self.hashring.start()
    
    def stop(self):
        """Stop handler."""
        #Stop the hashring which will remove all of our positions.
        #Wait for the hashring to be stopped before stopping our parent,
        #since this will stop the zookeeper client which is required
        #to stop the hashring.
        self.hashring.stop()
        self.hashring.join()
        self.replicator.stop()
        self.persister.stop()

        super(ChatServiceHandler, self).stop()

        #Trigger messages which will cause all open
        #getMessage requests to return.
        self.chat_sessions_manager.trigger_messages()

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
                self.hashring,
                self.replicator,
                self.persister,
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

    def getPreferenceList(self, requestContext, chatSessionToken):
        """Return a preference list of HashringNode's for chatSessionToken.
        
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
            chatSessionToken: chat session token
        Returns:
            Ordered list of HashringNode's.
        """
        merge_nodes = not settings.REPLICATION_ALLOW_SAME_HOST
        preference_list = self.hashring.preference_list(
                chatSessionToken,
                merge_nodes=merge_nodes)
        return self._convert_hashring_nodes(preference_list)

    def getMessages(self, requestContext, chatSessionToken, asOf, block, timeout):
        """Long poll for new chat messages.

        Args:
            requestContext: RequestContext object.
            chatSessionToken: chat session token
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
        primary_node = self._primary_node(chatSessionToken)
        if primary_node is None:
            raise UnavailableException("no nodes available")

        if self._is_remote_node(primary_node):
            proxy = self._service_proxy(primary_node)
            return proxy.getMessages(requestContext, chatSessionToken, asOf, block, timeout)
        
        try:
            chat_session = self.chat_sessions_manager.get(chatSessionToken)
            messages = chat_session.get_messages(asOf, block, timeout)
            return messages
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
        primary_node = self._primary_node(message.header.chatSessionToken)
        if primary_node is None:
            raise UnavailableException("no nodes available")

        if self._is_remote_node(primary_node):
            proxy = self._service_proxy(primary_node)
            return proxy.sendMessage(requestContext, message, N, W)

        try:
            chat_session = self.chat_sessions_manager.get(message.header.chatSessionToken)
            self.message_handler_manager.handle(requestContext, chat_session, message)
            chat_session.send_message(message)
            async_result = self.replicator.replicate(chat_session, [message], N, W)
            async_result.get(block=True, timeout=settings.REPLICATION_TIMEOUT)
            self.persister.persist(chat_session, [message])
            return message
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

    def replicate(self, requestContext, replicationSnapshot):
        """Store a replication snapshot from another node.

        Args:
            requestContext: RequestContext object
            replicationSnapshot: ReplicationSnapshot object
        """
        chat_session_snapshot = replicationSnapshot.chatSessionSnapshot
        chat_session = self.chat_sessions_manager.get(chat_session_snapshot.token)

        if chat_session_snapshot.startTimestamp > 0:
            chat_session.start = tz.timestamp_to_utc(chat_session_snapshot.startTimestamp)

        if chat_session_snapshot.endTimestamp > 0:
            chat_session.end = tz.timestamp_to_utc(chat_session_snapshot.endTimestamp)

        chat_session.persisted = chat_session_snapshot.persisted

        for message in chat_session_snapshot.messages:
            chat_session.store_replicated_message(message)

    def expireZookeeperSession(self, requestContext, timeout):
        result = False
        if settings.ENV == "default" or \
                settings.ENV == "test":
            result = expire_zookeeper_client_session(self.zookeeper_client, timeout)
        return result


class ChatMongrel2Handler(GMongrel2Handler):
    """Chat mongrel2 handler."""

    URL_HANDLERS = [
        (r'^/chat/messages$', 'handle_get_chat_messages'),
        (r'^/chat/message$', 'handle_post_chat_message'),
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

        self.message_factory = MessageFactory()

        self.riak_client_factory = RiakClientFactory(
                host=settings.RIAK_HOST,
                port=settings.RIAK_PORT,
                transport_class=riak_gevent.RiakPbcTransport)

        self.session_store_pool = RiakSessionStorePool(
                self.riak_client_factory,
                settings.RIAK_SESSION_BUCKET,
                settings.RIAK_SESSION_POOL_SIZE,
                queue_class=gevent.queue.Queue)
        
    def _handle_message(self, request, session):
        """Message helper..

        Args:
            request: Mongrel SafeRequest object.
            session: Session object.
        Returns:
            (request_context, chat_session_token) tuple.
        """
        session_data = session.get_data()

        #Use the user_id in the "chat_session", since
        #session.user_id() will not be set in the case
        #of anonymous user.
        #user_id = session.user_id()
        user_id = session_data["chat_session"]["user_id"]
        chat_session_token = session_data["chat_session"]["chat_session_token"]

        request_context = RequestContext(userId = user_id, sessionId = session.get_key())

        return (request_context, chat_session_token)

    def handle_disconnect(self, request):
        """Mongrel connection disconnect handler."""
        pass

    @session_required
    def handle_get_chat_messages(self, request, session):
        """Get chat messages handler.

        Args:
            request: Mongrel SafeRequest object
            session: Session object
        Returns:
            JsonResponse object.
        Raises:
            HttpError
        """
        request_context, chat_session_token = self._handle_message(request, session)
        asOf = float(request.param("asOf"))

        try:
            messages =  self.service_handler.getMessages(
                    request_context,
                    chat_session_token,
                    asOf,
                    block=True,
                    timeout=settings.CHAT_LONG_POLL_WAIT)
            response = self.JsonResponse(data=json.dumps(messages, cls=MessageEncoder))
            return response
        except UnavailableException as error:
            self.log.error("get failed: %s" % error.fault)
            raise HttpError(503, "service unavailable")
        except Exception as error:
            self.log.exception(error)
            raise HttpError(500, "internal error")


    @session_required
    def handle_post_chat_message(self, request, session):
        """Send chat messages handler.

        Args:
            request: Mongrel SafeRequest object
            session: Session object
        Returns:
            JsonResponse object.
        Raises:
            HttpError
        """
        request_context, chat_session_token = self._handle_message(request, session)

        header = request.data().get("header")
        header["userId"] = request_context.userId
        header["chatSessionToken"] = chat_session_token
        msg = request.data().get("msg")

        message = self.message_factory.create(header, msg)

        try:
            response = self.service_handler.sendMessage(
                    request_context,
                    message,
                    settings.REPLICATION_N,
                    settings.REPLICATION_W)
            result = self.JsonResponse(data=json.dumps(response, cls=MessageEncoder))
            return result
        except InvalidMessageException as error:
            self.log.error("post failed: %s" % error.fault)
            raise HttpError(400, "invalid message")
        except UnavailableException as error:
            self.log.error("post failed: %s" % error.fault)
            raise HttpError(503, "service unavailable")
        except Exception as error:
            self.log.exception(error)
            raise HttpError(500, "internal error")
