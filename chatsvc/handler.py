import json
import logging
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.insert(0, PROJECT_ROOT)

import gevent.queue

from trpycore import riak_gevent
from trpycore.greenlet.util import join
from trpycore.riak_common.factory import RiakClientFactory
from trsvcscore.mongrel2.decorator import session_required
from trsvcscore.proxy.basic import BasicServiceProxyPool
from trsvcscore.service_gevent.handler import GMongrel2Handler
from trsvcscore.session.riak import RiakSessionStorePool
from trsvcscore.hashring.zookeeper import ZookeeperServiceHashring
from tridlcore.gen.ttypes import RequestContext
from trchatsvc.gen import TChatService


import version
import settings
from session import ChatSessionsManager
from message import MessageFactory, MessageEncoder
from replication import GreenletPoolReplicator

URL_HANDLERS = [
    (r'^/chat/messages$', 'handle_get_chat_messages'),
    (r'^/chat/message$', 'handle_post_chat_message'),
]

class ChatServiceHandler(TChatService.Iface, GMongrel2Handler):
    def __init__(self):
        super(ChatServiceHandler, self).__init__(
                url_handlers=URL_HANDLERS,
                name=settings.SERVICE,
                interface=settings.SERVER_INTERFACE,
                port=settings.SERVER_PORT,
                version=version.VERSION,
                build=version.BUILD,
                zookeeper_hosts=settings.ZOOKEEPER_HOSTS)

        self.chat_sessions_manager =  ChatSessionsManager()

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
        
        self.hashring = ZookeeperServiceHashring(
                zookeeper_client=self.zookeeper_client,
                service_name=settings.SERVICE,
                service_port=settings.SERVER_PORT,
                positions=[None, None, None],
                data={"hostname": "localhost"})

        self.replicator = GreenletPoolReplicator(
                self,
                size=20,
                N=2,
                W=1,
                max_connections_per_service=1)


    def _handle_message(self, request, session):
        session_data = session.get_data()
        #user_id = session.user_id()
        user_id = session_data["chat_session"]["user_id"]
        chat_session_token = session_data["chat_session"]["chat_session_token"]

        request_context = RequestContext(userId = user_id, sessionId = session.get_key())

        return (request_context, chat_session_token)

    def start(self):
        super(ChatServiceHandler, self).start()
        self.hashring.start()
        self.replicator.start()
    
    def stop(self):
        #Stop the hashring which will remove all of our positions.
        #Wait for the hashring to be stopped before stopping our parent,
        #since this will stop the zookeeper client which is required
        #to stop the hashring.
        self.hashring.stop()
        self.hashring.join()
        self.replicator.stop()

        super(ChatServiceHandler, self).stop()
        #super(ChatServiceHandler, self).join()
        self.chat_sessions_manager.trigger_messages()

    def join(self, timeout=None):
        greenlets = [self.hashring, self.replicator, super(ChatServiceHandler, self)]
        join(greenlets, timeout)
    
    def handle_disconnect(self, request):
        pass

    @session_required
    def handle_get_chat_messages(self, request, session):
        request_context, chat_session_token = self._handle_message(request, session)
        asOf = float(request.param("asOf"))
        messages =  self.getMessages(
                request_context,
                chat_session_token,
                asOf,
                block=True,
                timeout=settings.CHAT_LONG_POLL_WAIT)
        response = self.JsonResponse(data=json.dumps(messages, cls=MessageEncoder))
        return response

    @session_required
    def handle_post_chat_message(self, request, session):
        request_context, chat_session_token = self._handle_message(request, session)

        header = request.data().get("header")
        header["userId"] = request_context.userId
        header["chatSessionToken"] = chat_session_token
        msg = request.data().get("msg")

        message = self.message_factory.create(header, msg)
        response = self.sendMessage(request_context, message)
        result = self.JsonResponse(data=json.dumps(response, cls=MessageEncoder))
        return result
    
    def getHashRing(self, requestContext):
        #TODO implement
        return []

    def getPreferenceList(self, requestContext, chatSessionToken):
        #TODO implement
        return [],

    def getMessages(self, requestContext, chatSessionToken, asOf, block, timeout):
        primary_node = self._primary_node(chatSessionToken)
        if primary_node is None:
            raise RuntimeError("oops")

        if self._remote_node(primary_node):
            proxy_pool = BasicServiceProxyPool(
                    "chatsvc",
                    primary_node.data["hostname"],
                    primary_node.data["service_port"],
                    1,
                    TChatService,
                    is_gevent=True)
            with proxy_pool.get() as proxy:
                return proxy.getMessages(requestContext, chatSessionToken, asOf, block, timeout)
        else:
            chat_session = self.chat_sessions_manager.get(chatSessionToken)
            messages = chat_session.get_messages(asOf, block, timeout)
            return messages

    def sendMessage(self, requestContext, message):
        primary_node = self._primary_node(message.header.chatSessionToken)
        if primary_node is None:
            raise RuntimeError("oops")

        if self._remote_node(primary_node):
            proxy_pool = BasicServiceProxyPool(
                    "chatsvc",
                    primary_node.data["hostname"],
                    primary_node.data["service_port"],
                    1,
                    TChatService,
                    is_gevent=True)
            with proxy_pool.get() as proxy:
                return proxy.sendMessage(requestContext, message)
        else:
            chat_session = self.chat_sessions_manager.get(message.header.chatSessionToken)
            async_result = self.replicator.replicate(chat_session.token, [message])
            try:
                async_result.get(block=True, timeout=5)
                chat_session.send_message(message)
                return message
            except gevent.Timeout:
                #TODO replace with custom exception
                raise RuntimeError("oops")

    def storeReplicatedMessages(self, requestContext, chatSessionToken, messages):
        for message in messages:
            chat_session = self.chat_sessions_manager.get(chatSessionToken)
            chat_session.store_replicated_message(message)
    
    def _remote_node(self, node):
        return node.service_port != self.port

    def _primary_node(self, chat_session_token):
        result = None
        preference_list = self.hashring.preference_list(chat_session_token)
        if preference_list:
            result = preference_list[0]
        return result

    def _request_context(self):
        return RequestContext(
                userId=0,
                impersonatingUserId=0,
                sessionId="sessionid",
                context="")

