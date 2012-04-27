import json
import logging
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.insert(0, PROJECT_ROOT)

import gevent.queue

from trpycore import riak_gevent
from trpycore.riak_common.factory import RiakClientFactory
from trsvcscore.mongrel2.decorator import session_required
from trsvcscore.service_gevent.handler import GMongrel2Handler
from trsvcscore.session.riak import RiakSessionStorePool
from trsvcscore.hashring.zookeeper import ZookeeperServiceHashring
from tridlcore.gen.ttypes import RequestContext
from trchatsvc.gen import TChatService

import version
import settings
from session import ChatSession
from message import MessageFactory, MessageEncoder

URL_HANDLERS = [
    (r'^/chat/messages$', 'handle_get_chat_messages'),
    (r'^/chat/message/minute$', 'handle_minute_create'),
    (r'^/chat/message/minute/(?P<minute_id>\w+)$', 'handle_minute_update'),
    (r'^/chat/message/tag$', 'handle_tag_create'),
    (r'^/chat/message/tag/(?P<tag_id>\w+)$', 'handle_tag_delete'),
    (r'^/chat/message/whiteboard$', 'handle_whiteboard_create'),
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
                num_positions=3,
                data=None)

        self.chat_sessions = {}
    

    def start(self):
        super(ChatServiceHandler, self).start()
        self.hashring.start()
    
    def stop(self):
        self.hashring.stop()
        super(ChatServiceHandler, self).stop()
    
    def _handle_message(self, request, session):
        session_data = session.get_data()
        user_id = session_data["_auth_user_id"]
        chat_session_token = session_data["chat_session"]["chat_session_token"]

        request_context = RequestContext(userId = user_id, sessionId = session.get_key())

        return (request_context, chat_session_token)

    def _get_chat_session(self, chat_session_token):
        if chat_session_token not in self.chat_sessions:
            self.chat_sessions[chat_session_token] = ChatSession(chat_session_token)
        return self.chat_sessions[chat_session_token]
    
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
        return json.dumps(messages, cls=MessageEncoder)

    @session_required
    def handle_minute_create(self, request, session):
        request_context, chat_session_token = self._handle_message(request, session)
        topic_id = request.param("topicId")
        message = self.message_factory.minute_create_message(
                chat_session_token,
                request_context.userId,
                topic_id)
        message = self.sendMessage(request_context, message)
        return json.dumps(message, cls=MessageEncoder)

    @session_required
    def handle_minute_update(self, request, session, minute_id):
        request_context, chat_session_token = self._handle_message(request, session)
        topic_id = request.param("topicId")
        start_timestamp = request.param("startTimestamp")
        message = self.message_factory.minute_update_message(
                chat_session_token,
                request_context.userId,
                minute_id,
                topic_id,
                start_timestamp)
        message = self.sendMessage(request_context, message)
        return json.dumps(message, cls=MessageEncoder)

    @session_required
    def handle_tag_create(self, request, session):
        request_context, chat_session_token = self._handle_message(request, session)
        name = request.param("name")
        message = self.message_factory.tag_create_message(chat_session_token, request_context.userId, name)
        message = self.sendMessage(request_context, message)
        return json.dumps(message, cls=MessageEncoder)


    @session_required
    def handle_tag_delete(self, request, session, tag_id):
        request_context, chat_session_token = self._handle_message(request, session)
        message = self.message_factory.tag_delete_message(chat_session_token, request_context.userId, tag_id)
        message = self.sendMessage(request_context, message)
        return json.dumps(message, cls=MessageEncoder)

    @session_required
    def handle_whiteboard_create(self, request, session):
        request_context, chat_session_token = self._handle_message(request, session)
        name = request.param("name")
        message = self.message_factory.whiteboard_create_message(chat_session_token, request_context.userId, name)
        message = self.sendMessage(request_context, message)
        return json.dumps(message, cls=MessageEncoder)

    def getMessages(self, requestContext, chatSessionToken, asOf, block, timeout):
        chat_session = self._get_chat_session(chatSessionToken)
        messages = chat_session.get_messages(asOf, block, timeout)
        return messages

    def sendMessage(self, requestContest, message):
        chat_session = self._get_chat_session(message.header.chatSessionToken)
        chat_session.send_message(message)
        return message
