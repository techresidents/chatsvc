import json
import logging
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.insert(0, PROJECT_ROOT)

import riak
from trpycore import riak_gevent
from trsvcscore.decorators.mongrel2 import session_required
from trsvcscore.service_gevent.handler import Mongrel2Handler
from trsvcscore.session.riak import RiakSessionStore
from tridlcore.gen.ttypes import RequestContext
from trchatsvc.gen import TChatService

import version
import settings
from session import ChatSession
from message import MessageFactory, MessageEncoder


class ChatServiceHandler(TChatService.Iface, Mongrel2Handler):
    def __init__(self):
        super(ChatServiceHandler, self).__init__(
                name=settings.SERVICE,
                host=settings.SERVER_HOST,
                port=settings.SERVER_PORT,
                version=version.VERSION,
                build=version.BUILD,
                zookeeper_hosts=settings.ZOOKEEPER_HOSTS)

        self.message_factory = MessageFactory()

        self.riak_client = riak.RiakClient(
                host=settings.RIAK_HOST,
                port=settings.RIAK_PORT,
                transport_class=riak_gevent.RiakPbcTransport)

        self.session_store = RiakSessionStore(self.riak_client, settings.RIAK_SESSION_BUCKET)

        self.chat_sessions = {}
    
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
    def handle_post_chat_message_tag(self, request, session):
        request_context, chat_session_token = self._handle_message(request, session)
        name = request.param("name")
        message = self.message_factory.create_tag_message(chat_session_token, request_context.userId, name)
        message = self.sendMessage(request_context, message)
        return json.dumps(message, cls=MessageEncoder)

    @session_required
    def handle_post_chat_message_whiteboard(self, request, session):
        request_context, chat_session_token = self._handle_message(request, session)
        data = request.param("data")
        message = self.message_factory.create_whiteboard_message(chat_session_token, request_context.userId, data)
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
