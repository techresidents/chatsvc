#!/usr/bin/env python

import cgi
import json
import sys
import urlparse

import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.insert(0, PROJECT_ROOT)
import version

import settings
from session import ChatSession
from message import MessageEncoder, TagMessage, WhiteboardMessage

class ChatServiceHandler(object):
    def __init__(self):
        self.chat_sessions = {}

    def handle(self, connection, unsafe_request):
        if unsafe_request.is_disconnect():
            return
        
        request = SafeRequest(unsafe_request)

        url = request.header("PATH")
        method = request.method()
        chat_session_token = request.param("chatSessionToken")
        user_id = int(request.param("userId"))

        if chat_session_token not in self.chat_sessions:
            self.chat_sessions[chat_session_token] = ChatSession(chat_session_token)

        chat_session = self.chat_sessions[chat_session_token]

        try:
            handler_name = "handle_%s%s" % (method, url.replace("/", "_"))
            handler = getattr(self, handler_name.lower())
            response = handler(request, chat_session, user_id)
            connection.reply_http(unsafe_request, response)
        except AttributeError as error:
            connection.reply_http(unsafe_request, "not found", code=404);
        except Exception as error:
            connection.reply_http(unsafe_request, "internal error",  code=503);

    def handle_get_chat_messages(self, request, chat_session, user_id):
        asOf = float(request.param("asOf"))
        messages = chat_session.getMessages(asOf)
        if not messages:
            chat_session.waitMessage(10)
            messages = chat_session.getMessages(asOf)
        return json.dumps(messages, cls=MessageEncoder)
    
    def handle_post_chat_message_tag(self, request, chat_session, user_id):
        name = request.param("name")
        message = TagMessage(chat_session.id, user_id, name)
        chat_session.sendMessage(message)
        return json.dumps(message, cls=MessageEncoder)

    def handle_post_chat_message_whiteboard(self, request, chat_session, user_id):
        data = request.param("data")
        message = WhiteboardMessage(chat_session.id, user_id, data)
        chat_session.sendMessage(message)
        return json.dumps(message, cls=MessageEncoder)


    def getVersion(self, context):
        return version.VERSION or "Unknown"
     
    def getBuildNumber(self, context):
        return version.BUILD or "Unknown"

class SafeRequest(object):
    def __init__(self, req):
        self.req = req
        self.url_params = None
        self.post_params = None
        
        if self.header("QUERY"):
            self.url_params = urlparse.parse_qs(self.header("QUERY"))

        if self.header("METHOD") == "POST":
            self.post_params = urlparse.parse_qs(self.req.body)
    
    def header(self, name):
        if name in self.req.headers:
            return self.req.headers[name]
        else:
            return None
    
    def param(self, name, escape=True):
        if self.method() == "GET":
            return self.url_param(name, escape)
        else:
            return self.post_param(name, escape)
    
    def url_param(self, name, escape=True):
        value = self.url_params[name][0]
        if escape:
            return self._escape(value)
        else:
            return value

    def post_param(self, name, escape=True):
        value = self.post_params[name][0]
        if escape:
            return self._escape(value)
        else:
            return value

    def method(self):
        return self.header("METHOD")

    def _escape(self, input):
        return cgi.escape(input)
