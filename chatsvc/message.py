#!/usr/bin/env python

import json
import time
import uuid

class Message(object):
    def __init__(self, type, chat_session_token, user_id,  id=None, timestamp=None):
        self.type = type
        self.chat_session_token = chat_session_token
        self.user_id = user_id
        self.id = id or uuid.uuid4().hex
        self.timestamp = timestamp or time.time()
    
    def json(self):
        return {
            "header": {
                "id": self.id,
                "type": self.type,
                "chatSessionToken": self.chat_session_token,
                "userId": self.user_id,
                "timestamp": self.timestamp
                }, 
            "msg": None
            }

class TagMessage(Message):
    def __init__(self, chat_session_token, user_id, name):
        self.name = name
        super(TagMessage, self).__init__("tag", chat_session_token, user_id)
    
    def json(self):
        result = super(TagMessage, self).json()
        result["msg"] = {"name": self.name}
        return result

class WhiteboardMessage(Message):
    def __init__(self, chat_session_token, user_id, data):
        self.data = data
        super(WhiteboardMessage, self).__init__("whiteboard", chat_session_token, user_id)
    
    def json(self):
        result = super(WhiteboardMessage, self).json()
        result["msg"] = {"data": self.data}
        return result

class MessageEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Message):
            return obj.json()
        else:
            return super(MessageEncoder, self).default(obj)
