#!/usr/bin/env python

from gevent.event import Event

class ChatSession(object):
    def __init__(self, chat_session_token):
        self.id = chat_session_token
        self.event = Event()
        self.messages = []

    def getMessages(self, asOf=None):
        if asOf:
            return [m for m in self.messages if m.timestamp > asOf]
        else:
            return self.messages
    
    def waitMessage(self, timeout=None):
        self.event.wait(timeout)
    
    def sendMessage(self, message):
        self.messages.append(message)
        self.event.set()
        self.event.clear()

