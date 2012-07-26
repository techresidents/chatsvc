import bisect

from gevent.event import Event

class ChatSession(object):
    def __init__(self, chat_session_token):
        self.id = chat_session_token
        self.token = chat_session_token
        self.event = Event()
        self.messages = []
        self.message_timestamps = []
        self.message_history = {}
    
    def _store_message(self, message):
        if message.header.id not in self.message_history:
            index = bisect.bisect(self.message_timestamps, message.header.timestamp)
            self.message_history[message.header.id] = message
            self.message_timestamps.insert(index, message.header.timestamp)
            self.messages.insert(index, message)
    
    def trigger_messages(self):
        print "trigger messages"
        self.event.set()
        self.event.clear()

    def get_messages(self, asOf=None, block=False, timeout=None):
        if asOf is not None:
            index = bisect.bisect(self.message_timestamps, asOf)
            messages = self.messages[index:]
            if not messages and block:
                self.event.wait(timeout)
                messages = [m for m in self.messages if m.header.timestamp > asOf]
            return messages
        else:
            return self.messages
    
    def send_message(self, message):
        if message.header.id not in self.message_history:
            self._store_message(message)
            print len(self.messages)
            self.trigger_messages()

    def store_replicated_message(self, message):
        if message.header.id not in self.message_history:
            self._store_message(message)
            print len(self.messages)
    


class ChatSessionsManager(object):
    def __init__(self):
        self._chat_sessions = {}

    def all(self):
        return self._chat_sessions
    
    def get(self, chat_session_token):
        if chat_session_token not in self._chat_sessions:
            self._chat_sessions[chat_session_token] = ChatSession(chat_session_token)
        return self._chat_sessions[chat_session_token]
    
    def remove(self, chat_session_token):
        if chat_session_token in self._chat_sessions:
            del self._chat_sessions[chat_session_token]
    
    def trigger_messages(self, chat_session_token=None):
        if chat_session_token:
            self.get(chat_session_token).trigger_messages()
        else:
            for chat_session in self.all().itervalues():
                chat_session.trigger_messages()
