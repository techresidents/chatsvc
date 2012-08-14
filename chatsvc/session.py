import bisect

from gevent.event import Event
from sqlalchemy.orm import joinedload

from trpycore.timezone import tz
from trsvcscore.models import ChatSession as ChatSessionModel

class ChatSession(object):
    """Chat session class."""

    def __init__(self, service_handler, chat_session_token):
        self.service_handler = service_handler
        self.token = chat_session_token
        self.id = None
        self.start = None
        self.end = None
        self.chat = None
        self.persisted = False

        self.loaded_event = Event()
        self.message_event = Event()

        self.messages = []
        self.message_timestamps = []
        self.message_history = {}

    def _store_message(self, message):
        if message.header.id not in self.message_history:
            index = bisect.bisect(self.message_timestamps, message.header.timestamp)
            self.message_history[message.header.id] = message
            self.message_timestamps.insert(index, message.header.timestamp)
            self.messages.insert(index, message)
    
    @property
    def loaded(self):
        return self.loaded_event.is_set()

    @property
    def started(self):
        if self.start:
            return tz.utcnow() > self.start
        else:
            return False

    @property
    def active(self):
        return not self.expired and not self.completed

    @property
    def completed(self):
        if self.end:
            return tz.utcnow() > self.end
        else:
            return False

    @property
    def expired(self):
        if self.loaded:
            return tz.utcnow() > self.chat.end
        else:
            return True

    def load(self):
        if not self.loaded_event.is_set():
            session = self.service_handler.get_database_session()
            model = session.query(ChatSessionModel)\
                    .options(joinedload("chat"))\
                    .filter_by(token=self.token)\
                    .one()
            
            self.id = model.id
            self.start = model.start
            self.end = model.end
            self.chat = model.chat
            self.loaded_event.set()
            session.commit()

    def save(self):
        session = self.service_handler.get_database_session()
        model = session.query(ChatSessionModel)\
                .options(joinedload("chat"))\
                .filter_by(token=self.token)\
                .one()
        model.start = self.start
        model.end = self.end
        session.commit()
    
    def wait_load(self, timeout=None):
        self.loaded_event.wait(timeout)

    def trigger_messages(self):
        self.message_event.set()
        self.message_event.clear()

    def get_messages(self, asOf=None, block=False, timeout=None):
        if asOf is not None:
            index = bisect.bisect(self.message_timestamps, asOf)
            messages = self.messages[index:]
            if not messages and block:
                self.message_event.wait(timeout)
                messages = [m for m in self.messages if m.header.timestamp > asOf]
            return messages
        else:
            return self.messages
    
    def send_message(self, message):
        if message.header.id not in self.message_history:
            self._store_message(message)
            self.trigger_messages()

    def store_replicated_message(self, message):
        if message.header.id not in self.message_history:
            self._store_message(message)


class ChatSessionsManager(object):
    """Chat sessions manager."""

    def __init__(self, service_handler):
        self.service_handler = service_handler
        self._chat_sessions = {}

    def all(self):
        return self._chat_sessions
    
    def get(self, chat_session_token):
        if chat_session_token not in self._chat_sessions:
            chat_session = ChatSession(self.service_handler, chat_session_token)
            self._chat_sessions[chat_session_token] = chat_session
            chat_session.load()

        chat_session = self._chat_sessions[chat_session_token]
        if not chat_session.loaded:
            chat_session.wait_load()

        return chat_session

    def remove(self, chat_session_token):
        if chat_session_token in self._chat_sessions:
            del self._chat_sessions[chat_session_token]
    
    def trigger_messages(self, chat_session_token=None):
        if chat_session_token:
            self.get(chat_session_token).trigger_messages()
        else:
            for chat_session in self.all().itervalues():
                chat_session.trigger_messages()
