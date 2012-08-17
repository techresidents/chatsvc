import bisect

from gevent.event import Event
from sqlalchemy.orm import joinedload

from trpycore.timezone import tz
from trsvcscore.db.models import ChatSession as ChatSessionModel, ChatPersistJob
from trchatsvc.gen.ttypes import MessageRouteType

class ChatSession(object):
    """Chat session class.

    This class represents a chat session, and consists of
    data included in the chat session model, and additional
    data (persisted flag, etc...) which is useful for
    replication.
    """

    def __init__(self, service_handler, chat_session_token):
        """ChatSession constructor.

        Args:
            service_handler: ChatServiceHandler object
            chat_session_token: chat session token.
        """
        self.service_handler = service_handler
        self.token = chat_session_token

        #model attributes
        self.id = None
        self.start = None
        self.end = None

        #Chat model
        self.chat = None

        #additional attributes
        self.persisted = False
        
        #loaded event which will be triggered
        #when the chat session is successfully
        #loaded from the database.
        self.loaded_event = Event()

        #message event which will be triggered
        #when a new message is added to the 
        #chat session.
        self.message_event = Event()
        
        #chat session Message objects
        self.messages = []

        #sorted list of Message object timestamps
        #to allow for binary search by message
        #timestamp. Note that the message indexes in
        #self.message_timestamps correlate exactly
        #with the indexes of self.messages.
        self.message_timestamps = []
        
        #dict of {message_id: message} to prevent
        #the addition of duplicate messages.
        self.message_history = {}

    def _store_message(self, message):
        """Helper method to store message in session.

        Args:
            message: Message object.
        """
        if message.header.id not in self.message_history:
            index = bisect.bisect(self.message_timestamps, message.header.timestamp)
            self.message_history[message.header.id] = message
            self.message_timestamps.insert(index, message.header.timestamp)
            self.messages.insert(index, message)
    
    def _filter_messages(self, messages, user_id=None):
        """Helper method to filter messages.

        Args:
            messages: list of Message objects to filter
            user_id: optional user_id to filter messages for.
        """
        result = []
        for message in messages or []:
            if message.header.route.type == MessageRouteType.NO_ROUTE:
                continue
            elif message.header.route.type == MessageRouteType.TARGETED_ROUTE:
                if user_id and user_id not in message.header.route.recpiients:
                    continue
            result.append(message)
        return result

    @property
    def loaded(self):
        """Check if chat session is loaded from database.

        Returns:
            True if chat session is loaded from databae, False othrewise.
        """
        return self.loaded_event.is_set()

    @property
    def active(self):
        """Check if chat session is active.

        Returns True if chat session is currently
        taking place and not yet completed, False
        otherwise.
        """
        
        if self.loaded:
            now = tz.utcnow()
            return now > self.chat.start \
                    and now < self.chat.end \
                    and not self.completed
        else:
            return False

    @property
    def started(self):
        """Check if chat session is started.
        
        Returns:
            True if chat session is started (STARTED_MARKER received),
            False otherwise.
        """
        if self.start:
            return tz.utcnow() > self.start
        else:
            return False

    @property
    def completed(self):
        """Check if chat session is completed.

        Returns:
            True if chat session is completed (ENDED_MARKER received),
            False otherwise.
        """
        if self.end:
            return tz.utcnow() > self.end
        else:
            return False

    @property
    def expired(self):
        """Check if chat session is expired.

        Returns:
            True if chat session is expired (past chat end time),
            False otherwise.
        """
        if self.loaded:
            return tz.utcnow() > self.chat.end
        else:
            return True

    def load(self):
        """Load chat session from database.

        Raises:
            Exception (sqlalchemy)
        """
        if not self.loaded_event.is_set():
            try:
                session = self.service_handler.get_database_session()
                model = session.query(ChatSessionModel)\
                        .options(joinedload("chat"))\
                        .filter_by(token=self.token)\
                        .one()

                self.id = model.id
                self.start = model.start
                self.end = model.end
                self.chat = model.chat
                
                job = session.query(ChatPersistJob)\
                        .filter_by(chat_session_id=model.id)\
                        .first()
                
                if job is not None:
                    self.persisted = True
                else:
                    self.persisted = False
                
                #Do not commit the session, so that
                #that the chat model will be properly
                #detached when session.close() is called.
                #This will allow us to access chat model
                #attributes without database accesses.
                #session.commit()

            except Exception:
                raise

            finally:
                session.close()

            self.loaded_event.set()

    def save(self):
        """Save chat session to the database.

        Updates chat session start / end times.

        Raises:
            Exception (sqlalchemy)
        """
        try:
            session = self.service_handler.get_database_session()
            model = session.query(ChatSessionModel)\
                    .options(joinedload("chat"))\
                    .filter_by(token=self.token)\
                    .one()
            model.start = self.start
            model.end = self.end
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def wait_load(self, timeout=None):
        """Wait for chat session load to complete.

        Args:
            timeout: timeout in seconds.
        Returns:
            True if session is loaded, False otherwise.
        """
        self.loaded_event.wait(timeout)
        return self.loaded_event.is_set()

    def trigger_messages(self):
        """Trigger messages event.

        This is useful in the event that a service is shutting down,
        and following the removal of its nodes from the hashring,
        wants to respond to all active long polling requests.
        """
        self.message_event.set()
        self.message_event.clear()

    def get_messages(self, asOf=None, block=False, timeout=None, user_id=None):
        """Get messages from chat_session.
        
        Args:
            asOf: timestamp boundary for which messages should
                be returned. If a message's timestamp is greater
                than asOf it will be included in the result.
            block: optional flag indicating that the method
                should block and wait for new messages if
                no messages are currently available.
            timeout: optional timeout in seconds for blocking requests.
            user_id: optional user_id for which to filter messages.
                If this is None, messages will not be filtered
                and include ALL messages.
        Returns:
            list of Message objects
        """
        #TODO improve performance
        #The below is not optimal since a single messages event is used for all waiters.
        #This means that if a message arrivals for a single user, all users in this
        #session will be awoken, and messages will be returned. For users not
        #receiving the messages an empty list will be returned. At some point, the
        #single event should be replaced with a seperate event for each user_id
        #and one event for waiters will no user_id.
        #This change will also require changing to trigger_messages().

        messages = []
        if asOf is not None:
            index = bisect.bisect(self.message_timestamps, asOf)
            messages = self.messages[index:]
            if user_id is not None:
                messages = self._filter_messages(messages, user_id)
            if not messages and block:
                self.message_event.wait(timeout)
                index = bisect.bisect(self.message_timestamps, asOf)
                messages = self.messages[index:]
                if user_id is not None:
                    messages = self._filter_messages(messages, user_id)
        else:
            messages = self.self.messages
            if user_id is not None:
                messages = self._filter_messages(messages, user_id)
        
        return messages

    def send_messages(self, messages):
        """Send new messages to the chat session.
        
        Adds new messages to the chat session and
        triggers the messages event to notify
        waiters in get_messages().
        Args:
            messages: list of Message objects.
        """
        for message in messages:
            if message.header.id not in self.message_history:
                self._store_message(message)
        self.trigger_messages()

    def store_replicated_messages(self, messages):
        """Store replicate message in chat session.
        
        This is equivalent to send_message() except
        the messages event will not be triggered.

        Args:
            messages: list of Message object.
        """
        for message in messages:
            if message.header.id not in self.message_history:
                self._store_message(message)


class ChatSessionsManager(object):
    """Chat sessions manager.

    Convenience manager for accessing, loading, and removing
    chat sessions.
    """

    def __init__(self, service_handler):
        """ChatSessionsManager constructor.
        
        Args:
            service_handler: ChatServiceHandler object.
        """
        self.service_handler = service_handler

        #dict of {chat_session_token: ChatSession object}
        self._chat_sessions = {}

    def all(self):
        """Get dict of all chat sessions.

        Returns:
            dict of {chat_sesison_token: ChatSession object}
        """
        return self._chat_sessions
    
    def get(self, chat_session_token):
        """Get chat session for the given chat session token.

        Args:
            chat_session_token: chat session token

        Returns:
            loaded ChatSession object.
        """
        if chat_session_token not in self._chat_sessions:
            chat_session = ChatSession(self.service_handler, chat_session_token)
            self._chat_sessions[chat_session_token] = chat_session
            chat_session.load()

        chat_session = self._chat_sessions[chat_session_token]
        if not chat_session.loaded:
            chat_session.wait_load()

        return chat_session

    def remove(self, chat_session_token):
        """Remove chat for the given chat session token."""
        if chat_session_token in self._chat_sessions:
            del self._chat_sessions[chat_session_token]
    
    def trigger_messages(self, chat_session_token=None):
        """Trigger chat session messages event.

        Args:
            chat_session_token: optional chat session token
                for which to trigger the messages event.
                If None, messages event will be triggered
                for all chat sessions.
        """
        if chat_session_token:
            self.get(chat_session_token).trigger_messages()
        else:
            for chat_session in self.all().itervalues():
                chat_session.trigger_messages()
