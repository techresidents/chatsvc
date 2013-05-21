import bisect

from gevent.event import Event

from trsvcscore.db.models import Chat as ChatModel
from trchatsvc.gen.ttypes import MessageRouteType

class Chat(object):
    """Chat object.

    This class represents a chat instance, and consists of
    data included in the chat model, and additional
    data which is useful for bookkeeping and  replication.
    """

    def __init__(self, service_handler, chat_token):
        """Chat constructor.

        Args:
            service_handler: ChatServiceHandler object
            chat_token: chat token.
        """
        self.service_handler = service_handler
        self.token = chat_token

        #model attributes
        self.id = None
        self.start = None
        self.end = None

        #Chat model
        self.chat_model = None

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
        """Check if chat_model is loaded from database.

        Returns:
            True if chat_model is loaded from databae, False othrewise.
        """
        return self.loaded_event.is_set()

    @property
    def active(self):
        """Check if chat is active.

        Returns True if chat is currently
        taking place and not yet completed, False
        otherwise.
        """
        result = False
        if self.start and not self.end:
            result = True
        return result

    @property
    def started(self):
        """Check if chat is started.
        
        Returns:
            True if chat is started, False otherwise.
        """
        if self.start:
            return True
        else:
            return False

    @property
    def completed(self):
        """Check if chat is completed.

        Returns:
            True if chat is completed, False otherwise.
        """
        if self.end:
            return True
        else:
            return False

    def load(self):
        """Load chat model from database.

        Raises:
            Exception (sqlalchemy)
        """
        if not self.loaded_event.is_set():
            try:
                session = self.service_handler.get_database_session()
                self.chat_model = session.query(ChatModel)\
                        .filter_by(token=self.token)\
                        .one()
                self.id = self.chat_model.id
                
                #Do not commit the session, so that
                #that the chat model will be properly
                #detached when session.close() is called.
                #This will allow us to access chat model
                #attributes without database accesses.
                #session.commit()

            except:
                self.loaded_event.set()
                self.loaded_event.clear()
                raise

            finally:
                session.close()
            
            print 'load success'
            self.loaded_event.set()

    def save(self):
        """Save chat to the database.

        Updates chat start / end times.

        Raises:
            Exception (sqlalchemy)
        """
        try:
            session = self.service_handler.get_database_session()
            session.query(ChatModel) \
                    .update({
                        "start": self.start,
                        "end": self.end
                    }).where(ChatModel.id == self.chat_model.id)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def wait_load(self, timeout=None):
        """Wait for chat model load to complete.

        Args:
            timeout: timeout in seconds.
        Returns:
            True if session is loaded, False otherwise.
        """
        print 'wait load'
        self.loaded_event.wait(timeout)
        print 'wait load done'
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
        """Send new messages to the chat.
        
        Adds new messages to the chat and
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
        """Store replicate message in chat.
        
        This is equivalent to send_message() except
        the messages event will not be triggered.

        Args:
            messages: list of Message object.
        """
        for message in messages:
            if message.header.id not in self.message_history:
                self._store_message(message)


class ChatManager(object):
    """Chat anager.

    Convenience manager for accessing, loading, and removing chats.
    """

    def __init__(self, service_handler):
        """ChatManager constructor.
        
        Args:
            service_handler: ChatServiceHandler object.
        """
        self.service_handler = service_handler

        #dict of {chat_token: Chat object}
        self._chats = {}

    def all(self):
        """Get dict of all chats.

        Returns:
            dict of {chat_token: Chat object}
        """
        return self._chats
    
    def get(self, chat_token):
        """Get chat for the given chat token.

        Args:
            chat_token: chat token

        Returns:
            loaded Chat object.
        """
        if chat_token not in self._chats:
            chat = Chat(self.service_handler, chat_token)
            self._chats[chat_token] = chat
            try:
                chat.load()
            except:
                self.remove(chat_token)
                raise KeyError(chat_token)

        chat = self._chats[chat_token]
        if not chat.loaded:
            chat.wait_load()

        return chat

    def remove(self, chat_token):
        """Remove chat for the given chat token."""
        if chat_token in self._chats:
            del self._chats[chat_token]
    
    def trigger_messages(self, chat_token=None):
        """Trigger chat messages event.

        Args:
            chat_token: optional chat token
                for which to trigger the messages event.
                If None, messages event will be triggered
                for all chats.
        """
        if chat_token:
            self.get(chat_token).trigger_messages()
        else:
            for chat in self.all().itervalues():
                chat.trigger_messages()
