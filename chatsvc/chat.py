import bisect
import logging

from gevent.event import Event
from sqlalchemy.orm.exc import NoResultFound

from trpycore.timezone import tz
from trsvcscore.db.models import Chat as ChatModel
from trchatsvc.gen.ttypes import MessageRouteType, ChatState, ChatStatus

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
        self.log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))
        self.service_handler = service_handler
        self.state = ChatState(
                token=chat_token,
                status=ChatStatus.PENDING,
                maxDuration=0,
                maxParticipants=0,
                startTimestamp=0,
                endTimestamp=0,
                users={},
                persisted=False,
                session={},
                messages=[])
        self.token = chat_token

        #database model id
        self.id = None

        #loaded event which will be triggered
        #when the chat session is successfully
        #loaded from the database.
        self.loaded_event = Event()

        #message event which will be triggered
        #when a new message is added to the 
        #chat session.
        self.message_event = Event()
        
        #sorted list of Message object timestamps
        #to allow for binary search by message
        #timestamp. Note that the message indexes in
        #self.message_timestamps correlate exactly
        #with the indexes of self.messages.
        self.message_timestamps = []
        
        #dict of {message_id: message} to prevent
        #the addition of duplicate messages.
        self.message_history = {}
        
        #Additional number of seconds beyond max_duration
        #which a chat is allowed to proceed before it's
        #considered expired and inaccessible.
        self.expiration_threshold = 360
    
    def _store_message(self, message):
        """Helper method to store message in session.

        Args:
            message: Message object.
        """
        if message.header.id not in self.message_history:
            index = bisect.bisect(self.message_timestamps, message.header.timestamp)
            self.message_history[message.header.id] = message
            self.message_timestamps.insert(index, message.header.timestamp)
            self.state.messages.insert(index, message)
    
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
    def start(self):
        if self.state.startTimestamp == 0: 
            result = None
        else:            
            result = tz.timestamp_to_utc(self.state.startTimestamp)
        return result
    
    @property
    def end(self):
        if self.state.endTimestamp == 0: 
            result = None
        else:            
            result = tz.timestamp_to_utc(self.state.endTimestamp)
        return result
    
    @property
    def started(self):
        return self.state.startTimestamp > 0

    @property
    def ended(self):
        return self.state.endTimestamp > 0

    @property
    def completed(self):
        return self.state.endTimestamp > 0

    @property
    def expired(self):
        result = False
        start = self.state.startTimestamp
        max_duration = self.state.maxDuration

        if start and max_duration:
            now = tz.timestamp()
            result = now > start + max_duration + self.expiration_threshold
        return result    

    def load(self):
        """Load chat model from database.

        Raises:
            Exception (sqlalchemy)
        """
        if not self.loaded_event.is_set():
            try:
                session = self.service_handler.get_database_session()
                model = session.query(ChatModel)\
                        .filter_by(token=self.token)\
                        .one()
                self.id = model.id
                session.commit()
                
                self.state.maxDuration = model.max_duration
                self.state.maxParticipants = model.max_participants

                if model.end:
                    self.state.status = ChatStatus.ENDED
                    self.state.startTimestamp = tz.utc_to_timestamp(model.start)
                    self.state.endTimesamp = tz.utc_to_timestamp(model.end)
                elif model.start:
                    self.state.status = ChatStatus.STARTED
                    self.state.startTimestamp = tz.utc_to_timestamp(model.start)
            
            except NoResultFound:
                self.loaded_event.set()
                self.loaded_event.clear()
            except Exception as error:
                logging.exception(error)
                self.loaded_event.set()
                self.loaded_event.clear()
                raise

            finally:
                session.close()
                   
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
                    .filter(ChatModel.id == self.id)\
                    .update({
                        "start": self.start,
                        "end": self.end
                    })
            session.commit()
        except Exception as error:
            logging.exception(error)
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
            messages = self.state.messages[index:]
            if user_id is not None:
                messages = self._filter_messages(messages, user_id)
            if not messages and block:
                self.message_event.wait(timeout)
                index = bisect.bisect(self.message_timestamps, asOf)
                messages = self.state.messages[index:]
                if user_id is not None:
                    messages = self._filter_messages(messages, user_id)
        else:
            messages = self.self.state.messages
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
                #it's important that the message timestamp be set
                #here to ensure that messages are ordered properly.
                #if this were set in a message handler and the
                #message handler yielded due to i/o and another
                #message were processed we could get out of
                #order messages.
                message.header.timestamp = tz.timestamp()
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
