import abc
import json
import logging

import gevent.event
import gevent.queue
from sqlalchemy.sql import func

from trchatsvc.gen.ttypes import MessageType, ChatStatus
from trsvcscore.hashring.base import ServiceHashringEvent
from trsvcscore.db.models import ChatArchiveJob

class PersistException(Exception):
    """Persist exception class."""
    pass

class PersistAsyncResult(gevent.event.AsyncResult):
    """Async replication result.
    
    This class encapsulates an async, persist result.
    """
    pass

class PersistEvent(object):
    """Persist event."""

    CHAT_PERSISTED_EVENT = "CHAT_PERSISTED_EVENT"

    def __init__(self, event_type, chat):
        self.event_type = event_type
        self.chat = chat

class Persister(object):
    """Persister abstract base class.

    Useful base class for concrete persister implementations.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(
            self,
            service,
            hashring,
            chat_manager,
            database_session_factory):
        """Persister constructor.

        Args:
            service: ChatService object
            hashring: ServiceHashring object
            chat_manager: ChatManager object
            database_session_factory: sqlalchemy database session
            factory method.
        """
        self.service = service
        self.hashring = hashring
        self.chat_manager = chat_manager
        self.database_session_factory = database_session_factory

        #add hashring observer
        self.hashring.add_observer(self._hashring_observer)
    
    @abc.abstractmethod
    def start(self):
        """Start persister."""
        return

    @abc.abstractmethod
    def stop(self):
        """Stop persister."""
        return

    @abc.abstractmethod
    def join(self, timeout=None):
        """Join persister.

        Join persister waiting for the completion of all threads
        or greenlets.

        Args:
            timeout: optional maximum number of seconds to wait for the completion
                of all threads or greenlets.
        """
        return
    
    @abc.abstractmethod
    def persist(self, chat, messages=None, all=False, zombie=False):
        """Persist messages for the given chat.

        Args:
            chat: Chat object
            messages: optional list of Message objects to persist
            all: optional flag which can be used as an alternative
                to passing in 'messages', which when True indicates
                that all messages in the chat which need
                persisting, should be persisted.
            zombie: optional flag which indicates that the given
                chat is a zombie (expired but not completed)
                and that chat should be persisted.
        
        Returns:
            PersistAsyncResult object.
        """
        return

    @abc.abstractmethod
    def add_observer(self, observer):
        """Add persister observer.

        Add a method which will be invoked with a PersistEvent object,
        upon persister related events.
        Args:
            observer: method to be invoked with PersistEvent object
        """
        return

    @abc.abstractmethod
    def remove_observer(self, observer):
        """Remove persister observer.

        Args:
            observer: method to be invoked with PersistEvent object
        """
        return

    def _get_database_session(self):
        """Get a new sqlalchemy database session.

        Returns:
            New sqlalchemy database session.
        """
        return self.database_session_factory()

    def _hashring_observer(self, hashring, event):
        """Observer method which will be invoked upon hashring changes.
        
        This method loops through all chat, upon a change
        to the hashring, and initiates a full persist for chat
        which it is taking eover. This ensures that
        messages which may have been in the memory of the previously
        responsible service, but not yet persisted, will be 
        persisted.

        Args:
            hashring: ServiceHashring object
            event: ServiceHashringEvent object
        """
        if event.event_type == ServiceHashringEvent.CHANGED_EVENT:
            service_info = self.service.info()

            #Loop through all of the chat and initiate 
            #a full persist for all chat which we are
            #taking over, since they may have add messages
            #in memory which were not permitted.
            for chat_token, chat in self.chat_manager.all().items():
                previous_preference_list = self.hashring.preference_list(
                        chat_token,
                        event.previous_hashring)
                current_preference_list = self.hashring.preference_list(
                        chat_token,
                        event.current_hashring)

                if previous_preference_list and current_preference_list:
                    previous_primary = previous_preference_list[0]
                    current_primary = current_preference_list[0]
                    if current_primary.service_info.key == service_info.key and \
                       previous_primary.service_info.key != service_info.key:
                           self.persist(chat, None, all=True)


class GreenletPoolPersister(Persister):
    """Persister which delegations replications to a pool of greenlets."""

    STOP_ITEM = object()

    class PersistItem(object):
        """Persist item class.

        Represents persist work which needs to be done.
        """
        def __init__(self, chat, messages, all, zombie, result):
            """PersistItem constructor.

            Args:
                chat: Chat object
                messages: optional list of Message objects to persist
                all: flag which can be used as an alternative
                    to passing in 'messages', which when True indicates
                    that all messages in the chat which need
                    persisting, should be persisted.
                zombie: flag which indicates that the given
                    chat is a zombie (expired but not completed)
                    and that chat should be persisted.
            """
            self.chat = chat
            self.messages = messages
            self.all = all
            self.zombie = zombie
            self.result = result

    def __init__(
        self,
        service,
        hashring,
        chat_manager,
        database_session_factory,
        size,
        max_queue_size=100):
        """GreenletPoolPersister constructor.

        Args:
            service: ChatService object
            hashring: ServiceHashring object
            chat_manager: ChatManager object
            database_session_factory: sqlalchemy database session
                factory method.
            size: number of greenlets in pool
            max_queue_size: maximum number of persist work items
                to allow in the queue before additional attempts
                will block.
        """
        super(GreenletPoolPersister, self).__init__(
            service,
            hashring,
            chat_manager,
            database_session_factory)
        self.size = size
        self.queue = gevent.queue.Queue(max_queue_size)
        self.observers = []
        self.workers = []
        self.running = False
        self.log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))

    def _notify_observers(self, event):
        """Notify persist observers of event.

        Args:
            event: PersitEvent object
        """
        for observer in self.observers:
            try:
                observer(event)
            except Exception as error:
                self.log.error("persist observer exception.")
                self.log.exception(error)
    
    def _is_chat_ended_message(self, message):
        """Check if message indicates that chat has ended.

        Args:
            message: Message object
        """
        result = False
        if message.header.type == MessageType.CHAT_STATUS:
            msg = message.chatStatusMessage
            if msg.status == ChatStatus.ENDED:
                result = True
        return result
    
    def _persist_item(self, item):
        """Persist item to database.
        
        This method does the bulk of the persist work, and will
        write all Message objects to the database and also create
        a ChatPersistJob if an ENDED_MARKER message is received
        or the zombie flag is set to True.

        Args:
            item: PersistItem object
        Raises:
            Exception (SQLAlchemy)
        """
        try:
            chat_persisted = False
            chat = item.chat
            session = self._get_database_session()

            if item.all:
                #TODO fix this if we persist messages in the future
                #messages = self._get_unpersisted_messages(chat)
                pass
            else:
                messages = item.messages or []

            for message in messages:
                #no longer persisting chat messages.
                #this may change in the future.

                if self._is_chat_ended_message(message) and\
                   not chat.state.persisted:
                    self._persist_ended_chat(chat, session)
                    chat_persisted = True
            
            if item.zombie and not chat.state.persisted:
                self._persist_ended_chat(chat, session)
                chat_persisted = True

            session.commit()
            item.result.set(None)
            
            if chat_persisted:
                event = PersistEvent(
                        PersistEvent.CHAT_PERSISTED_EVENT,
                        chat)
                self._notify_observers(event)
            
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.debug("Done persisting %s message(s)" % len(messages))

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def _persist_ended_chat(self, chat, session):
        """Finish peristing ended chat.

        Args:
            chat: Chat object
            session: SQLAlchemy Sesison object
        """
        chat.state.persisted = True
        
        #convert chat session to pure json
        data = dict(chat.state.session);
        if "twilio_data" in data:
            data["twilio_data"] = json.loads(data["twilio_data"])
        data = json.dumps(data)

        #create chat archive job
        job = ChatArchiveJob(
               chat_id=chat.id,
               created=func.current_timestamp(),
               not_before=func.current_timestamp(),
               data=data,
               retries_remaining=4)

        session.add(job)

    def add_observer(self, observer):
        """Add persister observer.

        Add a method which will be invoked with a PersistEvent object,
        upon persister related events.
        Args:
            observer: method to be invoked with PersistEvent object
        """
        self.observers.append(observer)

    def remove_observer(self, observer):
        """Remove persister observer.

        Args:
            observer: method to be invoked with PersistEvent object
        """
        self.observers.remove(observer)

    def start(self):
        """Start persister."""
        if not self.running:
            self.log.info("Starting %s ..." % self.__class__.__name__)
            self.running = True
            for i in range(0, self.size):
                worker = gevent.spawn(self.run)
                self.workers.append(worker)

    def run(self):
        """Run persister."""
        while self.running:
            try:
                item = self.queue.get()
                if item is self.STOP_ITEM:
                    break
                
                self._persist_item(item)
                
            except Exception as error:
                self.log.exception(error)
                item.result.set_exception(PersistException(str(error)))


    def stop(self):
        """Stop persister."""
        if self.running:
            self.log.info("Stopping %s ..." % self.__class__.__name__)
            self.running = False
            for i in range(0, self.size):
                self.queue.put(self.STOP_ITEM)

    def join(self, timeout=None):
        """Join persister.

        Join persister waiting for the completion of all threads
        or greenlets.

        Args:
            timeout: optional maximum number of seconds to wait for the completion
                of all threads or greenlets.
        """
        gevent.joinall(self.workers, timeout)

    def persist(self, chat, messages, all=False, zombie=False):
        """Persist messages for the given chat.

        Args:
            chat: Chat object
            messages: optional list of Message objects to persist
            all: optional flag which can be used as an alternative
                to passing in 'messages', which when True indicates
                that all messages in the chat which need
                persisting, should be persisted.
            zombie: optional flag which indicates that the given
                chat is a zombie (expired but not completed)
                and that a ChatPersistJob should be created.
        
        Returns:
            PersistAsyncResult object.
        """
        item = self.PersistItem(
                chat=chat,
                messages=messages,
                all=all,
                zombie=zombie,
                result=PersistAsyncResult())
        self.queue.put(item)
        return item.result
