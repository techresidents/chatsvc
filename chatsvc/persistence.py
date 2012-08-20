import abc
import logging

import gevent.event
import gevent.queue
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import desc

from trchatsvc.gen.ttypes import MessageType, MarkerType
from trpycore.thrift.serialization import serialize
from trpycore.timezone import tz
from trsvcscore.hashring.base import ServiceHashringEvent
from trsvcscore.db.models import ChatMessage as ChatMessageModel, ChatPersistJob

#ChatMessage database id's.
#TODO replace hard coded id's with dynamic enum.
MESSAGE_TYPE_IDS= {
    "MARKER_CREATE": 1,
    "MINUTE_CREATE": 2,
    "MINUTE_UPDATE": 3,
    "TAG_CREATE": 4,
    "TAG_DELETE": 5,
    "WHITEBOARD_CREATE": 6,
    "WHITEBOARD_DELETE": 7,
    "WHITEBOARD_CREATE_PATH": 8,
    "WHITEBOARD_DELETE_PATH": 9,
}

#ChatMessageFormatType database id's.
#TODO replace hard coded id's with dynamic enum.
MESSAGE_FORMAT_TYPE_IDS = {
    "JSON": 1,
    "THRIFT_BINARY_B64": 2,
}

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

    SESSION_PERSISTED_EVENT = "SESSION_PERSISTED_EVENT"

    def __init__(self, event_type, chat_session):
        self.event_type = event_type
        self.chat_session = chat_session

class Persister(object):
    """Persister abstract base class.

    Useful base class for concrete persister implementations.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(
            self,
            service,
            hashring,
            chat_sessions_manager,
            database_session_factory):
        """Persister constructor.

        Args:
            service: ChatService object
            hashring: ServiceHashring object
            chat_sessions_manager: ChatSessionsManager object
            database_session_factory: sqlalchemy database session
            factory method.
        """
        self.service = service
        self.hashring = hashring
        self.chat_sessions_manager = chat_sessions_manager
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
    def persist(self, chat_session, messages=None, all=False, zombie=False):
        """Persist messages for the given chat session.

        Args:
            chat_session: ChatSession object
            messages: optional list of Message objects to persist
            all: optional flag which can be used as an alternative
                to passing in 'messages', which when True indicates
                that all messages in the chat_session which need
                persisting, should be persisted.
            zombie: optional flag which indicates that the given
                chat_session is a zombie (expired but not completed)
                and that a ChatPersistJob should be created.
        
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

    def _get_unpersisted_messages(self, chat_session):
        """Get all unpersisted messages for the given chat_session.

        This method determines which messages exist in the
        chat session object, but not in the database.

        Args:
            chat_session: ChatSession object
        Returns:
            list of Message objects needing persistence.
        """
        result = []

        try:
            session = self._get_database_session()
            persisted_messages = session.query(ChatMessageModel)\
                    .filter_by(chat_session_id=chat_session.id)\
                    .order_by(desc(ChatMessageModel.timestamp))\
                    .with_entities(ChatMessageModel.message_id)\
                    .all()
            
            persisted = {}
            for message in persisted_messages:
                persisted[message.message_id] = True

            for message in chat_session.messages:
                if message.header.id not in persisted:
                    result.append(message)

            session.commit()

        except Exception:
            result = chat_session.messages
        finally:
            session.close()
        
        return result

    def _hashring_observer(self, hashring, event):
        """Observer method which will be invoked upon hashring changes.
        
        This method loops through all chat session, upon a change
        to the hashring, and initiates a full persist for chat
        sessions which it is taking eover. This ensures that
        messages which may have been in the memory of the previously
        responsible service, but not yet persisted, will be 
        persisted.

        Args:
            hashring: ServiceHashring object
            event: ServiceHashringEvent object
        """
        if event.event_type == ServiceHashringEvent.CHANGED_EVENT:
            service_info = self.service.info()

            #Loop through all of the chat sessions and initiate 
            #a full persist for all chat sessions which we are
            #taking over, since they may have add messages
            #in memory which were not permitted.
            for chat_session_token, chat_session in self.chat_sessions_manager.all().items():
                previous_preference_list = self.hashring.preference_list(
                        chat_session_token,
                        event.previous_hashring)
                current_preference_list = self.hashring.preference_list(
                        chat_session_token,
                        event.current_hashring)

                if previous_preference_list and current_preference_list:
                    previous_primary = previous_preference_list[0]
                    current_primary = current_preference_list[0]
                    if current_primary.service_info.key == service_info.key and \
                       previous_primary.service_info.key != service_info.key:
                           self.persist(chat_session, None, all=True)


class GreenletPoolPersister(Persister):
    """Persister which delegations replications to a pool of greenlets."""

    STOP_ITEM = object()

    class PersistItem(object):
        """Persist item class.

        Represents persist work which needs to be done.
        """
        def __init__(self, chat_session, messages, all, zombie, result):
            """PersistItem constructor.

            Args:
                chat_session: ChatSession object
                messages: optional list of Message objects to persist
                all: flag which can be used as an alternative
                    to passing in 'messages', which when True indicates
                    that all messages in the chat_session which need
                    persisting, should be persisted.
                zombie: flag which indicates that the given
                    chat_session is a zombie (expired but not completed)
                    and that a ChatPersistJob should be created.
                    chat_session: ChatSession object
                    messages: list of Message objects needing persisting
            """
            self.chat_session = chat_session
            self.messages = messages
            self.all = all
            self.zombie = zombie
            self.result = result

    def __init__(
        self,
        service,
        hashring,
        chat_sessions_manager,
        database_session_factory,
        size,
        max_queue_size=100):
        """GreenletPoolPersister constructor.

        Args:
            service: ChatService object
            hashring: ServiceHashring object
            chat_sessions_manager: ChatSessionsManager object
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
            chat_sessions_manager,
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
    
    def _is_chat_ended_marker(self, message):
        """Check if message is ENDED_MARKER message.

        Args:
            message: Message object
        """
        result = False
        if message.header.type == MessageType.MARKER_CREATE:
            marker = message.markerCreateMessage.marker
            if marker.type == MarkerType.ENDED_MARKER:
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
            chat_session_persisted = False
            chat_session = item.chat_session
            session = self._get_database_session()

            if item.all:
                messages = self._get_unpersisted_messages(chat_session)
            else:
                messages = item.messages or []

            for message in messages:
                model = ChatMessageModel(
                        message_id=message.header.id,
                        chat_session_id=chat_session.id,
                        type_id=MESSAGE_TYPE_IDS[MessageType._VALUES_TO_NAMES[message.header.type]],
                        format_type_id=MESSAGE_FORMAT_TYPE_IDS["THRIFT_BINARY_B64"],
                        timestamp=message.header.timestamp,
                        time=tz.timestamp_to_utc(message.header.timestamp),
                        data=serialize(message))
                session.add(model)

                if self._is_chat_ended_marker(message) and not chat_session.persisted:
                    self._create_persist_job(chat_session, session)
                    chat_session_persisted = True
            
            if item.zombie and not chat_session.persisted:
                self._create_persist_job(chat_session, session)
                chat_session_persisted = True

            session.commit()
            item.result.set(None)
            
            if chat_session_persisted:
                event = PersistEvent(
                        PersistEvent.SESSION_PERSISTED_EVENT,
                        chat_session)
                self._notify_observers(event)
            
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.debug("Done persisting %s message(s)" % len(messages))

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def _create_persist_job(self, chat_session, session):
        """Create a ChatPersistJob in the database.

        Args:
            chat_session: ChatSession object
            session: SQLAlchemy Sesison object
        """
        chat_session.persisted = True
        job = ChatPersistJob(
               chat_session_id=chat_session.id,
               created=func.current_timestamp())
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

    def persist(self, chat_session, messages, all=False, zombie=False):
        """Persist messages for the given chat session.

        Args:
            chat_session: ChatSession object
            messages: optional list of Message objects to persist
            all: optional flag which can be used as an alternative
                to passing in 'messages', which when True indicates
                that all messages in the chat_session which need
                persisting, should be persisted.
            zombie: optional flag which indicates that the given
                chat_session is a zombie (expired but not completed)
                and that a ChatPersistJob should be created.
        
        Returns:
            PersistAsyncResult object.
        """
        item = self.PersistItem(
                chat_session=chat_session,
                messages=messages,
                all=all,
                zombie=zombie,
                result=PersistAsyncResult())
        self.queue.put(item)
        return item.result
