import abc
import logging

import gevent.event
import gevent.queue
from sqlalchemy.sql import func

from trchatsvc.gen.ttypes import MessageType, MarkerType
from trpycore.thrift.serialization import serialize
from trpycore.timezone import tz
from trsvcscore.models import ChatMessage as ChatMessageModel, ChatPersistJob

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

class Persister(object):
    """Persister abstract base class."""
    __metaclass__ = abc.ABCMeta

    def __init__(self, service_handler, chat_sessions_manager):
        self.service_handler = service_handler
        self.chat_sessions_manager = chat_sessions_manager

    @abc.abstractmethod
    def start(self):
        return

    @abc.abstractmethod
    def stop(self):
        return

    @abc.abstractmethod
    def join(self, timeout=None):
        return
    
    @abc.abstractmethod
    def persist(self, chat_session, messages):
        return


class GreenletPoolPersister(Persister):

    STOP_ITEM = object()

    class PersistItem(object):
        def __init__(self, chat_session, messages, result):
            self.chat_session = chat_session
            self.messages = messages
            self.result = result

    def __init__(
        self,
        service_handler,
        chat_sessions_manager,
        size,
        max_queue_size=100):
        super(GreenletPoolPersister, self).__init__(
            service_handler,
            chat_sessions_manager)
        self.size = size
        self.queue = gevent.queue.Queue(max_queue_size)
        self.workers = []
        self.running = False
        self.log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))
    
    def _is_chat_ended_marker(self, message):
        result = False
        if message.header.type == MessageType.MARKER_CREATE:
            marker = message.markerCreateMessage.marker
            if marker.type == MarkerType.ENDED_MARKER:
                result = True
        return result

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
                
                chat_session = item.chat_session
                session = self.service_handler.get_database_session()
                for message in item.messages:
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
                        chat_session.persisted = True
                        job = ChatPersistJob(
                                chat_session_id=chat_session.id,
                                created=func.current_timestamp())
                        session.add(job)

                session.commit()

                item.result.set(None)
                
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

    def persist(self, chat_session, messages):
        item = self.PersistItem(chat_session, messages, PersistAsyncResult())
        self.queue.put(item)
        return item.result
