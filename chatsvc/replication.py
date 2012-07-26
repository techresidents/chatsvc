import abc
import logging
import socket
from collections import deque

import gevent
import gevent.coros
import gevent.event
import gevent.queue

from trsvcscore.proxy.basic import BasicServiceProxyPool
from trsvcscore.hashring.base import ServiceHashringEvent
from tridlcore.gen.ttypes import RequestContext
from trchatsvc.gen import TChatService

class ReplicationAsyncResult(gevent.event.AsyncResult):
    def __init__(self, N, W, max_errors=2):
        super(ReplicationAsyncResult, self).__init__()
        self.N = N
        self.W = W
        self.max_errors = max_errors
        self.values = []
        self.exceptions = []
    
    def set(self, value=None):
        self.values.append(value)
        if len(self.values) >= self.W:
            super(ReplicationAsyncResult, self).set(self.values[0])

    def set_exception(self, exception):
        self.exceptions.append(exception)
        if len(self.exceptions) > self.max_errors:
            super(ReplicationAsyncResult, self).set_exception(self.exceptions[0])
    
    def completed(self):
        return len(self.values) >= self.N



class Replicator(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, handler, N, W, max_connections_per_service=1):
        self.handler = handler
        self.N = N
        self.W = W
        self.max_connections_per_service = max_connections_per_service
        self.chat_sessions_manager = handler.chat_sessions_manager
        self.hashring = handler.hashring
        self.service_proxy_pools = {}

        #add hashring observer
        self.hashring.add_observer(self._hashring_observer)

    @abc.abstractmethod
    def start(self):
        return
    
    @abc.abstractmethod
    def replicate(self, chat_session_token, messages, N=None, W=None, nodes=None):
        return

    @abc.abstractmethod
    def replicate_node_change(self, previous_hashring, current_hashring, N=None, W=None):
        return

    @abc.abstractmethod
    def stop(self):
        return

    @abc.abstractmethod
    def join(self, timeout=None):
        return

    def _request_context(self):
        return RequestContext(
                userId=0,
                impersonatingUserId=0,
                sessionId="sessionid",
                context="")

    def _remote_node(self, node):
        return socket.gethostname() != node.hostname and \
                self.handler.port != node.service_port

    def _preference_list(self, chat_session_token, hashring=None):
        return self.hashring.preference_list(chat_session_token, hashring, merge_nodes=False)
    
    def _service_proxy_pool(self, node):
        if node.service_key not in self.service_proxy_pools:
            proxy_pool = BasicServiceProxyPool(
                    "chatsvc",
                    node.hostname,
                    node.service_port,
                    self.max_connections_per_service,
                    TChatService,
                    is_gevent=True)
            self.service_proxy_pools[node.service_key] = proxy_pool
        return self.service_proxy_pools[node.service_key]

    def _coordinate_replication(self, chat_session_token, messages, N, W, nodes, result):
        workers = []
        preference_list = nodes or self._preference_list(chat_session_token)
        preference_queue = deque(preference_list)
        
        semaphore = gevent.coros.Semaphore(N-1)
        def release(greenlet):
            semaphore.release()

        while True:
            try:
                semaphore.acquire()
                if not preference_queue or result.completed():
                    semaphore.release()
                    break
                node = preference_queue.popleft()
                if self._remote_node(node):
                    proxy_pool = self._service_proxy_pool(node)
                    worker = gevent.spawn(self._replicate_to_node, chat_session_token, messages, proxy_pool, result)
                    #worker.link(lambda greenlet: semaphore.release())
                    worker.link(release)
                    workers.append(worker)
                else:
                    semaphore.release()
            except Exception as error:
                logging.exception(error)
                semaphore.release()

    def _replicate_to_node(self, chat_session_token, messages, service_proxy_pool, result):
        try:
            with service_proxy_pool.get() as proxy:
                context = self._request_context()
                proxy.storeReplicatedMessages(context, chat_session_token, messages)
                result.set(None)
        except Exception as error:
            logging.exception(error)
            result.set_exception(error)

    def _replication_nodes(self, previous_hashring, current_hashring, chat_session_token):
        result = []
        current_preference_list = self._preference_list(chat_session_token, current_hashring)
        previous_preference_list = self._preference_list(chat_session_token, previous_hashring)
        previous_service_keys = {n.service_key: True for n in previous_preference_list}
        print previous_service_keys

        print "prev pl: %s" % ["%s:%s (%s)" % (n.hostname, n.service_port, n.service_key) for n in previous_preference_list]
        print "curr pl: %s" % ["%s:%s (%s)" % (n.hostname, n.service_port, n.service_key) for n in current_preference_list]

        if (previous_preference_list and not self._remote_node(previous_preference_list[0])) or \
           (current_preference_list and not self._remote_node(current_preference_list[0])):
            if previous_preference_list != current_preference_list:
                for node in current_preference_list:
                    if node.service_key not in previous_service_keys:
                        result.append(node)
        return result            

    def _hashring_observer(self, hashring, event):
        if event.event_type == ServiceHashringEvent.CHANGED_EVENT:
            self.replicate_node_change(event.previous_hashring, event.current_hashring)
    

class GreenletPoolReplicator(Replicator):
    STOP_ITEM = object()

    class ReplicationItem:
        def __init__(self, chat_session_token, messages, N, W, nodes, result):
            self.chat_session_token = chat_session_token
            self.messages = messages
            self.N = N
            self.W = W
            self.nodes = nodes
            self.result = result

    def __init__(self, handler, N, W, size, max_connections_per_service=1, max_queue_size=100):
        super(GreenletPoolReplicator, self).__init__(
                handler,
                N,
                W,
                max_connections_per_service)
        self.size = size
        self.queue = gevent.queue.Queue(max_queue_size)
        self.workers = []
        self.running = False
    
    def start(self):
        if not self.running:
            self.running = True
            for i in range(0, self.size):
                worker = gevent.spawn(self.run)
                self.workers.append(worker)

    def run(self):
        while self.running:
            try:
                item = self.queue.get()
                if item is self.STOP_ITEM:
                    break
                
                self._coordinate_replication(
                        chat_session_token=item.chat_session_token,
                        messages=item.messages,
                        N=item.N,
                        W=item.W,
                        nodes=item.nodes,
                        result=item.result)

            except Exception as error:
                logging.exception(error)

    def stop(self):
        if self.running:
            self.running = False
            for i in range(0, self.size):
                self.queue.put(self.STOP_ITEM)

    def join(self, timeout=None):
        gevent.joinall(self.workers, timeout)

    def replicate(self, chat_session_token, messages, N=None, W=None, nodes=None):
        N = N or self.N
        W = W or self.W

        result = ReplicationAsyncResult(N, W)
        result.set(None)
        if N > 1:
            item = self.ReplicationItem(
                    chat_session_token=chat_session_token,
                    messages=messages,
                    N=N,
                    W=W,
                    nodes=nodes,
                    result=result)

            self.queue.put(item)

        return result

    def replicate_node_change(self, previous_hashring, current_hashring):
        print "replicate node change..."
        for chat_session_token, chat_session in self.chat_sessions_manager.all().items():
            print "token: %s" % chat_session_token
            replication_nodes = self._replication_nodes(previous_hashring, current_hashring, chat_session_token)
            print "replication nodes: %s" % ["%s:%s" % (n.hostname, n.service_port) for n in replication_nodes]
            if replication_nodes:
                self.replicate(
                        chat_session_token=chat_session_token,
                        messages=chat_session.messages,
                        N=len(replication_nodes)+1,
                        W=len(replication_nodes)+1,
                        nodes=replication_nodes)
