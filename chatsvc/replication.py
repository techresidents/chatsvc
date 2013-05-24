import abc
import logging
from collections import deque

import gevent
import gevent.coros
import gevent.event
import gevent.queue

from trsvcscore.proxy.basic import BasicServiceProxyPool
from trsvcscore.hashring.base import ServiceHashringEvent
from tridlcore.gen.ttypes import RequestContext
from trchatsvc.gen import TChatService
from trchatsvc.gen.ttypes import ChatState, ChatSnapshot

def node_to_string(node):
    """Helper method to convert hashring node to string.

    Args:
        node: ServiceHashringNode object
    """
    return str(node)

def nodes_to_string(nodes):
    """Helper method to convert hashring nodes to string.

    Args:
        node: list of ServiceHashringNode objects
    """
    return "\n".join(["%s" % node_to_string(n) for n in nodes])


class ReplicationException(Exception):
    """Replication exception class."""
    pass

class ReplicationAsyncResult(gevent.event.AsyncResult):
    """Async replication result.
    
    This class encapsulates an async, replication result.
    It is intended to be returned from methods which
    start an async replication governed by the following
    parameters:
        N: The total number of nodes needing the data,
            including the node performing the replication.
            For example if you need 3 copies of your data,
            set N=3.
        W: The total number of nodes needing the data
            before a write is considered successful.
            For example you may desire 3 copies of 
            your data (N=3), but will consider
            a write successful once there are 2 
            copies (W=2).

    Following replication invocation, an instance of this
    class can be used to monitor the replication. Calling
    get() on the object will block until the W copies
    of the data exist.
    """
    def __init__(self, N, W, max_errors=2):
        """ReplicationAsyncResult constructor.

        Args:
            N: The total number of nodes needing the data,
                including the node performing the replication.
                For example if you need 3 copies of your data,
                set N=3.
            W: The total number of nodes needing the data
                before a write is considered successful.
                For example you may desire 3 copies of 
                your data (N=3), but will consider
                a write successful once there are 2 
                copies (W=2).
            max_errors: maximum number of errors to allow
                before giving up the replication and
                triggering an exception.
        """
        super(ReplicationAsyncResult, self).__init__()
        self.N = N
        self.W = W
        self.max_errors = max_errors
        self.values = []
        self.exceptions = []
    
    def set(self, value=None):
        """Add a replication result.

        Note that this will not trigger a result
        until the Wth set invocation.
        """
        self.values.append(value)
        if self.w_satisfied():
            super(ReplicationAsyncResult, self).set(self.values[0])

    def set_exception(self, exception):
        """Set an exceptional replication result.
        
        Note that this will not trigger an exception
        until max_errors is exceeded.
        """
        self.exceptions.append(exception)
        if len(self.exceptions) > self.max_errors:
            super(ReplicationAsyncResult, self).set_exception(self.exceptions[0])
    
    def fail(self, exception):
        """Fail the replication.
        
        This method will fail the replication, triggering an
        exception for all waiters. Note that this method
        should only be invoked when replication is no
        longer possible.
        """
        super(ReplicationAsyncResult, self).set_exception(exception)
    
    def w_satisfied(self):
        """Check if W writes have been completed.

        Returns:
            True if set() has been invoked W or more times,
            False otherwise.
        """
        return len(self.values) >= self.W

    def n_satisfied(self):
        """Check if N writes have been completed.

        Returns:
            True if set() has been invoked N or more times,
            False otherwise.
        """
        return len(self.values) >= self.N

    def num_completed(self):
        """Get the number of completed replications."""
        return len(self.values)

    def num_errors(self):
        """Get the number of failed replications."""
        return len(self.exceptions)


class Replicator(object):
    """Abstract base Replicator class.

    This is a convenient base class for chat message replicators.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(
            self,
            service,
            hashring,
            chat_manager,
            N,
            W,
            max_connections_per_service=1,
            allow_same_host_replications=False):
        """Replicator constructor.

        Args:
            service: Service object
            hashring: ServiceHashring object
            chat_manager: ChatManager object
            N: The total number of nodes to write data to,
                including the node performing the replication.
                For example if you need 3 copies of your data,
                set N=3.
            W: The total number of nodes needing the data
                written before a write is considered successful.
                For example you may desire 3 copies of your
                data (N=3), but will consider a write successful
                once there are 2 copies (W=2).
            max_connections_per_service: maximum number of replication connections
                for each service. This limits the number of concurrent replications,
                per service, which are allowed.
            allow_same_host_replications: boolean indicating if replications
                are allowed to reside in a different process on the
                same host.
        """
        self.service = service
        self.hashring = hashring
        self.chat_manager = chat_manager
        self.N = N
        self.W = W
        self.max_connections_per_service = max_connections_per_service
        self.allow_same_host_replications = allow_same_host_replications

        self.service_proxy_pools = {}
        self.service_info = service.info()
        self.log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))

        #add hashring observer
        self.hashring.add_observer(self._hashring_observer)


    @abc.abstractmethod
    def start(self):
        """Start replicator."""
        return
    
    @abc.abstractmethod
    def replicate(self, chat, messages, N=None, W=None, nodes=None):
        """Replicate messages for the specified chat.
        
        Replicates messages for the specified chat. Upon success,
        the replication will result in N total copies of the messages,
        and the returned ReplicationAsyncResult will unblock after W copies
        of the messages have been written. If nodes are provided, it
        will be used as the replication preference list. Otherwise,
        the hashring will be used to determine the preference list.
        
        Args:
            chat: Chat object
            messages: list of chat Message objects to replicate.
            N: The total number of nodes to write messages to.
                If not provided, self.N will be used.
            W: The total number of nodes to write messages to
                before the write is considered successful.
            nodes: list of ServiceHashringNode objects to use
                as the replication preference list. If not
                provided, the hashring will be used to
                determine the preference list.

        Returns:
            ReplicationAsyncResult object
        """
        return

    @abc.abstractmethod
    def replicate_node_change(self, previous_hashring, current_hashring, N=None, W=None):
        """Replicates messages as needed for the addition/removal of nodes.
        
        Replicates messages as needed for the addition and removal of 
        service hashring nodes, according to the N/W arguments.
        The replication preference list will be determined based on
        current_hashring.

        Note that each service is only responsible for replicating messages
        for chat for which it is currently responsible, and,
        also chat for which it was is previously responsible.

        Args:
            previous_hashring: List of ServiceHashringNode's representing
                the hashring prior to the node change.
            current_hashring: List of ServiceHashringNode's representing
                the hashring following the node change.
        """
        return

    @abc.abstractmethod
    def stop(self):
        """Stop replicator."""
        return

    @abc.abstractmethod
    def join(self, timeout=None):
        """Join replicator.

        Join replicator waiting for the completion of all threads
        or greenlets.

        Args:
            timeout: optional maximum number of seconds to wait for the completion
                of all threads or greenlets.
        """
        return

    def _build_request_context(self):
        """Build RequestContext object for use with service calls.

        Returns:
            RequestContext object.
        """
        return RequestContext(
                userId=0,
                impersonatingUserId=0,
                sessionId="sessionid",
                context="")

    def _build_chat_snapshot(self, chat, messages=None):
        """Build ChatSnapshot object for replication.

        Args:
            chat: Chat object
            messages: list of Message objects
            full_snapshot: Option flag indicating if this is a full or
                partial snapshot.

        Returns:
            ReplicationSnapshot object
        """
        messages = messages or []
        full_snapshot = len(chat.state.messages) == len(messages)

        state = ChatState(
                token=chat.state.token,
                status=chat.state.status,
                maxDuration=chat.state.maxDuration,
                maxParticipants=chat.state.maxParticipants,
                startTimestamp=chat.state.startTimestamp,
                endTimestamp=chat.state.endTimestamp,
                users=chat.state.users,
                persisted=chat.state.persisted,
                session=chat.state.session,
                messages=messages)

        snapshot = ChatSnapshot(
                fullSnapshot=full_snapshot,
                state=state)

        return snapshot

    def _service_proxy_pool(self, node):
        """Get service proxy pool for the given hashring node.

        Args:
            node: ServiceHashringNode object
        
        Returns:
            ServiceProxyPool object to be used to connect
                to the specified node.
        """
        if node.service_info.key not in self.service_proxy_pools:
            server_endpoint = node.service_info.default_endpoint()
            proxy_pool = BasicServiceProxyPool(
                    node.service_info.name,
                    server_endpoint.address,
                    server_endpoint.port,
                    self.max_connections_per_service,
                    TChatService,
                    is_gevent=True)
            self.service_proxy_pools[node.service_info.key] = proxy_pool
        return self.service_proxy_pools[node.service_info.key]

    def _is_remote_node(self, node):
        """Check if node is remotely located.

        Note that a remote node may be a  different service
        on the same physical host, or a different host altogether.

        Args:
            node: ServiceHashringNode object
        
        Returns:
            True if the node is remote.
        """
        return node.service_info.key != self.service_info.key

    def _preference_list(self, chat_token, hashring=None):
        """Get the replication preference list for the given chat.
        
        Note that if self.allow_same_host_replications is True,
        the preference list may contain service instances located
        on the same physical host.

        Args:
            chat_token: chat token
            hashring: optional list of ServiceHashringNode's
                to use to determine the preference list. If
                not provided, the current hashring will be used.
        
        Returns:
            list of ServiceHashringNode's to be used as the
            replication preference list.
        
        """
        #Merging nodes will only return a single node per host.
        #If self.allow_same_host_replications is set to True,
        #we should not merge nodes.
        merge_nodes = not self.allow_same_host_replications
        return self.hashring.preference_list(chat_token, hashring, merge_nodes=merge_nodes)

    def _replication_nodes(self, previous_hashring, current_hashring, chat_token):
        """Determine nodes needing a replication based on a hashring change.

        This method will determine which nodes need a replication of the specified
        chat based on the hashring change.

        Args:
            previous_hashring: list of ServiceHashringNode objects
                for the hashring prior to the change.
            current_hashring: list of ServiceHashringNode objects
                for the hashring following the change.
            chat_token: chat token

        Returns:
            list of ServiceHashringNode's needing a replication.
        """
        result = []

        #Get the current and previous preference lists (only first N nodes).
        #The first N nodes in the current preference list must have a copy
        #of all the messages in the chat.
        #If a node exists in the current preference list, which did not
        #exist in the previous preference list, than the service
        #responsible for that chat token may have some replication
        #work to do. The exception is if an existing service, which
        #was previously in the preference list, is occupying a new
        #position on the hashring (closer to chat) which
        #has replaced its old position.
        current_preference_list = self._preference_list(chat_token, current_hashring)[:self.N]
        previous_preference_list = self._preference_list(chat_token, previous_hashring)[:self.N]
        previous_service_keys = {n.service_info.key: True for n in previous_preference_list}
        
        #Check if we are currently or were previously responsible for this chat
        if (previous_preference_list and not self._is_remote_node(previous_preference_list[0])) or \
           (current_preference_list and not self._is_remote_node(current_preference_list[0])):
            
            # Check if any nodes needs replication
            if previous_preference_list != current_preference_list:
                for node in current_preference_list:

                    #Make sure this isn't a service which was previously
                    #in the preference list under a different hashring node.
                    #This can happen when a service first comes up, and
                    #occupies on multiple positions on the hashring.
                    #Sometimes the last postion occupied may bump
                    #out one of its previous tokens.
                    if node.service_info.key not in previous_service_keys:
                        result.append(node)

        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("Determining replication nodes for chat: %s" % chat_token)
            self.log.debug("Previous hashring: [\n%s\n]" % nodes_to_string(previous_hashring))
            self.log.debug("Current hashring: [\n%s\n]" % nodes_to_string(current_hashring))
            self.log.debug("Previous preference list: [\n%s\n]" % nodes_to_string(previous_preference_list))
            self.log.debug("Current preference list: [\n%s\n]" % nodes_to_string(current_preference_list))
            self.log.debug("Replication nodes: [\n%s\n]" % nodes_to_string(result))

        return result            
    

    def _coordinate_replication(self, chat, messages, N, W, nodes, result):
        """Coordinate chat messages replication.

        This method will perform the replication for the given arguments.

        Note that a maximum of W-1 concurrent replications will be allowed.
        The minus one is used to discount for the copy of data which exists
        on the service performing the replication (us).

        This can result in data being replicated to an additonal nodes
        if W-1>1, since as soon as one replication ends we start the
        next. This will not cause any issues, but is slightly
        inefficient.

        For example, assume N=3, W=3:
            1) Since we currently have one copy, we need to replicate
            to 3 more nodes.

            2) W-1 concurrent replications will be allowed (2 in this case)

            3) We spawn 2 concurrent replications

            4) First replication finishes and we now have 2 copies
            of the data. Since W=3, we still need one more copy
            before the write can be considered completed.

            5) We start a new replication since we still need
            another copy and we only have one outstanding
            replication (2 concurrent replications allowed). 

            6) Second replication finishes, and we we now have
            3 copies of the data, and W=3 is satisfied,
            so the write is considered complete.

            7) Third replication finishes resulting in 4 copies
            of the data. 


        Args:
            chat: Chat object
            messages: List of Message objects to replicate
            N: The total number of nodes to write messages to.
            W: The total number of nodes to write messages to
            nodes: list of ServiceHashringNode objects to
                use as the replication preference list.
                If not provided, the preference list will
                be calculated from the current hashring.
            result: ReplicationAsyncResult object to update
                with replication results.
        """
        workers = []
        preference_list = nodes or self._preference_list(chat.token)
        preference_queue = deque(preference_list)
        
        #Use a semaphore to limit the number of concurrent replications.
        #We will allow max of W concurrent replications, since
        #the service needs W copies of the data before it can
        #consider the write successful.
        semaphore = gevent.coros.Semaphore(max(W-result.num_completed(), 1))
        
        while True:
            try:
                semaphore.acquire()

                #Stop if we've tried all nodes in the preference list or
                #the replication is complete.
                if not preference_queue or result.n_satisfied():
                    semaphore.release()
                    break

                #Get the next node in line for replication
                node = preference_queue.popleft()

                #Spawn a greenlet to perform the replication
                #if this is not us (remote node)
                if self._is_remote_node(node):
                    worker = gevent.spawn(self._replicate_to_node, chat, messages, node, result)
                    worker.link(lambda greenlet: semaphore.release())
                    workers.append(worker)
                else:
                    semaphore.release()
            except Exception as error:
                self.log.exception(error)
                semaphore.release()
        
        #If the result is not completed (N succeessful writes)
        #wait for all outstanding replications to complete.
        if not result.n_satisfied():
            gevent.joinall(workers)
            
            #All replications have now finished
            if not result.n_satisfied():
                message = "replication(N=%s, W=%s): (attempts=%s, completed=%s, failed=%s)" % (
                        N, W, len(preference_list),
                        result.num_completed(), result.num_errors())

                if not result.w_satisfied():
                    error_message = "failed %s" % message
                    self.log.error(error_message)
                    result.fail(RuntimeError(error_message))
                elif not result.n_satisfied():
                    error_message = "uncompleted %s" % message
                    self.log.warn(error_message)

    def _replicate_to_node(self, chat, messages, node, result):
        """Replicate chat messages to a single node.

        This method will peform a single replication to exactly one node, 
        using the service_proxy_pool for the connection.

        Args:
            chat: Chat object
            messages: list of Message objects to replicate
            node: ServiceHashringNode to replicate messages to.
            result: ReplicationAsyncResult object to update 
                with the replication result.
        """
        try:
            service_proxy_pool = self._service_proxy_pool(node)

            #Wait for a service proxy to the node to be available.
            #This may be block and is limited by max_service_connections.
            with service_proxy_pool.get() as proxy:
                if self.log.isEnabledFor(logging.DEBUG):
                    self.log.debug("Replicating %s message(s) to [\n%s\n]" % (len(messages), node_to_string(node)))

                context = self._build_request_context()
                snapshot = self._build_chat_snapshot(chat, messages)
                proxy.replicate(context, snapshot)

                #Signal to the result that our replication is completed.
                result.set(None)

                if self.log.isEnabledFor(logging.DEBUG):
                    self.log.debug("Done replicating %s message(s) to %s" % (len(messages), node_to_string(node)))
        except Exception as error:
            self.log.exception(error)
            result.set_exception(ReplicationException(str(error)))


    def _hashring_observer(self, hashring, event):
        """Observer method which will be invoked upon hashring changes.

        Args:
            hashring: ServiceHashring object
            event: ServiceHashringEvent object
        """
        if event.event_type == ServiceHashringEvent.CHANGED_EVENT:
            self.log.info("Hashring change ...")
            self.log.info("Previous hashring: [\n%s\n]" % nodes_to_string(event.previous_hashring))
            self.log.info("Current hashring: [\n%s\n]" % nodes_to_string(event.current_hashring))
            self.replicate_node_change(event.previous_hashring, event.current_hashring)
    

class GreenletPoolReplicator(Replicator):
    """Replicator which delegations replications to a pool of greenlets."""

    STOP_ITEM = object()

    class ReplicationItem:
        """Item representing a replication which needs to be performed."""
        def __init__(self, chat, messages, N, W, nodes, result):
            """ReplicationItem constructor.
                chat: Chat object
                messages: list of Message objects needing replication
                N: The total number of nodes to write data to,
                    including the node performing the replication.
                    For example if you need 3 copies of your data,
                    set N=3.
                W: The total number of nodes needing the data
                    written before a write is considered successful.
                    For example you may desire 3 copies of your
                    data (N=3), but will consider a write successful
                    once there are 2 copies (W=2).
                nodes: list of ServiceHashringNode objects to be 
                    used as the replication preference list.
                result: ReplicationAsyncResult object to be
                    updated with replication results.
            """
            self.chat = chat
            self.messages = messages
            self.N = N
            self.W = W
            self.nodes = nodes
            self.result = result

    def __init__(
            self,
            service,
            hashring,
            chat_manager,
            N,
            W,
            size,
            max_connections_per_service=1,
            allow_same_host_replications=False,
            max_queue_size=100):
        """Replicator constructor.
        Args:
            service: Service object
            hashring: ServiceHashring object
            chat_manager: ChatManager object
            N: The total number of nodes to write data to,
                including the node performing the replication.
                For example if you need 3 copies of your data,
                set N=3.
            W: The total number of nodes needing the data
                written before a write is considered successful.
                For example you may desire 3 copies of your
                data (N=3), but will consider a write successful
                once there are 2 copies (W=2).
            size: number of greenlets to use for replications
            max_connections_per_service: maximum number of replication connections
                for each service. This limits the number of concurrent replications,
                per service, which are allowed.
            allow_same_host_replications: boolean indicating if replications
                are allowed to reside in a different process on the
                same host.
            max_queue_size: maximum number of ReplicationItem's which
                can be added to the replication queue before
                blocking.
        """
        super(GreenletPoolReplicator, self).__init__(
                service,
                hashring,
                chat_manager,
                N,
                W,
                max_connections_per_service,
                allow_same_host_replications)
        self.size = size
        self.queue = gevent.queue.Queue(max_queue_size)
        self.workers = []
        self.running = False
        self.log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))
    
    def start(self):
        """Start replicator."""
        if not self.running:
            self.log.info("Starting %s(N=%s, W=%s, size=%s) ..." % (
                self.__class__.__name__, self.N, self.W, self.size))

            self.running = True
            for i in range(0, self.size):
                worker = gevent.spawn(self.run)
                self.workers.append(worker)

    def run(self):
        """Run replicator."""
        while self.running:
            try:
                item = self.queue.get()

                if item is self.STOP_ITEM:
                    break
                
                self._coordinate_replication(
                        chat=item.chat,
                        messages=item.messages,
                        N=item.N,
                        W=item.W,
                        nodes=item.nodes,
                        result=item.result)

            except Exception as error:
                self.log.exception(error)

    def stop(self):
        """Stop replicator."""
        if self.running:
            self.log.info("Stopping %s(N=%s, W=%s, size=%s) ..." % (
                self.__class__.__name__, self.N, self.W, self.size))

            self.running = False
            for i in range(0, self.size):
                self.queue.put(self.STOP_ITEM)

    def join(self, timeout=None):
        """Join replicator.

        Join replicator waiting for the completion of all threads
        or greenlets.

        Args:
            timeout: optional maximum number of seconds to wait for the completion
                of all threads or greenlets.
        """
        gevent.joinall(self.workers, timeout)

    def replicate(self, chat, messages, N=None, W=None, nodes=None):
        """Replicate messages for the specified chat.
        
        Replicates messages for the specified chat. Upon success,
        the replication will result in N total copies of the messages,
        and the returned ReplicationAsyncResult will unblock after W copies
        of the messages have been written. If nodes are provided, it
        will be used as the replication preference list. Otherwise,
        the hashring will be used to determine the preference list.
        
        Args:
            chat: Chat object
            messages: list of chat Message objects to replicate.
            N: The total number of nodes to write messages to.
                If not provided, self.N will be used.
            W: The total number of nodes to write messages to
                before the write is considered successful.
            nodes: list of ServiceHashringNode objects to use
                as the replication preference list. If not
                provided, the hashring will be used to
                determine the preference list.

        Returns:
            ReplicationAsyncResult object
        """
        if N is None or N == -1:
            N = self.N
        if W is None or W == -1:
            W = self.W
        
        #Create the async replication result to track replication
        result = ReplicationAsyncResult(N, W)

        #Signal to the result that 1 copy of the data exists (ours).
        result.set(None)
        if N > 1:
            item = self.ReplicationItem(
                    chat=chat,
                    messages=messages,
                    N=N,
                    W=W,
                    nodes=nodes,
                    result=result)
            
            self.queue.put(item)

        return result

    def replicate_node_change(self, previous_hashring, current_hashring):
        """Replicates messages as needed for the addition/removal of nodes.
        
        Replicates messages as needed for the addition and removal of 
        service hashring nodes, according to the N/W arguments.
        The replication preference list will be determined based on
        current_hashring.

        Note that each service is only responsible for replicating messages
        for chat for which it is currently responsible, and,
        also chat for which it was is previously responsible.

        Args:
            previous_hashring: List of ServiceHashringNode's representing
                the hashring prior to the node change.
            current_hashring: List of ServiceHashringNode's representing
                the hashring following the node change.
        """

        #Loop through all of the chat to see if we need to
        #replicate any of them to new nodes.
        for chat_token, chat in self.chat_manager.all().items():

            #Get the new nodes needing the data.
            #Note that this will only return us nodes for chat
            #which we are currently or were previously responsible for.
            replication_nodes = self._replication_nodes(previous_hashring, current_hashring, chat_token)
            if replication_nodes:
                #Note that we add 1 to N/W, to adjust for our copy of the data.
                #Adding one to N/W will force the data to be replicated
                #to all nodes in replication_nodes.
                self.replicate(
                        chat=chat,
                        messages=chat.state.messages,
                        N=len(replication_nodes)+1,
                        W=len(replication_nodes)+1,
                        nodes=replication_nodes)
