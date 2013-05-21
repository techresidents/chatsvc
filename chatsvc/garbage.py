import logging

import gevent

class GarbageCollectionEvent(object):
    ZOMBIE_SESSION_EVENT = "ZOMBIE_SESSION_EVENT"

    def __init__(self, event_type, chat):
        self.event_type = event_type
        self.chat = chat

class GarbageCollector(object):
    
    def __init__(
            self,
            service,
            hashring,
            chat_manager,
            interval,
            throttle):
        self.service = service
        self.hashring = hashring
        self.chat_manager = chat_manager
        self.interval = interval
        self.throttle = throttle
        self.observers = []
        self.running = False
        self.greenlet = None
        self.log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))
  
    def _notify_observers(self, event):
        for observer in self.observers:
            try:
                observer(event)
            except Exception as error:
                self.log.error("gc observer exception")
                self.log.exception(error)

    def _gc_chat(self, chat):
        if chat.completed or chat.expired:
            if chat.persisted:
                self.log.info("garbage collecting chat (id=%s)" \
                    % chat.id)
                self.chat_manager.remove(chat.token)
            else:
                self.log.info("zombie chat dectected (id=%s)" \
                        % chat.id)
                event = GarbageCollectionEvent(
                        GarbageCollectionEvent.ZOMBIE_SESSION_EVENT,
                        chat)
                self._notify_observers(event)
  
    def add_observer(self, observer):
        self.observers.append(observer)

    def remove_observer(self, observer):
        self.observers.remove(observer)

    def start(self):
        if not self.running:
            self.log.info("Starting %s(interval=%s, throttle=%s) ..." \
                    % (self.__class__.__name__, self.interval, self.throttle))
            self.running = True
            self.greenlet = gevent.spawn(self.run)
    
    def run(self):
        while self.running:
            try:
                #note that itervalues should not be used in place of values,
                #since we will be modifying the underlying dict
                for chat in self.chat_manager.all().values():
                    #if chat.completed or chat.expired:
                    #    self._gc_chat(chat)
                    if self.throttle:
                        gevent.sleep(self.throttle)
            except gevent.GreenletExit:
                break
            except Exception as error:                
                self.log.exception(error)
            finally:
                gevent.sleep(self.interval)
        
        self.running = False

    def stop(self):
        if self.running:
            self.log.info("Stopping %s(interval=%s, throttle=%s) ..." \
                    % (self.__class__.__name__, self.interval, self.throttle))
            self.running = False
            self.greenlet.kill()

    def join(self, timeout=None):
        if self.greenlet:
            self.greenlet.join(timeout)



