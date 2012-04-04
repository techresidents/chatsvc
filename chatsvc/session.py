from gevent.event import Event

class ChatSession(object):
    def __init__(self, chat_session_token):
        self.id = chat_session_token
        self.event = Event()
        self.messages = []

    def get_messages(self, asOf=None, block=False, timeout=None):
        if asOf is not None:
            messages = [m for m in self.messages if m.header.timestamp > asOf]
            if not messages and block:
                self.event.wait(timeout)
                messages = [m for m in self.messages if m.header.timestamp > asOf]
            return messages
        else:
            return self.messages
    
    def send_message(self, message):
        self.messages.append(message)
        self.event.set()
        self.event.clear()
