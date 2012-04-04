import json
import time
import uuid

from trchatsvc.gen.ttypes import MessageType, MessageHeader, Message, TagMessage, WhiteboardMessage

class MessageFactory(object):
    def __init__(self):
        pass

    def create_header(self, chat_session_token, user_id, type, id=None, timestamp=None):
        id = id or uuid.uuid4().hex
        timestamp = timestamp or time.time()

        return MessageHeader(
                id=id,
                type=type,
                chatSessionToken = chat_session_token,
                userId=user_id,
                timestamp=timestamp)

    def create_tag_message(self, chat_session_token, user_id, name, id=None, timestamp=None):
        header = self.create_header(chat_session_token, user_id, MessageType.TAG, id, timestamp)
        message = TagMessage(name=name)
        return Message(header=header, tagMessage=message)

    def create_whiteboard_message(self, chat_session_token, user_id, data, id=None, timestamp=None):
        header = self.create_header(chat_session_token, user_id, MessageType.WHITEBOARD, id, timestamp)
        message = WhiteboardMessage(data=data)
        return Message(header=header, whiteboardMessage=message)


class MessageEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        super(MessageEncoder, self).__init__(*args, **kwargs)

        self.message_type_encoder = {
            MessageType.TAG: self.encode_tag_message,
            MessageType.WHITEBOARD: self.encode_whiteboard_message,
        }

    def default(self, obj):
        if isinstance(obj, Message):
            return self.encode_message(obj)
        else:
            return super(MessageEncoder, self).default(obj)
    
    def encode_message(self, message):
        return {
            "header": self.encode_message_header(message.header),
            "msg": self.message_type_encoder[message.header.type](message)
        }

    def encode_message_header(self, obj):
        return {
            "id": obj.id,
            #"type": obj.type,
            "type": MessageType._VALUES_TO_NAMES[obj.type].lower(),
            "chatSessionToken": obj.chatSessionToken,
            "userId": obj.userId,
            "timestamp": obj.timestamp
        }

    def encode_tag_message(self, message):
        message = message.tagMessage
        return {
            "name": message.name
        }

    def encode_whiteboard_message(self, message):
        message = message.whiteboardMessage
        return {
            "data": message.data
        }
