import json
import uuid

import trchatsvc.gen.ttypes as ttypes
from trpycore.timezone import tz

class MessageFactory(object):
    def __init__(self):
        pass

    def create_header(self, chat_session_token, user_id, type, id=None, timestamp=None):
        id = id or uuid.uuid4().hex
        timestamp = timestamp or tz.timestamp()

        return ttypes.MessageHeader(
                id=id,
                type=type,
                chatSessionToken = chat_session_token,
                userId=user_id,
                timestamp=timestamp)

    def tag_create_message(self, chat_session_token, user_id, minute_id, name, tag_reference_id=None):
        header = self.create_header(chat_session_token, user_id, ttypes.MessageType.TAG_CREATE)
        message = ttypes.TagCreateMessage(
                tagId=uuid.uuid4().hex,
                minuteId=minute_id,
                name=name,
                tagReferenceId=tag_reference_id)
        return ttypes.Message(header=header, tagCreateMessage=message)

    def tag_delete_message(self, chat_session_token, user_id, tag_id):
        header = self.create_header(chat_session_token, user_id, ttypes.MessageType.TAG_DELETE)
        message = ttypes.TagDeleteMessage(tagId=tag_id)
        return ttypes.Message(header=header, tagDeleteMessage=message)

    def whiteboard_create_message(self, chat_session_token, user_id, name):
        header = self.create_header(chat_session_token, user_id, ttypes.MessageType.WHITEBOARD_CREATE)
        message = ttypes.WhiteboardCreateMessage(
                whiteboardId=uuid.uuid4().hex,
                name=name)
        return ttypes.Message(header=header, whiteboardCreateMessage=message)

    def whiteboard_delete_message(self, chat_session_token, user_id, whiteboard_id):
        header = self.create_header(chat_session_token, user_id, ttypes.MessageType.WHITEBOARD_DELETE)
        message = ttypes.WhiteboardDeleteMessage(whiteboardId=whiteboard_id)
        return ttypes.Message(header=header, whiteboardDeleteMessage=message)

    def whiteboard_create_path_message(self, chat_session_token, user_id, whiteboard_id, path_data):
        header = self.create_header(chat_session_token, user_id, ttypes.MessageType.WHITEBOARD_CREATE_PATH)
        message = ttypes.WhiteboardCreatePathMessage(
                pathId=uuid.uuid4().hex,
                pathData=path_data)
        return ttypes.Message(header=header, whiteboardCreatePathMessage=message)

    def whiteboard_delete_path_message(self, chat_session_token, user_id, whiteboard_id, path_id):
        header = self.create_header(chat_session_token, user_id, ttypes.MessageType.WHITEBOARD_DELETE_PATH)
        message = ttypes.WhiteboardDeletePathMessage(whiteboardId=whiteboard_id, pathId=path_id)
        return ttypes.Message(header=header, whiteboardDeletePathMessage=message)

    def minute_create_message(self, chat_session_token, user_id, topic_id):
        header = self.create_header(chat_session_token, user_id, ttypes.MessageType.MINUTE_CREATE)
        message = ttypes.MinuteCreateMessage(
                minuteId=uuid.uuid4().hex,
                topicId=topic_id,
                startTimestamp=tz.timestamp())
        return ttypes.Message(header=header, minuteCreateMessage=message)

    def minute_update_message(self, chat_session_token, user_id, minute_id, topic_id, start_timestamp):
        header = self.create_header(chat_session_token, user_id, ttypes.MessageType.MINUTE_UPDATE)
        message = ttypes.MinuteUpdateMessage(
                minuteId=minute_id,
                topicId=topic_id,
                startTimestamp=start_timestamp,
                endTimestamp=tz.timestamp())
        return ttypes.Message(header=header, minuteUpdateMessage=message)

class MessageEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        super(MessageEncoder, self).__init__(*args, **kwargs)

        self.message_type_encoder = {
            ttypes.MessageType.TAG_CREATE: self.encode_tag_create_message,
            ttypes.MessageType.TAG_DELETE: self.encode_tag_delete_message,
            ttypes.MessageType.WHITEBOARD_CREATE: self.encode_whiteboard_create_message,
            ttypes.MessageType.WHITEBOARD_DELETE: self.encode_whiteboard_delete_message,
            ttypes.MessageType.WHITEBOARD_CREATE_PATH: self.encode_whiteboard_create_path_message,
            ttypes.MessageType.WHITEBOARD_DELETE_PATH: self.encode_whiteboard_delete_path_message,
            ttypes.MessageType.MINUTE_CREATE: self.encode_minute_create,
            ttypes.MessageType.MINUTE_UPDATE: self.encode_minute_update,
        }

    def default(self, obj):
        if isinstance(obj, ttypes.Message):
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
            "type": ttypes.MessageType._VALUES_TO_NAMES[obj.type],
            "chatSessionToken": obj.chatSessionToken,
            "userId": obj.userId,
            "timestamp": obj.timestamp,
        }

    def encode_tag_create_message(self, message):
        message = message.tagCreateMessage
        return {
            "tagId": message.tagId,
            "minuteId": message.minuteId,
            "name": message.name,
            "tagReferenceId": message.tagReferenceId,
        }

    def encode_tag_delete_message(self, message):
        message = message.tagDeleteMessage
        return {
            "tagId": message.tagId,
        }

    def encode_whiteboard_create_message(self, message):
        message = message.whiteboardCreateMessage
        return {
            "whiteboardId": message.whiteboardId,
            "name": message.name,
        }

    def encode_whiteboard_delete_message(self, message):
        message = message.whiteboardDeleteMessage
        return {
            "whiteboardId": message.whiteboardId,
        }

    def encode_whiteboard_create_path_message(self, message):
        message = message.whiteboardCreatePathMessage
        return {
            "whiteboardId": message.whiteboardId,
            "pathId": message.pathId,
            "pathdata": message.pathData,
        }

    def encode_whiteboard_delete_path_message(self, message):
        message = message.whiteboardDeletePathMessage
        return {
            "whiteboardId": message.whiteboardId,
            "pathId": message.pathId,
        }
    
    def encode_minute_create(self, message):
        message = message.minuteCreateMessage
        return {
            "minuteId": message.minuteId,
            "topicId": message.topicId,
            "startTimestamp": message.startTimestamp,
            "endTimestamp": message.endTimestamp,
        }

    def encode_minute_update(self, message):
        message = message.minuteUpdateMessage
        return {
            "minuteId": message.minuteId,
            "topicId": message.topicId,
            "startTimestamp": message.startTimestamp,
            "endTimestamp": message.endTimestamp,
        }
