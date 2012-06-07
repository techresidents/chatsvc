import json
import uuid

import trchatsvc.gen.ttypes as ttypes
from trpycore.timezone import tz

from marker import MarkerFactory, MarkerEncoder

class MessageFactory(object):
    def __init__(self):
        self.message_type_map = {
            ttypes.MessageType.MARKER_CREATE: self.marker_create_message,
            ttypes.MessageType.MINUTE_CREATE: self.minute_create_message,
            ttypes.MessageType.MINUTE_UPDATE: self.minute_update_message,
            ttypes.MessageType.TAG_CREATE: self.tag_create_message,
            ttypes.MessageType.TAG_DELETE: self.tag_delete_message,
            ttypes.MessageType.WHITEBOARD_CREATE: self.whiteboard_create_message,
            ttypes.MessageType.WHITEBOARD_DELETE: self.whiteboard_delete_message,
            ttypes.MessageType.WHITEBOARD_CREATE_PATH: self.whiteboard_create_path_message,
            ttypes.MessageType.WHITEBOARD_DELETE_PATH: self.whiteboard_delete_path_message,
        }

    def create(self, header, msg):
        type = ttypes.MessageType._NAMES_TO_VALUES.get(header.get("type"))
        factory_method = self.message_type_map.get(type)
        return factory_method(header, msg)

    def create_header(self, header):    
        return ttypes.MessageHeader(
                id=uuid.uuid4().hex,
                type=ttypes.MessageType._NAMES_TO_VALUES.get(header.get("type")),
                chatSessionToken=header.get("chatSessionToken"),
                userId=header.get("userId"),
                timestamp=tz.timestamp())
    
    def marker_create_message(self, header, msg):
        header = self.create_header(header)
        message = ttypes.MarkerCreateMessage(
                markerId=uuid.uuid4().hex,
                marker=MarkerFactory.create(msg.get("marker")))
        return ttypes.Message(header=header, markerCreateMessage=message)

    def minute_create_message(self, header, msg):
        header = self.create_header(header)
        message = ttypes.MinuteCreateMessage(
                minuteId=uuid.uuid4().hex,
                topicId=msg.get("topicId"),
                startTimestamp=tz.timestamp())
        return ttypes.Message(header=header, minuteCreateMessage=message)

    def minute_update_message(self, header, msg):
        header = self.create_header(header)
        message = ttypes.MinuteUpdateMessage(
                minuteId=msg.get("minuteId"),
                topicId=msg.get("topicId"),
                startTimestamp=msg.get("startTimestamp"),
                endTimestamp=tz.timestamp())
        return ttypes.Message(header=header, minuteUpdateMessage=message)

    def tag_create_message(self, header, msg):
        header = self.create_header(header)
        message = ttypes.TagCreateMessage(
                tagId=uuid.uuid4().hex,
                minuteId=msg.get("minuteId"),
                name=msg.get("name"),
                tagReferenceId=msg.get("tagReferenceId"))
        return ttypes.Message(header=header, tagCreateMessage=message)

    def tag_delete_message(self, header, msg):
        header = self.create_header(header)
        message = ttypes.TagDeleteMessage(tagId=msg.get("tagId"))
        return ttypes.Message(header=header, tagDeleteMessage=message)

    def whiteboard_create_message(self, header, msg):
        header = self.create_header(header)
        message = ttypes.WhiteboardCreateMessage(
                whiteboardId=uuid.uuid4().hex,
                name=msg.get("name"))
        return ttypes.Message(header=header, whiteboardCreateMessage=message)

    def whiteboard_delete_message(self, header, msg):
        header = self.create_header(header)
        message = ttypes.WhiteboardDeleteMessage(whiteboardId=msg.get("whiteboardId"))
        return ttypes.Message(header=header, whiteboardDeleteMessage=message)

    def whiteboard_create_path_message(self, header, msg):
        header = self.create_header(header)
        message = ttypes.WhiteboardCreatePathMessage(
                whiteboardId=msg.get("whiteboardId"),
                pathId=uuid.uuid4().hex,
                pathData=msg.get("pathData"))
        return ttypes.Message(header=header, whiteboardCreatePathMessage=message)

    def whiteboard_delete_path_message(self, header, msg):
        header = self.create_header(header)
        message = ttypes.WhiteboardDeletePathMessage(
                whiteboardId=msg.get("whiteboardId"),
                pathId=msg.get("pathId"))
        return ttypes.Message(header=header, whiteboardDeletePathMessage=message)



class MessageEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        super(MessageEncoder, self).__init__(*args, **kwargs)
        self.marker_encoder = MarkerEncoder()

        self.message_type_encoder = {
            ttypes.MessageType.MARKER_CREATE: self.encode_marker_create_message,
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
        elif isinstance(obj, ttypes.Marker):
            return self.marker_encoder.default(obj)
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

    def encode_marker_create_message(self, message):
        message = message.markerCreateMessage
        return {
            "markerId": message.markerId,
            "marker": message.marker,
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
            "pathData": message.pathData,
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
