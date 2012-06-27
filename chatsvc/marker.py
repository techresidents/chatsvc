import json

import trchatsvc.gen.ttypes as ttypes

def create_joined_marker(marker):
    return ttypes.Marker(
            type=ttypes.MarkerType.JOINED_MARKER,
            connectedMarker=ttypes.JoinedMarker(
                userId=marker.get("userId"),
                name=marker.get("name")
                )
            )

def create_connected_marker(marker):
    return ttypes.Marker(
            type=ttypes.MarkerType.CONNECTED_MARKER,
            connectedMarker=ttypes.ConnectedMarker(
                userId=marker.get("userId"),
                isConnected=marker.get("isConnected")
                )
            )

def create_publishing_marker(marker):
    return ttypes.Marker(
            type=ttypes.MarkerType.PUBLISHING_MARKER,
            publishingMarker=ttypes.PublishingMarker(
                userId=marker.get("userId"),
                isPublishing=marker.get("isPublishing")
                )
            )

def create_speaking_marker(marker):
    return ttypes.Marker(
            type=ttypes.MarkerType.SPEAKING_MARKER,
            speakingMarker=ttypes.SpeakingMarker(
                userId=marker.get("userId"),
                isSpeaking=marker.get("isSpeaking")
                )
            )


MARKER_TYPE_MAP = {
    ttypes.MarkerType.JOINED_MARKER: create_joined_marker,
    ttypes.MarkerType.CONNECTED_MARKER: create_connected_marker,
    ttypes.MarkerType.PUBLISHING_MARKER: create_publishing_marker,
    ttypes.MarkerType.SPEAKING_MARKER: create_speaking_marker,
}

class MarkerFactory(object):
    @staticmethod
    def create(marker):
        marker_type = ttypes.MarkerType._NAMES_TO_VALUES[marker.get("type")]
        return MARKER_TYPE_MAP[marker_type](marker)


class MarkerEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        super(MarkerEncoder, self).__init__(*args, **kwargs)

        self.marker_type_encoder = {
            ttypes.MarkerType.JOINED_MARKER: self.encode_joined_marker,
            ttypes.MarkerType.CONNECTED_MARKER: self.encode_connected_marker,
            ttypes.MarkerType.PUBLISHING_MARKER: self.encode_publishing_marker,
            ttypes.MarkerType.SPEAKING_MARKER: self.encode_speaking_marker,
        }

    def default(self, obj):
        if isinstance(obj, ttypes.Marker):
            return self.encode_marker(obj)
        else:
            return super(MarkerEncoder, self).default(obj)
    
    def encode_marker(self, marker):
        return self.marker_type_encoder[marker.type](marker);
    
    def encode_joined_marker(self, marker):
        type = ttypes.MarkerType._VALUES_TO_NAMES[marker.type]
        marker = marker.connectedMarker
        return {
            "type": type,
            "userId": marker.userId,
            "name": marker.name,
        }

    def encode_connected_marker(self, marker):
        type = ttypes.MarkerType._VALUES_TO_NAMES[marker.type]
        marker = marker.connectedMarker
        return {
            "type": type,
            "userId": marker.userId,
            "isConnected": marker.isConnected,
        }

    def encode_publishing_marker(self, marker):
        type = ttypes.MarkerType._VALUES_TO_NAMES[marker.type]
        marker = marker.publishingMarker
        return {
            "type": type,
            "userId": marker.userId,
            "isPublishing": marker.isPublishing,
        }

    def encode_speaking_marker(self, marker):
        type = ttypes.MarkerType._VALUES_TO_NAMES[marker.type]
        marker = marker.speakingMarker
        return {
            "type": type,
            "userId": marker.userId,
            "isSpeaking": marker.isSpeaking,
        }
