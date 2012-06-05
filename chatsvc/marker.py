import json

import trchatsvc.gen.ttypes as ttypes

def create_speaking_marker(marker):
    return ttypes.Marker(
            type=ttypes.MarkerType.SPEAKING_MARKER,
            speakingMarker=ttypes.SpeakingMarker(
                userId=marker.get("userId"),
                isSpeaking=marker.get("isSpeaking")
                )
            )

MARKER_TYPE_MAP = {
    ttypes.MarkerType.SPEAKING_MARKER: create_speaking_marker
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
            ttypes.MarkerType.SPEAKING_MARKER: self.encode_speaking_marker,
        }

    def default(self, obj):
        if isinstance(obj, ttypes.Marker):
            return self.encode_marker(obj)
        else:
            return super(MarkerEncoder, self).default(obj)
    
    def encode_marker(self, marker):
        return self.marker_type_encoder[marker.type](marker);

    def encode_speaking_marker(self, marker):
        type = ttypes.MarkerType._VALUES_TO_NAMES[marker.type]
        marker = marker.speakingMarker
        return {
            "type": type,
            "userId": marker.userId,
            "isSpeaking": marker.isSpeaking,
        }
