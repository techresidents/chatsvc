namespace java com.techresidents.services.chatsvc.gen
namespace py trchatsvc.gen

include "core.thrift"

/* Message types */

enum MessageType {
    TAG_CREATE = 100,
    TAG_DELETE = 101,
    WHITEBOARD_CREATE = 200,
    WHITEBOARD_DELETE = 201,
    WHITEBOARD_CREATE_PATH = 202,
    WHITEBOARD_DELETE_PATH = 203,
    MINUTE_CREATE = 300,
    MINUTE_UPDATE = 301,
    MARKER_CREATE = 400,
}


/* Message header */

struct MessageHeader {
    1: optional string id,
    2: MessageType type,
    3: string chatSessionToken,
    4: i32 userId,
    5: double timestamp,
}


/* Chat Markers */

enum MarkerType {
    JOINED_MARKER,
    CONNECTED_MARKER,
    PUBLISHING_MARKER,
    SPEAKING_MARKER,
}

struct JoinedMarker {
    1: i32 userId,
    2: string name,
}

struct ConnectedMarker {
    1: i32 userId,
    2: bool isConnected,
}

struct PublishingMarker {
    1: i32 userId,
    2: bool isPublishing,
}

struct SpeakingMarker {
    1: i32 userId,
    2: bool isSpeaking,
}

struct Marker {
    1: MarkerType type,
    2: optional JoinedMarker joinedMarker,
    3: optional ConnectedMarker connectedMarker,
    4: optional PublishingMarker publishingMarker,
    5: optional SpeakingMarker speakingMarker,
}


/* Chat Messages */

struct MarkerCreateMessage {
    1: optional string markerId,
    2: Marker marker,
}

struct MinuteCreateMessage {
    1: optional string minuteId, 
    2: i32 topicId,   
    3: optional i64 startTimestamp,
    4: optional i64 endTimestamp,
}

struct MinuteUpdateMessage {
    1: string minuteId, 
    2: i32 topicId,   
    3: i64 startTimestamp,
    4: optional i64 endTimestamp,
}

struct TagCreateMessage {
    1: optional string tagId, 
    2: optional i32 tagReferenceId,   
    3: string minuteId,   
    4: string name, 
}

struct TagDeleteMessage {
    1: string tagId,   
}

struct WhiteboardCreateMessage {
    1: optional string whiteboardId, 
    2: string name,
}

struct WhiteboardDeleteMessage {
    1: string whiteboardId, 
}

struct WhiteboardCreatePathMessage {
    1: string whiteboardId,
    2: optional string pathId, 
    3: string pathData,
}

struct WhiteboardDeletePathMessage {
    1: string whiteboardId,
    2: string pathId, 
}

struct Message {
    1: MessageHeader header,
    2: optional TagCreateMessage tagCreateMessage,
    3: optional TagDeleteMessage tagDeleteMessage,
    4: optional WhiteboardCreateMessage whiteboardCreateMessage,
    5: optional WhiteboardDeleteMessage whiteboardDeleteMessage,
    6: optional WhiteboardCreatePathMessage whiteboardCreatePathMessage,
    7: optional WhiteboardDeletePathMessage whiteboardDeletePathMessage,
    8: optional MinuteCreateMessage minuteCreateMessage,
    9: optional MinuteUpdateMessage minuteUpdateMessage,
    10: optional MarkerCreateMessage markerCreateMessage,
}


/* Hash Ring */

struct HashRingNode {
    1: string token,
    2: string serviceName,
    3: i32 servicePort,
    4: string hostname,
    5: string fqdn,
}


/* Service interface */

service TChatService extends core.TRService
{
    list<HashRingNode> getHashRing(1: core.RequestContext requestContext),
    
    list<HashRingNode> getPreferenceList(
            1: core.RequestContext requestContext,
            2: string chatSessionToken),

    list<Message> getMessages(
            1: core.RequestContext requestContext,
            2: string chatSessionToken,
            3: double asOf,
            4: bool block,
            5: i32 timeout),

    Message sendMessage(
            1: core.RequestContext requestContext,
            2: Message message),

    void storeReplicatedMessages(
            1: core.RequestContext requestContext,
            2: string chatSessionToken,
            3: list<Message> messages),
}
