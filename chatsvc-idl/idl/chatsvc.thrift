namespace java com.techresidents.services.chatsvc.gen
namespace py trchatsvc.gen

include "core.thrift"

enum MessageType {
    TAG = 0,
    WHITEBOARD = 1,
}

struct MessageHeader {
    1: i32 id,
    2: MessageType type,
    3: string chatSessionToken,
    4: i32 userId,
    5: i64 timestamp,
}

struct TagMessage {
    1: string name,   
}

struct WhiteboardMessage {
    1: string data,
}

struct Message {
    1: MessageHeader header,
    2: optional TagMessage tagMessage,
    3: optional WhiteboardMessage whiteboardMessage,
}

service TChatService extends core.TRService
{
    list<Message> getMessages(
            1: core.RequestContext requestContext,
            2: string chatSessionToken,
            3: i64 asOf,
            4: bool block,
            5: i32 timeout),

    Message sendMessage(1: core.RequestContext requestContext, 2: Message message),
}
