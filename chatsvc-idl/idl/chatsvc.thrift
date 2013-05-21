namespace java com.techresidents.services.chatsvc.gen
namespace py trchatsvc.gen

include "core.thrift"

/* Exceptions */

exception UnavailableException {
    1: string fault
}

exception InvalidChatException {
    1: string fault
}

exception InvalidMessageException {
    1: string fault
}

/* Message types */

enum MessageType {
    USER_STATE,
    CHAT_STATE
}

/* Message route types */
enum MessageRouteType {
    NO_ROUTE,
    BROADCAST_ROUTE,
    TARGETED_ROUTE
}

/* MessageRoute */

struct MessageRoute {
    1: MessageRouteType type,
    2: optional list<i32> recipients
}

/* Message header */

struct MessageHeader {
    1: optional string id,
    2: MessageType type,
    3: string chatToken,
    4: i32 userId,
    5: double timestamp,
    6: MessageRoute route
}

/* User State */

enum UserState {
    UNAVAILABLE,
    AVAILABLE,
    CONNECTED
}

/* Chat State */
enum ChatState {
    PENDING,
    STARTED,
    ENDED
}

/* Chat Messages */

struct UserStateMessage {
    1: i32 userId,
    2: UserState state
}

struct ChatStateMessage {
    1: i32 userId,
    2: ChatState state
}


struct Message {
    1: MessageHeader header,
    2: optional UserStateMessage userStateMessage,
    3: optional ChatStateMessage chatStateMessage
}


/* Hashring */

struct HashringNode {
    1: string token,
    2: string serviceName,
    3: string serviceAddress,
    4: i32 servicePort,
    5: string hostname,
    6: string fqdn
}

/* Replication */

/* User */
struct User {
    1: i32 userId,
    2: UserState state
}

/* Chat */
struct Chat {
    1: string token,
    2: ChatState state,
    3: double startTimestamp,
    4: double endTimestamp,
    5: list<User> users,
    6: list<Message> messages
}

struct ChatSnapshot {
    1: bool fullSnapshot,
    2: Chat chat 
}


/* Service interface */

service TChatService extends core.TRService
{
    list<HashringNode> getHashring(1: core.RequestContext requestContext),
    
    list<HashringNode> getPreferenceList(
            1: core.RequestContext requestContext,
            2: string chatToken),

    list<Message> getMessages(
            1: core.RequestContext requestContext,
            2: string chatToken,
            3: double asOf,
            4: bool block,
            5: i32 timeout) throws (
                1:UnavailableException unavailableException,
                2:InvalidChatException invalidChatException)

    Message sendMessage(
            1: core.RequestContext requestContext,
            2: Message message,
            3: i32 N,
            4: i32 W) throws (
                1:UnavailableException unavailableException,
                2:InvalidChatException invalidChatException, 
                3:InvalidMessageException invalidMessageException), 

    string twilioRequest(
            1: core.RequestContext requestContext,
            2: string path
            3: map<string, string> params) throws (
                1:UnavailableException unavailableException,
                2:InvalidChatException invalidChatException)

    void replicate(
            1: core.RequestContext requestContext,
            2: ChatSnapshot chatSnapshot),

    bool expireZookeeperSession(
            1: core.RequestContext requestContext,
            2: i32 timeout),
}
