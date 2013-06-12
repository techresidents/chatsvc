import logging
import unittest
import os
import sys

import gevent

#Use test environment to support replication.
os.environ["SERVICE_ENV"] = "test"

SERVICE_NAME = "chatsvc"

#Add PROJECT_ROOT to python path, for version import.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.insert(0, PROJECT_ROOT)

#Add SERVICE_ROOT to python path, for imports.
SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../", SERVICE_NAME))
sys.path.insert(0, SERVICE_ROOT)

from tridlcore.gen.ttypes import RequestContext
from trpycore.timezone import tz
from trpycore.zookeeper_gevent.client import GZookeeperClient
from trsvcscore.db.models import Chat
from trsvcscore.proxy.zoo import ZookeeperServiceProxy
from trsvcscore.service_gevent.default import GDefaultService
from trsvcscore.service_gevent.server.default import GThriftServer
from trchatsvc.gen import TChatService
from trchatsvc.gen.ttypes import MessageHeader, MessageType, Message, \
        UserStatusMessage, UserStatus, MessageRoute, MessageRouteType

from handler import ChatServiceHandler

class ChatService(GDefaultService):
    def __init__(self, hostname, port):
        self.handler = ChatServiceHandler(self)

        self.server = GThriftServer(
                name="%s-thrift" % SERVICE_NAME,
                interface="0.0.0.0",
                port=port,
                handler=self.handler,
                processor=TChatService.Processor(self.handler),
                address=hostname)

        super(ChatService, self).__init__(
                name=SERVICE_NAME,
                version="unittest-version",
                build="unittest-build",
                servers=[self.server],
                hostname=hostname,
                fqdn=hostname)
 

class IntegrationTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

        cls.service = ChatService("localhost", 9090)
        cls.service.start()
        gevent.sleep(1)

        cls.service_name = SERVICE_NAME
        cls.service_class = TChatService

        cls.zookeeper_client = GZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        gevent.sleep(1)

        cls.service_proxy = ZookeeperServiceProxy(cls.zookeeper_client, cls.service_name, cls.service_class, keepalive=True)
        cls.request_context = RequestContext(userId=0, impersonatingUserId=0, sessionId="dummy_session_id", context="")


    @classmethod
    def tearDownClass(cls):
        cls.zookeeper_client.stop()
        cls.zookeeper_client.join()
        cls.service.stop()
        cls.service.join()


class DistributedTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

        cls.service = ChatService("localhost", 9090)
        cls.service.start()
        gevent.sleep(1)

        cls.service2 = ChatService("localhost", 9091)
        cls.service2.start()
        gevent.sleep(1)

        cls.service_name = SERVICE_NAME
        cls.service_class = TChatService

        cls.zookeeper_client = GZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        gevent.sleep(1)

        cls.service_proxy = ZookeeperServiceProxy(cls.zookeeper_client, cls.service_name, cls.service_class, keepalive=True)
        cls.request_context = RequestContext(userId=0, impersonatingUserId=0, sessionId="dummy_session_id", context="")


    @classmethod
    def tearDownClass(cls):
        cls.zookeeper_client.stop()
        cls.zookeeper_client.join()
        cls.service.stop()
        cls.service.join()
        cls.service2.stop()
        cls.service2.join()


#Helper methods
def delete_chat(session, chat):
    session.delete(chat)
    session.commit()

def create_chat(session, token):
    chat = session.query(Chat)\
            .filter_by(token=token)\
            .first()

    if chat is not None:
        delete_chat(session, chat)
    
    chat = Chat(topic_id=1, max_participants=1, token=token)
    session.add(chat)
    session.commit()
    return chat

def build_user_status_message(token, status=UserStatus.AVAILABLE, userId=1):
    header = MessageHeader(
            type=MessageType.USER_STATUS, 
            chatToken=token,
            userId=1,
            timestamp=tz.timestamp(),
            route=MessageRoute(MessageRouteType.BROADCAST_ROUTE)
            )

    message = Message(
            header=header,
            userStatusMessage=UserStatusMessage(userId=userId, status=status)
            )

    return message
