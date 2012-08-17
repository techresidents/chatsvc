import logging
import unittest

import gevent

from trpycore.thrift.serialization import deserialize
from trsvcscore.db.models import ChatMessage
from trchatsvc.gen.ttypes import Message
from testbase import DistributedTestCase, create_chat_session, delete_chat_session, build_tag_create_message


class PersistenceTest(DistributedTestCase):

    @classmethod
    def setUpClass(cls):
        DistributedTestCase.setUpClass()
        
        cls.chat_session_token = "UNITTEST_CHAT_SESSION_TOKEN"
        cls.session = cls.service.handler.get_database_session()
        cls.chat_session = create_chat_session(
                cls.session,
                cls.chat_session_token)

    @classmethod
    def tearDownClass(cls):
        DistributedTestCase.tearDownClass()

        try:
            delete_chat_session(cls.session, cls.chat_session)
        except Exception as error:
            logging.exception(error)
        finally:
            cls.session.close()

    
    def test_persist(self):
        message = build_tag_create_message(self.chat_session_token, "UNITTEST_TAG")
        
        #basic replication
        self.service_proxy.sendMessage(
                requestContext=self.request_context,
                message=message,
                N=2,
                W=1)

        gevent.sleep(1)

        session = self.service.handler.get_database_session()
        chat_message = session.query(ChatMessage)\
                .filter_by(chat_session=self.chat_session, format_type_id=2)\
                .one()
        
        self.assertIsNotNone(chat_message)

        read_message = deserialize(Message(), chat_message.data)

        #check header
        self.assertEqual(message.header.id, read_message.header.id)
        self.assertEqual(message.header.type, read_message.header.type)
        self.assertEqual(message.header.timestamp, read_message.header.timestamp)
        self.assertEqual(message.header.userId, read_message.header.userId)
        self.assertEqual(message.header.chatSessionToken, read_message.header.chatSessionToken)
        self.assertEqual(message.header.route.type, read_message.header.route.type)
        self.assertEqual(message.header.route.recipients, read_message.header.route.recipients)

        #check tag
        self.assertEqual(message.tagCreateMessage.tagId, read_message.tagCreateMessage.tagId)
        self.assertEqual(message.tagCreateMessage.tagReferenceId, read_message.tagCreateMessage.tagReferenceId)
        self.assertEqual(message.tagCreateMessage.minuteId, read_message.tagCreateMessage.minuteId)
        self.assertEqual(message.tagCreateMessage.name, read_message.tagCreateMessage.name)

if __name__ == '__main__':
    unittest.main()
