import logging
import unittest

import gevent

from testbase import DistributedTestCase, create_chat_session, delete_chat_session, build_tag_create_message



class ReplicationTest(DistributedTestCase):

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

    
    def test_replication(self):
        chat_session = self.service.handler.chat_sessions_manager.get(self.chat_session_token)
        chat_session2 = self.service2.handler.chat_sessions_manager.get(self.chat_session_token)

        length = len(chat_session.messages)
        length2 = len(chat_session2.messages)

        message = build_tag_create_message(self.chat_session_token, "UNITTEST_TAG")
        
        #basic replication
        self.service_proxy.sendMessage(
                requestContext=self.request_context,
                message=message,
                N=2,
                W=1)

        gevent.sleep(1)
        
        self.assertEqual(len(chat_session.messages), length+1)
        self.assertEqual(len(chat_session2.messages), length2+1)
  
    def test_replication_reconnection(self):
        chat_session = self.service.handler.chat_sessions_manager.get(self.chat_session_token)
        chat_session2 = self.service2.handler.chat_sessions_manager.get(self.chat_session_token)

        length = len(chat_session.messages)
        length2 = len(chat_session2.messages)

        message = build_tag_create_message(self.chat_session_token, "UNITTEST_TAG")
        
        #basic replication
        self.service_proxy.sendMessage(
                requestContext=self.request_context,
                message=message,
                N=2,
                W=1)

        gevent.sleep(1)
        
        self.assertEqual(len(chat_session.messages), length+1)
        self.assertEqual(len(chat_session2.messages), length2+1)
        self.service.handler.hashring.stop()
        self.service.handler.hashring.join()
        gevent.sleep(1)
        self.service.handler.chat_sessions_manager.remove(self.chat_session_token)
        self.service.handler.hashring.start()
        gevent.sleep(1)

        chat_session = self.service.handler.chat_sessions_manager.get(self.chat_session_token)
        self.assertEqual(len(chat_session.messages), length+1)

if __name__ == '__main__':
    unittest.main()
