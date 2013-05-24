import logging
import unittest

import gevent

from testbase import DistributedTestCase, create_chat, delete_chat, build_user_status_message


class ReplicationTest(DistributedTestCase):

    @classmethod
    def setUpClass(cls):
        DistributedTestCase.setUpClass()
        
        cls.chat_token = "UNITTEST_CHAT_TOKEN"
        cls.session = cls.service.handler.get_database_session()
        cls.chat = create_chat(cls.session, cls.chat_token)

    @classmethod
    def tearDownClass(cls):
        DistributedTestCase.tearDownClass()

        try:
            delete_chat(cls.session, cls.chat)
        except Exception as error:
            logging.exception(error)
        finally:
            cls.session.close()

    
    def test_replication(self):
        chat = self.service.handler.chat_manager.get(self.chat_token)
        chat2 = self.service2.handler.chat_manager.get(self.chat_token)

        length = len(chat.state.messages)
        length2 = len(chat2.state.messages)

        message = build_user_status_message(self.chat_token)
        
        self.service_proxy.sendMessage(
                requestContext=self.request_context,
                message=message,
                N=2,
                W=1)

        message2 = build_user_status_message(self.chat_token)
        self.service_proxy.sendMessage(
                requestContext=self.request_context,
                message=message2,
                N=2,
                W=1)

        gevent.sleep(1)
        
        self.assertEqual(len(chat.state.messages), length+2)
        self.assertEqual(len(chat2.state.messages), length2+2)
  
    def test_replication_reconnection(self):
        chat = self.service.handler.chat_manager.get(self.chat_token)
        chat2 = self.service2.handler.chat_manager.get(self.chat_token)

        length = len(chat.state.messages)
        length2 = len(chat2.state.messages)

        message = build_user_status_message(self.chat_token)
        
        #basic replication
        self.service_proxy.sendMessage(
                requestContext=self.request_context,
                message=message,
                N=2,
                W=1)

        gevent.sleep(1)
        
        self.assertEqual(len(chat.state.messages), length+1)
        self.assertEqual(len(chat2.state.messages), length2+1)
        self.service.handler.hashring.stop()
        self.service.handler.hashring.join()
        gevent.sleep(1)
        self.service.handler.chat_manager.remove(self.chat_token)
        self.service.handler.hashring.start()
        gevent.sleep(1)

        chat = self.service.handler.chat_manager.get(self.chat_token)
        self.assertEqual(len(chat.state.messages), length+1)

if __name__ == '__main__':
    unittest.main()
