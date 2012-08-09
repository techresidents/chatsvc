import unittest
import logging
import time
 
from trpycore.zookeeper.client import ZookeeperClient
from trsvcscore.proxy.zookeeper import ZookeeperServiceProxy

from tridlcore.gen.ttypes import RequestContext
from trchatsvc.gen import TChatService

class LoopTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.service_name = "chatsvc"
        cls.service_class = TChatService

        cls.zookeeper_client = ZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        time.sleep(1)

        cls.service = ZookeeperServiceProxy(cls.zookeeper_client, cls.service_name, cls.service_class, keepalive=True)
        cls.request_context = RequestContext(userId=0, impersonatingUserId=0, sessionId="dummy_session_id", context="")

        logging.basicConfig(level=logging.DEBUG)

    @classmethod
    def tearDownClass(cls):
        cls.zookeeper_client.stop()
        cls.zookeeper_client.join()

    def test_getVersion(self):
        for i in range(60):
            try:
                result = self.service.getVersion(self.request_context)
                logging.info(result)
            except Exception as error:
                logging.error(str(error))
            finally:
                time.sleep(1)

if __name__ == '__main__':
    unittest.main()
