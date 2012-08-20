import logging
import unittest

import gevent

from testbase import IntegrationTestCase
 
class LoopTest(IntegrationTestCase):

    @classmethod
    def setUpClass(cls):
        IntegrationTestCase.setUpClass()

    @classmethod
    def tearDownClass(cls):
        IntegrationTestCase.tearDownClass()

    def test_getVersion(self):
        for i in range(60):
            try:
                result = self.service_proxy.getVersion(self.request_context)
                logging.info(result)
            except Exception as error:
                logging.error(str(error))
            finally:
                gevent.sleep(1)

if __name__ == '__main__':
    unittest.main()
