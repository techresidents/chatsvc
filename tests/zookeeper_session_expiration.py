import unittest

from testbase import IntegrationTestCase

class ZookeeperSessionExpirationTest(IntegrationTestCase):

    @classmethod
    def setUpClass(cls):
        IntegrationTestCase.setUpClass()

    @classmethod
    def tearDownClass(cls):
        IntegrationTestCase.tearDownClass()

    def test_expireZookeeperSession(self):
        expired = self.service_proxy.expireZookeeperSession(self.request_context, 10)
        self.assertEqual(expired, True)

if __name__ == '__main__':
    unittest.main()
