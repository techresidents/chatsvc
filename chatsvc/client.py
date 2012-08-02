import logging
import sys
import time
 
from trpycore.zookeeper.client import ZookeeperClient
from trsvcscore.proxy.zookeeper import ZookeeperServiceProxy

from tridlcore.gen.ttypes import RequestContext
from trchatsvc.gen import TChatService


def main(argv):
    logging.basicConfig(level=logging.DEBUG)
    try:
        zookeeper_client = ZookeeperClient(["localdev:2181"])
        zookeeper_client.start()
        time.sleep(1)
        chatsvc = ZookeeperServiceProxy(zookeeper_client, "chatsvc", TChatService, keepalive=True)

        context = RequestContext(userId=0, impersonatingUserId=0, sessionId="sessionid", context="")

        while True:
            try:
                print chatsvc.getHashring(context)
                print "\n"
                print chatsvc.getPreferenceList(context, '0')
            except Exception as error:
                print str(error)

            time.sleep(3)
    
    except Exception as error:
        print str(error)
    finally:
        zookeeper_client.stop()            
        zookeeper_client.join()

if __name__ == '__main__':
    sys.exit(main(sys.argv))
