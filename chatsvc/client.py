import logging
import sys
import time
 
from trpycore.zookeeper.client import ZookeeperClient
from trsvcscore.proxy.zookeeper import ZookeeperServiceProxy
from trsvcscore.hashring.zookeeper import ZookeeperServiceHashring

from tridlcore.gen.ttypes import RequestContext
from trchatsvc.gen import TChatService


def main(argv):
    logging.basicConfig()
    try:
        zookeeper_client = ZookeeperClient(["localdev:2181"])
        zookeeper_client.start()
        time.sleep(1)
        chatsvc = ZookeeperServiceProxy(zookeeper_client, "chatsvc", TChatService, keepalive=True)

        context = RequestContext(userId=0, impersonatingUserId=0, sessionId="sessionid", context="")

        hashring = ZookeeperServiceHashring(
                zookeeper_client=zookeeper_client,
                service_name="chatsvc",
                service_port = 9091,
                positions = [None, None, None],
                position_data = {"blah": "blah" })
        #hashring.start()
        time.sleep(1)
        
        while True:
            try:
                print chatsvc.getCounters(context)
            except Exception as error:
                print str(error)

            nodes = hashring.hashring()
            for node in nodes:
                print node.token
                print node.data
            time.sleep(3)
    
    except Exception as error:
        print str(error)
    finally:
        zookeeper_client.stop()            
        zookeeper_client.join()

if __name__ == '__main__':
    sys.exit(main(sys.argv))
