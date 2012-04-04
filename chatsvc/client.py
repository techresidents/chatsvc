import sys
 
from thrift import Thrift
from thrift.protocol import TBinaryProtocol
 

from trpycore.thrift_gevent.transport import TSocket
from tridlcore.gen.ttypes import RequestContext
from trchatsvc.gen import TChatService

def main(argv):
 transport = TSocket.TSocket('localhost', 9090)
 protocol = TBinaryProtocol.TBinaryProtocol(transport)
 client = TChatService.Client(protocol)
 transport.open()
  
 context = RequestContext(userId=0, impersonatingUserId=0, sessionId="sessionid", context="")
  
 print client.getVersion(context)
 print client.getBuildNumber(context)
 print client.getMessages(context, 0, False, 0)
 #client.shutdown(context)
 
if __name__ == '__main__':
 sys.exit(main(sys.argv))
