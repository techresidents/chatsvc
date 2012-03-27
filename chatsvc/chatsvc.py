#!/usr/bin/env python

import logging
import logging.config
import os
import signal
import sys
import gevent

from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


from trpycore.mongrel2_gevent.handler import Connection

from trpycore.thrift_gevent.server import TGeventServer
from trpycore.thrift_gevent.transport import TSocket
from trchatsvc.gen import TChatService

from trpycore.process.pid import pidfile, PidFileException

import settings
from handler import ChatServiceHandler


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))

class ChatService(object):
    def __init__(self):
        self.running = False
        self.handler = ChatServiceHandler()
        self.greenlet = None
        self.mongrel2_greenlet = None
    
    def start(self):
        if not self.running:
            self.running = True
            self.greenlet = gevent.spawn(self.run)
            self.mongrel2_greenlet = gevent.spawn(self.run_mongrel2)
    
    def run(self):
        processor = TChatService.Processor(self.handler)
        transport = TSocket.TServerSocket(settings.SERVER_HOST, settings.SERVER_PORT)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        server = TGeventServer(processor, transport, tfactory, pfactory)
        server.serve()
    
    def run_mongrel2(self):
        connection = Connection(
                settings.MONGREL_SENDER_ID,
                settings.MONGREL_PULL_ADDR,
                settings.MONGREL_PUB_ADDR)

        while True:
            request = connection.recv()
            gevent.spawn(self.handler.handle, connection, request)

    
    def stop(self):
        if self.running:
            self.running = False

            self.greenlet.kill()
            self.greenlet = None

            self.mongrel2_greenlet.kill()
            self.mongrel2_greenlet = None
    
    def join(self):
        if self.greenlet is not None:
            self.greenlet.join()

        if self.mongrel2_greenlet is not None:
            self.mongrel2_greenlet.join()
 
def main(argv):
    try:
        with pidfile(settings.SERVICE_PID_FILE, create_directory=True):
            
            #Configure logger
            logging.config.dictConfig(settings.LOGGING)
            
            #Create service
            service = ChatService()
            
            def signal_handler():
                service.stop()
            
            gevent.signal(signal.SIGTERM, signal_handler);

            service.start()
            service.join()
    
    except PidFileException as error:
        logging.error("Service is already running: %s" % str(error))

    except KeyboardInterrupt:
        service.stop()
        service.join()

if __name__ == '__main__':
    sys.exit(main(sys.argv))
