#!/usr/bin/env python

import logging
import logging.config
import os
import signal
import sys
import gevent

#Add PROJECT_ROOT to python path, for version import.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.insert(0, PROJECT_ROOT)

import settings
import version

from trpycore.process.pid import pidfile, PidFileException
from trsvcscore.service_gevent.default import GDefaultService
from trsvcscore.service_gevent.server.default import GThriftServer
from trsvcscore.service_gevent.server.mongrel2 import GMongrel2Server
from trchatsvc.gen import TChatService

from handler import ChatServiceHandler, ChatMongrel2Handler


class ChatService(GDefaultService):
    """Chat service."""
    def __init__(self):
        handler = ChatServiceHandler(self)

        server = GThriftServer(
                name="%s-thrift" % settings.SERVICE,
                interface=settings.THRIFT_SERVER_INTERFACE,
                port=settings.THRIFT_SERVER_PORT,
                handler=handler,
                processor=TChatService.Processor(handler),
                address=settings.THRIFT_SERVER_ADDRESS)

        mongrel2_handler = ChatMongrel2Handler(handler)
        mongrel2_server = GMongrel2Server(
                name="%s-mongrel" % settings.SERVICE,
                mongrel2_sender_id=settings.MONGREL_SENDER_ID,
                mongrel2_pull_addr=settings.MONGREL_PULL_ADDR,
                mongrel2_pub_addr=settings.MONGREL_PUB_ADDR,
                handler=mongrel2_handler)

        super(ChatService, self).__init__(
                name=settings.SERVICE,
                version=version.VERSION,
                build=version.BUILD,
                servers=[server, mongrel2_server],
                hostname=settings.SERVICE_HOSTNAME,
                fqdn=settings.SERVICE_FQDN)
 
def main(argv):
    try:
        #Configure logger
        logging.config.dictConfig(settings.LOGGING)

        with pidfile(settings.SERVICE_PID_FILE, create_directory=True):

            
            #Create service
            service = ChatService()
            
            def sigterm_handler():
                service.stop()

            gevent.signal(signal.SIGTERM, sigterm_handler);

            service.start()
            service.join()
        
    except PidFileException as error:
        logging.error("Service is already running: %s" % str(error))

    except KeyboardInterrupt:
        service.stop()
        service.join()

if __name__ == '__main__':
    sys.exit(main(sys.argv))
