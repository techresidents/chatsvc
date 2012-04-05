#!/usr/bin/env python

import logging
import logging.config
import signal
import sys
import gevent

import settings

from trpycore.process.pid import pidfile, PidFileException
from trsvcscore.service_gevent.base import GMongrel2Service
from trchatsvc.gen import TChatService

from handler import ChatServiceHandler


class ChatService(GMongrel2Service):
    def __init__(self):

        handler = ChatServiceHandler()

        super(ChatService, self).__init__(
                name=settings.SERVICE,
                interface=settings.SERVER_INTERFACE,
                port=settings.SERVER_PORT,
                handler=handler,
                processor=TChatService.Processor(handler),
                mongrel2_sender_id=settings.MONGREL_SENDER_ID,
                mongrel2_pull_addr=settings.MONGREL_PULL_ADDR,
                mongrel2_pub_addr=settings.MONGREL_PUB_ADDR)
 
def main(argv):
    try:
        with pidfile(settings.SERVICE_PID_FILE, create_directory=True):

            #Configure logger
            logging.config.dictConfig(settings.LOGGING)
            
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
