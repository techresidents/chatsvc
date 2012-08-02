"""Test settings which support running multiple instances of chatsvc locally.

To run multiple instances:

    $ SERVICE_ENV=test SERVICE_INSTANCE=0 python chatsvc/chatsvc.py
    $ SERVICE_ENV=test SERVICE_INSTANCE=1 python chatsvc/chatsvc.py
    $ SERVICE_ENV=test SERVICE_INSTANCE=2 python chatsvc/chatsvc.py
"""

import hashlib
import os
import socket

ENV = os.getenv("SERVICE_ENV", "default")

INSTANCE_OPTIONS = range(0, 5)
INSTANCE = int(os.getenv("SERVICE_INSTANCE", 0))

#Service Settings
SERVICE = "chatsvc"
SERVICE_PID_FILE = "%s.%s-%s.pid" % (SERVICE, ENV, INSTANCE)
#SERVICE_HOSTNAME = socket.gethostname()
SERVICE_HOSTNAME = "localhost"
SERVICE_FQDN = socket.gethostname()

#Server settings
#THRIFT_SERVER_ADDRESS = socket.gethostname()
THRIFT_SERVER_ADDRESS = "localhost"
THRIFT_SERVER_INTERFACE = "0.0.0.0"
THRIFT_SERVER_PORT = 9090 + INSTANCE

#Zookeeper settings
ZOOKEEPER_HOSTS = ["localdev:2181"]

#Mongrel settings
MONGREL_SENDER_ID = "chatsvc_" + hashlib.md5(THRIFT_SERVER_ADDRESS+str(THRIFT_SERVER_PORT)).hexdigest()
MONGREL_PUB_ADDR = "tcp://localdev:9996"
MONGREL_PULL_ADDR = "tcp://localdev:9997"

#Riak settings
RIAK_HOST = "localdev"
RIAK_PORT = 8087
RIAK_SESSION_BUCKET = "tr_sessions"
RIAK_SESSION_POOL_SIZE = 4

#Chat settings
CHAT_LONG_POLL_WAIT = 10
CHAT_ALLOW_REQUEST_FORWARDING = True

#Replication settings
REPLICATION_N = 3 
REPLICATION_W = 2
REPLICATION_POOL_SIZE = 20
REPLICATION_TIMEOUT = 10
REPLICATION_MAX_CONNECTIONS_PER_SERVICE = 1
REPLICATION_ALLOW_SAME_HOST = True
REPLICATION_TIMEOUT = 5

#Logging settings
LOGGING = {
    "version": 1,

    "formatters": {
        "brief_formatter": {
            "format": "%(levelname)s: %(message)s"
        },

        "long_formatter": {
            "format": "%(asctime)s %(levelname)s: %(name)s %(message)s"
        }
    },

    "handlers": {

        "console_handler": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "brief_formatter",
            "stream": "ext://sys.stdout"
        },

        "file_handler": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "formatter": "long_formatter",
            "filename": "%s.%s-%s.log" % (SERVICE, ENV, INSTANCE),
            "when": "midnight",
            "interval": 1,
            "backupCount": 7
        }
    },
    
    "root": {
        "level": "DEBUG",
        "handlers": ["console_handler", "file_handler"]
    }
}
