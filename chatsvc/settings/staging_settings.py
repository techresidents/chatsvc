import hashlib
import socket

from default_settings import *

ENV = "staging"

#Server settings
SERVER_HOST = socket.gethostname()
SERVER_INTERFACE = "0.0.0.0"
SERVER_PORT = 9090

#Service settings
SERVICE_PID_FILE = "/opt/tr/data/%s/pid/%s.%s.pid" % (SERVICE, SERVICE, ENV)

#Zookeeper settings
ZOOKEEPER_HOSTS = ["localhost:2181"]

#Mongrel settings
MONGREL_SENDER_ID = "chatsvc_" + hashlib.sha1(SERVER_HOST+str(SERVER_PORT)).hexdigest()
MONGREL_PUB_ADDR = "tcp://localhost:9996"
MONGREL_PULL_ADDR = "tcp://localhost:9997"

#Riak settings
RIAK_HOST = "localhost"
RIAK_PORT = 8087
RIAK_SESSION_BUCKET = "tr_sessions"
RIAK_SESSION_POOL_SIZE = 4

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
            "level": "ERROR",
            "class": "logging.StreamHandler",
            "formatter": "brief_formatter",
            "stream": "ext://sys.stdout"
        },

        "file_handler": {
            "level": "INFO",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "formatter": "long_formatter",
            "filename": "/opt/tr/data/%s/logs/%s.%s.log" % (SERVICE, SERVICE, ENV),
            "when": "midnight",
            "interval": 1,
            "backupCount": 7
        }
    },
    
    "root": {
        "level": "INFO",
        "handlers": ["console_handler", "file_handler"]
    }
}
