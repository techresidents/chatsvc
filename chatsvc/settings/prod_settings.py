import hashlib
import socket

from default_settings import *

ENV = "prod"

#Service Settings
SERVICE = "chatsvc"
SERVICE_PID_FILE = "/opt/tr/data/%s/pid/%s.%s.pid" % (SERVICE, SERVICE, ENV)
SERVICE_HOSTNAME = socket.gethostname()
SERVICE_FQDN = socket.gethostname()

#Thrift Server settings
THRIFT_SERVER_ADDRESS = socket.gethostname()
THRIFT_SERVER_INTERFACE = "0.0.0.0"
THRIFT_SERVER_PORT = 9090

#Zookeeper settings
ZOOKEEPER_HOSTS = ["localhost:2181"]

#Mongrel settings
MONGREL_SENDER_ID = "chatsvc_" + hashlib.md5(THRIFT_SERVER_ADDRESS+str(THRIFT_SERVER_PORT)).hexdigest()
MONGREL_PUB_ADDR = "tcp://localhost:9996"
MONGREL_PULL_ADDR = "tcp://localhost:9997"

#Riak settings
RIAK_HOST = "localhost"
RIAK_PORT = 8087
RIAK_SESSION_BUCKET = "tr_sessions"
RIAK_SESSION_POOL_SIZE = 4

#Chat settings
CHAT_LONG_POLL_WAIT = 10    
CHAT_ALLOW_REQUEST_FORWARDING = True

#Replication settings
REPLICATION_N = 1
REPLICATION_W = 1
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

