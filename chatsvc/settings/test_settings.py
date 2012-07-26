import hashlib
import os
import socket

ENV = os.getenv("SERVICE_ENV", "default")

INSTANCE_OPTIONS = range(0, 5)
INSTANCE = int(os.getenv("SERVICE_INSTANCE", 0))

#Service Settings
SERVICE = "chatsvc"
SERVICE_PID_FILE = "%s.%s-%s.pid" % (SERVICE, ENV, INSTANCE)

#Server settings
SERVER_HOST = socket.gethostname()
SERVER_INTERFACE = "0.0.0.0"
SERVER_PORT = 9090 + INSTANCE

#Zookeeper settings
ZOOKEEPER_HOSTS = ["localdev:2181"]

#Mongrel settings
MONGREL_SENDER_ID = "chatsvc_" + hashlib.sha1(SERVER_HOST+str(SERVER_PORT)).hexdigest()
MONGREL_PUB_ADDR = "tcp://localdev:9996"
MONGREL_PULL_ADDR = "tcp://localdev:9997"

#Riak settings
RIAK_HOST = "localdev"
RIAK_PORT = 8087
RIAK_SESSION_BUCKET = "tr_sessions"
RIAK_SESSION_POOL_SIZE = 4

#Chat settings
CHAT_LONG_POLL_WAIT = 10

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
