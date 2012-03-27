#!/usr/bin/env python

ENV = "default"

#Service Settings
SERVICE = "chatsvc"
SERVICE_PID_FILE = "%s.%s.pid" % (SERVICE, ENV)

#Server settings
SERVER_HOST = "localhost"
SERVER_PORT = "9090"

#Mongrel settings
MONGREL_SENDER_ID = "sender_id"
MONGREL_PUB_ADDR = "tcp://localdev:9996"
MONGREL_PULL_ADDR = "tcp://localdev:9997"

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
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "formatter": "long_formatter",
            "filename": "%s.%s.log" % (SERVICE, ENV),
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
