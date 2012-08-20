#!/usr/bin/env python

import argparse
import errno
import logging
import os
import signal
import sys
import time
import traceback

from trpycore.process.daemon import exec_daemon
from trpycore.process.pid import pid_exists


PROJECT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))

SERVICE =  os.path.basename(PROJECT_DIRECTORY)
SERVICE_DIRECTORY = os.path.join(PROJECT_DIRECTORY, SERVICE)
SERVICE_PATH = os.path.join(SERVICE_DIRECTORY, "%s.py" % SERVICE)

#Add service directory path so we can import settings
#Note that this module may need to reloaded follwing
#the setting of the SERVICE_ENV and SERVICE_INSTANCE
#environment variables to ensure proper settings
#values.
sys.path.insert(0, SERVICE_DIRECTORY)
import settings

class ManagerException(Exception):
    pass

#Operations

def start(env=None, instance=None, user=None, group=None):
    #Modify process environment and import settings.
    #It's neccessary to modify environment prior to 
    #import so that the appropriate environment
    #settings are loaded.
    if env:
        os.environ["SERVICE_ENV"] = env

    if instance:
        os.putenv("SERVICE_INSTANCE", instance)

    #reload settings with adjusted environment variables
    reload(settings)

    if instance:
        start_instance(env, instance, user, group)
    else:
        instance_options = getattr(settings, "INSTANCE_OPTIONS", [None])
        exceptions = []
        for instance_option in instance_options:
            try:
                start_instance(env, str(instance_option), user, group)
            except Exception as error:
                logging.exception(error)
                exceptions.append(error)
        
        if exceptions:
            raise ManagerException("Failed to start service")


def start_instance(env=None, instance=None, user=None, group=None):
    #Set environment variable for forked daemon
    #This will not change os.environ for current process.
    if env:
        os.environ["SERVICE_ENV"] = env

    if instance:
        os.environ["SERVICE_INSTANCE"] = instance
    
    #If env directory exists, use that python.
    #Otherwise backoff to /usr/bin/env python.
    if os.path.exists(os.path.join(PROJECT_DIRECTORY, "env")):
        python = os.path.join(PROJECT_DIRECTORY, "env", "bin", "python")
        command = [python, SERVICE_PATH]
    else:
        command = ["/usr/bin/env", "python", SERVICE_PATH]

    pid = exec_daemon(
            command[0],
            command,
            umask=None,
            working_directory=PROJECT_DIRECTORY,
            username=user,
            groupname=group)

    #Sleep for a second and do quick sanity check to make sure process is running
    time.sleep(1)
    if not pid_exists(pid):
        raise ManagerException("Failed to start service")


def stop(env=None, instance=None, block=False, timeout=None):
    #Modify process environment and import settings.
    #It's neccessary to modify environment prior to 
    #import so that the appropriate environment
    #settings are loaded.
    if env:
        os.environ["SERVICE_ENV"] = env

    if instance:
        os.environ["SERVICE_INSTANCE"] = instance
    
    #reload settings with adjusted environment variables
    reload(settings)

    if instance:
        stop_instance(env, instance, block, timeout)
    else:
        instance_options = getattr(settings, "INSTANCE_OPTIONS", [None])
        exceptions = []
        for instance_option in instance_options:
            try:
                stop_instance(env, str(instance_option), block, timeout)
            except Exception as error:
                logging.exception(error)
                exceptions.append(error)
        
        if exceptions:
            raise ManagerException("Failed to stop service")

def stop_instance(env=None, instance=None, block=False, timeout=None):
    #Modify process environment and import settings.
    #It's neccessary to modify environment prior to 
    #import so that the appropriate environment
    #settings are loaded.
    if env:
        os.environ["SERVICE_ENV"] = env

    if instance:
        os.environ["SERVICE_INSTANCE"] = instance
    
    #reload settings with adjusted environment variables
    reload(settings)
    
    #Nothing to do if pid file does not exist
    if not os.path.exists(settings.SERVICE_PID_FILE):
        logging.warning("pid file '%s' does not exist." % settings.SERVICE_PID_FILE)
        return
    
    #Open pidfile and send pid SIGTERM for graceful exit.
    with open(settings.SERVICE_PID_FILE, 'r') as pidfile:
        pid = int(pidfile.read())
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError as error:
            if error.errno == errno.EPERM:
                raise ManagerException("Failed to stop service: permission denied.")
        else:
            #If blocking wait for service to exit or timeout to be exceeded
            start_time = time.time()
            while block and pid_exists(pid):
                running_time = time.time() - start_time
                if timeout and (running_time > timeout):
                    raise ManagerException("Failed to stop service: timeout.")
                time.sleep(1)
        

def restart(env=None, instance=None, block=False, timeout=None, user=None, group=None):
    stop(env, instance, block, timeout)
    start(env, instance, user, group)


#Command handlers
def startCommandHandler(args):
    """Start service as daemon process"""
    start(args.env, args.instance, args.user, args.group)

startCommandHandler.examples = """Examples:
    manager.py start             #Start service
    manager.py --env prod start  #Start prod service
"""


def stopCommandHandler(args):
    """Stop service"""
    stop(args.env, args.instance, args.wait, args.timeout)

stopCommandHandler.examples = """Examples:
    manager.py stop                      #Stop service
    manager.py stop --wait               #Stop service (blocking)
    manager.py stop --wait --timeout 10  #Stop service (blocking w/ timeout)
"""


def restartCommandHandler(args):
    """Restart service"""
    restart(args.env, args.instance, True, args.timeout, args.user, args.group)

restartCommandHandler.examples = """Examples:
    manager.py restart               #Restart service
    manager.py restart --timeout 10  #Restart service (w/ stop timeout)
"""



def main(argv):

    def parse_arguments():
        parser = argparse.ArgumentParser(description="manager.py controls Tech Residents services")
        parser.add_argument("-e", "--env", help="service environment")

        commandParsers = parser.add_subparsers()

        #start parser
        startCommandParser = commandParsers.add_parser(
                "start",
                help="start service",
                description=startCommandHandler.__doc__,
                epilog=startCommandHandler.examples,
                formatter_class=argparse.RawDescriptionHelpFormatter
                )
        startCommandParser.set_defaults(command="start", commandHandler=startCommandHandler)
        startCommandParser.add_argument("-u", "--user", help="Drop privileges to user (also requires --group)")
        startCommandParser.add_argument("-g", "--group", help="Drop privileges to group (also requires --user)")
        startCommandParser.add_argument("-i", "--instance", help="Specific service instance to start (if not provided all instances will be started")

        #stop parser
        stopCommandParser = commandParsers.add_parser(
                "stop",
                help="stop service",
                description=stopCommandHandler.__doc__,
                epilog=stopCommandHandler.examples,
                formatter_class=argparse.RawDescriptionHelpFormatter
                )
        stopCommandParser.set_defaults(command="stop", commandHandler=stopCommandHandler)
        stopCommandParser.add_argument("-w", "--wait", action="store_true", help="Wait for service to stop.")
        stopCommandParser.add_argument("-t", "--timeout", type=int, help="Wait timeout in seconds.")
        stopCommandParser.add_argument("-i", "--instance", help="Specific service instance to stop (if not provided all instances will be stopped")

        #restart parser
        restartCommandParser = commandParsers.add_parser(
                "restart",
                help="restart service",
                description=restartCommandHandler.__doc__,
                epilog=restartCommandHandler.examples,
                formatter_class=argparse.RawDescriptionHelpFormatter
                )
        restartCommandParser.set_defaults(command="restart", commandHandler=restartCommandHandler)
        restartCommandParser.add_argument("-t", "--timeout", default="15", type=int, help="Timeout in seconds for service to stop.")
        restartCommandParser.add_argument("-u", "--user", help="Drop privileges to user (also requires --group)")
        restartCommandParser.add_argument("-g", "--group", help="Drop privileges to group (also requires --user)")
        restartCommandParser.add_argument("-i", "--instance", help="Specific service instance to restart (if not provided all instances will be restarted")

        return parser.parse_args(argv[1:])


    #configure logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setLevel(logging.INFO)
    logger.addHandler(consoleHandler)
    
    log = logging.getLogger("main")

    args = parse_arguments()

    try:
        #Invoke command handler
        args.commandHandler(args)

        return 0
    
    except ManagerException as error:
        log.error(str(error))
        return 1

    except KeyboardInterrupt:
        return 2 
    
    except Exception as error:
        log.error("Unhandled exception: %s" % str(error))
        log.error(traceback.format_exc())
        return 3 

if __name__ == '__main__':
    sys.exit(main(sys.argv))
    
