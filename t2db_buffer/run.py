import logging
import signal
import argparse
import sys

from threading import Event
from threading import Barrier

from t2db_objects import utilities
from t2db_objects import objects
from t2db_objects import daemon

from t2db_buffer.buffer import BufferServer

# create logger
logger = logging.getLogger('Run')
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

# Main Function
def startBuffer(config):
    stopEvent = Event()
    barrier = Barrier(3)#Three threads. Main, Server, Signal
    finalise = True
    setGlobalVariable(stopEvent, barrier, finalise)
    # Call server function
    bs = BufferServer(config.socket_port, config.max_connection, 
            stopEvent, barrier, config.timeout, config.timer_seconds, 
            config.urldatabase, config.user, config.password)
    bs.start()
    barrier.wait()

# Global variables for signal_handler function
gStopEvent = None
gBarrier = None
gFinalise = None
gFirst = True

def setGlobalVariable(stopEvent, barrier, finalise):
    global gStopEvent
    global gBarrier
    global gFinalise
    gStopEvent = stopEvent
    gBarrier = barrier
    gFinalise = finalise

## this function controls SIGINT signal (Ctrl+C).
def signal_handler(signal, frame):
    global gStopEvent
    global gBarrier
    global gFinalise
    global gFirst
    if gFirst:
        logger.info ("You pressed Ctrl+C!, stoping")
        gFirst = False
        gStopEvent.set()
        logger.info ("StopEvent triggered")
        logger.debug("Waiting in barrier: " + str(gBarrier))
        gBarrier.wait()
        if gFinalise:
            sys.exit(0)

def readConfigFile(args):
    ## Create configuration
    configurationFields = [
        {"name":"urldatabase", "kind":"mandatory", "type":str},
        {"name":"user", "kind":"mandatory", "type":str},
        {"name":"password", "kind":"mandatory", "type":str},
        {"name":"timer_seconds", "kind":"mandatory", "type":int},
        {"name":"socket_port", "kind":"mandatory", "type":int},
        {"name":"max_connection", "kind":"mandatory", "type":int},
        {"name":"timeout", "kind":"mandatory", "type":int},
        ]
    rawConfigurationNoFormat = utilities.readConfigFile(args.config)
    rawConfiguration = objects.formatHash(rawConfigurationNoFormat
        , configurationFields)
    configuration = objects.Configuration(configurationFields
        , rawConfiguration)
    return configuration
        
def main():
    ## Start signal detection
    signal.signal(signal.SIGINT, signal_handler)

    ## Parser input arguments
    parser = argparse.ArgumentParser()
    # positionals
    parser.add_argument('--config',
        help = 'The configure file path for buffer server',
        type = str,
        required = True)

    args = parser.parse_args()

    try:
        configuration = readConfigFile(args)
    except Exception as e:
        logger.error("Program configuration failed: " + str(e))
        sys.exit(1)
    
    ## Start program!
    try:
        startBuffer(configuration)
    except Exception as e:
        logger.error("Program end unexpectely: " + str(e))
        sys.exit(2)
    logger.info("Program ended!")
    ## End program!
    sys.exit(0)

## Class mydaemon. 
class MyDaemon(daemon.Daemon):
    def run(self, *args):
        startBuffer(args[0][0])

## Main daemon
def main_daemon():
    ## Start signal detection
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Daemon mode")

    # Default options
    default_pidfile = '/tmp/t2db-buffer.pid'
    default_stdin = '/dev/null'
    default_stdout = '/tmp/t2db-buffer.stdout'
    default_stderr = '/tmp/t2db-buffer.stdout'

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', 
        help = 'Running mode: console, start, stop, restart',
        choices = ["console", "start", "stop", "restart"],
        type = str,
        required = True)
    parser.add_argument('--config',
        help = 'The configure file path for buffer server',
        type = str,
        required = True)
    #optionals
    parser.add_argument('--pidfile', 
        help='Pid file. Default = '+default_pidfile,
        default = default_pidfile,
        required=False)
    parser.add_argument('--stdin',
        help='Stdin file. Default = '+default_stdin,
        default = default_stdin,
        required=False)
    parser.add_argument('--stdout',
        help='Stdout file. Default = '+default_stdout,
        default = default_stdout,
        required=False)
    parser.add_argument('--stderr', 
        help='Stderr file. Default = '+default_stderr, 
        default = default_stderr,
        required=False)
    args = parser.parse_args()

    try:
        configuration = readConfigFile(args)
    except Exception as e:
        logger.error("Program configuration failed: " + str(e))
        sys.exit(1)

    mode = args.m
    daemon = MyDaemon(args.pidfile, args.stdin, args.stdout, args.stderr)

    if 'start' == mode:
        daemon.start(configuration)
    elif 'stop' == mode:
        daemon.stop()
    elif 'restart' == mode:
        daemon.restart(configuration)
    elif 'console' == mode:
        daemon.console(configuration)
    else:
        logger.error("Unknown mode: " + mode)
        sys.exit(3)
    logger.info("Finishing daemon tool")
    sys.exit(0)
