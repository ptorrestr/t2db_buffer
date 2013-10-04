import signal
import argparse
import sys
import logging

from threading import Lock
from threading import Thread
from threading import Timer

from t2db_buffer import communicator
from t2db_objects import psocket
from t2db_objects import utilities
from t2db_objects import objects
from t2db_worker import buffer_communicator as bc

# create logger
logger = logging.getLogger('Buffer')
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


# Send data to the DB in fixed time periods.
def timer(localBuffer, communicator):
    # Get data and clean. Thread-safe operation
    bufferCopy = localBuffer.getDataAndClean()
    # Send data
    if len(bufferCopy.tweetList.list) > 0 :
        communicator.sendData(bufferCopy.tweetList, bufferCopy.userList,
            bufferCopy.tweetStreamingList, bufferCopy.tweetSearchList)
        logger.debug("Sent tweets: " + str(communicator.sentTweets))
        logger.debug("Sent users: " + str(communicator.sentUsers))
        logger.debug("Sent tweetStreaming: " + str(communicator.sentTweetStreamings))
        logger.debug("Sent tweetSearch: " + str(communicator.sentTweetSearches))

class GlobalBuffer(object):
    def __init__(self):
        self.localBuffer = bc.LocalBuffer()
        self.localBufferLock = Lock()

    def saveData(self, tweetList, userList, tweetStreamingList, tweetSearchList):
        self.localBufferLock.acquire()
        try:
            for tweet in tweetList.getList():
                try:
                    self.localBuffer.addTweet(tweet)
                except Exception as e:
                    logger.warn(str(e))
            
            for user in userList.getList():
                try:
                    self.localBuffer.addUser(user)
                except Exception as e:
                    logger.warn(str(e))

            for tweetStreaming in tweetStreamingList.getList():
                try:
                    self.localBuffer.addTweetStreaming(tweetStreaming)
                except Exception as e:
                    logger.warn(str(e))

            for tweetSearch in tweetSearchList.getList():
                try:
                    self.localBuffer.addTweetSearch(tweetSearch)
                except Exception as e:
                    logger.warn(str(e))
        finally:
            self.localBufferLock.release()

    # Get data and clean buffer. Thread-safe
    def getDataAndClean(self):
        self.localBufferLock.acquire()
        try:
            localBufferCopy = self.localBuffer
            self.localBuffer = bc.LocalBuffer()
        finally:
            self.localBufferLock.release()
        return localBufferCopy

def worker(socketControl, globalBuffer):
    try:
        tweetList = socketControl.recvObject()
        userList = socketControl.recvObject()
        tweetStreamingList = socketControl.recvObject()
        tweetSearchList = socketControl.recvObject()
    except Exception as e:
        logger.error(str(e))
    finally:
        socketControl.close()
    logger.debug("Received tweets in thread: " + str(len(tweetList.list)))
    logger.debug("Received users in thread: " + str(len(userList.list)))
    logger.debug("Received tweetStreaming in thread: " + str(len(tweetStreamingList.list)))
    logger.debug("Received tweetSearch in thread: " + str(len(tweetSearchList.list)))
    globalBuffer.saveData(tweetList, userList, tweetStreamingList, tweetSearchList)

# Wrapper thread of worker function
class WorkerThread(Thread):
    def __init__(self, *args):
        self.args = args
        Thread.__init__(self)

    def run(self):
        worker(*self.args)

# This function controls the interacction among the buffer server and the
# workers
def server(bufferServer):
    logger.info("Server thread started")
    bufferServer.startTimer()
    logger.info("Timer started")
    while not bufferServer.stopEvent.isSet():
        try:
            socketControl = bufferServer.socketServer.accept()
            # Start thread to attend new connection
            worker = WorkerThread(socketControl, bufferServer.globalBuffer)
            worker.start()
            # Sequentially, remove for parallel
            worker.join()
        except:
            logger.warn("Timeout incoming connection")
    #TODO:wait for child threads
    #Send signal to reporter. Server ends
    bufferServer.stopTimer()
    logger.info("Timer stoped")
    bufferServer.barrier.wait()
    bufferServer.socketServer.close()
    logger.info("Server thread finished")

class ServerThread(Thread):
    def __init__(self, *args):
        self.args = args
        Thread.__init__(self)
    
    def run(self):
        server(*self.args)

class BufferServer(object):
    def __init__(self, socketPort, maxConnection, stopEvent, barrier, timeout,
            seconds, host, user, password):
        self.socketPort = socketPort
        self.maxConnection = maxConnection
        self.stopEvent = stopEvent
        self.barrier = barrier
        self.globalBuffer = GlobalBuffer()
        self.communicator = communicator.WebserviceDb(host, user, password)
        self.timerThread = Timer(seconds, timer, [self.globalBuffer,
            self.communicator])
        try:
            #create a socket server
            logger.info("Starting Socket Server")
            self.socketServer = psocket.SocketServer(socketPort, maxConnection)
            self.socketServer.setTimeout(timeout)
            self.hostName = self.socketServer.getHostName()
        except Exception as e:
            raise Exception("Could not create socket server:" + str(e))
            
    def getHostName(self):
        return self.hostName

    def startTimer(self):
        self.timerThread.start()

    def stopTimer(self):
        self.timerThread.cancel()

    def start(self):
        serverThread = ServerThread(self)
        serverThread.start()

# Global variables for signal_handler function
gStopEvent = None
gBarrier = None
gFinalise = None

# Main Function
def startBuffer(config):
    stopEvent = Event()
    barrier = Barrier(2)
    finalise = True
    setGlobalVariable(stopEvent, barrier, finalise)
    # Call server function
    server(config.socket_port, config.max_connection, stopEvent, barrier, config.timeout, config.timer_seconds, config.urldatabase, config.user, config.password)

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
    print ("You pressed Ctrl+C!, stoping")
    gStopEvent.set()
    gBarrier.wait()
    if gFinalise:
        sys.exit(0)
        
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
    try:
        rawConfigurationNoFormat = utilities.readConfigFile(args.config)
        rawConfiguration = objects.formatHash(rawConfigurationNoFormat
            , configurationFields)
        configuration = objects.Configuration(configurationFields
            , rawConfiguration)
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
