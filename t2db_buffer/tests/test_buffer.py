import unittest
import time
import _thread as thread
from threading import Barrier
from threading import Lock
from threading import Event

from t2db_buffer.buffer import GlobalBuffer
from t2db_buffer.buffer import BufferServer
from t2db_objects.objects import Tweet
from t2db_objects.objects import User
from t2db_objects.objects import TweetStreaming
from t2db_objects.objects import TweetSearch
from t2db_objects.objects import Streaming
from t2db_objects.objects import Search
from t2db_objects.objects import ObjectList
from t2db_objects.tests.common import randomInteger
from t2db_objects.tests.common import randomTweetStreaming
from t2db_objects.tests.common import randomTweetSearch
from t2db_objects.tests.common import randomStreaming
from t2db_objects.tests.common import randomSearch
from t2db_worker.parser import ParserStatus
from t2db_worker.buffer_communicator import BufferCommunicator
from t2db_worker.buffer_communicator import LocalBuffer
from t2db_worker.tests.test_parser import getOneStatus

getOneStatusLock = Lock()

def getOneStatusTS():
    getOneStatusLock.acquire()
    try:
        status = getOneStatus()
    finally:
        getOneStatusLock.release()
    return status

def addOneElement(sharedList):
    status = getOneStatusTS()
    ps = ParserStatus(status)
    tweet = Tweet(ps.getTweet())
    sharedList.addElement(tweet)

def addManyElements(sharedList, randomElements):
    status = getOneStatusTS()
    localList = []
    for i in range(0, randomElements):
        ps = ParserStatus(status)
        tweet = Tweet(ps.getTweet())
        user = User(ps.getUser())
        localList.append(tweet)
        localList.append(user)
    sharedList.addManyElements(localList)

def oneThread(barrier, fun, *args):
    fun(*args)
    barrier.wait()

def oneThreadUpSync(barrier, fun, *args):
    barrier.wait()
    fun(*args)

def oneThreadDoubleSync(barrier1, barrier2, fun, *args):
    barrier1.wait()
    fun(*args)
    barrier2.wait()

def createData(base):
    status = getOneStatusTS()
    randomTweets = base + randomInteger(99) + 1
    tweetList = ObjectList()
    userList = ObjectList()
    streamingList = ObjectList()
    searchList = ObjectList()
    for i in range(base, randomTweets):
        status["id"] = i
        status["user"]["id"] = i
        ps = ParserStatus(status)
        tweet = Tweet(ps.getTweet())
        user = User(ps.getUser())
        tweetList.append(tweet)
        userList.append(user)
        streamingList.append(TweetStreaming(randomTweetStreaming(i, 1)))
        searchList.append(TweetSearch(randomTweetSearch(i, 1)))
    return tweetList, userList, streamingList, searchList

sharedListDataLock = Lock()
sharedListData = []
idNumber = 0

def fakeClient(host, port):
    global idNumber
    global sharedListDataLock
    global sharedListData
    sharedListDataLock.acquire()
    try:
        [tweetList, userList, streamingList, searchList] = createData(idNumber)
        idNumber += len(tweetList.list)
    finally:
        sharedListDataLock.release()
    bc = BufferCommunicator(host, port)
    bc.sendData(tweetList, userList, streamingList, searchList)
    sharedListDataLock.acquire()
    try:
        sharedListData.append(tweetList)
        sharedListData.append(userList)
        sharedListData.append(streamingList)
        sharedListData.append(searchList)
    finally:
        sharedListDataLock.release()
"""
class TestSharedElementList(unittest.TestCase):
    def setUp(self):
        self.sharedList = SharedElementList()

    def test_addElement(self):
        addOneElement(self.sharedList)
        self.assertEqual(len(self.sharedList.elementList), 1)

    def test_addManyElements(self):
        randomElements = randomInteger(100)
        addManyElements(self.sharedList, randomElements)
        self.assertEqual(len(self.sharedList.elementList), randomElements*2)

    def test_addTwoThreads(self):
        barrier = Barrier(2)
        thread.start_new_thread(oneThread, (barrier, addOneElement, self.sharedList,))
        addOneElement(self.sharedList)
        barrier.wait()
        self.assertEqual(len(self.sharedList.elementList), 2)

    def test_addTwoThreadsManyElements(self):
        barrier = Barrier(2)
        randomElements = randomInteger(100)
        thread.start_new_thread(oneThread, (barrier, addManyElements, self.sharedList,randomElements,))
        addManyElements(self.sharedList, randomElements)
        barrier.wait()
        totalElements = randomElements*2*2
        self.assertEqual(len(self.sharedList.elementList), totalElements)

    def test_addManyThreadsManyElements(self):
        randomThreads = randomInteger(8) + 2 #Always graeter or equal than 2
        barrier = Barrier(randomThreads + 1)# Include main thread
        randomElements = randomInteger(100)
        for i in range(0, randomThreads):
            thread.start_new_thread(oneThread, (barrier, addManyElements, self.sharedList, randomElements,))
        barrier.wait()
        totalElements = randomElements*randomThreads*2
        self.assertEqual(len(self.sharedList.elementList), totalElements)

    def test_addGetAllElementsAndClean(self):
        randomElements = randomInteger(100)
        addManyElements(self.sharedList, randomElements)
        copyElementList = self.sharedList.getAllElementsAndClean()
        self.assertEqual(len(self.sharedList.elementList), 0)
        self.assertEqual(len(copyElementList), randomElements*2)

    def test_addGetAllElementsAndCleanWhileAdding(self):
        barrier1 = Barrier(2)
        barrier2 = Barrier(2)
        randomElements = randomInteger(100)
        thread.start_new_thread(oneThreadDoubleSync, (barrier1, barrier2, addManyElements, self.sharedList,randomElements,))
        barrier1.wait()
        copyElementList = self.sharedList.getAllElementsAndClean()
        barrier2.wait()
        totalElements = len(copyElementList) + len(self.sharedList.elementList)
        self.assertEqual(randomElements*2, totalElements)

"""
def countData(lb, tweetList, userList, tweetStreamingList, tweetSearchList):
    # count originals
    for tweet in tweetList.list:
        try:
            lb.addTweet(tweet)
        except:
            continue
    for user in userList.list:
        try:
            lb.addUser(user)
        except:
            continue
    for tweetStreaming in tweetStreamingList.list:
        try:
            lb.addTweetStreaming(tweetStreaming)
        except:
            continue
    for tweetSearch in tweetSearchList.list:
        try:
            lb.addTweetSearch(tweetSearch)
        except:
            continue
    return lb

class TestServer(unittest.TestCase):
    def setUp(self):
        global sharedListData
        sharedListData = []

    def test_serverOneClient(self):
        global sharedListData
        # Create event
        stopEvent = Event()
        # Create server barrier
        sBarrier = Barrier(2)
        # Create server
        bs = BufferServer(13001, 5, stopEvent, sBarrier, 5, 5, "http://localhost:8000", "quiltro", "perroCallejero")
        streamingList = ObjectList()
        streamingList.append(Streaming(randomStreaming(1)))
        bs.communicator.service.postStreamings(streamingList)
        searchList = ObjectList()
        searchList.append(Search(randomSearch(1)))
        bs.communicator.service.postSearches(searchList)
        bs.start()
        # Create barrier for client
        cBarrier = Barrier(2)
        # Create client
        thread.start_new_thread(oneThread, (cBarrier, fakeClient, bs.getHostName(), 13001,))
        cBarrier.wait()
        time.sleep(5)
        # Stop server
        stopEvent.set()
        # Wait for server
        sBarrier.wait()
        time.sleep(5)
        # Get data and compare
        numberTweets = len(bs.globalBuffer.localBuffer.tweetList.list)
        numberUsers = len(bs.globalBuffer.localBuffer.userList.list)
        numberTweetStreaming = len(bs.globalBuffer.localBuffer.tweetStreamingList.list)
        numberTweetSearch = len(bs.globalBuffer.localBuffer.tweetSearchList.list)
        self.assertEqual(numberTweets, 0)
        self.assertEqual(numberUsers, 0)
        self.assertEqual(numberTweetStreaming, 0)
        self.assertEqual(numberTweetSearch, 0)

        # count originals
        lb = LocalBuffer()
        lb = countData(lb, sharedListData[0], sharedListData[1], sharedListData[2]
            , sharedListData[3])
        originalNumberTweets = len(lb.tweetList.list)
        originalNumberUsers = len(lb.userList.list)
        originalNumberTweetStreaming = len(lb.tweetStreamingList.list)
        originalNumberTweetSearch = len(lb.tweetSearchList.list)
        self.assertEqual(originalNumberTweets, bs.communicator.sentTweets)
        self.assertEqual(originalNumberUsers, bs.communicator.sentUsers)
        self.assertEqual(originalNumberTweetStreaming, bs.communicator.sentTweetStreamings)
        self.assertEqual(originalNumberTweetSearch, bs.communicator.sentTweetSearches)

    def test_serverFiveOrLessClient(self):
        global sharedListData
        # Create stop event
        stopEvent = Event()
        # Create server barrier
        sBarrier = Barrier(2)
        # Create server
        bs = BufferServer(13001, 5, stopEvent, sBarrier, 5, 5, "http://localhost:8000", "quiltro", "perroCallejero")
        streamingList = ObjectList()
        streamingList.append(Streaming(randomStreaming(1)))
        bs.communicator.service.postStreamings(streamingList)
        searchList = ObjectList()
        searchList.append(Search(randomSearch(1)))
        bs.communicator.service.postSearches(searchList)
        bs.start()
        # Create barrier for N clients
        randomClients = randomInteger(3) + 2
        cBarrier = Barrier(randomClients + 1)
        # Create N clients
        for i in range(0, randomClients):
            thread.start_new_thread(oneThread, (cBarrier, fakeClient, 
                bs.getHostName(), 13001, ))
        cBarrier.wait()
        time.sleep(5)
        # stop server
        stopEvent.set()
        # Wait for server
        sBarrier.wait()
        time.sleep(5)
        # Get data and compare
        numberTweets = len(bs.globalBuffer.localBuffer.tweetList.list)
        numberUsers = len(bs.globalBuffer.localBuffer.userList.list)
        numberTweetStreaming = len(bs.globalBuffer.localBuffer.tweetStreamingList.list)
        numberTweetSearch = len(bs.globalBuffer.localBuffer.tweetSearchList.list)
        self.assertEqual(numberTweets, 0)
        self.assertEqual(numberUsers, 0)
        self.assertEqual(numberTweetStreaming, 0)
        self.assertEqual(numberTweetSearch, 0)
        # count originals
        originalNumberTweets = 0
        originalNumberUsers = 0
        originalNumberTweetStreaming = 0
        originalNumberTweetSearch = 0
        lb = LocalBuffer()
        for i in range(0, randomClients):
            lb = countData(lb, sharedListData[i*4 + 0], sharedListData[i*4 + 1],
                sharedListData[i*4 + 2], sharedListData[i*4 + 3])
        originalNumberTweets += len(lb.tweetList.list)
        originalNumberUsers += len(lb.userList.list)
        originalNumberTweetStreaming += len(lb.tweetStreamingList.list)
        originalNumberTweetSearch += len(lb.tweetSearchList.list)
        self.assertEqual(originalNumberTweets, bs.communicator.sentTweets)
        self.assertEqual(originalNumberUsers, bs.communicator.sentUsers)
        self.assertEqual(originalNumberTweetStreaming, bs.communicator.sentTweetStreamings)
        self.assertEqual(originalNumberTweetSearch, bs.communicator.sentTweetSearches)

