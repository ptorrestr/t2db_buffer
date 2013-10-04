import unittest

from t2db_buffer.communicator import Webservice
from t2db_buffer.communicator import WebserviceDb
from t2db_objects.objects import Tweet
from t2db_objects.objects import User
from t2db_objects.objects import Streaming
from t2db_objects.objects import Search
from t2db_objects.objects import TweetStreaming
from t2db_objects.objects import TweetSearch
from t2db_objects.objects import ObjectList
from t2db_objects.tests.common import randomTweet
from t2db_objects.tests.common import randomUser
from t2db_objects.tests.common import randomStreaming
from t2db_objects.tests.common import randomSearch
from t2db_objects.tests.common import randomTweetStreaming
from t2db_objects.tests.common import randomTweetSearch
from t2db_objects.tests.common import randomInteger

def generateOneUser(userId):
    user = User(randomUser(userId, "1-10-2013", "pablo"))
    userList = ObjectList()
    userList.append(user)
    return userList

def generateOneTweet(tweetId, userId):
    tweet = Tweet(randomTweet(tweetId, "1-10-2013", userId))
    tweetList = ObjectList()
    tweetList.append(tweet)
    return tweetList

def generateOneStreaming(streamingId):
    streaming = Streaming(randomStreaming(streamingId))
    streamingList = ObjectList()
    streamingList.append(streaming)
    return streamingList

def generateOneSearch(searchId):
    search = Search(randomSearch(searchId))
    searchList = ObjectList()
    searchList.append(search)
    return searchList

def generateOneTweetStreaming(tweetId, streamingId):
    tweetStreaming = TweetStreaming(randomTweetStreaming(tweetId, streamingId))
    tweetStreamingList = ObjectList()
    tweetStreamingList.append(tweetStreaming)
    return tweetStreamingList

def generateManyUsers(randomUsers):
    userList = ObjectList()
    for i in range(0, randomUsers):
        user = User(randomUser(i, "1-10-2013", "pablo"+str(i)))
        userList.append(user)
    return userList

def generateManyTweets(randomTweets, userId):
    tweetList = ObjectList()
    for i in range(0, randomTweets):
        tweet = Tweet(randomTweet(i, "1-10-2013", userId))
        tweetList.append(tweet)
    return tweetList

def generateManyStreamings(randomStreamings):
    streamingList = ObjectList()
    for i in range(0, randomStreamings):
        streaming = Streaming(randomStreaming(i))
        streamingList.append(streaming)
    return streamingList
    
def generateManyTweetStreamings(randomTweetStreamings, tweetId, streamingId):
    tweetStreamingList = ObjectList()
    for i in range(0, randomTweetStreamings):
        tweetStreaming = TweetStreaming(randomTweetStreaming(tweetId, streamingId))
        tweetStreamingList.append(tweetStreaming)
    return tweetStreamingList

def generateManyTweetSearches(randomTweetSearches, tweetId, searchId):
    tweetSearchList = ObjectList()
    for i in range(0, randomTweetSearches):
        tweetSearch = TweetSearch(randomTweetSearch(tweetId, searchId))
        tweetSearchList.append(tweetSearch)
    return tweetSearchList

class TestWebservice(unittest.TestCase):
    def setUp(self):
        self.ws = Webservice("http://localhost:8000", "quiltro", "perroCallejero")
        
    def test_postOneUser(self):
        self.ws.postUsers(generateOneUser(userId = 1))

    def test_postManyUsers(self):
        self.ws.postUsers(generateManyUsers(randomInteger(100)))

    def test_postOneTweet(self):
        self.ws.postUsers(generateOneUser(userId = 1))
        self.ws.postTweets(generateOneTweet(tweetId = 1 , userId = 1))

    def test_postManyTweets(self):
        self.ws.postUsers(generateOneUser(userId = 1))
        self.ws.postTweets(generateManyTweets(randomInteger(100), userId = 1))

    def test_postOneStreaming(self):
        self.ws.postStreamings(generateOneStreaming(streamingId = 1))

    def test_postManyStreamings(self):
        self.ws.postStreamings(generateManyStreamings(randomInteger(100)))

    def test_postOneTweetStreaming(self):
        self.ws.postUsers(generateOneUser(userId = 1))
        self.ws.postTweets(generateOneTweet(tweetId = 1, userId = 1))
        self.ws.postStreamings(generateOneStreaming(streamingId = 1))
        self.ws.postTweetStreamings(generateOneTweetStreaming(tweetId = 1, 
            streamingId = 1))

    def test_postManyTweetStreamings(self):
        self.ws.postUsers(generateOneUser(userId = 1))
        self.ws.postTweets(generateOneTweet(tweetId = 1, userId = 1))
        self.ws.postStreamings(generateOneStreaming(streamingId = 1))
        self.ws.postTweetStreamings(generateManyTweetStreamings(
            randomInteger(100), tweetId = 1, streamingId = 1 ))

    def test_postOneSearch(self):
        self.ws.postSearches(generateOneSearch(searchId = 1))

    def test_postManyTweetSearches(self):
        self.ws.postUsers(generateOneUser(userId = 1))
        self.ws.postTweets(generateOneTweet(tweetId = 1, userId = 1))
        self.ws.postSearches(generateOneSearch(searchId = 1))
        self.ws.postTweetSearches(generateManyTweetSearches(
            randomInteger(100), tweetId = 1, searchId = 1 ))

class TestWebserviceDb(unittest.TestCase):
    def setUp(self):
        self.wsDb = WebserviceDb("http://localhost:8000", "quiltro", "perroCallejero")
        self.wsDb.service.postStreamings(generateOneStreaming(streamingId = 1))
        self.wsDb.service.postSearches(generateOneSearch(searchId = 1))
        
    def test_sendData(self):
        userList = generateOneUser(userId = 1)
        tweetList = generateManyTweets(randomInteger(100), userId = 1)
        tweetStreamingList = ObjectList()
        tweetSearchList = ObjectList()
        for tweet in tweetList.getList():
            tweetStreaming = TweetStreaming(randomTweetStreaming(tweet.id, 1))
            tweetStreamingList.append(tweetStreaming)
            tweetSearch = TweetSearch(randomTweetSearch(tweet.id, 1))
            tweetSearchList.append(tweetSearch)
        self.wsDb.sendData(tweetList, userList, tweetStreamingList, tweetSearchList)

