# Require install requests (easy_install3 requests)
import requests
import json

from t2db_worker.search import Search

# Communicate worker with database webservice
class Webservice(object):
    def __init__(self, urlbase, user, password):
        self.urlbase = urlbase
        self.urlInsertTweet = urlbase + "/tweets.json"
        self.urlInsertUser = urlbase + "/users.json"
        self.urlInsertTweetStreaming = urlbase + "/tweetstreamings.json"
        self.urlInsertTweetSearch = urlbase + "/tweetsearches.json"
        self.urlInsertStreaming = urlbase + "/streamings.json"
        self.urlInsertSearch = urlbase + "/searches.json"
        self.headers = {'content-type': 'application/json'}
        self.auth = (user, password)

    def hashList(self, objectList):
        first = True
        data = []
        for element in objectList.list:
            data.append(element.toHash())
        return data

    def postUsers(self, users):
        #Parse list
        data = self.hashList(users)
        r = requests.post(self.urlInsertUser, json.dumps(data), 
            auth = self.auth, headers = self.headers)
        if r.status_code != 200:
            raise Exception("Post users failed, status code = " +
                                str(r.status_code) + ", message = " + r.text)

    def postTweets(self, tweets):
        #Parse list
        data = self.hashList(tweets)
        r = requests.post(self.urlInsertTweet, json.dumps(data), 
            auth = self.auth, headers = self.headers)
        if r.status_code != 200:
            raise Exception("Post tweets failed, status code = " +
                                str(r.status_code) + ", message = " + r.text)

    def postStreamings(self, streamings):
        #Parse list
        data = self.hashList(streamings)
        r = requests.post(self.urlInsertStreaming, json.dumps(data), 
            auth = self.auth, headers = self.headers)
        if r.status_code != 200:
            raise Exception("Post streamings failed, status code = " + 
                                str(r.status_code) + ", message = " + r.text)

    def postSearches(self, searches):
        #Parse list
        data = self.hashList(searches)
        r = requests.post(self.urlInsertSearch, json.dumps(data), 
            auth = self.auth, headers = self.headers)
        if r.status_code != 200:
            raise Exception("Post searches failed, status code = " +
                                str(r.status_code) + ", message = " + r.text)

    def postTweetStreamings(self, tweetStreamings):
        data = self.hashList(tweetStreamings)
        r = requests.post(self.urlInsertTweetStreaming, json.dumps(data), 
            auth = self.auth, headers = self.headers)
        if r.status_code != 200:
            raise Exception("Post tweetStreamings failed, " + "status code = " +
                             str(r.status_code) + ", message = " + r.text)

    def postTweetSearches(self, tweetSearches):
        data = self.hashList(tweetSearches)
        r = requests.post(self.urlInsertTweetSearch, json.dumps(data), 
            auth = self.auth, headers = self.headers)
        if r.status_code != 200:
            raise Exception("Post tweet_search failed, " + "status code = " +
                            str(r.status_code) + ", message = " + r.text)

#Class called from worker. This class receive tweets individually and store them
#in the buffers created for that purpose. 
class WebserviceDb(object):
    def __init__(self, urlbase, user, password):
        self.service = Webservice(urlbase, user, password)
        self.sentTweets = 0
        self.sentUsers = 0
        self.sentTweetStreamings = 0
        self.sentTweetSearches = 0

    def sendData(self, tweetList, userList, tweetStreamingList, tweetSearchList):
        self.service.postUsers(userList)
        self.service.postTweets(tweetList)
        self.service.postTweetStreamings(tweetStreamingList)
        self.service.postTweetSearches(tweetSearchList)
        self.sentUsers += len(userList.list)
        self.sentTweets += len(tweetList.list)
        self.sentTweetStreamings += len(tweetStreamingList.list)
        self.sentTweetSearches += len(tweetSearchList.list)

