from pymongo import Connection
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import datetime

connection = Connection('localhost', 27017)
db = connection.TwitterStream
db.tweets.ensure_index("id", unique=True, dropDups=True)
collection = db.tweets

keywords = ['#politics', '#Democrats', '#Trump']

language = ['en']
consumer_key = "ADD YOUR CONSUMER KEY HERE"
consumer_secret = "ADD YOUR CONSUMER SECRET HERE"
access_token = "ADD YOUR ACCESS TOKEN HERE"
access_token_secret = "ADD YOUR ACCESS TOKEN SECRET HERE"

class StdOutListener(StreamListener):

    def on_data(self, data):

        t = json.loads(data)

        tweet_id = t['id_str']  # The Tweet ID from Twitter in string format
        username = t['user']['screen_name']  # The username of the Tweet author
        followers = t['user']['followers_count']  # The number of followers the Tweet author has
        text = t['text']  # The entire body of the Tweet
        hashtags = t['entities']['hashtags']  # Any hashtags used in the Tweet
        dt = t['created_at']  # The timestamp of when the Tweet was created
        language = t['lang']  # The language of the Tweet

        created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')

        tweet = {'id':tweet_id, 'username':username, 'followers':followers, 'text':text, 'hashtags':hashtags, 'language':language, 'created':created}

        collection.save(tweet)

        print username + ':' + ' ' + text
        return True

    def on_error(self, status):
        print status

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=keywords, languages=language)
