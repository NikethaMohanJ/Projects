from kafka import KafkaProducer
import kafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
access_token = '1252722715955548160-dBVuFALTKVNi8z9l77qWbMJBFAyvV8'
access_secret = 'R15nEzcogN6XAOuPFS7PUFPKGSLgSUNk18t2pdGXRH7SF'
consumer_key = 'hN1eepcm80EnLiEGzYOTqfnSP'
consumer_secret = 'a6bQ3h2C8CHy5UwdVBmOFj7gR6UD31ywrijKHNkY4aoBNoy00j'

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Get Producer that has topic name is Twitter
   
    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.producer.send("twitter", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

# Produce Data that has trump hashtag (Tweets)
# twitter_stream.filter(track=['#trump'])
twitter_stream.filter(track=['#coronavirus'])
