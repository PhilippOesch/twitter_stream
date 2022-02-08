import logging

from Consumer.TweetConsumer import TweetConsumer
from general import utils, sentiment_analysis
from general.Neo4JHelper import Neo4JHelper
from general.project_dataclasses import Tweet


class SecondaryTweetConsumer(TweetConsumer):
    def __init__(self, kafka_server, topic, database: Neo4JHelper):
        print("SecondaryTweetConsumer started")
        super().__init__(kafka_server, topic, database)

    def process_message(self, message):
        print(message)
        logging.info("SecondaryTweetConsumer called")
        tweet: Tweet = utils.message_to_tweet(message)
        tweet.user.type = "SecondaryUser"
        self.process_tweet(tweet)
