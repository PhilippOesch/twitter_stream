import logging

from Streaming_Architekture.Consumer.TweetConsumer import TweetConsumer
from Streaming_Architekture.general import utils
from Streaming_Architekture.general.Neo4JHelper import Neo4JHelper
from Streaming_Architekture.general.project_dataclasses import Tweet


class PrimaryTweetConsumer(TweetConsumer):
    def __init__(self, kafka_server, topic, database: Neo4JHelper):
        print("PrimaryTweetConsumer started")
        super().__init__("tweet_group", kafka_server, topic, database)

    def process_message(self, message):
        print(message)
        logging.info("PrimaryTweetConsumer called")
        tweet: Tweet = utils.message_to_tweet(message)
        tweet.user.type = "PrimaryUser"
        self.process_tweet(tweet)