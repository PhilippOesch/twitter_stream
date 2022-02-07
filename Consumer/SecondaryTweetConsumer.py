import logging

from Consumer.TweetConsumer import TweetConsumer
from general import utils, sentiment_analysis
from general.Neo4JHelper import Neo4JHelper
from general.project_dataclasses import Tweet


class SecondaryTweetConsumer(TweetConsumer):
    def __init__(self, kafka_server, topic, database: Neo4JHelper):
        super().__init__(kafka_server, topic, database)
        print("SecondaryTweetConsumer started")

    def process_message(self, message):
        logging.info("SecondaryTweetConsumer called")
        tweet: Tweet = utils.message_to_tweet(message)
        tweet.user.type = "SecondaryUser"
        tweet.user.tweet_count += 1
        self.database.create_or_merge_user(tweet.user)
        polarity = sentiment_analysis.get_sentiment(tweet.full_text)
        self.create_relationships(tweet, polarity)
