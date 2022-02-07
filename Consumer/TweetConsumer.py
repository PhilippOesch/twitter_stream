import json
import logging

import dacite

from kafka import KafkaConsumer

import env_vars
from general import utils, sentiment_analysis
from general.Neo4JHelper import Neo4JHelper
from general.project_dataclasses import Tweet, Relationship, User


class TweetConsumer:
    def __init__(self, kafka_server, topic, database: Neo4JHelper):
        self.database = database
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_server,
        )
        for message in self.consumer:
            self.process_message(json.loads(message.value))

    def process_message(self, message):
        logging.info("TweetConsumer reached")
        tweet: Tweet = utils.message_to_tweet(message)
        print(tweet)
        tweet.user.tweet_count += 1
        self.database.create_or_merge_user(tweet.user)
        polarity = sentiment_analysis.get_sentiment(tweet.full_text)
        self.create_relationships(tweet, polarity)

    def create_relationships(self, tweet, polarity: int):
        print(tweet.mentions)
        if tweet.mentions is not None:
            for mention in tweet.mentions:
                if tweet.user.screen_name != mention.screen_name:
                    rel, _, _ = self.get_relationship(tweet.user, mention)
                    if rel is not None:
                        new_rel = self.calc_new_values(rel, polarity)
                        self.update_relationship(new_rel)
                    else:
                        self.create_new_relationship(tweet.user, mention, polarity)

    def create_new_relationship(self, start_user: User, end_user: User, polarity):
        self.database.create_or_merge_user(end_user)
        rel_id = f"{start_user.screen_name}_{end_user.screen_name}"
        rel = Relationship(rel_id, start_user.screen_name, end_user.screen_name, 1, polarity, polarity)
        return self.database.create_or_merge_relationship(rel)

    def update_relationship(self, new_rel):
        return self.database.create_or_merge_relationship(new_rel)

    def get_relationship(self, start_user: User, end_user: User) -> tuple[Relationship, User, User]:
        result = self.database.get_relationship(start_user.screen_name, end_user.screen_name)
        rel, user_start, user_end = utils.neo4j_result_infos(result)
        return rel, user_start, user_end

    def calc_new_values(self, old_rel: Relationship, polarity: int):
        new_weight = old_rel.weight + 1
        new_polarity = ((old_rel.avg_polarity * old_rel.weight) + polarity) / new_weight
        new_weighted_polarity = new_weight * new_polarity
        return Relationship(old_rel.id, old_rel.start_user, old_rel.end_user, new_weight, new_polarity,
                            new_weighted_polarity)
