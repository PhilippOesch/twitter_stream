import threading

import env_vars
from Streaming_Architekture.Consumer.TweetConsumer import TweetConsumer
from general.Neo4JHelper import Neo4JHelper


def start_primary_consumer(_database: Neo4JHelper):
    primary_consumer = TweetConsumer("tweet_group", env_vars.KAFKA_SERVER, env_vars.PRIMARY_TWEET_TOPIC, _database,
                                     "PrimaryUser")


def start_secondary_consumer(_database: Neo4JHelper):
    secondary_consumer = TweetConsumer("tweet_group", env_vars.KAFKA_SERVER, env_vars.SECONDARY_TWEET_TOPIC, _database,
                                       "SecondaryUser")


if __name__ == "__main__":
    database: Neo4JHelper = Neo4JHelper()

    primary_tweet_consumer_thread = threading.Thread(target=start_primary_consumer, args=[database])
    secondary_tweet_consumer_thread = threading.Thread(target=start_secondary_consumer, args=[database])

    primary_tweet_consumer_thread.start()
    secondary_tweet_consumer_thread.start()
