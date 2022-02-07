import threading

from Consumer.PrimaryTweetConsumer import PrimaryTweetConsumer
import env_vars
from Consumer.SecondaryTweetConsumer import SecondaryTweetConsumer
from general.Neo4JHelper import Neo4JHelper


def start_primary_consumer(database: Neo4JHelper):
    primary_consumer = PrimaryTweetConsumer(env_vars.KAFKA_SERVER, env_vars.PRIMARY_TWEET_TOPIC, database)


def start_secondary_consumer(database: Neo4JHelper):
    secondary_consumer = SecondaryTweetConsumer(env_vars.KAFKA_SERVER, env_vars.SECONDARY_TWEET_TOPIC, database)


if __name__ == "__main__":
    database: Neo4JHelper = Neo4JHelper()
    database.reset()
    # primary_consumer = PrimaryTweetConsumer(env_vars.KAFKA_SERVER)
    primary_tweet_consumer_thread = threading.Thread(target=start_primary_consumer, args=[database])
    secondary_tweet_consumer_thread = threading.Thread(target=start_secondary_consumer, args=[database])

    primary_tweet_consumer_thread.start()
    secondary_tweet_consumer_thread.start()
