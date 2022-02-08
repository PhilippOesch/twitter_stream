import dataclasses

from Consumer.PrimaryTweetConsumer import PrimaryTweetConsumer
import env_vars
from Producer.TwitterProducer import TwitterProducer
from general import utils
from general.Neo4JHelper import Neo4JHelper
from general.TwitterStreamer import TwitterStreamer
from general.project_dataclasses import BearerToken, RuleSet, Tweet

RULE_SET_PATH = "./data/rules.json"

creds: BearerToken = utils.load_creds("./general/bearertoken.json")

if __name__ == "__main__":
    # rule_sets: RuleSet = utils.load_rules(RULE_SET_PATH)
    custom_rules = [
        {
            "value": "python",
            "tag": "python"
        },
        {
            "value": "java",
            "tag": "java"
        }
    ]
    # producer = TwitterStreamer(env_vars.KAFKA_SERVER, RULE_SET_PATH, creds, custom_rules=custom_rules)
    producer = TwitterProducer(creds, env_vars.KAFKA_SERVER, RULE_SET_PATH, custom_rules=custom_rules)
    producer.initialise()
    producer.start_filter_stream()
    # tweet = Tweet("24214214", "gsdhfdhfdhfhfh", "philipp")
    # producer.send(env_vars.PRIMARY_TWEET_TOPIC, "24214214", dataclasses.asdict(tweet))
    # producer.flush()
