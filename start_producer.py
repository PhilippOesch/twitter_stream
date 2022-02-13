import dataclasses

from Consumer.PrimaryTweetConsumer import PrimaryTweetConsumer
import env_vars
from Producer.TwitterProducer import TwitterProducer
from general import utils
from general.project_dataclasses import BearerToken, RuleSet, Tweet

RULE_SET_PATH = "./data/rules.json"

creds: BearerToken = utils.load_creds("credentials/bearertoken.json")

if __name__ == "__main__":
    rule_sets: RuleSet = utils.load_rules(RULE_SET_PATH)
    producer = TwitterProducer(creds, env_vars.KAFKA_SERVER, RULE_SET_PATH, ruleset= rule_sets)
    producer.initialise()
    producer.start_stream()
