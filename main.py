import json

from general import utils
from general.TwitterStreamer import TwitterStreamer
from general.project_dataclasses import RuleSet, BearerToken

creds: BearerToken = utils.load_creds("credentials/bearertoken.json")

if __name__ == "__main__":
    rule_sets: RuleSet = utils.load_rules("./data/rules.json")
    stream = TwitterStreamer("./data/rules.json", creds, rule_sets)
    stream.initialise()
    stream.start_filter_stream()
