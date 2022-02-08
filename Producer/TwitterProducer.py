import dataclasses
import json

from kafka import KafkaProducer

import env_vars
from general import utils
from general.TwitterStreamer import TwitterStreamer
from general.project_dataclasses import BearerToken, RuleSet, ConversationRule, Tweet


class TwitterProducer(TwitterStreamer):
    def __init__(self, bearer_token: BearerToken, kafka_server: str, rule_save_path: str, ruleset: RuleSet = None,
                 custom_rules: list = None):
        super().__init__(bearer_token)
        self.producer: KafkaProducer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.custom_rules = custom_rules
        self.ruleset: RuleSet = ruleset
        self.rule_save_path: str = rule_save_path
        self.kafka_server: str = kafka_server

    def initialise(self):
        self.delete_all_rules()
        if self.ruleset is not None:
            self.setup_rules()
        elif self.custom_rules is not None:
            self.set_rules(self.custom_rules)
            self.ruleset = RuleSet(None, {})

    # overwriting
    def on_data(self, response_data) -> None:
        json_response = json.loads(response_data)
        self.produce_tweet(json_response)
        # self.twitter_producer.send(env_vars.PRIMARY_TWEET_TOPIC, tweet.id, dataclasses.asdict(tweet))

    def produce_primary_tweet(self, tweet: Tweet):
        self.send(env_vars.PRIMARY_TWEET_TOPIC, tweet_id= tweet.id, data=dataclasses.asdict(tweet))
        self.flush()

    def produce_secondary_tweet(self, tweet: Tweet):
        self.send(env_vars.SECONDARY_TWEET_TOPIC, tweet_id= tweet.id, data=dataclasses.asdict(tweet))
        self.flush()

    def update_primary_tweets(self, tweet: Tweet, tag: str):
        if tag in self.ruleset.conversations:
            conversation_ids = self.ruleset.conversations[tag].tweet_ids
            conversation_ids.append(tweet.id)
            conversation_rules = self.setup_conversation_rule([], conversation_ids, tag)
            new_rule_id = self.update_rules([self.ruleset.conversations[tag].rule_id], conversation_rules)
            print("update: ", new_rule_id)
        else:
            conversation_ids = [tweet.id]
            conversation_rules = self.setup_conversation_rule([], conversation_ids, tag)
            new_rule_id = self.set_rules(conversation_rules)
            print("new: ", new_rule_id)

        self.ruleset.conversations[tag] = ConversationRule(conversation_ids, new_rule_id[0])
        utils.save_rules(self.rule_save_path, self.ruleset)

    def produce_tweet(self, json_response):
        tag = json_response["matching_rules"][0]["tag"]
        rule_id = json_response["matching_rules"][0]["id"]
        tweet = utils.create_tweet_from_stream(json_response)

        if tag == "java":
            self.update_primary_tweets(tweet, tag)
            self.produce_primary_tweet(tweet)
        elif tag == "python":
            self.produce_secondary_tweet(tweet)

    def setup_conversation_rule(self, result: list, conversation_ids: list, user: str):
        with_prefix = ["conversation_id:" + conversation for conversation in conversation_ids]
        result.append({
            "value": " OR ".join(with_prefix),
            "tag": user
        })
        return result

    def setup_conversation_rules(self, conversations: dict[str] = None) -> list:
        result = []
        for user in conversations:
            result = self.setup_conversation_rule(result, conversations[user].tweet_ids, user)
        return result

    def update_conversation_rule_ids(self, conversation_rules: list):
        conversation_rule_ids = self.set_rules(conversation_rules)
        conversation_rule_id_dict = {}

        for entry in conversation_rule_ids:
            conversation_rule_id_dict[entry[0]] = entry[1]

        print(conversation_rule_id_dict)
        for user in self.ruleset.followers.users:
            if user in conversation_rule_id_dict:
                self.ruleset.conversations[user] = ConversationRule(self.ruleset.conversations[user].tweet_ids,
                                                                    conversation_rule_id_dict[user])

    def setup_follower_rules(self, followers: list[str]) -> list:
        with_prefix_followers = ["from:" + follower for follower in followers]
        return [{
            "value": " OR ".join(with_prefix_followers),
            "tag": "primary tweet"
        }]

    def setup_rules(self) -> None:
        language_rule = [{
            "value": "lang:" + env_vars.LANGUAGE,
            "tag": "language"
        }]
        self.set_rules(language_rule)
        follower_rule: list = self.setup_follower_rules(self.ruleset.followers.users)
        self.ruleset.followers.rule_id = self.set_rules(follower_rule)[0][1]
        print("follower_rule_ids", self.ruleset.followers.rule_id)

        if self.ruleset.conversations is None or len(self.ruleset.conversations) == 0:
            self.ruleset.conversations = {}
            utils.save_rules(self.rule_save_path, self.ruleset)
            return

        conversation_rules = self.setup_conversation_rules(self.ruleset.conversations)
        self.update_conversation_rule_ids(conversation_rules)

        utils.save_rules(self.rule_save_path, self.ruleset)

    def send(self, topic: str, tweet_id: str, data: dict) -> None:
        self.producer.send(topic, value=data, key=tweet_id)

    def flush(self):
        self.producer.flush()
