import dataclasses
import json

import requests

import env_vars
from Producer.TwitterProducer import TwitterProducer
from general import utils
from general.project_dataclasses import RuleSet, BearerToken, ConversationRule, Tweet

TWEET_FIELDS = ["author_id", "created_at", "conversation_id", "entities", "in_reply_to_user_id", "lang", "source"]
LANGUAGE = "de"


class TwitterStreamer:
    def __init__(self, kafka_server: str, rule_save_path: str, bearer_token: BearerToken, ruleset: RuleSet = None,
                 custom_rules: list = None):
        self.bearer_token: BearerToken = bearer_token
        self.custom_rules = custom_rules
        self.ruleset: RuleSet = ruleset
        self.rule_save_path: str = rule_save_path
        self.kafka_server: str = kafka_server
        self.twitter_producer: TwitterProducer = TwitterProducer(self.kafka_server)

    def initialise(self):
        self.delete_all_rules()
        if self.ruleset is not None:
            self.setup_rules()
        elif self.custom_rules is not None:
            self.set_rules(self.custom_rules)

    def bearer_oauth(self, r: requests) -> requests:
        r.headers["Authorization"] = f"Bearer {self.bearer_token.access_token}"
        r.headers["Content-type"] = "application/json"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r

    def get_rules(self) -> dict:
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print(json.dumps(response.json()))
        return response.json()

    def delete_rules(self, ids: list[str]) -> None:
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        print(json.dumps(response.json()))

    def delete_all_rules(self) -> None:
        rules = self.get_rules()
        if rules is None or "data" not in rules:
            return None

        ids = list(map(lambda rule: rule["id"], rules["data"]))
        self.delete_rules(ids)

    def set_rules(self, rules) -> list:
        payload = {"add": rules}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload,
        )
        json_result = response.json()
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )

        rule_ids = []
        for entry in json_result["data"]:
            rule_ids.append((entry["tag"], entry["id"]))

        return rule_ids

    def get_stream(self) -> None:
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream",
            auth=self.bearer_oauth,
            stream=True,
            params={
                "tweet.fields": ",".join(TWEET_FIELDS),
                "expansions": "author_id",
            }
        )
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        for response_line in response.iter_lines():
            if response_line:
                self.on_data(response_line)

    def on_data(self, response_data) -> None:
        json_response = json.loads(response_data)

        self.produce_tweet(json_response)
        # self.twitter_producer.send(env_vars.PRIMARY_TWEET_TOPIC, tweet.id, dataclasses.asdict(tweet))

    def produce_primary_tweet(self, tweet: Tweet):
        self.twitter_producer.send(env_vars.PRIMARY_TWEET_TOPIC, tweet.id, dataclasses.asdict(tweet))
        self.twitter_producer.flush()

    def produce_secondary_tweet(self, tweet: Tweet):
        self.twitter_producer.send(env_vars.SECONDARY_TWEET_TOPIC, tweet.id, dataclasses.asdict(tweet))
        self.twitter_producer.flush()

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

    def update_primary_tweets(self, tweet: Tweet, tag: str):
        if tag in self.ruleset.conversations:
            conversation_ids = self.ruleset.conversations[tag].tweet_ids
            conversation_ids.append(tweet.id)
            conversation_rules = self.setup_conversation_rule([], conversation_ids, tag)
            new_rule_id = self.update_rules([self.ruleset.conversations[tag].rule_id], conversation_rules)
        else:
            conversation_ids = [tweet.id]
            conversation_rules = self.setup_conversation_rule([], conversation_ids, tag)
            new_rule_id = self.set_rules(conversation_rules)[0]

        self.ruleset.conversations[tag] = ConversationRule(conversation_ids, new_rule_id)
        utils.save_rules(self.rule_save_path, self.ruleset)

    def setup_follower_rules(self, followers: list[str]) -> list:
        with_prefix_followers = ["from:" + follower for follower in followers]
        return [{
            "value": " OR ".join(with_prefix_followers),
            "tag": "primary tweet"
        }]

    def update_rules(self, ids: list, rules: list):
        self.delete_rules(ids)
        rule_ids = self.set_rules(rules)
        return rule_ids

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

    def setup_rules(self) -> None:
        language_rule = [{
            "value": "lang:" + LANGUAGE,
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

    def start_filter_stream(self) -> None:
        self.get_stream()

    def send(self, topic, tweet_id, data: dict) -> None:
        self.twitter_producer.send(topic, data=data, tweet_id=tweet_id)

    def flush(self):
        self.twitter_producer.flush()
