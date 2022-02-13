import dataclasses
import json

from kafka import KafkaProducer

from Streaming_Architekture import env_vars
from Streaming_Architekture.general import utils
from Streaming_Architekture.general.TwitterStreamFilterer import FilteredTwitterStream
from Streaming_Architekture.general.project_dataclasses import BearerToken, RuleSet, ConversationRule, Tweet


def setup_follower_rules(followers: list[str]) -> list:
    with_prefix_followers = ["from:" + follower for follower in followers]
    return [{
        "value": "( " + (" OR ".join(with_prefix_followers)) + ") -is:retweet lang:%s" % env_vars.LANGUAGE,
        "tag": "primary_tweet"
    }]


def get_single_conversation_rule(conversation_ids: list, user: str):
    with_prefix = ["conversation_id:" + conversation for conversation in conversation_ids]
    return {
        "value": " OR ".join(with_prefix),
        "tag": user
    }


def setup_conversation_rules_for_ids(result: list, conversation_ids: list, user: str):
    # with_prefix = ["conversation_id:" + conversation for conversation in conversation_ids]
    conv = []
    for i in range(len(conversation_ids)):
        conv.append(conversation_ids[i])
        if (i + 1) % 10 == 0:
            result.append(get_single_conversation_rule(list(conv), user))
            conv = []

    if len(conv) > 0:
        result.append(get_single_conversation_rule(list(conv), user))

    return result


def setup_conversation_rules(user: str, conversations: ConversationRule = None) -> list:
    return setup_conversation_rules_for_ids([], conversations.tweet_ids, user)


class TwitterProducer(FilteredTwitterStream):
    def __init__(self, bearer_token: BearerToken, kafka_server: str, rule_save_path: str, ruleset: RuleSet = None,
                 custom_rules: list = None):
        super().__init__(bearer_token, rule_save_path, ruleset, custom_rules)
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

    def produce_primary_tweet(self, tweet: Tweet):
        self.send(env_vars.PRIMARY_TWEET_TOPIC, tweet_id=tweet.id, data=dataclasses.asdict(tweet))
        self.flush()

    def produce_secondary_tweet(self, tweet: Tweet):
        self.send(env_vars.SECONDARY_TWEET_TOPIC, tweet_id=tweet.id, data=dataclasses.asdict(tweet))
        self.flush()

    def update_primary_tweets(self, tweet: Tweet, tag: str):
        if tag in self.ruleset.conversations:
            conversation_ids = self.ruleset.conversations[tag].tweet_ids
            conversation_id_set = set(conversation_ids)
            conversation_id_set.add(tweet.conversation_id)
            conversation_rules = setup_conversation_rules_for_ids([], list(conversation_id_set), tag)
            new_rule_ids = self.update_rules(self.ruleset.conversations[tag].rule_ids, conversation_rules)
            print("update: ", new_rule_ids)
        else:
            conversation_id_set = set()
            conversation_id_set.add(tweet.conversation_id)
            conversation_rules = setup_conversation_rules_for_ids([], list(conversation_id_set), tag)
            new_rule_ids = self.set_rules(conversation_rules)
            print("new: ", new_rule_ids)

        self.ruleset.conversations[tag] = ConversationRule(list(conversation_id_set), new_rule_ids)
        utils.save_rules(self.rule_save_path, self.ruleset)

    def produce_tweet(self, json_response):
        tag = json_response["matching_rules"][0]["tag"]
        rule_id = json_response["matching_rules"][0]["id"]
        tweet = utils.create_tweet_from_stream(json_response)
        print("Producing Tweet", tweet)

        if tag == "primary_tweet":
            self.update_primary_tweets(tweet, tweet.user.screen_name)
            self.produce_primary_tweet(tweet)
        else:
            self.produce_secondary_tweet(tweet)

    def update_conversation_rule_ids(self, conversation_rules, user: str):
        conversation_rule_ids = self.set_rules(conversation_rules)
        self.ruleset.conversations[user] = ConversationRule(self.ruleset.conversations[user].tweet_ids,
                                                            conversation_rule_ids)

    # def setup_rules(self) -> None:
    #     follower_rule: list = setup_follower_rules(self.ruleset.followers.users)
    #     self.ruleset.followers.rule_id = self.set_rules(follower_rule)[0]
    #     print("follower_rule_ids", self.ruleset.followers.rule_id)
    #
    #     if self.ruleset.conversations is None or len(self.ruleset.conversations) == 0:
    #         self.ruleset.conversations = {}
    #         utils.save_rules(self.rule_save_path, self.ruleset)
    #         return
    #
    #     for user in self.ruleset.conversations:
    #         conversation_rules = setup_conversation_rules(user, self.ruleset.conversations[user])
    #         self.update_conversation_rule_ids(conversation_rules, user)
    #
    #     utils.save_rules(self.rule_save_path, self.ruleset)

    def send(self, topic: str, tweet_id: str, data: dict) -> None:
        self.producer.send(topic, value=data, key=tweet_id)

    def flush(self):
        self.producer.flush()
