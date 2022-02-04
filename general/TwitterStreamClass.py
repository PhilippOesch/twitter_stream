import json

import requests

from general import utils
from general.project_dataclasses import RuleSet, BearerToken, ConversationRule


class TwitterStreamer:
    def __init__(self, rule_save_path: str, bearer_token: BearerToken, ruleset: RuleSet):
        self.bearer_token: BearerToken = bearer_token
        self.ruleset: RuleSet = ruleset
        self.rule_save_path: str = rule_save_path

    def initialise(self):
        self.delete_all_rules()
        self.setup_rules()

    def bearer_oauth(self, r):
        r.headers["Authorization"] = f"Bearer {self.bearer_token.access_token}"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r

    def get_rules(self):
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", auth=self.bearer_oauth
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print(json.dumps(response.json()))
        return response.json()

    def delete_rules(self, ids: list[str]):
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

    def delete_all_rules(self):
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

    def get_stream(self):
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream", auth=self.bearer_oauth, stream=True,
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

    def on_data(self, response_data):
        json_response = json.loads(response_data)
        print(json.dumps(json_response, indent=4, sort_keys=True))

    def setup_follower_rules(self, followers: list[str]):
        with_prefix_followers = ["from:" + follower for follower in followers]
        return [{
            "value": " OR ".join(with_prefix_followers),
            "tag": "primary tweet"
        }]

    def setup_conversation_rules(self, conversations: dict[str] = None) -> list:
        result = []
        for user in conversations:
            with_prefix = ["conversation_id:" + conversation for conversation in conversations[user].tweet_ids]
            result.append({
                "value": " OR ".join(with_prefix),
                "tag": user
            })
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
        follower_rule: list = self.setup_follower_rules(self.ruleset.followers.users)
        self.ruleset.followers.rule_id = self.set_rules(follower_rule)[0][1]
        print("follower_rule_ids", self.ruleset.followers.rule_id)

        if self.ruleset.conversations is None or len(self.ruleset.conversations)== 0:
            self.ruleset.conversations = {}
            utils.save_rules(self.rule_save_path, self.ruleset)
            return

        conversation_rules = self.setup_conversation_rules(self.ruleset.conversations)
        self.update_conversation_rule_ids(conversation_rules)

        utils.save_rules(self.rule_save_path, self.ruleset)

    def start_filter_stream(self):
        self.get_stream()
