import json

import requests

from Streaming_Architekture import env_vars
from Streaming_Architekture.general.models import BearerToken


class TwitterStreamer:
    def __init__(self, bearer_token: BearerToken):
        self.bearer_token: BearerToken = bearer_token

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
            rule_ids.append(entry["id"])
        print("------rule_Ids", rule_ids)

        return rule_ids

    def start_stream(self) -> None:
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream",
            auth=self.bearer_oauth,
            stream=True,
            params={
                "tweet.fields": ",".join(env_vars.TWEET_FIELDS),
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
        print(response_data)

    def update_rules(self, ids: list, rules: list):
        if len(ids) <= 0:
            return
        self.delete_rules(ids)
        rule_ids = self.set_rules(rules)
        return rule_ids
