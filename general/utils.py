import dataclasses
import json

from general.project_dataclasses import RuleSet, BearerToken, FollowerRule, ConversationRule


def load_creds(path) -> BearerToken:
    with open(path, "r") as file:
        creds = json.load(file)

    return BearerToken(creds["token_type"], creds["access_token"])


def load_rules(path) -> RuleSet:
    with open(path, "r") as file:
        rule_set_dict = json.load(file)

    return RuleSet(convert_follower_rule(rule_set_dict["followers"]), convert_conversation_rule(rule_set_dict["conversations"]))


def convert_follower_rule(_input: dict) -> FollowerRule:
    return FollowerRule(_input["users"], _input["rule_id"])


def convert_conversation_rule(_input: dict[str]):
    if _input is None:
        return None

    result: dict[str] = {}
    for entry in _input:
        result[entry] = ConversationRule(_input[entry]["tweet_ids"], _input[entry]["rule_id"])

    return result


def save_rules(path, ruleSet) -> None:
    with open(path, 'w') as f:
        json.dump(dataclasses.asdict(ruleSet), f, indent=4)
