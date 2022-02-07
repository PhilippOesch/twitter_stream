import dataclasses
import datetime
import json
import time

import neo4j

from general.project_dataclasses import RuleSet, BearerToken, FollowerRule, ConversationRule, Tweet, User, Relationship


def load_creds(path) -> BearerToken:
    with open(path, "r") as file:
        creds = json.load(file)

    return BearerToken(creds["token_type"], creds["access_token"])


def load_rules(path) -> RuleSet:
    with open(path, "r") as file:
        rule_set_dict = json.load(file)

    return RuleSet(convert_follower_rule(rule_set_dict["followers"]),
                   convert_conversation_rule(rule_set_dict["conversations"]))


def convert_follower_rule(_input: dict) -> FollowerRule:
    return FollowerRule(_input["users"], _input["rule_id"])


def convert_conversation_rule(_input: dict[str]):
    if _input is None:
        return None

    result: dict[str] = {}
    for entry in _input:
        result[entry] = ConversationRule(_input[entry]["tweet_ids"], _input[entry]["rule_id"])

    return result


def create_user(screen_name: str, user_id: str, tweet_count: int = None) -> User:
    return User(screen_name, user_id, tweet_count)


def create_tweet_from_stream(_input: dict) -> Tweet:
    data = _input["data"]
    includes = _input["includes"]
    hashtags = None
    mentions = None

    if "entities" in data:
        if "hashtags" in data["entities"]:
            hashtags = [entry["tag"] for entry in data["entities"]["hashtags"]]

        if "mentions" in data["entities"]:
            # mentions = [entry["username"] for entry in data["entities"]["mentions"]]
            mentions = resolve_mentions_stream(data["entities"]["mentions"])

    print("Mentions:", mentions)

    in_reply_to_user_id = None
    if "in_reply_to_user_id" in data:
        in_reply_to_user_id = data["in_reply_to_user_id"]
    print(data)

    created_at = format_date(data["created_at"])
    return Tweet(id=data["id"],
                 created_at=created_at,
                 full_text=str(data["text"]).replace("\n", " "),
                 conversation_id=data["conversation_id"],
                 user=create_user(includes["users"][0]["username"], data["author_id"]),
                 mentions=mentions,
                 hashtags=hashtags,
                 in_reply_to_user_id=in_reply_to_user_id
                 )


def resolve_mentions_stream(data: dict) -> list[User]:
    result: list[User] = []
    for entry in data:
        result.append(User(entry["username"], entry["id"]))

    return result


def resolve_mentions_message(data: dict) -> list[User]:
    result: list[User] = []
    for entry in data:
        result.append(User(entry["screen_name"], entry["user_id"]))

    return result


def save_rules(path, ruleSet) -> None:
    with open(path, 'w') as f:
        json.dump(dataclasses.asdict(ruleSet), f, indent=4)


def message_to_tweet(message: dict) -> Tweet:
    mentions: list[User] = None
    if message["mentions"] is not None:
        mentions = resolve_mentions_message(message["mentions"])

    return Tweet(id=message["id"],
                 created_at=message["id"],
                 full_text=message["full_text"],
                 conversation_id=message["conversation_id"],
                 user=create_user(message["user"]["screen_name"], message["user"]["user_id"],
                                  message["user"]["tweet_count"]),
                 mentions=mentions,
                 hashtags=message["hashtags"],
                 in_reply_to_user_id=message["in_reply_to_user_id"]
                 )


def format_date(date_string: str) -> str:
    return time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ"))


def neo4j_result_infos(result) -> tuple[Relationship, User, User]:
    raw_rel = result[0]
    raw_user1 = result[1]
    raw_user2 = result[2]

    rel: Relationship = Relationship(raw_rel.properties["rel_id"], raw_user1.properties["screen_name"],
                                     raw_user2.properties["screen_name"], raw_rel.properties["weight"],
                                     raw_rel.properties["avg_polarity"], raw_rel.properties["weighted_polarity"])

    start_user: User = User(raw_user1.properties["screen_name"],
                            raw_user1.properties["id"],
                            raw_user1.properties["tweet_count"],
                            raw_user1.properties["type"])

    end_user: User = User(raw_user2.properties["screen_name"],
                          raw_user2.properties["id"],
                          raw_user2.properties["tweet_count"],
                          raw_user2.properties["type"])

    return rel, start_user, end_user
