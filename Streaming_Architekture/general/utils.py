import dataclasses
import json
import time

from neo4j.graph import Graph

from Streaming_Architekture import env_vars
from Streaming_Architekture.general.models import RuleSet, BearerToken, FollowerRule, ConversationRule, Tweet, User, Relationship


def load_creds(path) -> BearerToken:
    with open(path, "r") as file:
        creds = json.load(file)

    return BearerToken(creds["token_type"], creds["access_token"])


def convert_follower_rule(_input: dict) -> FollowerRule:
    return FollowerRule(_input["users"], _input["rule_id"])


def convert_conversation_rule(_input: dict[str]):
    if _input is None:
        return None

    result: dict[str] = {}
    for entry in _input:
        result[entry] = ConversationRule(_input[entry]["tweet_ids"], _input[entry]["rule_ids"])

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

    in_reply_to_user_id = None
    if "in_reply_to_user_id" in data:
        in_reply_to_user_id = data["in_reply_to_user_id"]

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


def load_rules(path) -> RuleSet:
    with open(path, "r") as file:
        rule_set_dict = json.load(file)

    return RuleSet(convert_follower_rule(rule_set_dict["followers"]),
                   convert_conversation_rule(rule_set_dict["conversations"]))


def message_to_tweet(message: dict) -> Tweet:
    mentions: list[User] = None
    if message["mentions"] is not None:
        mentions = resolve_mentions_message(message["mentions"])

    return Tweet(id=message["id"],
                 created_at=message["created_at"],
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


def neo4j_record_to_user(result) -> User:
    return User(result["u"].get("screen_name"),
                result["u"].get("id"),
                result["u"].get("tweet_count"),
                result["u"].get("type"))


def neo4j_result_infos(result: Graph) -> tuple[Relationship, User, User]:
    print(result)
    raw_rel = list(result._relationships.values())
    if len(raw_rel) <= 0:
        return None, None, None
    users = list(result._nodes.values())

    rel: Relationship = Relationship(raw_rel[0]._properties["rel_id"], users[0]._properties["screen_name"],
                                     users[1]._properties["screen_name"], raw_rel[0]._properties["weight"],
                                     raw_rel[0]._properties["avg_polarity"],
                                     raw_rel[0]._properties["weighted_polarity"])

    start_user: User = User(users[0]._properties["screen_name"],
                            users[0]._properties["id"],
                            users[0]._properties["tweet_count"],
                            users[0]._properties["type"])

    end_user: User = User(users[1]._properties["screen_name"],
                          users[1]._properties["id"],
                          users[1]._properties["tweet_count"],
                          users[1]._properties["type"])

    return rel, start_user, end_user


def save_rules(path, ruleSet) -> None:
    with open(path, 'w') as f:
        json.dump(dataclasses.asdict(ruleSet), f, indent=4)

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

def setup_follower_rules(followers: list[str]) -> list:
    with_prefix_followers = ["from:" + follower for follower in followers]
    return [{
        "value": "( " + (" OR ".join(with_prefix_followers)) + ") -is:retweet lang:%s" % env_vars.LANGUAGE,
        "tag": "primary_tweet"
    }]