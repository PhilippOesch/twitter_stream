from dataclasses import dataclass


@dataclass
class FollowerRule:
    users: list[str]
    rule_id: str = None,


@dataclass
class ConversationRule:
    tweet_ids: list[str]
    rule_id: str = None,


@dataclass
class RuleSet:
    followers: FollowerRule
    conversations: dict[str] = None


@dataclass
class BearerToken:
    token_type: str
    access_token: str
