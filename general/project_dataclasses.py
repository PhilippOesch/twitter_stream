from dataclasses import dataclass


@dataclass
class FollowerRule:
    users: list[str]
    rule_id: str = None,


@dataclass
class ConversationRule:
    tweet_ids: list[str]
    rule_ids: list[str] = None,


@dataclass
class RuleSet:
    followers: FollowerRule = None
    conversations: dict[str] = None


@dataclass
class BearerToken:
    token_type: str
    access_token: str


@dataclass
class User:
    screen_name: str
    user_id: str
    tweet_count: int = 1
    type: str = "SecondaryUser"


@dataclass
class Tweet:
    id: str
    created_at: str
    full_text: str
    conversation_id: str
    user: User
    mentions: list[User] = None
    hashtags: list[str] = None
    in_reply_to_status_id: str = None
    in_reply_to_user_id: str = None
    sentiment: float = None
    weight: int = None
    weighted_sentiment = None


@dataclass
class Relationship:
    id: str
    start_user: str
    end_user: str
    weight: int
    avg_polarity: float
    weighted_polarity: float
