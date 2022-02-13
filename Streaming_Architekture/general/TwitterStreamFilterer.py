from Streaming_Architekture.general import utils
from Streaming_Architekture.general.TwitterStreamer import TwitterStreamer
from Streaming_Architekture.general.models import ConversationRule, RuleSet, BearerToken


class FilteredTwitterStream(TwitterStreamer):
    def __init__(self, bearer_token: BearerToken, rule_save_path: str, ruleset: RuleSet = None,
                 custom_rules: list = None):
        super().__init__(bearer_token)
        self.custom_rules = custom_rules
        self.ruleset: RuleSet = ruleset
        self.rule_save_path: str = rule_save_path
        self.ruleset = None

    def initialise(self):
        self.delete_all_rules()
        if self.ruleset is not None:
            self.setup_rules()
        elif self.custom_rules is not None:
            self.set_rules(self.custom_rules)
            self.ruleset = RuleSet(None, {})

    def update_conversation_rule_ids(self, conversation_rules, user: str):
        conversation_rule_ids = self.set_rules(conversation_rules)
        self.ruleset.conversations[user] = ConversationRule(self.ruleset.conversations[user].tweet_ids,
                                                            conversation_rule_ids)

    def setup_rules(self) -> None:
        follower_rule: list = utils.setup_follower_rules(self.ruleset.followers.users)
        self.ruleset.followers.rule_id = self.set_rules(follower_rule)[0]
        print("follower_rule_ids", self.ruleset.followers.rule_id)

        if self.ruleset.conversations is None or len(self.ruleset.conversations) == 0:
            self.ruleset.conversations = {}
            utils.save_rules(self.rule_save_path, self.ruleset)
            return

        for user in self.ruleset.conversations:
            conversation_rules = utils.setup_conversation_rules(user, self.ruleset.conversations[user])
            self.update_conversation_rule_ids(conversation_rules, user)

        utils.save_rules(self.rule_save_path, self.ruleset)
