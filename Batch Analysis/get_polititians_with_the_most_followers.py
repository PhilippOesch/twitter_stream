

from general.tweepy_init import python_tweets
import json
from typing_extensions import TypedDict

mdb_id = "912241909002833921"  # ID der Bundestagsliste
polititian_count = 15


class TwitterUser(TypedDict):
    id: int
    id_str: str
    name: str
    screen_name: str
    followers_count: int


def get_bundestag_twitter_member():
    query_params = {
        "list_id": mdb_id,
        "count": 602
    }
    result = python_tweets.get_list_members(**query_params)

    users: list[TwitterUser] = []

    # format users and save in list
    for user in result["users"]:
        users.append({
            "id": user["id"],
            "id_str": user["id_str"],
            "name": user["name"],
            "screen_name": user["screen_name"],
            "followers_count": user["followers_count"]
        })

    # sorting
    sorted_by_followers = sorted(users, key=lambda user: user["followers_count"], reverse=True)

    # saving of politicians
    with open(f'data/dataset2/politicians.json', 'w', encoding="utf-8") as f:
        json.dump(sorted_by_followers[0:polititian_count], f, indent=5, ensure_ascii=False)


get_bundestag_twitter_member()
