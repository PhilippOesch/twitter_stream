import csv
import time
from dataclasses import dataclass
from enum import Enum
from operator import itemgetter
from networkx import edge_betweenness_centrality as betweenness


def format_hashtag(hashtags: list):
    array = [hashtag["text"] for hashtag in hashtags]
    if len(hashtags) == 0:
        return None
    delimiter = "','"
    x = delimiter.join(array)
    return "'" + x + "'"


def format_mentions(mentions: list):
    if len(mentions) == 0:
        return None
    delimiter = "','"
    x = delimiter.join(mentions)
    return "'" + x + "'"


def decode_array(encoded_array: str):
    array: list[str] = encoded_array.replace("'", "").split(",")
    if array[0] == "":
        return []
    else:
        return array


def tweet_user_reply_to_negativity(data, user: str):
    replies = {}
    for row in data:
        mentions_array: [] = decode_array(row["mentions"])
        mentions_set = set(mentions_array)

        if user in mentions_set:
            print(mentions_set)
            if row["in_reply_to_status_id"] not in replies:
                replies[row["in_reply_to_status_id"]] = {
                    "tweet_id": row["in_reply_to_status_id"],
                    "count": 1,
                    "avg_polarity": polarity_to_value(row["sentiment_bert"])
                }
            else:
                replies[row["in_reply_to_status_id"]]["count"] += 1
                replies[row["in_reply_to_status_id"]]["avg_polarity"] += polarity_to_value(row["sentiment_bert"])

    for reply in replies:
        replies[reply]["avg_polarity"] /= replies[reply]["count"]

    sorted_result = sorted(replies.values(), key=lambda x: x["count"], reverse=True)

    return sorted_result


def get_tweet_infos(paths: list[str], hashtag_treshhold=5, post_id_filter: str = None):
    hashtag_index = {}
    char_count = 0

    user_screen_names = {}

    for path in paths:
        with open(f'{path}', encoding="utf-8", newline='') as f:
            reader = csv.reader(f, delimiter='|', skipinitialspace=True)
            headers = next(reader)
            _data = [{h: x for (h, x) in zip(headers, row)} for row in reader]
            resdata = []
            for row in _data:
                # user_screen_names.add(row["screen_name"])
                if post_id_filter is not None:
                    if post_id_filter == row["id"]:
                        resdata.append(row)
                        user_screen_names[row["user_id"]] = row["screen_name"]
                        hashtag_result = decode_array(row["hashtags"])
                        char_count += len(row["full_text"])
                        if len(hashtag_result) != 0:
                            for hashtag in hashtag_result:
                                if hashtag in hashtag_index:
                                    hashtag_index[hashtag] += 1
                                else:
                                    hashtag_index[hashtag] = 1
                else:
                    resdata.append(row)
                    user_screen_names[row["user_id"]] = row["screen_name"]
                    hashtag_result = decode_array(row["hashtags"])
                    char_count += len(row["full_text"])
                    if len(hashtag_result) != 0:
                        for hashtag in hashtag_result:
                            if hashtag in hashtag_index:
                                hashtag_index[hashtag] += 1
                            else:
                                hashtag_index[hashtag] = 1

    sort_result = sorted(hashtag_index.items(), key=lambda x: x[1], reverse=True)
    sorted_dict = {}

    for index, value in sort_result:
        if value < hashtag_treshhold:
            break
        sorted_dict[index] = value

    return sorted_dict, resdata, headers, char_count, user_screen_names


def format_date(date_string: str):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(date_string, '%a %b %d %H:%M:%S +0000 %Y'))


def polarity_to_value(sentiment_str: str):
    switcher = {
        "positive": 1,
        "neutral": 0,
        "negative": -1,
    }
    return switcher[sentiment_str]


def normalize(value, _min, _max):
    return (value - _min) / (_max - _min)


def heaviest(graph):
    u, v, w = max(graph.edges(data="weight"), key=itemgetter(2))
    return (u, v)


def most_central_edge(graph):
    centrality = betweenness(graph, weight="weight")
    return max(centrality, key=centrality.get)


class NodeType(Enum):
    USER = "User"
    HASHTAG = "Hashtag"
    TWEET = "Tweet"


class RelType(Enum):
    COMMENTED_AT = "commentedAt"


@dataclass
class Node:
    id: int
    screen_name: str
    type: NodeType = None


@dataclass
class Relationship:
    start_id: int
    end_id: int
    weight: float
    polarity: float
    subjectivity: float = None
    type: RelType = None


def load_nodes(_filepath: str) -> list[Node]:
    result: list[Node] = []
    with open(_filepath, 'r', encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=';', skipinitialspace=True)
        headers = next(reader)
        print(headers)
        data = [{h: x for (h, x) in zip(headers, row)} for row in reader]
        #
        for row in data:
            result.append(Node(int(row["user_id:ID"]), row["screen_name:ID"], row[":Label"]))

    return result


def load_relationships(_filepath: str) -> list[Relationship]:
    result: list[Relationship] = []
    with open(_filepath, 'r', encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=';', skipinitialspace=True)
        headers = next(reader)
        print(headers)
        data = [{h: x for (h, x) in zip(headers, row)} for row in reader]

        for row in data:
            result.append(
                Relationship(int(row[":START_ID"]), int(row[":END_ID"]), float(row["weight:int"]),
                             float(row["polarity:float"]),
                             float(row["subjectivity:float"]) if "subjectivity:float" in row else "undefined"))

    return result
