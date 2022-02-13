import csv

from Streaming_Architekture.general import utils

import_header = [":START_ID", ":END_ID", "weight:int", "polarity:float", ":TYPE"]

_, primary_tweet_data, _, _, _ = utils.get_tweet_infos(
    'data/dataset_primary_tweets_1-1-2021_1-9-2021_textblob.csv',
    hashtag_treshhold=0)
analyzed_hashes_tweets, replydata, _header, char_count, user_ids1 = utils.get_tweet_infos(
    'data/dataset_1-1-2021_1-9-2021_textblob_mentions.csv',
    hashtag_treshhold=0)

full_data: [] = list()
full_data.extend(primary_tweet_data)
full_data.extend(replydata)


def create_sentiment_relationships(_data: [], _users: dict, _filepath, isTextBlob: bool, use_mentions: bool) -> None:
    full_dict: dict = {}
    reversed_users_dict = dict((v, k) for k, v in _users.items())
    print(_users)
    for row in _data:
        screen_name: str = row["screen_name"]
        if use_mentions:
            mentions_array: [] = utils.decode_array(row["mentions"])
            if screen_name in _users:
                for mention in mentions_array:
                    if mention in users:
                        relationship_string: str = users[screen_name] + "_" + users[mention]
                        if relationship_string in full_dict:
                            add_relationship_to_dictionary(relationship_string, row, full_dict, isTextBlob)

                        elif mention in users and (screen_name != mention):
                            create_new_relationship_in_dictionary(relationship_string, screen_name, mention, row, users,
                                                                  full_dict, isTextBlob)
        else:
            if row["in_reply_to_user_id"] in reversed_users_dict:
                mention = reversed_users_dict[row["in_reply_to_user_id"]]
                if screen_name in _users and mention in users:
                    relationship_string: str = users[screen_name] + "_" + users[mention]
                    if relationship_string in full_dict:
                        add_relationship_to_dictionary(relationship_string, row, full_dict, isTextBlob)

                    elif mention in users and (screen_name != mention):
                        create_new_relationship_in_dictionary(relationship_string, screen_name, mention, row, users,
                                                              full_dict, isTextBlob)

    with open(_filepath, 'w', encoding="utf-8", newline='') as f:
        writer = csv.writer(f, delimiter=';')
        if isTextBlob:
            header = import_header[:4] + ["subjectivity:float"] + import_header[4:]
            writer.writerow(header)
        else:
            writer.writerow(import_header)

        for row in full_dict:
            array = list(full_dict[row].values())
            array.append("commentedAt")
            writer.writerow(array)


def add_relationship_to_dictionary(_relationship_string: str, _row: dict, _full_dict: dict,
                                   _is_text_blob: bool) -> None:
    current_weight = _full_dict[_relationship_string]["weight"]
    new_weight = current_weight + 1
    if not _is_text_blob:
        current_avg_polarity = _full_dict[_relationship_string]["avg_polarity"]
        new_avg_polarity = calc_avg_polarity_bert(int(current_weight), float(current_avg_polarity),
                                                  _row["sentiment_bert"])
    else:
        current_avg_polarity = _full_dict[_relationship_string]["avg_polarity"]
        current_avg_subjectivity = _full_dict[_relationship_string]["avg_subjectivity"]
        new_avg_polarity, new_avg_subjectivity = calc_avg_polarity_and_subjectivity_textblob(
            int(current_weight),
            float(current_avg_polarity),
            float(_row["polarity"]),
            float(current_avg_subjectivity),
            float(_row["subjectivity"]))
        _full_dict[_relationship_string]["avg_subjectivity"] = new_avg_polarity
    _full_dict[_relationship_string]["weight"] = new_weight
    _full_dict[_relationship_string]["avg_polarity"] = new_avg_polarity


def create_new_relationship_in_dictionary(_relationship_string: str, screen_name: str, _mention: str, _row: dict,
                                          _users: dict, _fulldict: dict, _is_text_blob: bool) -> None:
    if not _is_text_blob:
        _fulldict[_relationship_string] = {
            "from_id": _users[screen_name],
            "to_id": _users[_mention],
            "weight": 1,
            "avg_polarity": utils.polarity_to_value(_row["sentiment_bert"])
        }
    else:
        _fulldict[_relationship_string] = {
            "from_id": _users[screen_name],
            "to_id": _users[_mention],
            "weight": 1,
            "avg_polarity": _row["polarity"],
            "avg_subjectivity": _row["subjectivity"]
        }


# sometimes Users comment themselfs, which is quiet uninteresting. Will start tryiing ignoring self comments

def prepare_user(_data: [], _filepath):
    result = {}
    user_ids: set = set()
    for row in _data:
        if not row["user_id"] in user_ids:
            result[row["screen_name"]] = row["user_id"]
            user_ids.add(row["user_id"])

    with open(_filepath, 'w', encoding="utf-8", newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(["screen_name:ID", "user_id:ID", ":Label"])
        for row in result:
            writer.writerow([row, result[row], "User"])

    return result


def calc_avg_polarity_bert(old_weight, old_polarity, new_polarity) -> float:
    return ((old_polarity * old_weight) + utils.polarity_to_value(
        new_polarity)) / (old_weight + 1)


def calc_avg_polarity_and_subjectivity_textblob(old_weight: int, old_polarity: float, new_polarity: float,
                                                old_subjectivity: float,
                                                new_subjectivity: float) -> tuple[float, float]:
    new_polarity: float = (((old_polarity * old_weight) + new_polarity) / (old_weight + 1))

    new_subjectivity: float = (((old_subjectivity * old_weight) + new_subjectivity) / (old_weight + 1))

    return new_polarity, new_subjectivity


users = prepare_user(full_data, 'data/import_data/all_users_mentions_textblob.csv')
print(len(users))
create_sentiment_relationships(replydata, users, './data/import_data/user_relationships_mentions_textblob.csv', True, True)

