import csv

from textblob_de import TextBlobDE as TextBlob
from general import utils

from germansentiment import SentimentModel

model = SentimentModel()


def analyse_sentiments_bert(filename: str, _data: [dict], _header, batchsize=100):
    _header.append("sentiment_bert")
    with open(f'{filename}', 'w', encoding="utf-8", newline='') as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerow(header)

    result = []

    cache_text = []
    cache_data = []
    for idx in range(len(_data)):
        cache_text.append(_data[idx]["full_text"])
        cache_data.append(_data[idx])
        if (idx + 1) % batchsize == 0 or idx + 1 >= len(_data):
            sentiments = model.predict_sentiment(list(cache_text))
            result.extend(sentiments)
            update_sentiments_bert(filename, list(cache_data), sentiments)
            cache_text = []
            cache_data = []
            print(len(result))

    print(len(result))


def update_sentiments_bert(filename: str, _data: [dict], sentiments):
    with open(f'{filename}', 'a', encoding="utf-8", newline='') as f:
        writer = csv.writer(f, delimiter='|')
        for idx in range(len(_data)):
            row = list(_data[idx].values())
            row.append(sentiments[idx])
            writer.writerow(row)


def analyse_sentiment_textblob(filename: str, _data: [dict], _header: [], batchsize=100):
    _header.extend(["polarity", "subjectivity"])
    with open(f'{filename}', 'w', encoding="utf-8", newline='') as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerow(header)

    cache_sentiment = []
    cache_data = []
    for idx in range(len(_data)):
        blob = TextBlob(_data[idx]["full_text"])
        cache_sentiment.append(blob.sentiment)
        cache_data.append(_data[idx])
        if (idx + 1) % batchsize == 0 or idx + 1 >= len(_data):
            update_sentiment_textblob(filename, list(cache_data), list(cache_sentiment))
            cache_sentiment = []
            cache_data = []
            print(idx)


def update_sentiment_textblob(filename: str, _data: [dict], sentiments: []):
    with open(f'{filename}', 'a', encoding="utf-8", newline='') as f:
        writer = csv.writer(f, delimiter='|')
        for idx in range(len(_data)):
            row = list(_data[idx].values())
            row.append(sentiments[idx].polarity)
            row.append(sentiments[idx].subjectivity)
            writer.writerow(row)


analyzed_hashes_tweets, data, header, char_count, userids = utils.get_tweet_infos('data/dataset2/primary_tweets.csv',
                                                                                  hashtag_treshhold=0)
print(char_count)
analyse_sentiment_textblob("data/dataset2/primary_tweets_sentiment_textblob.csv", data, header)
# analyse_sentiment_textblob("data/dataset2/primary_tweets_sentiment.csv", data, header)
