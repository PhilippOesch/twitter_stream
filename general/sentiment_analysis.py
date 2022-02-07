from germansentiment import SentimentModel

model = SentimentModel()


def polarity_to_value(sentiment_str: str) -> int:
    switcher = {
        "positive": 1,
        "neutral": 0,
        "negative": -1,
    }
    return switcher[sentiment_str]


def get_sentiment(text: str) -> int:
    sentiment = model.predict_sentiment([text])[0]
    return polarity_to_value(sentiment)
