import tweepy
import json

with open("twitter_credentials.json", "r") as file:
    creds = json.load(file)

# python_tweets = Twython(
#     creds['CONSUMER_KEY'],
#     creds['CONSUMER_SECRET'],
#     creds['ACCESS_TOKEN'],
#     creds['ACCESS_SECRET']
# )

auth = tweepy.OAuthHandler(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'])
auth.set_access_token(creds['ACCESS_TOKEN'], creds['ACCESS_SECRET'])

python_tweets = tweepy.API(auth)