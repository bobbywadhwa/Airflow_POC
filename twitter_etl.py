import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():

    access_key = "mS2KIksr2Iv60BekcPEA2GLBc"
    access_secret = "xkyvX3s0AQxD5QniV1WedRp8d2DzC06xqQ2zqfJOXmXg1ZJJJi"
    consumer_key = "1039005650443890688-nRKGYkk0uJzI6aixybiKBQiYuqHOkf"
    consumer_secret = "mzfLkL8ENl28upnBwfCu6WxebyYKKlyAPtCmVKESoiI0n"

    #twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    #Creating an api object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@elonmusk',
                            count=200,
                            include_rts = False,
                            tweet_mode = 'extended')

    print(tweets)
    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user":tweet.user.screen_name,
                        'text': text,
                        'favorite_count': tweet.favorite_count,
                        'retweet_count': tweet.retweet_count,
                        'created_at': tweet.created_at }
       
        tweet_list.append(refined_tweet)



    df = pd.DataFrame(tweet_list)
    df.to_csv("elon_musk_twitter_data.csv")
