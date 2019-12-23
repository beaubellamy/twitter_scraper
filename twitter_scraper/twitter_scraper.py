import os
import time
from datetime import datetime, timedelta
import pandas as pd
from searchtweets import load_credentials, collect_results, gen_rule_payload
from pandas.io.json import json_normalize
from twitter_keys import access_token, access_token_secret, consumer_key, consumer_secret

#from TwitterListener import TwitterListener
#import TwitterListener
from tweepy.streaming import StreamListener
from tweepy import Stream, OAuthHandler
import re

#Twitter sentiment
#-	Collect tweets
#-	Train sentiment model using movie reviews and twitter comments
#-	Perform sentiment analysis on full data set
#-	Perform sentiment analysis on weekly/monthly frequency
#Twitter engagement KPI.
#-	# of number of retweets
#-	Percentage of retweets of authored tweets (from @Qantas)
#-	Number of likes of authored tweets


def use_premium():

    search = 'Qantas'
    filename = 'scraper_results.csv'

    try:
        tweet_df = pd.read_csv(filename, dtype=str, encoding = 'ISO-8859-1')
    except FileNotFoundError:
        tweet_df = pd.DataFrame()

    search_stream = load_credentials(filename='./credentials.yaml',
                                     yaml_key='search_premium_30day_api',
                                     env_overwrite=False)

    from_date='2019-12-01'
    to_date='2019-12-15'
    while to_date > from_date:

        rule = gen_rule_payload(search, from_date=from_date, to_date=to_date, results_per_call=100)
        try:
            tweets = collect_results(rule, max_results=2000, result_stream_args=search_stream)
        except:
            break

        # if not use max=1000, and find the minmum date. Use this as to_date in next call.

        for idx, tweet in enumerate(tweets):
            tweet_df = tweet_df.append([json_normalize(tweet)], ignore_index=True, sort=False)
        
            if idx % 1000 == 0:
                print(f'{tweet["created_at"]}: {tweet["text"]}')
                tweet_df.to_csv(filename, index=False)
        
        tweet_df['created_at'] = pd.to_datetime(tweet_df['created_at'], utc=True)
        mindate = min(tweet_df['created_at']).date() - timedelta(hours=1)
        to_date = mindate.strftime('%Y-%m-%d %H:%M')
        


    tweet_df['created_at'] = pd.to_datetime(tweet_df['created_at'])
    min(tweet_df['created_at'])


    tweet_df.drop_duplicates(subset=['created_at', 'user.screen_name'], keep='first', inplace=True)
    tweet_df.sort_values(by='created_at', inplace=True)
    tweet_df.to_csv(filename, index=False)

# add the filename to the class
class TwitterListener(StreamListener):
    
    def __init__(self, api = StreamListener):
        #return super().__init__(api)
        self.filename = 'test.txt'

    
    def on_data(self, data):
        global tweet_count
        global n_tweets
        global stream
        global live_tweets
        if tweet_count < n_tweets:
            print(data)            

            with open(self.filename, 'a') as myfile:
                myfile.write(data)

            tweet_count += 1
            return True
        else:
            stream.disconnect()

    def on_error(self, status):
        print(status)

#with open(DATA_FILENAME, mode='w', encoding='utf-8') as f:
#    json.dump([], f)


#with open(DATA_FILENAME, mode='w', encoding='utf-8') as feedsjson:
#    entry = {'name': args.name, 'url': args.url}
#    feeds.append(entry)
#    json.dump(feeds, feedsjson)

def use_live_stream():
    
   
    # Create tracklist with the words that will be searched for
    tracklist = ['trump']
    # Initialize Global variable
    global tweet_count, n_tweets, stream, live_tweet_df, live_tweets
    tweet_count = 0

    # Input number of tweets to be downloaded
    n_tweets = 10

    #live_tweet_df = pd.DataFrame()
    live_tweets = []

    # Handles Twitter authentification and the connection to Twitter Streaming API
    l = TwitterListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=tracklist)
    #print (live_tweet_df.shape)

print ('starting here')
#use_premium()
use_live_stream()

