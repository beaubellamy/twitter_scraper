import os
import time
from datetime import datetime, timedelta
import pandas as pd
import json
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


class TwitterListener(StreamListener):
    '''
    Twitter Listening class
    '''
    
    def __init__(self, api = StreamListener, 
                 tweet_count=0, 
                 max_tweets=10,
                 live_tweets = []):

        # Set the initial vallues
        self.tweet_count = tweet_count
        self.max_tweets = max_tweets
        self.live_tweets = live_tweets

    
    def on_data(self, data):
        '''
        Calling routine to get the live twitter feed
        '''

        global stream
        if self.tweet_count < self.max_tweets:
            if self.max_tweets < 100:
                print(data)                        
            else:
                if self.tweet_count % 100 == 0:
                    print(data)

            self.live_tweets.append(data)
            self.tweet_count += 1
            return True
        else:
            print ('The stream still needs to be disconnected')
            return False

    def on_error(self, status):
        ''' Return the error from the twitter stream. '''
        print(status)

def tweets_to_csv(live_tweets, filename, append_mode='a'):

    assert append_mode in ['r', 'w', 'a', 'r+'], 'append mode must be one of "r", "w", "a", or "r+"'

    if append_mode == 'w' and os.path.exists('intermediate.json'):
        os.remove('intermediate.json')

    for tweet in live_tweets:
        with open('intermediate.json', 'a') as file:
            # Convert the tweets to dictionary items
            json.dump(json.loads(tweet), file)
            file.write('\n')

    df = pd.read_json('intermediate.json', lines=True)
    df.to_csv(filename)


def use_premium(search, filename, from_date, to_date, enpoint='full'):
    '''
    Collect historical tweets
    '''
    if endpoint == '30day':
        endpoint_key = 'search_premium_30day_api'
        #endpoint_key = 'search_lynxx_30day_api'
    else:
        endpoint_key = 'search_premium_full_api'
        #endpoint_key = 'search_lynxx_full_api'

    try:
        tweet_df = pd.read_csv(filename, dtype=str, encoding = 'ISO-8859-1')
    except FileNotFoundError:
        tweet_df = pd.DataFrame()

    # Extract the credentials for the endpoint.
    search_stream = load_credentials(filename='./credentials.yaml',
                                     yaml_key=endpoint_key,
                                     env_overwrite=False)

    # Collect tweets while we are permitted. 
    # Todo: Still dont know how to catch the re-try limit error?
    while to_date > from_date:

        rule = gen_rule_payload(search, from_date=from_date, to_date=to_date, results_per_call=100)
        try:
            tweets = collect_results(rule, max_results=2000, result_stream_args=search_stream)
        except:
            break

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


def use_live_stream(tracklist, filename, append_mode, n_tweets=10):
    '''
    Collect tweets from the live stream
    '''

    tweet_count = 0

    # Handles Twitter authentification and the connection to Twitter Streaming API
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    listener = TwitterListener(tweet_count=tweet_count, 
                               max_tweets=n_tweets,
                               live_tweets = [])

    stream = Stream(auth, listener)
    stream.disconnect()

    stream.filter(track=tracklist, languages=['en'])
    print (len(listener.live_tweets))
    
    tweets_to_csv(listener.live_tweets, filename, append_mode)
    print ('end')

if __name__ == '__main__':
    
    # Premium
    #search = 'Qantas'
    #filename = 'scraper_results.csv'
    #from_date='2019-12-01'
    #to_date='2019-12-15'

    # Can be 30day or fullarchive endpoints
    #use_premium(search, filename, from_date, to_date)

    # Live Stream
    tracklist = ','.join(['Airline', 
                     'Singapore Airlines', 'Singapore Air', 
                     'Air New Zealand', 
                     'Qantas', 
                     'Qatar Airways', 
                     'Virgin Australia', 'Virgin Air', 
                     'Emirates', 
                     'Nippon Air', 'All Nippon Airways', 
                     'EVA Air', 
                     'Cathay Pacific', 'Cathay', 
                     'Japan Air', 'Japan Airlines'])
    filename = 'airlines.csv'
    append_mode = 'w'
    n_tweets = 100

    use_live_stream(tracklist, filename, append_mode, n_tweets)
