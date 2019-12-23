import os
import time
from datetime import datetime, timedelta
import pandas as pd
from searchtweets import load_credentials, collect_results, gen_rule_payload
from pandas.io.json import json_normalize

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

        


print ('starting here')
use_premium()


