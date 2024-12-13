import yaml
import praw
import json
import boto3
import pandas as pd
from io import StringIO
from utils import (
    init_reddit, 
    test_csv_bucket_store, 
    store_df_in_bucket, 
    get_lasts_posts, 
    include_time_in_filename
)

def lambda_handler(event, context):
    bucket_name = 'bucket-iot-sentiment-analysis'
    bucket_location = 'eu-west-2'
    key_yaml = 'keys/reddit.yaml'
    df = get_lasts_posts(key_yaml, 
                        subreddit_name="all", 
                        query="Bitcoin", 
                        since_minutes=10, 
                        limit=10000
                        )
    save_in = 'reddit_comments/coins.csv'
    save_in = include_time_in_filename(save_in)
    status_code = store_df_in_bucket(df, save_in)
    return status_code