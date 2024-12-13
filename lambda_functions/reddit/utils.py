import praw
import yaml
import json
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
def store_df_in_bucket(
    df,
    file_key:str,
    bucket_name:str='bucket-iot-sentiment-analysis',
    bucket_location:str='eu-west-2'
):    
    st_client = boto3.client('s3', bucket_location)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    # Upload CSV to S3
    st_client.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=csv_buffer.getvalue()
    )
    return {
            "statusCode": 200,
            "body": f"CSV file with {len(df)} rows successfully uploaded to {bucket_name}/{file_key}"
        }


def test_csv_bucket_store(bucket_name = 'bucket-iot-sentiment-analysis',
                        bucket_location = 'eu-west-2',
                        file_key = '003.csv',
                            ):
    # TODO implement 
    """
    Test function to upload a simple DataFrame to an S3 bucket as a CSV file.

    Parameters
    ----------
    bucket_name : str, optional
        The name of the S3 bucket to upload the CSV file to. Default is 'bucket-iot-sentiment-analysis'.
    bucket_location : str, optional
        The AWS region where the S3 bucket is located. Default is 'eu-west-2'.
    file_key : str, optional
        The key (path) under which the CSV file will be stored in the S3 bucket. Default is '003.csv'.

    Returns
    -------
    dict
        A dictionary containing the status code and a message indicating the success or failure of the upload.
    """
    st_client = boto3.client('s3', bucket_location)
    data = {'name': ['maria', 'nicolas']}
    df = pd.DataFrame(data)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    try:
        # Upload CSV to S3
        st_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue()
        )
        return {
            "statusCode": 200,
            "body": f"CSV file successfully uploaded to {bucket_name}/{file_key}"
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }


def read_yaml(file_path):
    """Read YAML file and return the data as a Python object.
    
    Parameters
    ----------
    file_path : str
        Path to the YAML file to be read.
    
    Returns
    -------
    object
        The data from the YAML file loaded into a Python object.
    """
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data

def init_reddit(key_yaml:str):
    """
    Initialize a Reddit instance using the credentials stored in a YAML file.
    
    Parameters
    ----------
    key_yaml : str
        Path to the YAML file containing the Reddit API credentials.
    
    Returns
    -------
    reddit : praw.Reddit
        An instance of the Reddit class, ready to be used to interact with the Reddit API.
    
    Notes
    -----
    The YAML file should contain the following keys:
    
        - client_id
        - client_secret
        - username
        - password
    
    """
    keys = read_yaml(key_yaml)
    reddit = praw.Reddit(
        client_id=keys['client_id'],
        client_secret=keys['client_secret'],
        user_agent='senti',
        username=keys['username'],
        password=keys['password']
    )
    return reddit

def get_lasts_posts(key_yaml:str,
                    subreddit_name:str="all", 
                    query:str="Bitcoin", 
                    since_minutes:int=30, 
                    limit=1000):
    # Initialize Reddit instance
    """
    Fetch the last 'limit' posts from the specified subreddit that match the query and are newer than 'since_minutes' minutes ago.
    
    Parameters
    ----------
    key_yaml : str
        Path to the YAML file containing the Reddit API credentials.
    subreddit_name : str, optional
        The subreddit to search. Defaults to "all".
    query : str, optional
        The search query. Defaults to "Bitcoin".
    since_minutes : int, optional
        The time limit in minutes. Defaults to 30.
    limit : int, optional
        The maximum number of posts to fetch. Defaults to 1000.
    
    Returns
    -------
    df : Pandas DataFrame
        A DataFrame containing the fetched posts and their metadata. The columns include the post title, author, URL, creation time, upvotes, type (title or comment), and the number of comments.
    """
    reddit = init_reddit(key_yaml)  
    # Define the subreddit and query
    subreddit_name = "all"  # Replace with your subreddit of choice
    query = "Bitcoin"  # Search keyword
    # Search subreddit for posts
    subreddit = reddit.subreddit(subreddit_name)
    posts = subreddit.search(query, sort="new", limit=limit)  # Adjust limit as needed
    start_time = datetime.utcnow() - timedelta(minutes=since_minutes)
    main_posts = []
    for post in posts:
        created_time = datetime.utcfromtimestamp(post.created_utc)
        created_time_str = created_time.strftime('%Y-%m-%d %H:%M:%S')
        if created_time >= start_time:  # Filter by time
            list_comments = post.comments.list()
            post_dict = {
                'title': post.title,
                'body': post.selftext,
                'author': post.author, 
                'url': post.url, 
                'created_utc': created_time_str, 
                'upvotes': post.score, 
                'type':'title', 
                'comments':len(list_comments)
            }
            main_posts.append(post_dict)
            for comment in list_comments:
                utc_datetime = datetime.utcfromtimestamp(comment.created_utc)
                utc_datetime_str = utc_datetime.strftime('%Y-%m-%d %H:%M:%S')
                commment_dict = {
                    'title': comment.body, 
                    'body': '',  # Comments do not have an associated selftext
                    'author': comment.author, 
                    'url': comment.permalink, 
                    'created_utc': utc_datetime_str, 
                    'upvotes': comment.score, 
                    'type':'comment', 
                    'comments':None
                }
                main_posts.append(commment_dict)
    df = pd.DataFrame(main_posts)
    return df

def include_time_in_filename(filename:str):
    """
    Append the current UTC timestamp to a given filename.

    Parameters
    ----------
    filename : str
        The original filename to which the timestamp will be appended. 
        It should include the file extension.

    Returns
    -------
    str
        A new filename with the current UTC timestamp appended before the file extension,
        in the format 'YYYYMMDD_HHMMSS'.
    """
    utc_now = datetime.utcnow()
    # Store the date and hour in a variable to save a file name
    file_name, ext = filename.split('.')
    time_str = f"{utc_now.strftime('%Y%m%d_%H%M%S')}"
    save_in = file_name + '_' + time_str + '.' + ext
    return save_in

def init_finance_sentiment_analyzer(financial_terms_yaml:str="financial_terms.yaml"):
    # Initialize the VADER sentiment analyzer
    """
    Initialize a VADER sentiment analyzer with financial terms added to the lexicon.

    Parameters
    ----------
    financial_terms_yaml : str, optional
        Path to the YAML file containing the financial terms to be added to the lexicon.
        Defaults to 'financial_terms.yaml'.
    
    Returns
    -------
    SentimentIntensityAnalyzer
        A sentiment analyzer with the financial terms added to the lexicon.
    """
    analyzer = SentimentIntensityAnalyzer()
    # Add financial terms to the lexicon
    # Combine all terms into a single dictionary
    financial_terms = read_yaml(financial_terms_yaml)
    all_terms = {
        **financial_terms["positive_terms"],
        **financial_terms["negative_terms"],
        **financial_terms["neutral_terms"]
    }
    analyzer.lexicon.update(all_terms)
    return analyzer

def add_sentiments_to_df(df, analyzer, analyzer_type='vader'):
    """
    Add sentiment scores to a DataFrame.
    
    Parameters
    ----------
    df : Pandas DataFrame
        DataFrame containing the columns 'title' and 'body'.
    analyzer : SentimentIntensityAnalyzer
        A sentiment analyzer with the financial terms added to the lexicon.
    analyzer_type : str, optional
        The type of sentiment analyzer. Only 'vader' is supported. Defaults to 'vader'.
    
    Returns
    -------
    Pandas DataFrame
        The input DataFrame with additional columns for the sentiment scores.
    """
    sentiments = []
    for idx, row in df.iterrows(): 
        # Get sentiment scores
        text = row['title'] + " " + row['body']
        if analyzer_type == 'vader':
            scores = analyzer.polarity_scores(text)
        else:
            raise NotImplementedError
        sentiments.append(scores)
    sentiments = pd.DataFrame(sentiments)
    df  = pd.concat([df, sentiments], axis=1)
    return df