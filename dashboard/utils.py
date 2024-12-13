import os 
import json
import boto3
import requests
import pandas as pd

from io import StringIO

import plotly.graph_objs as go

from datetime import datetime, timedelta
import logging

BUCKET_NAME = 'bucket-iot-sentiment-analysis'
HOURS = 3

def get_s3_client():
    """Create and return an S3 client"""
    ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
    SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    REGION = os.getenv('AWS_DEFAULT_REGION')
    return boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION
    )
def fetch_initial_bitcoin_data(hours:int=3):
    """Fetch Bitcoin price data for the last `HOURS` hours once"""
    s3 = get_s3_client()
    twelve_hours_ago = datetime.utcnow() - timedelta(hours=hours)
    all_bitcoin_data = []
    for hour_offset in range(hours + 1):
        target_time = twelve_hours_ago + timedelta(hours=hour_offset)
        for i in range(0, 60, 10):
            file_prefix = target_time.strftime("coins/coins_%Y%m%d_%H") + f"{i:02d}"
            try:
                response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=file_prefix)
                if "Contents" not in response:
                    continue
                latest_file = max(response["Contents"], key=lambda x: x["LastModified"])
                file_key = latest_file["Key"]
                file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
                csv_content = file_obj["Body"].read().decode("utf-8")
                df = pd.read_csv(StringIO(csv_content))
                usd_data = df[df["currency"] == "usd"]
                if not usd_data.empty:
                    all_bitcoin_data.append(usd_data)
            except Exception as e:
                print(f"Error fetching data for {file_prefix}: {e}")

    if all_bitcoin_data:
        combined_df = pd.concat(all_bitcoin_data, ignore_index=True)
        combined_df['date'] = pd.to_datetime(combined_df['date'])
        combined_df = combined_df.sort_values('date')
        return combined_df

    return pd.DataFrame()


def fetch_new_bitcoin_data(bitcoin_data: pd.DataFrame):
    """Fetch and append new Bitcoin data"""
    s3 = get_s3_client()
    last_timestamp = bitcoin_data['date'].max() if not bitcoin_data.empty else None
    now = datetime.utcnow()
    file_prefix = now.strftime("coins/coins_%Y%m%d_%H")
    new_data = []
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=file_prefix)
        if "Contents" in response:
            df, last_file_time = read_last_modify_file_from_bucket(s3, response)
            usd_data = df[df["currency"] == "usd"]
            if last_timestamp is not None:
                usd_data = usd_data[usd_data['date'] > last_timestamp]
            if not usd_data.empty:
                new_data.append(usd_data)
    except Exception as e:
        print(f"Error fetching new data: {e}")

    if new_data:
        return pd.concat(new_data, ignore_index=True)
    return pd.DataFrame()


def fetch_initial_reddit_comments(hours:int=3, output:str='sentimets'):
    s3 = get_s3_client()
    hours_ago = datetime.utcnow() - timedelta(hours=hours)
    all_reddit_data = []
    for hour_offset in range(hours + 1):
        target_time = hours_ago + timedelta(hours=hour_offset)
        for i in range(0, 60, 10):
            minutes = f"{i:02d}" if i < 10 else f"{i:01d}"[:1]
            file_prefix = target_time.strftime("reddit_comments/coins_%Y%m%d_%H") + minutes
            try:
                response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=file_prefix)
                if "Contents" not in response:
                    continue
                df, latest_file_time = read_last_modify_file_from_bucket(s3, response)
                if output == 'sentimets':
                    df_dict = comments2count(df)    
                    latest_file_time_str = f"{latest_file_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    df_dict['date'] = latest_file_time_str
                    df_feelings = pd.DataFrame(df_dict, index=[0])
                    if not df_feelings.empty:
                        all_reddit_data.append(df_feelings)
                elif output == 'comments':
                    all_reddit_data.append(df)
                else:
                    raise notImplementedError
            except Exception as e:
                # print(f"Error fetching data for {file_prefix}: {e}")
                pass
    if all_reddit_data:
        combined_df = pd.concat(all_reddit_data, ignore_index=True)
        return combined_df
    
def read_last_modify_file_from_bucket(s3, response):
    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])
    file_key = latest_file["Key"]
    file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    csv_content = file_obj["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(csv_content))
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    return df, latest_file['LastModified']

def comments2count(df):
    positive_count = sum(df['compound'] > 0.05)
    negative_count = sum(df['compound'] < -0.05)
    neutral_count = sum((df['compound'] >= -0.05) & (df['compound'] <= 0.05))
    compound_mean = df['compound'].mean()
    dict_feelings = {
        'positive_count': positive_count,
        'negative_count': negative_count,
        'neutral_count': neutral_count,
        'compound_mean': compound_mean,
    }
    return dict_feelings

    
def fetch_new_reddit_data(reddit_data: pd.DataFrame):
    """Fetch and append new Reddit data"""
    s3 = get_s3_client()
    last_timestamp = reddit_data['date'].max() if not reddit_data.empty else None    
    last_timestamp = datetime.strptime(last_timestamp, '%Y-%m-%d %H:%M:%S')
    
    now = datetime.utcnow()
    file_prefix = now.strftime("reddit_comments/coins_%Y%m%d_%H")
    new_data = []
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=file_prefix)
        if "Contents" in response:
            df, latest_file_time = read_last_modify_file_from_bucket(s3, response)
            latest_file_time = latest_file_time.replace(tzinfo=None)
            if last_timestamp is not None and latest_file_time >= last_timestamp:
                dict_feelings = comments2count(df)
                dict_feelings['date'] = latest_file_time.strftime('%Y-%m-%d %H:%M:%S')
                df_feelings = pd.DataFrame(dict_feelings, index=[0])
                new_data.append(df_feelings)
    except Exception as e:
        print(f"Error fetching new data: {e}")
    if new_data:
        return pd.concat(new_data, ignore_index=True)
    return pd.DataFrame()

def send_whatsapp_message(message:str):
    ACCESS_TOKEN = os.getenv('WSP_TOKEN')
    PHONE_NUMBER_ID = os.getenv('WSP_PHONE')
    RECIPIENT_PHONE_NUMBER = os.getenv('WSP_PHONE_TARGET')

    # API URL
    URL = f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages"

    # Headers for the API request
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    # Data payload for custom text message
    data = {
        "messaging_product": "whatsapp",
        "to": RECIPIENT_PHONE_NUMBER,
        "type": "text",
        "text": {
            "body": f'{message}'
        }
    }

    # Sending the POST request
    response = requests.post(URL, headers=headers, data=json.dumps(data))

    # Check and print the response
    if response.status_code == 200:
        logging.info(f"Message sent successfully: {response.json()}")
    else:
        logging.info("Message sent successfully", exc_info=response.json())
        logging.info("Error:", exc_info=response.json())

# weighted sum
def compoud2index(compound:float):
    return (compound + 1) * 50

def get_fear_and_greed_index(sentiments_data:pd.DataFrame):
    sum_per_row = sentiments_data.iloc[:, :3].sum(axis=1)
    weight_per_row = sum_per_row / sum_per_row.sum()
    mean_compound = (weight_per_row * sentiments_data['compound_mean']).sum()
    return compoud2index(mean_compound).round(2)

def get_fear_and_greed_message(sentiment_value:float):
# Dictionary mapping ranges to labels and colors
    fear_greed_dict = {
        (0, 25): {"label": "Extreme Fear", "color": "red"},
        (26, 50): {"label": "Fear", "color": "orange"},
        (51, 75): {"label": "Greed", "color": "light green"},
        (76, 100): {"label": "Extreme Greed", "color": "dark green"},
    }

    # Function to get the label and color based on the value
    def get_fear_greed_label(value):
        for range_tuple, attributes in fear_greed_dict.items():
            if range_tuple[0] <= value <= range_tuple[1]:
                return attributes
        return {"label": "Unknown", "color": "gray"}  # Default for out-of-range values

    # Get the label and color for the sentiment value
    result = get_fear_greed_label(sentiment_value)

    # Generate the message
    message = (
        f"📊 **Bitcoin Sentiment Index**\n"
        f"- Current Sentiment: {sentiment_value}/100\n"
        f"- Interpretation: {result['label']} ({result['color']})"
        f"\n"
    )
    return message

def get_bitcoin_message(last_price, initial_price):
    price_change_percentage = ((last_price - initial_price) / initial_price) * 100
    price_change_percentage = price_change_percentage.round(2)
    sign = '+' if price_change_percentage >=0 else '-'
    text = '''
    📊 Daily Summary:
    🔹 Bitcoin Price: ${}
    🔹 Change (Last 24 Hours): {}{}%
    '''.format(last_price, sign, price_change_percentage)    
    return text
    
def get_state_update_message():
    return '''Stay updated for more insights! 🚀'''

def send_whatsapp_rebit_message(bitcoin_data:pd.DataFrame, sentiments_data:pd.DataFrame):
    if bitcoin_data.empty or sentiments_data.empty:
        return False
    logging.info("send_whatsapp_rebit_message called")
    if bitcoin_data.empty or sentiments_data.empty:
        return
    initial_price = bitcoin_data.iloc[0].bitcoin
    last_price = bitcoin_data.iloc[-1].bitcoin
    bitcoin_message = get_bitcoin_message(last_price, initial_price)
    fear_greed = get_fear_and_greed_index(sentiments_data)
    fear_greed_message = get_fear_and_greed_message(fear_greed)
    message = bitcoin_message + fear_greed_message + get_state_update_message()
    try:
        send_whatsapp_message(message)
        logging.info("Message sent successfully")
    except Exception as e:
        logging.error(f"Error sending message: {e}")
    return

def get_comments2sentiments_per_minutes(comments, minutes:int=10):
    # Ensure 'created_utc' is in datetime format
    comments['created_utc'] = pd.to_datetime(comments['created_utc'])

    starting_date_hour = str(comments.created_utc.min())[:-6]
    ending_date_hour = str(comments.created_utc.max())[:-6]

    current_time = datetime.strptime(starting_date_hour, '%Y-%m-%d %H')
    end_time = datetime.strptime(ending_date_hour, '%Y-%m-%d %H')
    sentiments = []
    while current_time <= end_time:
        current_time_plus_delta = current_time + timedelta(minutes=minutes)
        comments_interval = comments[(comments['created_utc'] >= current_time) & (comments['created_utc'] < current_time_plus_delta)]
        if comments_interval.shape[0] > 0:
            sentiments_interval = comments2count(comments_interval)
            sentiments_interval['date'] = current_time_plus_delta
            sentiments.append(sentiments_interval)
        current_time = current_time_plus_delta
    sentiments = pd.DataFrame(sentiments)
    return sentiments

def bitcoin_sentiment_scatter_norm(bitcoin_data:pd.DataFrame, sentiments:pd.DataFrame):
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=bitcoin_data['date'],
        y=bitcoin_data['bitcoin_norm'],
        mode='lines',
        name='Bitcoin Data Norm',
        marker=dict(color='blue')
    ))

    fig.add_trace(go.Scatter(
        x=sentiments['date'],
        y=sentiments['compound_norm'],
        mode='lines',
        name='Sentiment_norm',
        marker=dict(color='red')
    ))

    fig.update_layout(
        title='Bitcoin Data Norm and Sentiment Norm Scatter Plot',
        xaxis_title='Index',
        yaxis_title='Value'
    )

    fig.show()