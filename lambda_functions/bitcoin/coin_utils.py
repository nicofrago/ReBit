import boto3
import requests
import pandas as pd
from io import StringIO
from datetime import datetime


def fetch_crypto_prices(coins:list=['bitcoin', 'ethereum', 'solana', 'dogecoin', 'cardano']):
    # Fetch prices for Bitcoin, Ethereum, Solana, Dogecoin, and Cardano
    coins_names = ','.join(coins)
    url = 'https://api.coingecko.com/api/v3/simple/price'
    params = {
        'ids': coins_names,
        'vs_currencies': 'usd,eur,gbp'
    }
    response = requests.get(url, params=params)
    coins_price = response.json()
    coins_price = pd.DataFrame(coins_price)
    coins_price.index.name = 'currency'
    coins_price.reset_index(inplace=True)
    utc_now = datetime.utcnow()
    time_str = f"{utc_now.strftime('%Y-%m-%d %H:%M:%S')}"
    coins_price['date'] = time_str
    return coins_price

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

def store_df_in_bucket(df,
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
            "body": f"CSV file successfully uploaded to {bucket_name}/{file_key}"
        }