import json
from io import StringIO
from datetime import datetime
from coin_utils import (
    fetch_crypto_prices,
    store_df_in_bucket,
    include_time_in_filename
)

def lambda_handler(event, context):
    df_coins = fetch_crypto_prices()
    now = datetime.utcnow()
    save_in = 'coins/coins.csv'
    save_in = include_time_in_filename(save_in)
    response = store_df_in_bucket(df_coins, save_in)
    return response