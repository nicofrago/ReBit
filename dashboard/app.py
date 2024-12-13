import os
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import logging  # Add logging

from utils import (
    fetch_initial_bitcoin_data,
    fetch_initial_reddit_comments,
    fetch_new_bitcoin_data,
    fetch_new_reddit_data,
    send_whatsapp_rebit_message
)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create the Dash app
app = dash.Dash(__name__)
server = app.server  # Expose the underlying Flask server

# AWS S3 Configuration
BUCKET_NAME = 'bucket-iot-sentiment-analysis'
HOURS = 12

# Data cache
bitcoin_data = pd.DataFrame()  # Empty DataFrame to store data
reddit_data = pd.DataFrame()

def update_graph(n):
    """Update the graph with the latest Bitcoin price data"""
    global bitcoin_data
    if bitcoin_data.empty:
        bitcoin_data = fetch_initial_bitcoin_data(HOURS)

    new_data = fetch_new_bitcoin_data(bitcoin_data)
    if new_data is not None and not new_data.empty:
        bitcoin_data = pd.concat([bitcoin_data, new_data], ignore_index=True)
        bitcoin_data = bitcoin_data.drop_duplicates(subset=['date']).sort_values('date')

    # Filter the DataFrame to maintain an `HOURS`-hour window
    now = datetime.utcnow()
    twelve_hours_ago = now - timedelta(hours=HOURS)
    bitcoin_data = bitcoin_data[bitcoin_data['date'] >= twelve_hours_ago]

    if bitcoin_data.empty:
        return go.Figure().update_layout(title="No data available")

    # Create Plotly line chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=bitcoin_data['date'],
        y=bitcoin_data['bitcoin'],
        mode='lines+markers',
        name='Bitcoin Price',
        line=dict(color='blue')
    ))

    fig.update_layout(
        title=f"Bitcoin Price (Last {HOURS} Hours)",
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        template='plotly_white'
    )
    return fig

def update_reddit_graph(n):
    """Update the graph with the latest Reddit sentiment data"""
    global reddit_data
    if reddit_data.empty:
        reddit_data = fetch_initial_reddit_comments(HOURS)

    new_data = fetch_new_reddit_data(reddit_data)
    if new_data is not None and not new_data.empty:
        reddit_data = pd.concat([bitcoin_data, new_data], ignore_index=True)
    #     reddit_data = reddit_data.drop_duplicates(subset=['date']).sort_values('date')

    # Filter the DataFrame to maintain an `HOURS`-hour window
    now = datetime.utcnow()
    hours_ago = now - timedelta(hours=HOURS)
    reddit_data['date'] = reddit_data['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    reddit_data = reddit_data[reddit_data['date'] >= hours_ago]

    if reddit_data.empty:
        return go.Figure().update_layout(title="No data available")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=reddit_data['date'],
        y=reddit_data['compound_mean'],
        mode='lines+markers',
        name='Sentiment compound mean',
        line=dict(color='red')
    ))

    fig.update_layout(
        title=f"Sentiment Compound (Last {HOURS} Hours)",
        xaxis_title="Time",
        yaxis_title="Compound",
        template='plotly_white'
    )
    return fig

# Define the layout
app.layout = html.Div([
    html.H1(f"Bitcoin Price and Reddit Sentiment (Last {HOURS} Hours)", style={"textAlign": "center"}),
    html.Div([
        dcc.Graph(id="bitcoin-graph"),
        dcc.Graph(id="reddit-graph")
    ], style={"display": "flex"}),
    dcc.Interval(
        id="interval-component",
        interval=600000,  # Update every 10 minutes
        n_intervals=0
    )
])

# Define the callbacks
app.callback(
    Output("bitcoin-graph", "figure"),
    Input("interval-component", "n_intervals")
)(update_graph)

app.callback(
    Output("reddit-graph", "figure"),
    Input("interval-component", "n_intervals")
)(update_reddit_graph)

scheduler = BackgroundScheduler()
def scheduled_job():
    logging.info("Executing scheduled job")
    send_whatsapp_rebit_message(bitcoin_data, reddit_data)

scheduler.add_job(scheduled_job, 'interval', hours=24) # minutes, hours
scheduler.start()

# Run the app
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))  # Use Heroku's port if available
    while True:
        try:
            app.run_server(debug=False, port=port, host="0.0.0.0")
            break
        except OSError:
            port += 1