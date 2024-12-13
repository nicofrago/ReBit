import os
import pandas as pd
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import logging

from utils import (
    fetch_initial_bitcoin_data,
    fetch_initial_reddit_comments,
    fetch_new_bitcoin_data,
    fetch_new_reddit_data,
    send_whatsapp_rebit_message
)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize Dash App
app = dash.Dash(
    __name__,
    external_stylesheets=[
        "https://cdn.jsdelivr.net/npm/bootswatch@5.3.0/dist/flatly/bootstrap.min.css",
        "https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap"
    ]
)
server = app.server

# AWS S3 Configuration
BUCKET_NAME = 'bucket-iot-sentiment-analysis'
HOURS = 12

# Data cache
bitcoin_data = pd.DataFrame()
reddit_data = pd.DataFrame()

def update_graph(n):
    global bitcoin_data
    if bitcoin_data.empty:
        bitcoin_data = fetch_initial_bitcoin_data(HOURS)

    new_data = fetch_new_bitcoin_data(bitcoin_data)
    if new_data is not None and not new_data.empty:
        bitcoin_data = pd.concat([bitcoin_data, new_data], ignore_index=True)
        bitcoin_data = bitcoin_data.drop_duplicates(subset=['date']).sort_values('date')

    now = datetime.utcnow()
    twelve_hours_ago = now - timedelta(hours=HOURS)
    bitcoin_data = bitcoin_data[bitcoin_data['date'] >= twelve_hours_ago]

    if bitcoin_data.empty:
        return go.Figure().update_layout(title="No data available")

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
        template='plotly_white',
        margin=dict(l=30, r=30, t=40, b=30),
        font=dict(family="Inter, sans-serif", size=12, color="#333")
    )
    return fig

def update_reddit_graph(n):
    global reddit_data
    if reddit_data.empty:
        reddit_data = fetch_initial_reddit_comments(HOURS)

    new_data = fetch_new_reddit_data(reddit_data)
    if new_data is not None and not new_data.empty:
        reddit_data = pd.concat([reddit_data, new_data], ignore_index=True)

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
        template='plotly_white',
        margin=dict(l=30, r=30, t=40, b=30),
        font=dict(family="Inter, sans-serif", size=12, color="#333")
    )
    return fig

# App Layout
app.layout = dbc.Container(fluid=True, children=[
    dbc.Row([
        dbc.Col(html.H1(
            f"Bitcoin Price and Reddit Sentiment (Last {HOURS} Hours)",
            className="text-center mt-3 mb-3",
            style={"fontWeight": "700", "fontFamily": "Inter, sans-serif", "fontSize": "2rem", "color": "#333"}
        ), width=12)
    ]),
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader("Bitcoin Price Over Time", className="bg-transparent border-0"),
            dbc.CardBody(dcc.Graph(id="bitcoin-graph"))
        ], className="shadow-sm"), width=8),
        dbc.Col(dbc.Card([
            dbc.CardHeader("Reddit Sentiment Over Time", className="bg-transparent border-0"),
            dbc.CardBody(dcc.Graph(id="reddit-graph"))
        ], className="shadow-sm"), width=4),
    ], className="mb-4"),
    dcc.Interval(
        id="interval-component",
        interval=600000,  # 10 minutes
        n_intervals=0
    )
])

# Callbacks
@app.callback(Output("bitcoin-graph", "figure"), Input("interval-component", "n_intervals"))
def update_bitcoin_callback(n):
    return update_graph(n)

@app.callback(Output("reddit-graph", "figure"), Input("interval-component", "n_intervals"))
def update_reddit_callback(n):
    return update_reddit_graph(n)

scheduler = BackgroundScheduler()
def scheduled_job():
    logging.info("Executing scheduled job")
    send_whatsapp_rebit_message(bitcoin_data, reddit_data)

scheduler.add_job(scheduled_job, 'interval', hours=24)
scheduler.start()

# Run the App
if __name__ == "__main__":
    app.run_server(debug=True)

    # port = int(os.environ.get("PORT", 8050))
    # while True:
    #     try:
    #         app.run_server(debug=False, port=port, host="0.0.0.0")
    #         break
    #     except OSError:
    #         port += 1
