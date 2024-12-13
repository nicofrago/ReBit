import dash
import random
import numpy as np
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objects as go

# Load your own data
months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
channels = ['Direct', 'Organic', 'Referral']

# Create the DataFrame
df = pd.DataFrame({
    'month': months * 2,
    'sales': np.random.randint(10000, 20000, size=24),
    'visitors': [random.randint(1000, 5000) for _ in range(24)],
    'channel': random.choices(channels, k=24)
})

# Create the Dash app
app = dash.Dash(__name__, external_stylesheets=['https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css'])

# Define the layout
app.layout = html.Div(className='container-fluid', children=[
    # Header
    html.H1('My Custom Dashboard', className='text-center my-4'),

    # Metrics
    html.Div(className='row my-4', children=[
        html.Div(className='col-md-3', children=[
            html.Div(className='card', children=[
                html.Div(className='card-body', children=[
                    html.H5('Total Sales', className='card-title'),
                    html.P(f'${df["sales"].sum():,.2f}', className='card-text display-4 text-primary')
                ])
            ])
        ]),
        # Add more metric cards as needed
    ]),

    # Charts
    html.Div(className='row my-4', children=[
        html.Div(className='col-md-6', children=[
            dcc.Graph(
                id='sales-chart',
                figure=go.Figure(data=[
                    go.Bar(x=df['month'], y=df['sales'])
                ], layout={'title': 'Monthly Sales'})
            )
        ]),
        html.Div(className='col-md-6', children=[
            dcc.Graph(
                id='traffic-chart',
                figure=go.Figure(data=[
                    go.Pie(labels=df['channel'], values=df['visitors'])
                ], layout={'title': 'Traffic Channels'})
            )
        ])
    ])
])

if __name__ == '__main__':
    app.run_server(debug=True)