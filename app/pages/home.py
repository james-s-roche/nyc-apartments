import dash
from dash import dcc, html, callback
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from io import StringIO

from app.data_utils import data_aggregation

# Register the page
dash.register_page(__name__, path='/')

# Define metric options for the dropdown
METRIC_OPTIONS = {
    'price_mean': 'Mean Price',
    'price_median': 'Median Price',
    'bedroom_count_mean': 'Mean Bedrooms',
    'bedroom_count_median': 'Median Bedrooms',
    'living_area_size_mean': 'Mean Size (sqft)',
    'living_area_size_median': 'Median Size (sqft)',
    'listing_count': 'Number of Listings'
}

# Define the layout for the page
layout = dbc.Container([
    # This store is specific to the page and holds the aggregated data
    dcc.Store(id='agg-data-store'),

    dbc.Row([
        dbc.Col([
            html.H4("Controls"),
            dbc.Label("Group By:"),
            dbc.RadioItems(
                options=[
                    {'label': 'Neighborhood', 'value': 'area_name'},
                    {'label': 'Zip Code', 'value': 'zip_code'},
                ],
                value='area_name',
                id='group-by-radio',
                inline=True,
            ),
            html.Br(),
            dbc.Label("Color:"),
            dcc.Dropdown(
                id='metric-dropdown',
                options=[{'label': v, 'value': k} for k, v in METRIC_OPTIONS.items()],
                value='price_median'
            ),
            html.Br(),
            dbc.Label("Size:"),
            dcc.Dropdown(
                id='size-dropdown',
                options=[{'label': v, 'value': k} for k, v in METRIC_OPTIONS.items()],
                value='listing_count',
                clearable=True
            ),
        ], width=12, md=3, className="bg-light p-3"),
        
        dbc.Col([
            dcc.Loading(
                id="loading-map",
                type="circle",
                children=dcc.Graph(id='map-graph', style={'height': '70vh'})
            )
        ], width=12, md=9),
    ]),
], fluid=True)

# --- Callbacks ---

# 1. Aggregate data when grouping changes or when the main data store is loaded
@callback(
    Output('agg-data-store', 'data'),
    Input('group-by-radio', 'value'),
    Input('filtered-listings-store', 'data') # Listen to the new filtered store
)
def update_aggregated_data(group_by, listings_json):
    if not listings_json:
        return None
    
    df = pd.read_json(StringIO(listings_json), orient='split')
    agg_df = data_aggregation(df, group_by)
    return agg_df.to_json(date_format='iso', orient='split')

# 2. Update map when aggregated data or metric changes
@callback(
    Output('map-graph', 'figure'),
    Input('agg-data-store', 'data'),
    Input('metric-dropdown', 'value'), 
    Input('size-dropdown', 'value'),   
    Input('group-by-radio', 'value')
)
def update_map(agg_data_json, color_metric, size_metric, group_by):
    if not agg_data_json:
        return go.Figure().update_layout(title="No data available.")

    df = pd.read_json(StringIO(agg_data_json), orient='split')
    
    if df.empty:
        return go.Figure().update_layout(title="No data to display for the selected criteria.")
    
    def get_format_string(metric):
        """Returns a d3-format string based on the metric name."""
        if 'price' in metric:
            return ':$,.0f'  # Format as currency, no decimals
        if 'listing' in metric: 
            return ':.0f' 
        return ':.1f'       # Default to one decimal place for other floats (like size/bedrooms)

    # Define hover data, starting with the essentials
    hover_data = {
        'latitude': False,
        'longitude': False,
        'listing_count': True, # Always show listing count with default formatting
        color_metric: get_format_string(color_metric),
    }

    # If a size metric is selected and it's different from the color metric, add it to hover data
    if size_metric and size_metric != color_metric:
        hover_data[size_metric] = get_format_string(size_metric)

    # Create the map figure using Plotly Express for simplicity
    fig = px.scatter_mapbox(
        df,
        lat='latitude',
        lon='longitude',
        size=size_metric if size_metric else 5, # Use a default size if None
        color=color_metric,
        hover_name=group_by,
        hover_data=hover_data,
        color_continuous_scale='Turbo',
        range_color=[df[color_metric].min(), df[color_metric].max()] if color_metric else None,
        size_max=30, # Set max bubble size
        zoom=10
    )

    fig.update_layout(
        mapbox = {"style":"carto-positron"},
        margin={"r":0,"t":0,"l":0,"b":0}
    )

    return fig
