import dash
from dash import dcc, html, callback
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from io import StringIO 
from app.data_utils import neighborhood_aggregation_recursive

# Register the page with a specific path
dash.register_page(__name__, path='/treemap')

# Define metric options for the dropdown, similar to the map page
METRIC_OPTIONS = {
    'price_mean': 'Mean Price',
    'price_median': 'Median Price',
    'living_area_size_mean': 'Mean Size (sqft)',
    'listing_count': 'Number of Listings'
}

# Define the layout for the treemap page
layout = dbc.Container([
    # Store for the data prepared for the treemap (hierarchy + aggregated metrics)
    dcc.Store(id='treemap-data-store'),

    dbc.Row([
        dbc.Col([
            html.H4("Controls"),
            dbc.Label("Color By:"),
            dcc.Dropdown(
                id='treemap-metric-dropdown',
                options=[{'label': v, 'value': k} for k, v in METRIC_OPTIONS.items()],
                value='price_median'
            ),
        ], width=12, md=3, className="bg-light p-3"),

        dbc.Col([
            dcc.Loading(
                id="loading-treemap",
                type="circle",
                children=dcc.Graph(id='treemap-graph', style={'height': '80vh'})
            )
        ], width=12, md=9),
    ]),
], fluid=True)

# --- Callbacks ---

# 1. Prepare data for the treemap
@callback(
    Output('treemap-data-store', 'data'),
    Input('listings-data-store', 'data') # Trigger when the main data is loaded
)
def prepare_treemap_data(listings_json):
    if not listings_json:
        return None

    # Load listings data
    # df = pd.read_json(StringIO(listings_json), orient='split')
    hierarchical_data = neighborhood_aggregation_recursive()
    return hierarchical_data.to_json(date_format='iso', orient='split')


# 2. Update treemap when the prepared data or metric changes
@callback(
    Output('treemap-graph', 'figure'),
    Input('treemap-data-store', 'data'),
    Input('treemap-metric-dropdown', 'value')
)
def update_treemap(treemap_data_json, metric):
    if not treemap_data_json:
        return px.treemap().update_layout(title_text="Loading data...")

    df = pd.read_json(StringIO(treemap_data_json), orient='split')


    fig = px.treemap(
        df,
        names='name', 
        parents='parent_name',
        values='area_listing_count', # Size of the sectors
        color=metric, # Color of the sectors based on the selected metric
        # hover_data=[metric, 'total_listing_count'],
        hover_data={
            'name': True,
            'parent_name': False,
            'area_listing_count': False,
            'listing_count': True,
            metric: ':.2f' # format the metric
            
        },
        color_continuous_scale='Jet',
        title=f"NYC Neighborhoods by {METRIC_OPTIONS.get(metric, metric)}"
    )
    fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
    return fig
