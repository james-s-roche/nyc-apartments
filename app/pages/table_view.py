import dash
from dash import dcc, html, callback, dash_table
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
from io import StringIO

dash.register_page(__name__, path='/table')

# Define the columns to display in the table
TABLE_COLUMNS = [
    "neighborhood", "street", "unit", "zip_code",  "price", 
    "bedrooms", "bathrooms", "furnished",
    # "bedroom_count", "half_bedroom_count","full_bathroom_count", "half_bathroom_count",
    "size (sq ft)", "furnished", "available_date", "status", "building_type", "date_updated"
]

layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H4("Listings Data"),
            dcc.Loading(
                id="loading-table",
                type="circle",
                children=dash_table.DataTable(
                    id='listings-table',
                    columns=[
                        {"name": "Link", "id": "listing_url", "presentation": "markdown"},
                        *[{"name": i.replace('_', ' ').title(), "id": i} for i in TABLE_COLUMNS]
                    ],
                    # css=["dbc"],
                    page_size=50,
                    style_table={'overflowX': 'auto'},
                    sort_action="native",
                    filter_action="native",
                    style_cell={'verticalAlign': 'middle'},
                ),
            )
        ],                 
        className="dbc dbc-row-selectable",
        width=12),
    ]),
], fluid=True)

@callback(
    Output('listings-table', 'data'),
    Input('filtered-listings-store', 'data')
)
def update_table(filtered_data_json):
    if not filtered_data_json:
        return []
    
    df = pd.read_json(StringIO(filtered_data_json), orient='split')
    
    # Create a markdown link for the streeteasy url
    df['listing_url'] = df['url_path'].apply(lambda url: f'[View](https://streeteasy.com{url})')
    df['bathrooms'] = df['full_bathroom_count'] + df['half_bathroom_count'] * 0.5
    df['furnished'] = df['furnished'].apply(lambda x: 'Yes' if x else 'No')
    df.rename(columns={'bedroom_count': 'bedrooms',
                       'area_name': 'neighborhood',
                       'living_area_size': 'size (sq ft)',
                       }, 
              inplace=True)

    # Format date for display
    df['available_date'] = pd.to_datetime(df['available_date']).dt.strftime('%Y-%m-%d')
    return df.to_dict('records')