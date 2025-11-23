import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import plotly.express as px
from scraping.get_neighborhood_leaf_nodes import get_neighborhoods

def treemap(df):
    fig = px.treemap(names=df['name'], parents=df['parent_name'])
    output_file = 'img/neighborhood_treemap.html'
    fig.write_html(output_file)
    print(f"Diagram saved to {output_file}")

def sunburst(df):
    fig = px.sunburst(names=df['name'], parents=df['parent_name'])
    output_file = 'img/neighborhood_sunburst.html'
    fig.write_html(output_file)
    print(f"Diagram saved to {output_file}")

def icicle(df):
    # size should be the sum of all children listings, but for now just use count
    fig = px.icicle(names=df['name'], parents=df['parent_name'])
    output_file = 'img/neighborhood_icicle.html'
    fig.write_html(output_file)
    print(f"Diagram saved to {output_file}")

def main():
    """
    Generates a treemap of the neighborhoods and saves it as an HTML file.
    Modify with colors/sizes later 
        Can size/color by number of listings, average price, average sq ft, etc. once available
    """
    neighborhoods = get_neighborhoods()
    if not neighborhoods:
        print("No neighborhoods found to generate a diagram.")
        return

    df = pd.DataFrame(neighborhoods)
    df = df[~df['id'].isin([800000, 9999999])]  # Remove Unassigned and childless NNJ
    df['parent_name'] = df['parent_id'].map(df.set_index('id')['name'])
    df.sort_values(by=['level', 'name'], inplace=True)
    

    treemap(df)
    sunburst(df)
    icicle(df)


if __name__ == '__main__':
    main()