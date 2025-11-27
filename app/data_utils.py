import pandas as pd
import numpy as np
from database.mysql_client import MySQLClient
from scraping.get_neighborhood_leaf_nodes import get_neighborhoods

import logging
logging.basicConfig(level=logging.INFO)
def get_listings_data() -> pd.DataFrame:
    """
    Fetches all listings from the database and returns them as a pandas DataFrame.
    Performs basic type conversion and cleaning.
    """
    logging.info("Fetching listings data from database...")
    try:
        with MySQLClient() as db:
            listings_query = "SELECT * FROM listings"
            listings_df = pd.read_sql_query(listings_query, db.sqlalchemy_engine)
            logging.info(f"Successfully fetched {len(listings_df)} listings.")

            # Basic data cleaning
            numeric_cols = ['latitude', 'longitude', 'bedroom_count', 'price', 'living_area_size']
            for col in numeric_cols:
                listings_df[col] = pd.to_numeric(listings_df[col], errors='coerce')

            # lots of listings have living_area_size defaulted to 0. If unknown, set these to NaN
            listings_df['living_area_size'] = listings_df['living_area_size'].replace(0, np.nan)
            return listings_df
        
    except Exception as e:
        logging.error(f"Failed to fetch or process listings data: {e}")
        return pd.DataFrame()

def get_neighborhood_data() -> pd.DataFrame:
    """
    Fetches all listings from the database and returns them as a pandas DataFrame.
    Performs basic type conversion and cleaning.
    """
    logging.info("Fetching listings data from database...")
    try:
        with MySQLClient() as db:
            neighborhood_query = "SELECT * FROM neighborhoods"
            neighborhood_df = pd.read_sql_query(neighborhood_query, db.conn)
            logging.info(f"Successfully fetched {len(neighborhood_df)} neighborhoods.")
            neighborhood_df['parent_name'] = neighborhood_df['parent_id'].map(neighborhood_df.set_index('id')['name'])
            neighborhood_df = neighborhood_df[~neighborhood_df['id'].isin([800000, 9999999])]  # Remove Unassigned and childless NNJ
            neighborhood_df.sort_values(by=['level', 'name'], inplace=True)
            return neighborhood_df
        
    except Exception as e:
        logging.error(f"Failed to fetch or process neighborhood data: {e}")
        return pd.DataFrame()

def data_aggregation(df: pd.DataFrame, group_by_col: str) -> pd.DataFrame:
    if df.empty or group_by_col not in df.columns:
        return pd.DataFrame()

    logging.info(f"Aggregating data by {group_by_col}...")
    
    # Define aggregations
    aggs = {
        'latitude': 'mean',
        'longitude': 'mean',
        'bedroom_count': ['mean', 'median'],
        'price': ['mean', 'median'],
        'living_area_size': ['mean', 'median'],
        'id': 'count' # To get the number of listings per group
    }
    
    # # Filter out rows where the grouping key is null. Cannot happen based on data model
    # df_filtered = df.dropna(subset=[group_by_col])

    # Perform aggregation
    agg_df = df.groupby(group_by_col).agg(aggs).reset_index()

    # Flatten the multi-level column index
    agg_df.columns = ['_'.join(col).strip() if isinstance(col, tuple) and col[1] else col[0] for col in agg_df.columns.values]
    agg_df.rename(columns={
        f"{group_by_col}_": group_by_col,
        'latitude_mean': 'latitude',
        'longitude_mean': 'longitude',
        'id_count': 'listing_count'
    }, inplace=True)
    
    logging.info(f"Successfully aggregated data into {len(agg_df)} groups.")
    return agg_df


def neighborhood_aggregation_recursive() -> pd.DataFrame:
    """
    Alternative implementation of data aggregation by the specified column.
    This version includes logging and handles edge cases.
    
    :param df: The input DataFrame of listings.
    :param group_by_col: The column to group by ('area_name' or 'zip_code').
    :return: A DataFrame with aggregated metrics.
    """
    listings = get_listings_data()
    # listings.drop(columns=['id', 'source', 'slug'], inplace=True)
    listings = listings[['external_id', 'area_name', 'latitude', 'longitude', 'bedroom_count', 
                         'price', 'living_area_size', 'state', 'zip_code']]
    neighborhoods = get_neighborhood_data()
    neighborhoods = neighborhoods[['id', 'name', 'parent_id', 'level', 'parent_name']]
    df = pd.merge(neighborhoods, listings, left_on='name', right_on='area_name', how='left')  

    if df.empty:
        logging.warning("Input DataFrame is empty.")
        return pd.DataFrame()

    logging.info(f"Aggregating data by neighborhood...")
    
    # aggregate the minimum level, then include this data in aggregating the next level up
    agg_data = []
    max_level = df['level'].max()
    min_level = df['level'].min()
    for level in range(max_level, min_level - 1, -1):
        logging.info(f"Aggregating level: {level}")

        tdf = df[df['level'] == level].copy()
        agg_df = data_aggregation(tdf, 'name')
        agg_data.append(agg_df)
        #modify the original df so lower level is labeled as parent for next iteration
        df['name'] = np.where(df['level'] == level, df['parent_name'], df['name'])
        df['id'] = np.where(df['level'] == level, df['parent_id'], df['id'])
        
        # set the parent id and name as the grandparent name
        df['parent_id'] = np.where(df['level'] == level,
                                   df['id'].map(neighborhoods.set_index('id')['parent_id']),
                                   df['parent_id'])
        df['parent_name'] = np.where(df['level'] == level,
                                     df['id'].map(neighborhoods.set_index('id')['parent_name']),
                                     df['parent_name'])

        # Finally and update the level too
        df['level'] = np.where(df['level'] == level, df['level'] - 1, df['level'])

    final_agg_df = pd.concat(agg_data, ignore_index=True)
    # add neighborhood hierarchy columns back in
    final_agg_df = pd.merge(neighborhoods[['id', 'name', 'parent_id', 'level', 'parent_name']], 
                            final_agg_df, 
                            on='name', how='left')


    # # looks like the sizes of treemap shouldn't count their children (double sized). just overwrite this
    listing_counts = listings.groupby('area_name')['external_id'].agg('count').reset_index()
    listing_counts = listing_counts.rename(columns={'external_id': 'area_listing_count',
                                                    'area_name': 'name'})
    final_agg_df = pd.merge(final_agg_df, listing_counts, 
                            on='name', how='left')
    final_agg_df['area_listing_count'].fillna(0, inplace=True)

    # # refill the parent of the root node 
    # final_agg_df.loc[final_agg_df['id'] == 1, 'parent_name'] = 'All'
    # just kiding, drop it since it's useless visually
    final_agg_df.loc[final_agg_df['id'] == 1, 'parent_name'] = 'All'
    print(final_agg_df[final_agg_df['level'] == 0])
    # final_agg_df = final_agg_df[final_agg_df['level'] != 0]

    logging.info("Neighborhood hierarchical aggregation complete.")
    return final_agg_df


# OBSOLETE FUNCTIONS BELOW
# def get_aggregated_data(df: pd.DataFrame, group_by_col: str) -> pd.DataFrame:
#     """
#     Aggregates the listings data by the specified column (e.g., 'area_name' or 'zip_code').
    
#     :param df: The input DataFrame of listings.
#     :param group_by_col: The column to group by ('area_name' or 'zip_code').
#     :return: A DataFrame with aggregated metrics.
#     """
#     if df.empty or group_by_col not in df.columns:
#         return pd.DataFrame()

#     logging.info(f"Aggregating data by {group_by_col}...")
    
#     # Define aggregations
#     aggs = {
#         'latitude': 'mean',
#         'longitude': 'mean',
#         'bedroom_count': ['mean', 'median'],
#         'price': ['mean', 'median'],
#         'living_area_size': ['mean', 'median'],
#         'id': 'count' # To get the number of listings per group
#     }
    
#     # # Filter out rows where the grouping key is null. Cannot happen based on data model
#     # df_filtered = df.dropna(subset=[group_by_col])

#     # Perform aggregation
#     agg_df = df_filtered.groupby(group_by_col).agg(aggs).reset_index()
    
#     # Flatten the multi-level column index
#     agg_df.columns = ['_'.join(col).strip() if isinstance(col, tuple) and col[1] else col[0] for col in agg_df.columns.values]
#     agg_df.rename(columns={
#         f"{group_by_col}_": group_by_col,
#         'latitude_mean': 'latitude',
#         'longitude_mean': 'longitude',
#         'id_count': 'listing_count'
#     }, inplace=True)
    
#     print(agg_df)
#     logging.info(f"Successfully aggregated data into {len(agg_df)} groups.")
#     return agg_df

# def get_hierarchical_aggregated_data(listings_df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Performs a hierarchical aggregation of listings data.
#     1. Aggregates metrics for leaf neighborhoods.
#     2. Rolls up metrics to parent nodes (boroughs, etc.) using weighted averages.
#     :param listings_df: The raw DataFrame of listings.
#     :return: A DataFrame with aggregated metrics for all nodes in the hierarchy.
#     """
#     if listings_df.empty:
#         return pd.DataFrame()

#     logging.info("Performing hierarchical aggregation...")
#     neighborhood_hierarchy = get_neighborhood_hierarchy()
#     if neighborhood_hierarchy.empty:
#         logging.error("Cannot perform hierarchical aggregation: neighborhood hierarchy is missing.")
#         return pd.DataFrame()

#     # 1. Aggregate metrics at the leaf-node level (by string name)
#     leaf_agg = get_aggregated_data(listings_df, 'area_name')

#     # 2. Merge leaf aggregates into the full hierarchy
#     # This gives us a starting point with metrics only at the leaf level
#     print(neighborhood_hierarchy)
#     print(leaf_agg)
#     merged_df = pd.merge(neighborhood_hierarchy, leaf_agg, left_on='name', right_on='area_name', how='left')

#     # Define metrics to roll up
#     metrics_to_average = ['price_mean', 'price_median', 
#                           'bedroom_count_mean', 'bedroom_count_median', 
#                           'living_area_size_mean', 'living_area_size_median']
    
#     # 3. Iteratively roll up metrics from children to parents
#     # We iterate from the deepest level upwards
#     for level in sorted(merged_df['level'].unique(), reverse=True):
#         if level == 0: continue # Skip the root node

#         # Get all nodes at the current level that have data
#         children = merged_df[merged_df['level'] == level].dropna(subset=['listing_count'])
        
#         for parent_id, group in children.groupby('parent_id'):
#             parent_index = merged_df.index[merged_df['id'] == parent_id]
#             if parent_index.empty: continue

#             # Sum listing counts
#             total_listings = group['listing_count'].sum()
#             merged_df.loc[parent_index, 'listing_count'] = total_listings

#             # Calculate weighted averages for other metrics
#             for metric in metrics_to_average:
#                 if metric in group.columns:
#                     # Weighted average = sum(metric * weight) / sum(weight)
#                     # Here, weight is the listing_count for each child
#                     weighted_sum = (group[metric] * group['listing_count']).sum()
#                     if total_listings > 0:
#                         weighted_avg = weighted_sum / total_listings
#                         merged_df.loc[parent_index, metric] = weighted_avg
            
#             # For lat/lon, a simple mean is sufficient for centering maps
#             merged_df.loc[parent_index, 'latitude'] = group['latitude'].mean()
#             merged_df.loc[parent_index, 'longitude'] = group['longitude'].mean()

#     # Rename 'name' to 'neighborhood_name' for clarity
#     merged_df.rename(columns={'name': 'neighborhood_name'}, inplace=True)
#     logging.info("Hierarchical aggregation complete.")
#     print(merged_df)
#     return merged_df

# def get_neighborhood_hierarchy() -> pd.DataFrame:
    """
    Fetches the neighborhood hierarchy, creates parent-child relationships,
    and returns it as a pandas DataFrame ready for treemaps.
    """
    logging.info("Fetching neighborhood hierarchy...")
    try:
        neighborhoods_list = get_neighborhoods()
        if not neighborhoods_list:
            logging.warning("No neighborhood data found.")
            return pd.DataFrame()

        df = pd.DataFrame(neighborhoods_list)
        # Create a 'parent_name' column by mapping parent_id to the corresponding name
        df['parent_name'] = df['parent_id'].map(df.set_index('id')['name'])
        # # Add a 'borough' column for easier path creation in the treemap. Fill NaN for top-level (e.g., "NYC")
        # df['borough'] = df['parent_name'].where(df['level'] == 2, None).fillna(method='ffill')
        
        df = df[~df['id'].isin([800000, 9999999])]  # Remove Unassigned and childless NNJ
        df.sort_values(by=['level', 'name'], inplace=True)

        logging.info("Successfully processed neighborhood hierarchy.")
        return df
    except Exception as e:
        logging.error(f"Failed to fetch or process neighborhood hierarchy: {e}")
        return pd.DataFrame()
