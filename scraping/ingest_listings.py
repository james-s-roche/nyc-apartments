from typing import List, Dict
import pandas as pd
import re
from database.mysql_client import MySQLClient


def ingest_listings(listings: List[Dict], db: MySQLClient):
    """
    Ingests a list of listings into the database using an INSERT ... ON DUPLICATE KEY UPDATE query.
    
    :param listings: A list of dictionaries, where each dictionary represents a listing.
    :param db: An active MySQLClient instance.
    """
    if not listings:
        return

    df = pd.DataFrame(listings)

    # The API gives us lat/lon nested in geoPoint. Let's flatten it.
    if 'geoPoint' in df.columns:
        geo_points = df['geoPoint'].apply(pd.Series)
        df['latitude'] = geo_points['latitude']
        df['longitude'] = geo_points['longitude']
        df = df.drop(columns=['geoPoint'])

    # Define a function to convert camelCase to snake_case
    def camel_to_snake(name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    # Rename all columns from camelCase to snake_case
    df.columns = [camel_to_snake(col) for col in df.columns]

    # Rename specific columns that don't follow the pattern
    rename_map = {
        'id': 'external_id',
    }
    df = df.rename(columns=rename_map)

    # Get the list of columns from the database schema to ensure we only insert what's needed
    db_columns = db.get_table_columns('listings')
    
    # Filter DataFrame to only include columns that exist in the database
    df_to_insert = df[[col for col in df.columns if col in db_columns]]

    # Convert DataFrame to a list of tuples for insertion.
    # We must convert NaNs to Nones for the SQL driver.
    # Converting to 'object' dtype prevents pandas from re-casting None to NaN.
    columns_to_insert = df_to_insert.columns.tolist()
    df_prepared = df_to_insert.astype(object).where(pd.notna(df_to_insert), None)
    values_to_insert = [tuple(row) for row in df_prepared.to_numpy()]

    db.insert_many('listings', columns_to_insert, values_to_insert, on_duplicate='update')