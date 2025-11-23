from __future__ import annotations
import logging
from database.mysql_client import MySQLClient

def ingest_listings(listings_data: list, db: MySQLClient):
    """
    Ingests a list of listing data into the database.
    """
    if not listings_data:
        return 0

    rows = 0
    for listing in listings_data:
        # The listing data is already a dictionary parsed from JSON
        db.upsert_listing(listing)
        rows += 1
    
    logging.info(f"Successfully upserted {rows} listings.")
    return rows