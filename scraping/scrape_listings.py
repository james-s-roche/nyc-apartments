import click
import logging
from typing import Optional
import os

from scraping.streeteasy import StreetEasyScraper
from scraping.ingest_listings import ingest_listings
from database.mysql_client import MySQLClient

LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'scraping.log')),
        logging.StreamHandler()
    ]
)

@click.command()
@click.option('--pages', default=0, type=int, help='Number of pages to fetch per neighborhood. Default is 0 for unlimited.')
@click.option('--start-page', default=1, type=int, help='Page number to start scraping from. Default is 1.')
@click.option('--delay', default=None, type=float, help='Delay between requests in seconds')
@click.option('--timeout', default=None, type=int, help='Request timeout seconds')
def main(pages: int, start_page: int, delay: Optional[float], timeout: Optional[int]):
    """
    Scrapes listings for all of NYC from StreetEasy and ingests them into the database.
    """
    neighborhood_slug = "nyc"
    se = StreetEasyScraper(delay_seconds=delay, timeout_seconds=timeout)
    
    with MySQLClient() as db:
        logging.info(f"Scraping all listings for '{neighborhood_slug}'...")
        
        try:
            page = start_page
            while True:
                try:
                    # If a page limit is set (pages > 0), break if we have processed enough pages.
                    if pages > 0 and (page - start_page + 1) > pages:
                        logging.info(f"Reached page limit of {pages}. Stopping.")
                        break

                    logging.info(f"Fetching page {page} for '{neighborhood_slug}'...")
                    listings = se.search_rentals(neighborhood=neighborhood_slug, page=page)

                    if not listings:
                        logging.info(f"No more listings found for {neighborhood_slug} on page {page}. Scraping complete.")
                        break
                    
                    ingest_listings(listings, db)
                    logging.info(f"Ingested {len(listings)} listings from {neighborhood_slug}, page {page}.")

                except Exception as e:
                    logging.error(f"An error occurred while scraping {neighborhood_slug}, page {page}: {e}")
                    logging.info(f"To resume, run the script with --start-page={page}")
                    break
                
                page += 1
        except Exception as e:
            logging.error(f"A critical error occurred: {e}")



if __name__ == '__main__':
    main()
