import click
import logging
from typing import Optional
import os

from config.settings import load_config
from scraping.get_neighborhood_leaf_nodes import get_leaf_neighborhoods
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

PROCESSED_NEIGHBORHOODS_FILE = os.path.join(LOG_DIR, 'processed_neighborhoods.txt')

def get_processed_neighborhoods():
    if not os.path.exists(PROCESSED_NEIGHBORHOODS_FILE):
        return set()
    with open(PROCESSED_NEIGHBORHOODS_FILE, 'r') as f:
        return set(line.strip() for line in f)

def mark_neighborhood_as_processed(neighborhood_slug):
    with open(PROCESSED_NEIGHBORHOODS_FILE, 'a') as f:
        f.write(neighborhood_slug + '\n')

@click.command()
@click.option('--pages', default=None, type=int, help='Number of pages to fetch per neighborhood')
@click.option('--delay', default=None, type=float, help='Delay between requests in seconds')
@click.option('--timeout', default=None, type=int, help='Request timeout seconds')
def main(pages: Optional[int], delay: Optional[float], timeout: Optional[int]):
    """
    Scrapes listings for all leaf neighborhoods from StreetEasy and ingests them into the database.
    """
    cfg = load_config()
    if pages is None:
        pages = int(cfg.scrape.get('default_pages', 2))

    leaf_neighborhoods = get_leaf_neighborhoods()
    if not leaf_neighborhoods:
        logging.info("No neighborhoods to scrape.")
        return

    processed_neighborhoods = get_processed_neighborhoods()
    se = StreetEasyScraper(delay_seconds=delay, timeout_seconds=timeout)
    
    with MySQLClient() as db:
        for neighborhood in leaf_neighborhoods:
            neighborhood_slug = neighborhood['name'].lower().replace(' ', '-')
            if neighborhood_slug in processed_neighborhoods:
                logging.info(f"Skipping already processed neighborhood: {neighborhood_slug}")
                continue

            logging.info(f"Scraping {neighborhood_slug}...")
            
            try:
                for page in range(1, pages + 1):
                    try:
                        listings = se.search_rentals(neighborhood=neighborhood_slug, page=page)

                        if not listings:
                            logging.info(f"No more listings found for {neighborhood_slug} on page {page}.")
                            break
                        
                        ingest_listings(listings, db)
                        logging.info(f"Ingested {len(listings)} listings from {neighborhood_slug}, page {page}.")

                    except Exception as e:
                        logging.error(f"An error occurred while scraping {neighborhood_slug}, page {page}: {e}")
                        break
                mark_neighborhood_as_processed(neighborhood_slug)
            except Exception as e:
                logging.error(f"An error occurred while processing neighborhood {neighborhood_slug}: {e}")


if __name__ == '__main__':
    main()
