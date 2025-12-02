import click
import logging
from typing import Optional, Set
import os

from scraping.streeteasy import StreetEasyScraper
from scraping.ingest_listings import ingest_listings
from database.mysql_client import MySQLClient

LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

PROCESSED_NEIGHBORHOODS_FILE = os.path.join(LOG_DIR, 'processed_neighborhoods.txt')


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'scraping.log')),
        logging.StreamHandler()
    ]
)

def get_processed_neighborhoods() -> Set[str]:
    """Reads the list of already processed neighborhoods from the log file."""
    if not os.path.exists(PROCESSED_NEIGHBORHOODS_FILE):
        return set()
    with open(PROCESSED_NEIGHBORHOODS_FILE, 'r') as f:
        return set(line.strip() for line in f)

def mark_neighborhood_as_processed(neighborhood_name: str):
    """Appends a neighborhood name to the processed log file."""
    with open(PROCESSED_NEIGHBORHOODS_FILE, 'a') as f:
        f.write(neighborhood_name + '\n')


@click.command()
@click.option('--pages', default=0, type=int, help='Max number of pages to fetch per neighborhood. Overrides API page count if smaller.')
@click.option('--start-page', default=1, type=int, help='Page number to start scraping from for each neighborhood. Default is 1.')
@click.option('--delay', default=None, type=float, help='Delay between requests in seconds')
@click.option('--timeout', default=None, type=int, help='Request timeout seconds')
@click.option('--level', default=3, type=int, help='Neighborhood level to scrape')
def main(pages: int, start_page: int, delay: Optional[float], timeout: Optional[int], level: int):
    """
    Scrapes listings for all neighborhoods from StreetEasy and ingests them into the database.
    It tracks progress and can be resumed if it fails.
    """
    se = StreetEasyScraper(delay_seconds=delay, timeout_seconds=timeout)
    
    with MySQLClient() as db:
        # Get neighborhoods
        try:
            neighborhoods_to_scrape = db.execute_query(f"SELECT name, slug FROM neighborhoods WHERE level = {level}")
            neighborhood_map = {}
            for name, slug in neighborhoods_to_scrape:
                if slug and slug.strip():
                    neighborhood_map[name] = slug
                else:
                    neighborhood_map[name] = name.lower().replace(' ', '-')
        except Exception as e:
            logging.error(f"Failed to fetch neighborhoods from database: {e}")
            return

        processed_neighborhoods = get_processed_neighborhoods()
        
        neighborhoods_to_process = [name for name in neighborhood_map if name not in processed_neighborhoods]
        
        logging.info(f"Found {len(neighborhood_map)} level-{level} neighborhoods. {len(processed_neighborhoods)} already processed.")
        logging.info(f"Starting to scrape {len(neighborhoods_to_process)} new neighborhoods.")

        for neighborhood_name in neighborhoods_to_process:
            neighborhood_slug = neighborhood_map[neighborhood_name]
            logging.info(f"Scraping listings for '{neighborhood_name}' (slug: {neighborhood_slug})...")
            
            neighborhood_failed = False
            try:
                # Fetch the first page to determine pagination
                current_page = start_page
                logging.info(f"Fetching page {current_page} for '{neighborhood_slug}' to determine page count...")
                listings, total_pages_api = se.search_rentals(neighborhood=neighborhood_slug, page=current_page)

                if not listings:
                    logging.info(f"No listings found for '{neighborhood_slug}' on page {current_page}. Moving to next neighborhood.")
                    mark_neighborhood_as_processed(neighborhood_name)
                    continue

                ingest_listings(listings, db)
                logging.info(f"Ingested {len(listings)} listings from {neighborhood_slug}, page {current_page}.")

                # Determine the actual number of pages to scrape
                api_page_cap = 50 # Looks like level 1 allows 75 but others cap at 50
                page_limit = min(total_pages_api, api_page_cap)
                if pages > 0:  # User-defined limit
                    page_limit = min(page_limit, pages)
                
                logging.info(f"API reports {total_pages_api} pages. Will scrape up to {page_limit} pages for '{neighborhood_slug}'.")

                # Loop through the rest of the pages
                current_page += 1
                while current_page <= page_limit:
                    logging.info(f"Fetching page {current_page}/{page_limit} for '{neighborhood_slug}'...")
                    listings, _ = se.search_rentals(neighborhood=neighborhood_slug, page=current_page)

                    if not listings:
                        logging.info(f"No more listings found for '{neighborhood_slug}' on page {current_page}. Ending scrape for this neighborhood.")
                        break
                    
                    ingest_listings(listings, db)
                    logging.info(f"Ingested {len(listings)} listings from {neighborhood_slug}, page {current_page}.")
                    
                    current_page += 1

            except Exception as e:
                logging.error(f"An error occurred while scraping '{neighborhood_slug}': {e}")
                logging.info(f"To resume this neighborhood, run the script again. It will restart on '{neighborhood_name}'.")
                neighborhood_failed = True
            
            if neighborhood_failed:
                logging.error(f"Stopping script due to failure in neighborhood '{neighborhood_name}'.")
                break
            
            # Mark as processed only if the entire neighborhood was scraped without error
            mark_neighborhood_as_processed(neighborhood_name)
            logging.info(f"Finished scraping '{neighborhood_name}'. Marked as processed.")
        
        if not neighborhoods_to_process and neighborhood_map:
             logging.info(f"All level-{level} neighborhoods have already been processed.")
        
        logging.info("Scraping run finished.")


if __name__ == '__main__':
    main()
