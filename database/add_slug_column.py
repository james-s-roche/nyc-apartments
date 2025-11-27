import logging
from database.mysql_client import MySQLClient
import mysql.connector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def add_slug_column():
    """
    Adds a 'slug' column to the 'neighborhoods' table if it doesn't exist
    and populates it with default values.
    """
    logging.info("Starting migration to add 'slug' column to 'neighborhoods' table.")
    
    with MySQLClient() as db:
        try:
            # Step 1: Add the 'slug' column if it doesn't exist
            logging.info("Checking for 'slug' column...")
            try:
                db.cursor.execute("ALTER TABLE neighborhoods ADD COLUMN slug VARCHAR(128)")
                db.conn.commit()
                logging.info("Column 'slug' added successfully.")
            except mysql.connector.Error as err:
                if err.errno == 1060: # Error code for "Duplicate column name"
                    logging.info("Column 'slug' already exists. Skipping.")
                else:
                    raise

            # Step 2: Populate the 'slug' column for rows where it is NULL
            # The replacements were largely determined by trial and error. Ideally would find canonical mapping source
            logging.info("Populating 'slug' column with default values for NULL entries...")
            update_query = """
                UPDATE neighborhoods 
                SET slug = LOWER(
                            REPLACE(
                            REPLACE(
                            REPLACE(
                            REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(name, "[().']", ""),
                                    ' /', '-'),
                                'Lower East Side', 'les'),
                                'Stuyvesant Town/PCV', 'stuyvesant-town'),
                                'Turtle Bay', 'turtlebay'),
                                'West Side', 'west-side-hudson-county')
                )
                WHERE slug IS NULL
            """
            db.cursor.execute(update_query)
            updated_rows = db.cursor.rowcount
            db.conn.commit()
            
            if updated_rows > 0:
                logging.info(f"Successfully populated 'slug' for {updated_rows} neighborhoods.")
            else:
                logging.info("No neighborhoods needed updating.")

            logging.info("Migration completed successfully.")

        except mysql.connector.Error as err:
            logging.error(f"A database error occurred: {err}")
            db.conn.rollback()
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            db.conn.rollback()
