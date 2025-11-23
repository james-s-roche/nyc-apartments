import json
import os
import mysql.connector
from mysql.connector import errorcode

def get_db_connection():
    """Establishes a connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(
            host=os.environ.get("DB_HOST", "127.0.0.1"),
            user=os.environ.get("DB_USER", "root"),
            password=os.environ.get("DB_PASSWORD", ""),
            database="nyc_apartments"
        )
        print("Database connection successful.")
        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        return None

def main():
    """Main function to load, parse, and ingest neighborhood data."""
    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()

    with open('data/neighborhoods.json', 'r') as f:
        data = json.load(f)
    print(data)
    neighborhoods = data.get('data', {}).get('areas', [])
    
    for hood in neighborhoods:
        # We only want id, name, level, parent_id
        neighborhood_data = {
            'id': hood.get('id'),
            'name': hood.get('name'),
            'level': hood.get('level'),
            'parent_id': hood.get('parent_id')
        }
        
        sql = "INSERT INTO neighborhoods (id, name, level, parent_id) VALUES (%(id)s, %(name)s, %(level)s, %(parent_id)s) ON DUPLICATE KEY UPDATE name=VALUES(name), level=VALUES(level), parent_id=VALUES(parent_id)"
        cursor.execute(sql, neighborhood_data)

    conn.commit()
    print(f"Successfully upserted {cursor.rowcount} neighborhoods.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()