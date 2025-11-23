import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import mysql.connector
import pandas as pd

from config.settings import load_config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', required=True, help='Output CSV path')
    args = parser.parse_args()

    cfg = load_config()
    conn = mysql.connector.connect(
        host=cfg.db.host,
        port=cfg.db.port,
        database=cfg.db.name,
        user=cfg.db.user,
        password=cfg.db.password,
    )
    try:
        df = pd.read_sql(
            """
            SELECT l.id, l.source, l.external_id, l.url, l.address, l.neighborhood, l.borough,
                   l.beds, l.baths, l.sqft, l.price, l.fee, l.latitude, l.longitude,
                   l.building_name, l.unit, l.pets, l.amenities, l.broker, l.scraped_at
            FROM listings l
            ORDER BY l.scraped_at DESC
            """,
            conn,
        )
        df.to_csv(args.out, index=False)
        print(f"Wrote {len(df)} rows to {args.out}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
