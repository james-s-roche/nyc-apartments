import os
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path

from config.settings import load_config


def apply_schema(cursor, schema_sql: str):
    statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
    for stmt in statements:
        cursor.execute(stmt)


def main():
    cfg = load_config()
    conn = None
    try:
        conn = mysql.connector.connect(
            host=cfg.db.host,
            port=cfg.db.port,
            database=cfg.db.name,
            user=cfg.db.user,
            password=cfg.db.password,
            autocommit=True,
        )
        cur = conn.cursor()
        schema_path = Path(__file__).with_name('schema.sql')
        schema_sql = schema_path.read_text(encoding='utf-8')
        apply_schema(cur, schema_sql)
        print('Schema applied successfully')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Database {cfg.db.name} does not exist. Create it first.")
        else:
            print(f"MySQL error: {err}")
        raise
    finally:
        if conn is not None and conn.is_connected():
            conn.close()


if __name__ == '__main__':
    main()
