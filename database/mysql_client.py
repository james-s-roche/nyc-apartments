from __future__ import annotations
from typing import List, Any, Tuple
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import logging

from config.settings import load_config


class MySQLClient:
    """A client for interacting with a MySQL database."""

    def __init__(self):
        cfg = load_config()
        self.db_config = {
            'host': cfg.db.host,
            'port': cfg.db.port,
            'database': cfg.db.name,
            'user': cfg.db.user,
            'password': cfg.db.password,
        }
        self.db_name = cfg.db.name
        self.sqlalchemy_engine = create_engine(f"mysql+mysqlconnector://{cfg.db.user}:{cfg.db.password}@{cfg.db.host}:{cfg.db.port}/{cfg.db.name}")
        self.conn = None
        self.cursor = None
        self._column_cache = {}

    def __enter__(self) -> MySQLClient:
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
        except mysql.connector.Error as err:
            logging.error(f"Failed to connect to database: {err}")
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Retrieves the column names for a given table from the database schema.
        Caches the result to avoid redundant queries.
        """
        if table_name in self._column_cache:
            return self._column_cache[table_name]

        if not self.conn or not self.cursor:
            raise ConnectionError("Database connection is not available.")

        try:
            query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """
            self.cursor.execute(query, (self.db_name, table_name))
            columns = [row[0] for row in self.cursor.fetchall()]
            self._column_cache[table_name] = columns
            return columns
        except mysql.connector.Error as err:
            logging.error(f"Failed to get columns for table {table_name}: {err}")
            return []

    def execute_query(self, query: str, params: tuple = None) -> List[Tuple]:
        """
        Executes a SQL query and returns the results.

        :param query: The SQL query to execute.
        :param params: A tuple of parameters to pass to the query.
        :return: A list of tuples representing the rows returned by the query.
        """
        if not self.conn or not self.cursor:
            raise ConnectionError("Database connection is not available.")

        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except mysql.connector.Error as err:
            logging.error(f"Failed to execute query: {err}")
            return []

    def insert_many(self, table: str, columns: List[str], values: List[Tuple], on_duplicate: str = 'ignore'):
        """
        Inserts multiple rows into a table.
        
        :param table: The name of the table.
        :param columns: A list of column names.
        :param values: A list of tuples, where each tuple is a row.
        :param on_duplicate: 'ignore' or 'update'.
        """
        if not values:
            return

        query_template = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        
        if on_duplicate == 'update':
            update_clause = ", ".join([f"{col}=VALUES({col})" for col in columns])
            query_template += f" ON DUPLICATE KEY UPDATE {update_clause}"

        try:
            self.cursor.executemany(query_template, values)
            self.conn.commit()
        except mysql.connector.Error as err:
            logging.error(f"Database insert failed: {err}")
            self.conn.rollback()
            raise

