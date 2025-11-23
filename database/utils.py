import mysql.connector
from mysql.connector import errorcode
from config.settings import load_config

def get_db_connection():
    """Establishes a connection to the MySQL database."""
    cfg = load_config()
    try:
        conn = mysql.connector.connect(
            host=cfg.db.host,
            port=cfg.db.port,
            database=cfg.db.name,
            user=cfg.db.user,
            password=cfg.db.password,
        )
        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        return None
