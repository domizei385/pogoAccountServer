import mysql.connector
from config import Config


class DbConnection:
    # autocommit to always wait for queries to finish?
    # https://stackoverflow.com/a/54752005
    __config = {
        "host": Config.db_host,
        "port": Config.db_port,
        "user": Config.db_user,
        "passwd": Config.db_pw,
        "database": Config.db,
        "autocommit": True
    }

    def __init__(self):
        self.conn = mysql.connector.connect(**self.__config)
        self.cur = self.conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()
        try:
            self.conn.commit()
        except Exception as e:
            print(f"commit on exit failed: {e}")
        self.conn.close()

    def cursor(self, *args, **kwargs):
        return self.conn.cursor(*args, **kwargs)
