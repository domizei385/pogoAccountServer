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

    @classmethod
    def get_single_results(cls, *sqls):
        res: list = []
        with cls() as conn:
            for sql in sqls:
                conn.cur.execute(sql)
                # get the first element of the cursor (tuple) - or if it's none, get a list [None]
                # then return the first element of that result (the actual query result, or None)
                # https://stackoverflow.com/a/68186597
                res.append(next(conn.cur, [None])[0])
        return res
