import logging
import os
import sys
import time
from flask import Flask, request
from flask_basicauth import BasicAuth

from config import Config
from db_connection import DbConnection as Db

logger = logging.getLogger(__name__)
logFormat = '[%(asctime)s] [%(filename)s:%(lineno)3d] [%(levelname).1s] %(message)s'
logging.basicConfig(format=logFormat, level=logging.INFO, stream=sys.stdout)


class AccountServer:

    def __init__(self):
        logger.info("initializing server")
        self.host = Config.listen_host
        self.port = Config.listen_port
        self.resp_headers = {"Server": "pogoAccountServer"
                             }
        self.app = None
        self.load_accounts_from_file()
        self.launch_server()

    def launch_server(self):
        self.app = Flask(__name__)
        self.app.config['BASIC_AUTH_USERNAME'] = Config.auth_username
        self.app.config['BASIC_AUTH_PASSWORD'] = Config.auth_password
        basic_auth = BasicAuth(self.app)
        self.app.config['BASIC_AUTH_FORCE'] = True
        self.app.config['MAX_CONTENT_LENGTH'] = 16 * 1000 * 1000

        self.app.add_url_rule('/', "fallback", self.fallback, methods=['GET', 'POST'])
        self.app.add_url_rule('/<first>', "fallback", self.fallback, methods=['GET', 'POST'])
        self.app.add_url_rule('/<first>/<path:rest>', "fallback", self.fallback, methods=['GET', 'POST'])

        self.app.add_url_rule("/get/<device>", "get_account", self.get_account, methods=['GET', 'POST'])
        self.app.add_url_rule("/stats", "stats", self.stats, methods=['GET'])

        werkzeug_logger = logging.getLogger("werkzeug")
        werkzeug_logger.setLevel(logging.WARNING)
        logger.info(f"start listening on port {self.port}")
        self.app.run(host=self.host, port=self.port, debug=False, use_reloader=True)

    def load_accounts_from_file(self, file="accounts.txt"):
        accounts = []
        if not os.path.isfile(file):
            logger.warning(f"{file} not found - not adding accounts")
            return False
        with open(file, "r") as f:
            for line in f:
                try:
                    split = line.strip().split(",")
                    if len(split) > 2:
                        logger.warning(f"Invalid account entry: {line}")
                        continue
                    username, password = split
                    accounts.append((username, password))
                except Exception as e:
                    logger.warning(f"{e} trying to parse: {line}")
                    continue
        sql = ("INSERT INTO accounts (username, password) VALUES (%s, %s) ON DUPLICATE KEY UPDATE "
               "password=VALUES(password);")
        logger.info(f"Loaded {len(accounts)} from {file}")
        with Db() as conn:
            conn.cur.executemany(sql, accounts)
            conn.conn.commit()
        return True

    def resp_ok(self, data=None):
        standard = {"status": "ok"}
        if data is None:
            data = standard
        if "status" not in data:
            data = {"status": "ok", "data": data}
        if not data == standard:
            logger.debug(f"responding with 200, data: {data}")
        return data, 200, self.resp_headers

    def invalid_request(self, data=None, code=400):
        if data is None:
            data = {"status": "fail"}
        if "status" not in data:
            data = {"status": "fail", "data": data}
        logger.info(f"responding with 400, data: {data}")
        return data, code, self.resp_headers

    def fallback(self, first=None, rest=None):
        logger.info("Fallback called")
        if request.method == 'POST':
            logger.info(f"POST request to fallback at {first}/{rest}")
        if request.method == 'GET':
            logger.info(f"GET request to fallback at {first}/{rest}")
        return self.invalid_request()

    def get_account(self, device=None):
        if not device:
            return self.invalid_request()
        username = None
        pw = None
        now = int(time.time())
        cooldown_seconds = Config.cooldown_hours * 60 * 60
        last_returned_limit = now - cooldown_seconds
        reset = f"UPDATE accounts SET in_use_by = NULL, last_returned = '{now}' WHERE in_use_by = '{device}';"
        select = ("SELECT username, password from accounts WHERE in_use_by is NULL AND last_returned < "
                  f"{last_returned_limit} ORDER BY last_use ASC LIMIT 1;")
        with Db() as conn:
            conn.cur.execute(reset)
        with Db() as conn:
            conn.cur.execute(select)
            for elem in conn.cur:
                username = elem[0]
                pw = elem[1]
                break
        if not username or not pw:
            logger.warning(f"Unable to return an account for {device}")
            return self.invalid_request({"error": "No accounts available"})

        mark_used = (f"UPDATE accounts SET in_use_by = '{device}', last_use = '{int(time.time())}' WHERE "
                     f"username = '{username}';")
        with Db() as conn:
            conn.cur.execute(mark_used)
        logger.info(f"Request from {device}: return {username=}, {pw=}")
        logger.info(self.stats())
        return self.resp_ok({"username": username, "password": pw})

    def stats(self):
        # TODO: doubled code
        now = int(time.time())
        cooldown_seconds = Config.cooldown_hours * 60 * 60
        last_returned_limit = now - cooldown_seconds

        cd_sql = f"SELECT count(*) from accounts WHERE last_returned >= {last_returned_limit}"
        in_use_sql = "SELECT count(*) from accounts WHERE in_use_by IS NOT NULL"
        total_sql = "SELECT count(*) from accounts"

        cd, in_use, total = Db.get_single_results(cd_sql, in_use_sql, total_sql)
        available = total - in_use - cd

        return {"accounts": total, "in_use": in_use, "cooldown": cd, "available": available}


if __name__ == "__main__":
    serv = AccountServer()
    while True:
        time.sleep(1)
