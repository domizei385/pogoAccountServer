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
        self.config = Config()
        self.host = self.config.listen_host
        self.port = self.config.listen_port
        self.resp_headers = {"Server": "pogoAccountServer", 'Content-Type': 'application/json'}
        self.app = None
        self.load_accounts_from_file()
        self.launch_server()

    def launch_server(self):
        self.app = Flask(__name__)
        self.app.config['BASIC_AUTH_USERNAME'] = self.config.auth_username
        self.app.config['BASIC_AUTH_PASSWORD'] = self.config.auth_password
        basic_auth = BasicAuth(self.app)
        self.app.config['BASIC_AUTH_FORCE'] = True
        self.app.config['MAX_CONTENT_LENGTH'] = 16 * 1000 * 1000

        self.app.add_url_rule('/', "fallback", self.fallback, methods=['GET', 'POST'])
        self.app.add_url_rule('/<first>', "fallback", self.fallback, methods=['GET', 'POST'])
        self.app.add_url_rule('/<first>/<path:rest>', "fallback", self.fallback, methods=['GET', 'POST'])

        self.app.add_url_rule("/get/<device>", "get_account", self.get_account, methods=['GET', 'POST'])
        self.app.add_url_rule("/set/<device>/level/<int:level>", "set_level", self.set_level, methods=['POST'])
        self.app.add_url_rule("/set/<device>/burned", "set_burned", self.set_burned, methods=['POST'])
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

    def resp_ok(self, code=200, data=None):
        standard = {"status": "ok"}
        if data is None:
            data = standard
        if "status" not in data:
            data = {"status": "ok", "data": data}
        if not data == standard:
            logger.debug(f"responding with {code}, data: {data}")
        return data, code, self.resp_headers

    def invalid_request(self, data=None, code=400, logging=True):
        if data is None:
            data = {"status": "fail"}
        if "status" not in data:
            data = {"status": "fail", "data": data}
        if logging:
            logger.warning(f"responding with {code}, data: {data}")
        return data, code, self.resp_headers

    def fallback(self, first=None, rest=None):
        logger.info("Fallback called")
        if request.method == 'POST':
            logger.info(f"POST request to fallback at {first}/{rest}")
        if request.method == 'GET':
            logger.info(f"GET request to fallback at {first}/{rest}")
        return self.invalid_request()

    def get_account(self, device=None):
        # TODO: track last known account location and consider new location?
        if not device:
            return self.invalid_request()
        logger.info(
            f"get_account({device}): leveling={request.args.get('leveling')}, region={request.args.get('region', default='', type=str)}, reason={request.args.get('reason', default='', type=str)}")

        username = None
        pw = None

        reason = request.args.get('reason')  # None, level, maintenance, rotation, level, teleport, limit

        # sticky accounts (prefer account reusage unless burned)
        if not reason:
            last_returned_limit = self.config.get_cooldown_timestamp()
            select = f"SELECT username, password from accounts WHERE in_use_by = '{device}' AND last_returned < {last_returned_limit} LIMIT 1;"
            with Db() as conn:
                conn.cur.execute(select)
                for elem in conn.cur:
                    username = elem[0]
                    pw = elem[1]
                    break

        if not username or not pw:
            reset = (f"UPDATE accounts SET in_use_by = NULL, last_updated = '{int(time.time())}' WHERE in_use_by = '{device}';")
            with Db() as conn:
                conn.cur.execute(reset)

            # new account
            level_query = " AND level < 30" if int(request.args.get('leveling')) == 1 else " AND level >= 30"
            region = request.args.get('region', default='', type=str)
            region_query = f" AND (region IS NULL OR region = '' OR region = '{region}')" if region != '' else ""

            last_returned_limit = self.config.get_cooldown_timestamp()
            last_use_limit = self.config.get_short_cooldown_timestamp()
            select = (f"SELECT username, password from accounts WHERE in_use_by IS NULL AND last_returned < {last_returned_limit} AND last_use < "
                      f"{last_use_limit} {level_query} {region_query} ORDER BY last_use ASC LIMIT 1;")
            logger.debug(select)
            with Db() as conn:
                conn.cur.execute(select)
                for elem in conn.cur:
                    username = elem[0]
                    pw = elem[1]
                    break
            if not username or not pw:
                logger.debug(f"Unable to return an account for {device}")
                return self.resp_ok(code=204, data={"error": "No accounts available"})

        mark_used = (f"UPDATE accounts SET in_use_by = '{device}', last_use = '{int(time.time())}', last_updated = '{int(time.time())}', last_reason = NULL WHERE "
                     f"username = '{username}';")
        with Db() as conn:
            conn.cur.execute(mark_used)
        # logger.info(f"Request from {device}(leveling={request.args.get('leveling')}, reason={reason}) return {username=}, {pw=}")
        logger.info(self.stats())
        return self.resp_ok(data={"username": username, "password": pw})

    def set_level(self, device=None, level: int = None):
        if not device or not level:
            return self.invalid_request()

        check_update = f"SELECT count(*) FROM accounts WHERE in_use_by = '{device}' AND level <> {level}"
        if not int(Db.get_single_results(check_update)[0]):
            logger.debug(f"Request for device {device}")
            return self.resp_ok()

        logger.info(f"Request from {device} to set level to {level}")
        update = (f"UPDATE accounts SET level = {level}, last_updated = '{int(time.time())}' WHERE in_use_by = '{device}';")
        with Db() as conn:
            conn.cur.execute(update)

        return self.resp_ok()

    def set_burned(self, device=None):
        if not device:
            return self.invalid_request()

        username = None
        claimed_sql = f"SELECT username, last_use FROM accounts WHERE in_use_by = '{device}'"
        with Db() as conn:
            conn.cur.execute(claimed_sql)
            for elem in conn.cur:
                username = elem[0]
                last_used = int(elem[1])
                break

        if not username:
            return self.resp_ok()
        logger.info(f"Request from {device} to burn account {username} (acquired {last_used - time.time()} s ago)")

        args = request.get_json()
        last_reason = ''
        if 'reason' in args:
            last_reason = args['reason']

        reset = (f"UPDATE accounts SET in_use_by = NULL, last_returned = '{int(time.time())}', last_updated = '{int(time.time())}',"
                 f" last_reason = '{last_reason}' WHERE in_use_by = '{device}';")

        with Db() as conn:
            conn.cur.execute(reset)

        history = (f"INSERT INTO accounts_history SET username = '{username}', acquired = '{int(last_used)}', burned = '{int(time.time())}', reason = '{last_reason}'")
        if 'encounters' in args:
            history.__add__(f", encounters = '{int(args['encounters'])}'")
        with Db() as conn:
            conn.cur.execute(history)

        return self.resp_ok(data={"username": username, "status": "burned"})

    def stats(self):
        last_returned_limit = self.config.get_cooldown_timestamp()

        regions = ["EU", "US"]
        result = {}

        for region in regions:
            region_query = f"(region = '{region}' OR region IS NULL)"

            cd_sql = f"SELECT count(*) FROM accounts WHERE last_returned >= {last_returned_limit} AND {region_query}"
            in_use_sql = f"SELECT count(*) FROM accounts WHERE in_use_by IS NOT NULL AND {region_query}"  # consider added last_updated check
            unleveled_sql = f"SELECT count(*) FROM accounts WHERE level < 30 AND {region_query}"
            available_leveled_sql = f"SELECT count(*) FROM accounts WHERE last_returned < {last_returned_limit} AND in_use_by IS NULL AND {region_query} AND level >= 30"
            available_unleveled_sql = f"SELECT count(*) FROM accounts WHERE last_returned < {last_returned_limit} AND in_use_by IS NULL AND {region_query} AND level < 30"
            total_sql = f"SELECT count(*) FROM accounts WHERE {region_query}"

            cooldown, in_use, unleveled, total, a_leveled, a_unleveled = Db.get_single_results(cd_sql, in_use_sql, unleveled_sql, total_sql, available_leveled_sql,
                                                                                               available_unleveled_sql)
            result[region] = {
                "total": {
                    "accounts": total,
                    "in_use": in_use,
                    "cooldown": cooldown,
                    "unleveled": unleveled
                },
                "available": {
                    "total": a_leveled + a_unleveled,
                    "leveled": a_leveled,
                    "unleveled": a_unleveled
                }
            }

        cd_sql = f"SELECT count(*) FROM accounts WHERE last_returned >= {last_returned_limit} AND region IS NULL"
        in_use_sql = "SELECT count(*) FROM accounts WHERE in_use_by IS NOT NULL AND region IS NULL"
        available_leveled_sql = f"SELECT count(*) FROM accounts WHERE last_returned < {last_returned_limit} AND in_use_by IS NULL AND region IS NULL AND level >= 30"
        available_unleveled_sql = f"SELECT count(*) FROM accounts WHERE last_returned < {last_returned_limit} AND in_use_by IS NULL AND region IS NULL AND level < 30"
        unleveled_sql = "SELECT count(*) FROM accounts WHERE level < 30 AND region IS NULL"
        total_sql = "SELECT count(*) FROM accounts WHERE region IS NULL"

        cooldown, in_use, unleveled, total, a_leveled, a_unleveled = Db.get_single_results(cd_sql, in_use_sql, unleveled_sql, total_sql, available_leveled_sql,
                                                                                           available_unleveled_sql)

        result['shared'] = {
            "total": {
                "accounts": total,
                "in_use": in_use,
                "cooldown": cooldown,
                "unleveled": unleveled
            },
            "available": {
                "total": a_leveled + a_unleveled,
                "leveled": a_leveled,
                "unleveled": a_unleveled
            }
        }
        return result, 200, self.resp_headers


if __name__ == "__main__":
    serv = AccountServer()
    while True:
        time.sleep(1)
