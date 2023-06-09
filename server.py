import datetime
import json
import logging
import os
import time
from typing import Optional

import humanize as humanize
from flask import Flask, request
from flask_basicauth import BasicAuth
from loguru import logger

from DatetimeWrapper import DatetimeWrapper
from Location import Location
from config import Config
from db_connection import DbConnection as Db
from logs import setup_logger

setup_logger()

# TODO: add job to kill outdated assignments
# SELECT username, FROM_UNIXTIME(last_updated), region  FROM `accounts` where FROM_UNIXTIME(last_updated) < '2023-06-07' and in_use_by IS NOT NULl

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

        self.app.add_url_rule("/get/availability", "get_availability", self.get_availability, methods=['GET'])
        self.app.add_url_rule("/get/<device>", "get_account", self.get_account, methods=['GET', 'POST'])
        self.app.add_url_rule("/get/<device>/info", "get_account_info", self.get_account_info, methods=['GET'])
        self.app.add_url_rule("/set/<device>/level/<int:level>", "set_level", self.set_level, methods=['POST'])
        self.app.add_url_rule("/set/<device>/burned", "set_burned", self.set_burned, methods=['POST'])
        self.app.add_url_rule("/set/<device>/logout", "set_logout", self.set_logout, methods=['POST'])
        self.app.add_url_rule("/set/<device>/softban", "set_softban", self.set_softban, methods=['POST'])

        self.app.add_url_rule("/stats", "stats", self.stats, methods=['GET'])
        self.app.add_url_rule("/test", "test", self.test, methods=['GET'])

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
        wrapper_data = {"status": "fail"}
        if data:
            wrapper_data["data"] = data
        if logging:
            logger.warning(f"responding with {code}, data: {wrapper_data}")
        return wrapper_data, code, self.resp_headers

    def fallback(self, first=None, rest=None):
        logger.info("Fallback called")
        if request.method == 'POST':
            logger.info(f"POST request to fallback at {first}/{rest}")
        if request.method == 'GET':
            logger.info(f"GET request to fallback at {first}/{rest}")
        return self.invalid_request(data="Unhandled request")

    def get_availability(self):
        device = request.args.get('device', default='', type=str)
        leveling = request.args.get('leveling', default=0, type=int)
        region = request.args.get('region', default='', type=str)

        device_logger = logger.bind(name=device)
        device_logger.debug(f"get_availability({device}): leveling={leveling}, region={region}")

        last_returned_limit = self.config.get_cooldown_timestamp()
        last_returned_query = f"(last_returned IS NULL OR last_returned < {last_returned_limit} OR last_reason IS NULL)"

        level_query = " AND level < 30" if leveling else " AND level >= 30"
        is_reuse = f"SELECT 1 from accounts WHERE in_use_by = '{device}' {level_query} AND {last_returned_query} LIMIT 1;"
        resp = Db.get_single_results(is_reuse)
        if resp[0]:
            # we can reuse the account
            return self.resp_ok(data={"available": int(resp[0]), "type": "reuse"})
        all_stats = self._stats_data()

        region_stats = all_stats[region] if region else all_stats['shared']
        node_available = region_stats['available']
        available = int(node_available['unleveled']) if leveling else int(node_available['leveled'])

        return self.resp_ok(data={"available": available, "type": "pool"})

    def get_account_info(self, device=None):
        if not device:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)
        device_logger.debug(f"get_account_info()")

        select = (f"SELECT a.username, '***', a.level, a.last_returned, a.last_burned, SUM(ah.encounters), a.softban_time, a.softban_location "
                  f"  FROM accounts a LEFT OUTER JOIN accounts_history ah ON a.username = ah.username"
                  f" WHERE in_use_by = '{device}' "
                  f" GROUP BY a.username"
                  f" LIMIT 1;")

        with Db() as conn:
            conn.cur.execute(select)
            for elem in conn.cur:
                last_returned_limit = self.config.get_cooldown_timestamp()
                is_burnt = last_returned_limit < int(elem[2])
                encounters = int(elem[5]) if elem[5] else 0
                remaining_encounters = max(0, self.config.encounter_limit - encounters)
                softban_info = (elem[6], elem[7]) if elem[6] else None
                data = self._build_account_response(username=elem[0], password="", level=int(elem[2]), last_returned=elem[3], last_reason=elem[4], remaining_encounters=remaining_encounters,
                                                    is_burnt=1 if is_burnt else 0, softban_info=softban_info)
                return self.resp_ok(data=data)
        return self.resp_ok(code=204)

    def get_account(self, device=None):
        # TODO: track last known account location and consider new location?
        if not device:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)

        args = request.get_json()
        leveling = int(args['leveling']) if 'leveling' in args else 0
        do_log = int(args['logging']) if 'logging' in args else 0
        level_query = " level < 30" if leveling else " level >= 30"
        region = args['region'] if 'region' in args else None
        reason = args['reason'] if 'reason' in args else None  # None, level, maintenance, rotation, level, teleport, limit

        location = args['location'] if 'location' in args else None
        if location:
            location = json.dumps(location)
        device_logger.debug(
            f"get_account: leveling={leveling}, region={region}, reason={reason}, "
            f"location={location}")

        username, pw, level, last_returned, last_reason = None, None, None, None, None
        encounters = 0

        # sticky accounts (prefer account reusage unless burned)
        aggregate_encounters_from = DatetimeWrapper.now() - datetime.timedelta(hours=self.config.cooldown_hours)
        if not reason:
            last_returned_limit = self.config.get_cooldown_timestamp()
            last_returned_query = f"(last_returned IS NULL OR last_returned < {last_returned_limit} OR last_reason IS NULL)"
            select = (f"SELECT a.username, a.password, a.level, ah.total, a.softban_time, a.softban_location "
                      f"  FROM accounts a LEFT JOIN "
                      f"       (SELECT ax.username, SUM(ax.encounters) total FROM accounts_history ax "
                      f"         WHERE ax.returned > '{aggregate_encounters_from}' "
                      f"      GROUP BY ax.username"
                      f"        HAVING SUM(ax.encounters) < {self.config.encounter_limit * 0.1}"  # at least 10% of encounters left to prevent frequent relogins 
                      f"       ) ah ON a.username = ah.username"
                      f" WHERE a.in_use_by = '{device}'"
                      f"   AND {last_returned_query}"
                      f"   AND {level_query}"
                      f" GROUP BY a.username LIMIT 1;")
            with Db() as conn:
                if do_log:
                    device_logger.info(select)
                try:
                    conn.cur.execute(select)
                    elem = conn.cur.fetchone()
                    if elem:
                        username = elem[0]
                        pw = elem[1]
                        level = elem[2]
                        encounters = int(elem[3]) if elem[3] else 0
                        softban_info = (elem[4], elem[5]) if elem[4] else None
                except Exception as ex:
                    device_logger.error("Exception during query {}. Exception: {}", select, ex)

        if not username or not pw:
            # drop any previous usage of requesting device
            reset = (f"UPDATE accounts SET in_use_by = NULL, last_updated = '{int(time.time())}' WHERE in_use_by = '{device}';")
            with Db() as conn:
                conn.cur.execute(reset)

            # new account
            region_query = f" (region IS NULL OR region = '' OR region = '{region}')" if region else " 1=1 "

            last_returned_limit = self.config.get_cooldown_timestamp()
            last_returned_query = f"(last_returned IS NULL OR last_returned < {last_returned_limit} OR last_reason IS NULL)"
            last_use_limit = self.config.get_short_cooldown_timestamp()
            # TODO: incorporate last usage from history table
            order_by_query = "ORDER BY a.level DESC" if leveling == 1 else "ORDER BY a.last_use ASC"
            select = (f"SELECT a.username, a.password, a.level, ah.total, a.softban_time, a.softban_location "
                      f"  FROM accounts a LEFT JOIN "
                      f"       (SELECT ax.username, SUM(ax.encounters) total FROM accounts_history ax "
                      f"         WHERE ax.returned > '{aggregate_encounters_from}' "
                      f"      GROUP BY ax.username"
                      f"        HAVING SUM(ax.encounters) < {self.config.encounter_limit * 0.8}"  # at least 20% of encounters left to prevent frequent relogins 
                      f"       ) ah ON a.username = ah.username"
                      f" WHERE in_use_by IS NULL "
                      f"   AND {last_returned_query}"
                      f"   AND (last_use < {last_use_limit} OR level < 30)"
                      f"   AND {level_query}"
                      f"   AND {region_query}"
                      f" GROUP BY a.username"
                      f" {order_by_query} LIMIT 1;")
            if do_log:
                device_logger.info(select)
            else:
                device_logger.debug(select)
            with Db() as conn:
                try:
                    conn.cur.execute(select)
                    elem = conn.cur.fetchone()
                    if elem:
                        username = elem[0]
                        pw = elem[1]
                        level = elem[2]
                        encounters = int(elem[3]) if elem[3] else 0
                        softban_info = (elem[4], elem[5]) if elem[4] else None
                except Exception as ex:
                    device_logger.error("Exception during query {}. Exception: {}", select, ex)

            if not username or not pw:
                device_logger.debug(f"Found no suitable account")
                return self.resp_ok(code=204, data={"error": "No accounts available"})

        mark_used = (f"UPDATE accounts SET in_use_by = '{device}', last_use = '{int(time.time())}', last_updated = '{int(time.time())}', last_reason = NULL WHERE "
                     f"username = '{username}';")
        with Db() as conn:
            conn.cur.execute(mark_used)
        # device_logger.info(f"Request from {device}(leveling={request.args.get('leveling')}, reason={reason}) return {username=}, {pw=}")
        remaining_encounters = max(0, self.config.encounter_limit - encounters)
        data = self._build_account_response(username=username, password=pw, level=level, last_returned=None, last_reason=None, remaining_encounters=remaining_encounters,
                                            is_burnt=0, softban_info=softban_info)
        device_logger.debug("get_account: " + str(data))
        return self.resp_ok(data=data)

    def set_level(self, device=None, level: int = None):
        if not device or not level:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)

        check_update = f"SELECT count(*) FROM accounts WHERE in_use_by = '{device}' AND level <> {level}"
        if not int(Db.get_single_results(check_update)[0]):
            device_logger.debug(f"Request for device {device}")
            return self.resp_ok()

        device_logger.info(f"Set level to {level}")
        update = (f"UPDATE accounts SET level = {level}, last_updated = '{int(time.time())}' WHERE in_use_by = '{device}';")
        with Db() as conn:
            conn.cur.execute(update)

        return self.resp_ok()

    def set_softban(self, device=None):
        if not device:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)
        args = request.get_json()

        set_location = (f"UPDATE accounts SET softban_time = '{args['time']}',"
                 f" softban_location = '{args['location']}' WHERE in_use_by = '{device}';")
        # time_of_action = datetime.datetime.fromisoformat(args["time"])
        # location = datetime.datetime.fromisoformat(args["time"])
        with Db() as conn:
            conn.cur.execute(set_location)

        device_logger.debug(args)
        return self.resp_ok(code=204)

    def set_logout(self, device=None):
        if not device:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)

        username = None
        claimed_sql = f"SELECT username, last_use FROM accounts WHERE in_use_by = '{device}'"
        with Db() as conn:
            conn.cur.execute(claimed_sql)
            for elem in conn.cur:
                username = elem[0]
                last_used = int(elem[1])
                break

        if not username:
            device_logger.debug(f"Unable to logout due to missing assignment.")
            return self.resp_ok()

        args = request.get_json()

        encounters = 0
        if 'encounters' in args:
            encounters = int(args['encounters'])
        device_logger.info(f"Logout of {username} (usage {humanize.precisedelta(int(time.time()) - last_used)}, encounters = {encounters})")

        reset = (f"UPDATE accounts SET in_use_by = NULL, last_returned = '{int(time.time())}', last_updated = '{int(time.time())}',"
                 f" last_reason = NULL WHERE in_use_by = '{device}';")

        with Db() as conn:
            conn.cur.execute(reset)

        self._write_history(username, device, acquired=DatetimeWrapper.fromtimestamp(last_used), reason='logout', encounters=encounters)

        return self.resp_ok(data={"username": username, "status": "logged out"})

    def set_burned(self, device=None):
        if not device:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)

        username = None
        claimed_sql = f"SELECT username, last_use FROM accounts WHERE in_use_by = '{device}'"
        with Db() as conn:
            conn.cur.execute(claimed_sql)
            for elem in conn.cur:
                username = elem[0]
                last_used = int(elem[1])
                break

        if not username:
            device_logger.info(f"Unable to burn as device {device} has not claimed any username.")
            return self.resp_ok()
        device_logger.info(f"Request from {device} to burn account {username} (acquired {(time.time() - last_used) / 60 / 60} h ago)")

        args = request.get_json()
        last_reason = ''
        if 'reason' in args:
            last_reason = args['reason']

        last_burned_sql = ''
        if last_reason == "maintenance":
            last_burned_sql = f", last_burned = '{DatetimeWrapper.now()}'"

        reset = (f"UPDATE accounts SET in_use_by = NULL, last_returned = '{int(time.time())}', last_updated = '{int(time.time())}'"
                 f" {last_burned_sql}, last_reason = '{last_reason}' WHERE in_use_by = '{device}';")

        with Db() as conn:
            conn.cur.execute(reset)

        encounters = 0
        if 'encounters' in args:
            encounters = int(args['encounters'])
        self._write_history(username, device, acquired=DatetimeWrapper.fromtimestamp(last_used), reason=last_reason, encounters=encounters)

        return self.resp_ok(data={"username": username, "status": "burned"})

    def _write_history(self, username: str, device: str, acquired: Optional[datetime.datetime], reason: str, encounters: int,
        returned: Optional[datetime.datetime] = None):
        acquired_sql = f", acquired = '{acquired}'" if acquired else ''
        if not returned:
            returned = DatetimeWrapper.now()
        returned_sql = f", returned = '{returned}'" if returned else ''
        history = (
            f"INSERT INTO accounts_history SET username = '{username}', device = '{device}' {acquired_sql} {returned_sql}, reason = '{reason}', encounters = {encounters}")
        with Db() as conn:
            conn.cur.execute(history)

    def _stats_data(self):
        last_returned_limit = self.config.get_cooldown_timestamp()
        last_returned_query = f"(last_returned IS NULL OR last_returned < {last_returned_limit} OR last_reason IS NULL)"

        last_use_limit = self.config.get_short_cooldown_timestamp()

        regions = ["EU", "US"]
        result = {}

        for region in regions:
            region_query = f"(region = '{region}' OR region IS NULL)"

            cd_sql = f"SELECT count(*) FROM accounts WHERE last_returned >= {last_returned_limit} AND {region_query}"
            in_use_sql = f"SELECT count(*) FROM accounts WHERE in_use_by IS NOT NULL AND {region_query}"  # consider added last_updated check
            unleveled_sql = f"SELECT count(*) FROM accounts WHERE level < 30 AND {region_query}"
            available_leveled_sql = f"SELECT count(*) FROM accounts WHERE {last_returned_query} AND last_use < {last_use_limit} AND in_use_by IS NULL AND {region_query} AND level >= 30"
            available_unleveled_sql = f"SELECT count(*) FROM accounts WHERE {last_returned_limit} AND last_use < {last_use_limit} AND in_use_by IS NULL AND {region_query} AND level < 30"
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
        available_leveled_sql = f"SELECT count(*) FROM accounts WHERE {last_returned_query} AND last_use < {last_use_limit} AND in_use_by IS NULL AND region IS NULL AND level >= 30"
        available_unleveled_sql = f"SELECT count(*) FROM accounts WHERE {last_returned_query} AND last_use < {last_use_limit} AND in_use_by IS NULL AND region IS NULL AND level < 30"
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
        return result

    def test(self):
        sql = f"SELECT last_burned FROM accounts WHERE last_burned IS NOT NULL LIMIT 10"
        data = []
        with Db() as conn:
            conn.cur.execute(sql)
            for elem in conn.cur:
                data.append(elem[0])
        return data

    def stats(self):
        return self._stats_data(), 200, self.resp_headers

    def _build_account_response(self, username: str, password: str, level: int, last_returned: Optional[int], last_reason: Optional[str], remaining_encounters: int = None,
        is_burnt: int = 0, softban_info:tuple[str, str]=None):
        if not remaining_encounters:
            remaining_encounters = self.config.encounter_limit
        response = {"username": username, "password": password, "level": level, "remaining_encounters": remaining_encounters, "is_burnt": is_burnt}
        if last_returned:
            response["last_returned"] = last_returned
        if last_reason:
            response["last_reason"] = last_reason
        if softban_info:
            response["softban_info"] = {
                "time": softban_info[0],
                "location": softban_info[1]
            }
        logger.debug(response)
        return response


if __name__ == "__main__":
    serv = AccountServer()
    while True:
        time.sleep(1)
