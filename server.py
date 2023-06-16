import datetime
import json
import logging
import os
import time
from typing import Optional
from typing import Union

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

# Speed can be 60 km/h up to distances of 3km
QUEST_WALK_SPEED_CALCULATED = 16.67


def _purpose_to_level_query(device_logger, purpose):
    # IV_QUEST = "quest_iv"
    # LEVEL = "level"
    # QUEST = "quest"
    # IV = "iv"
    # MON_RAID = "mon_raid"

    if purpose == "iv" or purpose == "quest" or purpose == "quest_iv":
        return " (level >= 30)"
    elif purpose == "mon_raid":
        return " (level >= 8)"
    elif purpose == "level":
        return " (level < 30)"
    else:
        device_logger.warning(f"Unhandled purpose {purpose}")
        return " (1=1)"


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
        self.app.add_url_rule("/set/<device>/login", "set_login", self.track_login, methods=['POST'])
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
        purpose = request.args.get('purpose', default='', type=str)
        region = request.args.get('region', default='', type=str)

        device_logger = logger.bind(name=device)
        device_logger.debug(f"get_availability({device}): purpose={purpose}, region={region}")

        last_returned_limit = self.config.get_cooldown_timestamp()
        last_returned_query = f"(last_returned IS NULL OR last_returned < {last_returned_limit} OR last_reason IS NULL)"

        purpose_query = _purpose_to_level_query(device_logger, purpose)
        select_reuse = f"SELECT 1 from accounts WHERE in_use_by = '{device}' AND {purpose_query} AND {last_returned_query} LIMIT 1;"
        try:
            resp = Db.get_single_results(select_reuse)
            if resp[0]:
                # we can reuse the account
                return self.resp_ok(data={"available": int(resp[0]), "type": "reuse"})
        except Exception as ex:
            logger.exception(ex)
            logger.warning(f"Error during query: {select_reuse}")
            return self.invalid_request(code=500)

        account = self._get_next_account(device=device, region=region, purpose=purpose, scan_location=None, do_log=False, reserve=False)
        # TODO: fix and return more accurate count
        available = 1 if account else 0

        return self.resp_ok(data={"available": available, "type": "pool"})

    def get_account_info(self, device=None):
        if not device:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)
        device_logger.debug(f"get_account_info()")

        encounters_from = DatetimeWrapper.now() - datetime.timedelta(hours=self.config.cooldown_hours)
        select = (f"SELECT a.username, '***', a.level, a.last_returned, a.last_reason, COALESCE(ah.total, 0), a.softban_time, a.softban_location "
                  f"  FROM accounts a LEFT JOIN "
                  f"       (SELECT ax.username, SUM(ax.encounters) total FROM accounts_history ax "
                  f"         WHERE ax.returned > '{encounters_from}' "
                  f"      GROUP BY ax.username "
                  f"       ) ah ON a.username = ah.username"
                  f" WHERE in_use_by = '{device}' "
                  f" LIMIT 1;")

        try:
            data = None
            with Db() as conn:
                cursor = conn.cursor(buffered=True)
                cursor.execute(select)
                elem = cursor.fetchone()
                if elem:
                    last_returned_limit = self.config.get_cooldown_timestamp()
                    is_burnt = last_returned_limit < int(elem[2])
                    encounters = int(elem[5]) if elem[5] else 0
                    softban_info = (elem[6], elem[7]) if elem[6] else None
                    account = (elem[0], "", int(elem[2]), encounters, softban_info)
                    reason = elem[4] if elem[4] else None
                    data = self._build_account_response(account=account, last_returned=elem[3], last_reason=reason, is_burnt=1 if is_burnt else 0)
                if data:
                    select_reason = (f"SELECT ah.reason FROM accounts_history ah WHERE ah.username = '{data['username']}' AND device = '{device}'")
                    cursor.execute(select_reason)
                    reason_response = cursor.fetchone()
                    if reason_response:
                        data['last_reason'] = reason_response[0]
        except Exception as ex:
            logger.exception(ex)
            logger.warning(f"Error during query: {select}")
            return self.invalid_request(code=500)
        if data:
            return self.resp_ok(data=data)
        return self.resp_ok(code=204)

    def get_account(self, device=None):
        if not device:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)

        args = request.get_json()
        purpose = args['purpose'] if 'purpose' in args else None
        if not purpose:
            return self.invalid_request(data="Missing 'purpose' parameter")
        do_log = int(args['logging']) if 'logging' in args else 0
        region = args['region'] if 'region' in args else None
        reason = args['reason'] if 'reason' in args else None  # None, maintenance, rotation, level, teleport, limit

        location = args['location'] if 'location' in args else None

        # TODO: set time constraint so it works for C-DAY

        # if region == "US":
        #     end_leveling = datetime.datetime.fromisoformat('2023-06-10 13:25:00.000000-04:00')
        #     start_iv = datetime.datetime.fromisoformat('2023-06-10 13:55:00.000000-04:00')
        #     end_iv = datetime.datetime.fromisoformat('2023-06-10 17:00:00.000000-04:00')
        #     tz = pytz.timezone("America/New_York")
        #     now = DatetimeWrapper.now(tz)
        # else:
        #     end_leveling = datetime.datetime.fromisoformat('2023-06-10 13:25:00.000000+02:00')
        #     start_iv = datetime.datetime.fromisoformat('2023-06-10 13:55:00.000000+02:00')
        #     end_iv = datetime.datetime.fromisoformat('2023-06-10 17:00:00.000000+02:00')
        #     now = DatetimeWrapper.now()

        # if purpose == "level":
        #     if now > end_leveling and now < end_iv:
        #         device_logger.info(f"No leveling before cday {region}")
        #         return self.resp_ok(code=204, data={"error": "No accounts available"})
        # elif purpose == "iv" or purpose == "quest_iv":
        #     if now < start_iv or now > end_iv:
        #         device_logger.info(f"No iv. {region}")
        #         #device_logger.debug(f"Purpose = IV is disabled")
        #         return self.resp_ok(code=204, data={"error": "No accounts available"})

        if location:
            location = json.dumps(location)
        device_logger.debug(
            f"get_account: purpose={purpose}, region={region}, reason={reason}, "
            f"location={location}, purpose={purpose}")

        account = None

        # sticky accounts (prefer account reusage unless burned)
        try_reusing_previous_login = True
        if try_reusing_previous_login:
            encounters_from = DatetimeWrapper.now() - datetime.timedelta(hours=self.config.cooldown_hours)
            purpose_query = _purpose_to_level_query(device_logger, purpose)
            last_returned_limit = self.config.get_cooldown_timestamp()
            last_returned_query = f"(last_returned IS NULL OR last_returned < {last_returned_limit} OR last_reason IS NULL)"
            select = (f"SELECT a.username, a.password, a.level, COALESCE(ah.total, 0), a.softban_time, a.softban_location "
                      f"  FROM accounts a LEFT JOIN "
                      f"       (SELECT ax.username, SUM(ax.encounters) total FROM accounts_history ax "
                      f"         WHERE ax.returned > '{encounters_from}' "
                      f"      GROUP BY ax.username"
                      f"        HAVING SUM(ax.encounters) < {self.config.encounter_limit * 0.9}"  # at least 10% of encounters left to prevent frequent relogins 
                      f"       ) ah ON a.username = ah.username"
                      f" WHERE a.in_use_by = '{device}'"
                      f"   AND {last_returned_query}"
                      f"   AND {purpose_query}"
                      f" LIMIT 1 FOR UPDATE;")
            with Db() as conn:
                if do_log:
                    device_logger.info(select)
                cursor = conn.cursor()
                try:
                    cursor.execute(select)
                    elem = cursor.fetchone()
                    if elem:
                        username = elem[0]
                        pw = elem[1]
                        level = int(elem[2])
                        encounters = int(elem[3]) if elem[3] else 0
                        softban_info = (elem[4], elem[5]) if elem[4] else None

                        self._mark_account_used(username, device, purpose, cursor)

                        account = (username, pw, level, encounters, softban_info)
                except Exception as ex:
                    device_logger.error("Exception during query {}. Exception: {}", select, ex)
                finally:
                    cursor.close()

        if not account:
            # drop any previous usage of requesting device
            reset = (f"UPDATE accounts SET in_use_by = NULL, last_updated = '{int(time.time())}' WHERE in_use_by = '{device}';")
            reset_history = (
                f"UPDATE accounts_history SET returned = '{DatetimeWrapper.now()}', reason = 'reset' WHERE device = '{device}' AND returned IS NULL AND acquired > '{DatetimeWrapper.now() - datetime.timedelta(days=5)}' ORDER BY ID DESC LIMIT 1;")

            updated = 0
            with Db() as conn:
                conn.cur.execute(reset)
                if conn.cur.rowcount > 0:
                    device_logger.info(f"Reset 'accounts' for device as previous entry was still active.")
                conn.cur.execute(reset_history)
                if conn.cur.rowcount > 0:
                    device_logger.info(f"Reset 'accounts_history' for device as previous entry was still active.")
                updated += conn.cur.rowcount

            account = self._get_next_account(device=device, region=region, purpose=purpose, scan_location=location, do_log=do_log)

            if not account:
                device_logger.debug(f"Found no suitable account")
                return self.resp_ok(code=204, data={"error": "No accounts available"})

        # device_logger.debug(f"get_account(reason={reason}) returns: user {account[0]}, encounters {account[3]} ")

        self._write_history(username=account[0], device=device, acquired=DatetimeWrapper.now(), new_reason=reason, purpose=purpose)

        data = self._build_account_response(account=account, last_returned=None, last_reason=None, is_burnt=0)
        device_logger.info("get_account: " + str(data))
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
        with Db() as conn:
            conn.cur.execute(set_location)

        device_logger.debug(args)
        return self.resp_ok(code=204)

    def track_login(self, device=None):
        if not device:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)

        username = None
        claimed_sql = f"SELECT username FROM accounts WHERE in_use_by = '{device}'"
        with Db() as conn:
            conn.cur.execute(claimed_sql)
            for elem in conn.cur:
                username = elem[0]
                break

        if not username:
            device_logger.debug(f"Unable to track login due to missing assignment.")
            return self.resp_ok()

        device_logger.info(f"Login of {username}")

        self._write_history(username, device, new_reason='login')

        return self.resp_ok(data={"username": username, "status": "logged in"})

    def set_logout(self, device=None):
        if not device:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)

        username = None
        prev_level = 1
        claimed_sql = f"SELECT username, last_use, level FROM accounts WHERE in_use_by = '{device}'"
        with Db() as conn:
            conn.cur.execute(claimed_sql)
            for elem in conn.cur:
                username = elem[0]
                last_used = int(elem[1])
                prev_level = int(elem[2])
                break

        if not username:
            device_logger.debug(f"Unable to logout due to missing assignment.")
            return self.resp_ok()

        args = request.get_json()

        encounters = int(args['encounters']) if 'encounters' in args else None
        level = int(args['level']) if 'level' in args else None
        level_sql = f" , level = {level}" if level and level > prev_level else ""

        device_logger.info(f"Logout of {username} (usage {humanize.precisedelta(int(time.time()) - last_used)}, encounters = {encounters}, level = {level})")

        reset = (f"UPDATE accounts SET in_use_by = NULL, last_returned = '{int(time.time())}', last_updated = '{int(time.time())}', "
                 f"last_reason = NULL {level_sql} WHERE in_use_by = '{device}';")

        try:
            with Db() as conn:
                conn.cur.execute(reset)
        except Exception as ex:
            logger.warning(f"Exception in {reset}: {ex}")

        self._write_history(username, device, new_reason='logout', encounters=encounters, returned=DatetimeWrapper.now())

        return self.resp_ok(data={"username": username, "status": "logged out"})

    def set_burned(self, device=None):
        if not device:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)

        username = None
        prev_level = 1
        claimed_sql = f"SELECT username, last_use, level FROM accounts WHERE in_use_by = '{device}' LIMIT 1 FOR UPDATE"
        with Db() as conn:
            conn.cur.execute(claimed_sql)
            for elem in conn.cur:
                username = elem[0]
                last_used = int(elem[1])
                prev_level = int(elem[2])
                break

        if not username:
            device_logger.debug(f"Unable to burn account due to missing assignment.")
            return self.resp_ok()

        args = request.get_json()

        last_reason = args['reason'] if 'reason' in args else None
        last_burned_sql = ''
        if last_reason == "maintenance":
            last_burned_sql = f", last_burned = '{DatetimeWrapper.now()}'"
        last_reason_sql = f" last_reason = '{last_reason}'" if last_reason else " last_reason = NULL"
        level = int(args["level"]) if 'level' in args else None
        level_sql = f" level = '{level}'" if level and (level > prev_level) else " level = level"

        device_logger.info(f"Request to burn account {username} (reason: {last_reason}), acquired {humanize.precisedelta(int(time.time()) - last_used)} ago)")

        reset = (f"UPDATE accounts SET in_use_by = NULL, last_returned = '{int(time.time())}', last_updated = '{int(time.time())}'"
                 f" {last_burned_sql}, {last_reason_sql}, {level_sql}, purpose = NULL WHERE in_use_by = '{device}';")

        with Db() as conn:
            conn.cur.execute(reset)

        encounters = None
        if 'encounters' in args:
            encounters = int(args['encounters'])
        self._write_history(username, device, new_reason=last_reason, encounters=encounters, returned=DatetimeWrapper.now())

        return self.resp_ok(data={"username": username, "status": "burned"})

    def _write_history(self, username: str, device: str, new_reason: str, encounters: Optional[int] = None, acquired: Optional[datetime.datetime] = None,
        returned: Optional[datetime.datetime] = None, purpose: str = None):
        if not device:
            return self.invalid_request(data="Missing 'device' parameter")
        device_logger = logger.bind(name=device)

        # check whether we have an update candidate
        with Db() as conn:
            cursor = conn.cursor()
            returned_sql = f", returned = '{returned}'" if returned else ''
            reason_sql = f", reason = '{new_reason}'" if new_reason else ''
            encounters_sql = f", encounters = GREATEST(encounters, {int(encounters)})" if encounters else ''

            new_history_before = DatetimeWrapper.now() - datetime.timedelta(hours=24)
            find_candidate_query = f"SELECT id, reason, encounters from accounts_history WHERE device = '{device}' AND username = '{username}' AND returned IS NULL AND acquired > '{new_history_before}' ORDER BY ID desc LIMIT 1 FOR UPDATE;"
            history_query = None
            try:
                updating = False
                cursor.execute(find_candidate_query)
                elem = cursor.fetchone()
                if elem:
                    old_reason = elem[1] if elem[1] else None
                    if old_reason and old_reason == 'prelogin' and new_reason == 'logout' and encounters and encounters == 0:
                        reason_sql = f", reason = 'nologin'"
                    old_encounters = int(elem[2]) if elem[2] else None
                    if old_encounters and encounters and old_encounters > encounters > 0:
                        logger.warning(f"old_encounters {old_encounters} > encounters {encounters}. Incrementing.")
                        encounters_sql = f", encounters = encounters + {encounters}"

                    if returned_sql or reason_sql or encounters_sql:
                        history_query = (
                            f"UPDATE accounts_history SET device = device {returned_sql} {reason_sql} {encounters_sql} WHERE id = {int(elem[0])}")
                        updating = True
                if not updating:
                    acquired = acquired if acquired else DatetimeWrapper.now()
                    acquired_sql = f", acquired = '{acquired}'"
                    purpose_sql = f", purpose = '{purpose}'" if purpose else ''
                    history_query = (
                        f"INSERT INTO accounts_history SET username = '{username}', device = '{device}' {acquired_sql} {returned_sql} {reason_sql} {encounters_sql} {purpose_sql}")
                if history_query:
                    device_logger.info(f"History: {history_query}")
                    cursor.execute(history_query)
            except Exception as ex:
                device_logger.info(f"Unable to write history. Query: {find_candidate_query} / {history_query}: {ex}")
            finally:
                cursor.close()

    def _stats_data(self):
        last_returned_limit = self.config.get_cooldown_timestamp()
        last_returned_query = f"(last_returned IS NULL OR last_returned < {last_returned_limit} OR last_reason IS NULL)"

        last_use_limit = self.config.get_short_cooldown_timestamp()

        regions = ["EU", "US", "shared"]
        result = {}

        for region in regions:
            if region == "shared":
                region_query = "region IS NULL"
            else:
                region_query = f"region = '{region}'"

            in_use_sql = f"SELECT count(*) FROM accounts WHERE in_use_by IS NOT NULL AND {region_query}"  # consider added last_updated check
            unleveled_sql = f"SELECT count(*) FROM accounts WHERE level < 30 AND {region_query}"
            available_leveled_sql = f"SELECT count(*) FROM accounts WHERE {last_returned_query} AND last_use < {last_use_limit} AND in_use_by IS NULL AND {region_query} AND level >= 30"
            available_unleveled_sql = f"SELECT count(*) FROM accounts WHERE {last_returned_limit} AND last_use < {last_use_limit} AND in_use_by IS NULL AND {region_query} AND level < 30"
            total_sql = f"SELECT count(*) FROM accounts WHERE {region_query}"

            in_use, unleveled, total, a_leveled, a_unleveled = Db.get_single_results(in_use_sql, unleveled_sql, total_sql, available_leveled_sql,
                                                                                     available_unleveled_sql)
            cooldown = {}
            with Db() as conn:
                cursor = conn.cursor()
                cd_sql = f"SELECT COALESCE(last_reason, 'unknown'), count(*) FROM accounts WHERE last_returned >= {last_returned_limit} AND {region_query} GROUP BY last_reason"
                try:
                    cursor.execute(cd_sql)
                    res = cursor.fetchall()
                    for (reason, count) in res:
                        cooldown[reason] = int(count)
                except:
                    pass

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

        return result

    def test(self):
        device = request.args.get('device', default='test', type=str)
        region = request.args.get('region', default='EU', type=str)
        purpose = request.args.get('purpose', default='iv', type=str)
        lat = request.args.get('lat', default=0.0, type=float)
        lng = request.args.get('lng', default=0.0, type=float)

        account = self._get_next_account(device=device, region=region, purpose=purpose, scan_location=Location(lat, lng).to_json(), do_log=True, reserve=False)
        logger.info(account)
        if account and account[4]:
            softban_info = account[4]
            last_action_location = Location.from_json(softban_info[1])
            # logger.info(f"Location: {last_action_location}")
            distance_last_action = last_action_location.get_distance_from_in_meters(lat, lng)
            # logger.info(f"distance: {distance_last_action}")
            softban_time = datetime.datetime.fromisoformat(softban_info[0])
            cooldown_seconds = Location.calculate_cooldown(distance_last_action, QUEST_WALK_SPEED_CALCULATED)
            # logger.info(f"Cooldown: {cooldown_seconds}")
            usable = DatetimeWrapper.now() > softban_time + datetime.timedelta(seconds=cooldown_seconds)
            logger.info(f"Usable: {usable}")
            return self.resp_ok(data=account)
        return self.resp_ok(code=204)

    # TODO: add
    #     elif purpose in [AccountPurpose.LEVEL, AccountPurpose.QUEST, AccountPurpose.IV_QUEST]:
    #         # Depending on last softban action and distance to the location thereof
    #
    #         if not auth.last_softban_action_location:
    #             return True
    #         elif location_to_scan is None:
    #             return False
    #         last_action_location: Location = Location(auth.last_softban_action_location[0],
    #                                                   auth.last_softban_action_location[1])
    #         distance_last_action = get_distance_of_two_points_in_meters(last_action_location.lat,
    #                                                                     last_action_location.lng,
    #                                                                     location_to_scan.lat,
    #                                                                     location_to_scan.lng)
    #         cooldown_seconds = calculate_cooldown(distance_last_action, QUEST_WALK_SPEED_CALCULATED)
    #         usable: bool = DatetimeWrapper.now() > auth.last_softban_action \
    #                        + datetime.timedelta(seconds=cooldown_seconds)
    #         logger.debug2("Calculated cooldown: {}, thus usable: {}", cooldown_seconds, usable)
    #         return usable

    def stats(self):
        return self._stats_data(), 200, self.resp_headers

    def _build_account_response(self, account: tuple[str, str, int, int, tuple[str, str]], last_returned: Optional[int], last_reason: Optional[str], is_burnt: int = 0):
        remaining_encounters = max(0, self.config.encounter_limit - account[3])
        if not remaining_encounters:
            remaining_encounters = self.config.encounter_limit
        response = {"username": account[0], "password": account[1], "level": account[2], "remaining_encounters": remaining_encounters, "is_burnt": is_burnt}
        if last_returned:
            response["last_returned"] = last_returned
        if last_reason:
            response["last_reason"] = last_reason
        if account[4]:
            response["softban_info"] = {
                "time": account[4][0],
                "location": account[4][1]
            }
        logger.debug(response)
        return response

    def _get_next_account(self, device: str, region: str, purpose: str, scan_location: Optional[Union[bytes, str]], do_log: int, reserve: bool = True) -> Optional[
        tuple[str, str, int, int, tuple[str, str]]]:
        if not device:
            return None
        device_logger = logger.bind(name=device)

        # throttle device logins attempts per hour
        device_logins = (f"   SELECT COUNT(*) device_logins FROM accounts_history"
                         f"    WHERE acquired > '{DatetimeWrapper.now() - datetime.timedelta(hours=1)}'"
                         f"      AND device = '{device}'"
                         f"    LIMIT 1")
        with Db() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(device_logins)
                elem = cursor.fetchone()
                if elem and int(elem[0]) > self.config.device_max_logins_hour:
                    logger.warning(f"Device reached {int(elem[1])}/{self.config.device_max_logins_hour} new account assignments during the last hour. Cooling down.")
                    return None
            except:
                logger.warning("Unable to check for device logins. Query: " + device_logins)
                pass

        # reuse account
        region_query = f" (region IS NULL OR region = '' OR region = '{region}')" if region else " 1=1 "

        last_returned_query = f"(last_returned IS NULL OR last_returned < {self.config.get_cooldown_timestamp()} OR last_reason IS NULL)"
        order_by_query = "ORDER BY a.level DESC, a.last_use ASC" if purpose == 'level' else "ORDER BY a.last_use ASC"

        purpose_level_requirement = _purpose_to_level_query(device_logger, purpose)
        count_encounters_from = DatetimeWrapper.now() - datetime.timedelta(hours=self.config.cooldown_hours)

        account = None
        ignore_accounts = list()
        while not account and len(ignore_accounts) < 20:
            username_exclusion = f"AND a.username NOT IN ({','.join(ignore_accounts)})" if len(ignore_accounts) > 0 else ""
            select = (f"SELECT a.username, a.password, a.level, COALESCE(ah.total, 0), a.softban_time, a.softban_location, COALESCE(bh.user_logins, 0) "
                      f"  FROM accounts a LEFT JOIN "
                      f"       (SELECT username, SUM(encounters) total FROM accounts_history ah"
                      f"         WHERE returned > '{count_encounters_from}' "
                      f"      GROUP BY username"
                      f"        HAVING SUM(encounters) < {self.config.encounter_limit * 0.8}"  # at least 20% of encounters left to prevent frequent relogins 
                      f"       ) ah ON a.username = ah.username"
                      f"                  LEFT JOIN"
                      f"      (SELECT username, COUNT(*) user_logins FROM accounts_history bh"
                      f"        WHERE acquired > '{DatetimeWrapper.now() - datetime.timedelta(hours=1)}'"
                      f"     GROUP BY username"
                      f"      ) bh ON a.username = bh.username"
                      f" WHERE in_use_by IS NULL "
                      f"   AND {last_returned_query}"
                      f"   AND (last_use < {self.config.get_short_cooldown_timestamp()} OR level < 30)"
                      f"   AND {purpose_level_requirement}"
                      f"   AND {region_query}"
                      f"   AND bh.user_logins <= {self.config.account_max_logins_hour}"  # limit login attempts per account to 4/hour
                      f"   {username_exclusion}"
                      f" GROUP BY a.username"
                      f" {order_by_query} LIMIT 1"
                      f" {'FOR UPDATE' if reserve else ''}")
            if do_log:
                device_logger.info(select)
            else:
                device_logger.debug(select)
            with Db() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(select)
                    elem = cursor.fetchone()
                    if elem:
                        username = elem[0]
                        pw = elem[1]
                        level = int(elem[2])
                        encounters = int(elem[3]) if elem[3] else 0
                        softban_info = (elem[4], elem[5]) if elem[4] else None

                        if softban_info and scan_location and not self._account_suitable_for_location(device, softban_info, scan_location):
                            ignore_accounts.append(f"'{username}'")
                            device_logger.info(f"Account '{username}' not suitable. Skipping")
                            continue

                        if reserve:
                            if len(ignore_accounts) > 0:
                                device_logger.info(f"Using alternative account {username}")
                            self._mark_account_used(username, device, purpose, cursor)

                        account = (username, pw, level, encounters, softban_info)
                except Exception as ex:
                    logger.exception(ex)
                    logger.opt(exception=True).error("Exception during query {}. Exception: {}", select, ex)
                finally:
                    cursor.close()

            return account

    def _account_suitable_for_location(self, device: str, softban_info: tuple[str, str], scan_location: Optional[str]):
        device_logger = logger.bind(name=device)

        if not scan_location:
            device_logger.warning("No scan_location provided. Unable to reserve account.")
            return False
        scan_location = Location.from_json(scan_location)
        last_action_location = Location.from_json(softban_info[1])
        distance_last_action = last_action_location.get_distance_from_in_meters(scan_location.lat, scan_location.lng)
        softban_time = datetime.datetime.fromisoformat(softban_info[0])
        cooldown_seconds = Location.calculate_cooldown(distance_last_action, QUEST_WALK_SPEED_CALCULATED)
        usable = DatetimeWrapper.now() > softban_time + datetime.timedelta(seconds=cooldown_seconds)
        device_logger.debug(f"Last Location: {last_action_location}, New Location: {scan_location}, Cooldown: {cooldown_seconds}, Usable: {usable}")
        return usable

    def _mark_account_used(self, username, device, purpose, cursor):
        timestamp = int(time.time())
        mark_used = (f"UPDATE accounts SET in_use_by = '{device}', last_use = '{timestamp}', last_updated = '{timestamp}', last_reason = NULL,"
                     f"purpose = '{purpose}' WHERE username = '{username}';")
        cursor.execute(mark_used)


if __name__ == "__main__":
    serv = AccountServer()
    while True:
        time.sleep(1)
