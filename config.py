import argparse
import configparser
import logging
import time
from loguru import logger

config = configparser.ConfigParser()
config.read("config/config.ini")

parser = argparse.ArgumentParser(description='Pokemon GO PTC Account Server')
parser.add_argument('-v', '--verbose', action='store_true')
parser.add_argument('-vv', '--trace', action='store_true')

logger = logging.getLogger(__name__)

class Config:
    general = config["general"]
    listen_host = general.get("listen_host", "127.0.0.1")
    listen_port = general.getint("listen_port", 9009)
    auth_username = general.get("auth_username", None)
    auth_password = general.get("auth_password", None)
    cooldown_hours = general.getint("cooldown", 24)
    cooldown_seconds = cooldown_hours * 60 * 60
    short_cooldown_hours = general.getint("cooldown_reuse", 3)
    short_cooldown_seconds = short_cooldown_hours * 60 * 60
    encounter_limit = general.getint("encounter_limit", 6500)
    device_max_logins_hour = general.getint("device_max_logins_per_hour", 4)
    account_max_logins_hour = general.getint("account_max_logins_per_hour", 4)

    args = parser.parse_args()
    if args.verbose:
        loglevel = logging.DEBUG
    elif args.trace:
        loglevel = logging.TRACE
    else:
        loglevel = logging.INFO

    database = config["database"]
    db_host = database.get("host", "127.0.0.1")
    db_port = database.getint("port", 3306)
    db_user = database.get("user", None)
    db_pw = database.get("pass", None)
    db = database.get("db", None)

    def __init__(self):
        if self.db_user is None or self.db_pw is None or self.db is None or self.auth_username is None \
                or self.auth_password is None:
            logger.error("Missing required setting! Check your config.")

    def get_cooldown_timestamp(self):
        res = int(int(time.time()) - self.cooldown_seconds)
        logger.debug(f"calculated cooldown timestamp {res}")
        return res

    def get_short_cooldown_timestamp(self):
        res = int(int(time.time()) - self.short_cooldown_seconds)
        logger.debug(f"calculated short cooldown timestamp {res}")
        return res
