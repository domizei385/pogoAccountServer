import configparser
import logging
import time

config = configparser.ConfigParser()
config.read("config.ini")

logger = logging.getLogger(__name__)


class Config:
    general = config["general"]
    listen_host = general.get("listen_host", "127.0.0.1")
    listen_port = general.getint("listen_port", 9009)
    auth_username = general.get("auth_username", None)
    auth_password = general.get("auth_password", None)
    cooldown_hours = general.getint("cooldown", 24)
    cooldown_seconds = cooldown_hours * 60 * 60

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
