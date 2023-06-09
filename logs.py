import sys
from loguru import logger
from config import Config

def setup_logger():
    log_fmt_time = "[<cyan>{time:MM-DD HH:mm:ss.SS}</cyan>]"
    log_fmt_id = "[<cyan>{extra[name]: >12}</cyan>]"
    log_fmt_mod = "[<cyan>{module: >12}:{line: <4}</cyan>]"
    log_fmt_level = "[<lvl>{level: >1.1}</lvl>]"
    log_fmt_msg = "<level>{message}</level>"

    log_format_c = [log_fmt_time, log_fmt_mod, log_fmt_id, log_fmt_level, log_fmt_msg]
    log_format_console = ' '.join(log_format_c)

    logger.remove()
    logger.add(sys.stdout, format=log_format_console, level=Config.loglevel, colorize=True)

    logconfig = {
        "extra": {"name": ""},
    }
    logger.configure(**logconfig)