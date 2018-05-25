import logging
from etc import config
from lib.logrotate.cloghandler import ConcurrentRotatingFileHandler
import sys

def get_logger(class_name):
    logger = logging.getLogger(class_name)
    formatter = logging.Formatter(config.log['formatter_main'])
    if config.log['location']:
        handler = ConcurrentRotatingFileHandler(config.log['location']+"/"+class_name, "a", config.log['size'], config.log['backups'])
    else:
        handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.getLevelName(config.log['level']))
    return logger
