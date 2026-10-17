# This file configures the application logger used by collection, transformation and exports.
# Sends the same UTC-stamped messages to the console and logs/etl.log, appending
# to the file so starting the pipeline again does not erase earlier logs.

import logging
from pathlib import Path
import sys
import time


def configure_logging(log_dir):
    path = Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(path / 'etl.log', mode='a', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)sZ %(levelname)s %(name)s %(message)s',
                                  datefmt='%Y-%m-%dT%H:%M:%S')
    formatter.converter = time.gmtime
    logger = logging.getLogger('market_etl')
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in (logging.StreamHandler(sys.stdout), file_handler):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
