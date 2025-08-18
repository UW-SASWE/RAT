# Modified rom 
# https://github.com/UW-Hydro/RVIC/blob/5987da6d33cea8063c848b60215353915ac2bb10/rvic/core/log.py

import os
import sys
from time import gmtime, strftime
import datetime
import logging
import subprocess
from rat.utils.utils import create_directory

# -------------------------------------------------------------------- #
LOG_LEVEL1_NAME = 'run_rat'
LOG_NAME = 'rat-logger'
LOG_LEVEL = 'DEBUG'
NOTIFICATION = 25    # Setting level above INFO, below WARNING
logging.addLevelName(NOTIFICATION, "NOTIFICATION")
NOTIFICATION_FOMATTER = logging.Formatter(
    '%(asctime)s>> %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S %p')
# -------------------------------------------------------------------- #


# -------------------------------------------------------------------- #
# Fake stream file to handle stdin/stdout
class StreamToFile(object):
    '''
    Fake file-like stream object that redirects writes to a logger instance.
    http://www.electricmonk.nl/log/2011/08/14/redirect-stdout-and-stderr-to-a-logger-in-python/
    '''
    def __init__(self, logger_name=LOG_NAME, log_level=logging.INFO):
        self.logger = logging.getLogger(logger_name)
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass

class NotificationHandler(logging.Handler):
    def emit(self, record):
        subprocess.run(['/houston2/pritam/rat_mekong_v3/.condaenv/bin/ntfy', '-b', 'telegram', 'send', self.format(record)])
# -------------------------------------------------------------------- #

class Formatter(logging.Formatter):
    def __init__(self, fmt="%(asctime)s %(levelname)s:%(funcName)s>> %(message)s", datefmt="%Y-%m-%d %H:%M:%S"):
        super().__init__(fmt, datefmt=datefmt, style='%')

    def format(self, record):
        original_fmt = self._style._fmt

        if hasattr(record, 'worker'):
            self._style._fmt = '%(asctime)s %(levelname)s [worker %(worker)s]:%(funcName)s>> %(message)s'

        # Call the original formatter class to do the grunt work
        result = logging.Formatter.format(self, record)

        # Restore the original format configured by the user
        self._style._fmt = original_fmt

        return result

FORMATTER = Formatter()

def init_logger(log_dir='./', log_level='DEBUG', verbose=False, notify=False, logger_name=LOG_NAME, for_basin=True):
    ''' Setup the logger '''
    #Creating log directory if does not exist
    create_directory(log_dir)

    #Extracting basin name from the log_dir_path
    basin_name=os.path.basename(os.path.normpath(log_dir))

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    logger.propagate = True

    # ---------------------------------------------------------------- #
    # create log file handler
    if log_dir:
        if(for_basin):
            log_file = os.path.join(log_dir, 'RAT-'+ basin_name + strftime('%Y%m%d-%H%M%S',
                                gmtime()) + '.log')
        else:
            log_file = os.path.join(log_dir, 'RAT_run-'+ strftime('%Y%m%d-%H%M%S',
                                gmtime()) + '.log')
        fh = logging.FileHandler(log_file)
        fh.setLevel(log_level)
        fh.setFormatter(FORMATTER)
        logger.addHandler(fh)
        logger.filename = log_file
    else:
        log_file = None
    # ---------------------------------------------------------------- #

    # ---------------------------------------------------------------- #
    # If verbose, logging will also be sent to console
    if verbose:
        # print to console
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        ch.setFormatter(FORMATTER)
        logger.addHandler(ch)
    # ---------------------------------------------------------------- #

    if notify:
        # print to console
        nh = NotificationHandler()
        nh.setLevel(NOTIFICATION)
        nh.setFormatter(NOTIFICATION_FOMATTER)
        logger.addHandler(nh)
    # ---------------------------------------------------------------- #


    # ---------------------------------------------------------------- #
    # Redirect stdout and stderr to logger
    sys.stdout = StreamToFile()
    sys.stderr = StreamToFile(log_level=logging.ERROR)
    # ---------------------------------------------------------------- #

    logger.info('-------------------- INITIALIZED RAT-'+basin_name+' LOG ------------------')
    logger.info('TIME: %s', datetime.datetime.now())
    logger.info('LOG LEVEL: %s', log_level)
    logger.info('Logging To Console: %s', verbose)
    logger.info('LOG FILE: %s', log_file)
    logger.info('NOTIFY: %s', notify)
    logger.info('----------------------------------------------------------\n')

    return logger
# -------------------------------------------------------------------- #


def close_logger(logger_name=LOG_NAME):
    '''Close the handlers of the logger'''
    log = logging.getLogger(logger_name)
    x = list(log.handlers)
    for i in x:
        log.removeHandler(i)
        i.flush()
        i.close()
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
# -------------------------------------------------------------------- #