import os
import datetime
from logging import getLogger
import yaml

from utils.logging import LOG_NAME, NOTIFICATION

log = getLogger(LOG_NAME)

class RouteParameterFile:
    def __init__(self, config, runname=None):
        self.params = {
            
        }
        self.config = config