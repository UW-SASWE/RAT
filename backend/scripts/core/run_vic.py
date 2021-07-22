import pandas as pd
import rasterio as rio
import os
import datetime
import subprocess

from logging import getLogger
from utils.logging import LOG_NAME
from utils.utils import run_command

log = getLogger(LOG_NAME)


class VICRunner():
    