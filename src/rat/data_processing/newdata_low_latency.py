import pandas as pd
from logging import getLogger

from rat.plugins.forecasting.forecasting import get_gefs_precip
from rat.plugins.forecasting.forecasting import get_GFS_data
from rat.utils.logging import LOG_NAME, LOG_LEVEL, NOTIFICATION

# Getting the log-level 2
log = getLogger(LOG_NAME)
log.setLevel(LOG_LEVEL)

def get_newdata_low_latency(start, end , basin_bounds, raw_low_latency_dir, processed_low_latency_dir, low_latency_gfs_dir):
    """
    Downloads and processes low-latency weather data (precipitation, temperature, wind speed) for a given basin 
    over a specified date range.

    Args:
        start (datetime.datetime): The start date for the data retrieval.
        end (datetime.datetime): The end date for the data retrieval.
        basin_bounds (tuple or dict): The geographical bounds of the basin for which data is to be processed. 
                                      This can be a tuple of coordinates or a dictionary with bounding box details.
        raw_low_latency_dir (str): Directory path where raw low-latency GEFS data is stored.
        processed_low_latency_dir (str): Directory path where processed low-latency GEFS data will be saved.
        low_latency_gfs_dir (str): Directory path where GFS data (temperature, wind speed) is stored.

    Returns:
        None

    Description:
        This function iterates over a date range defined by `start` and `end`. For each date, it:
        1. Downloads and processes GEFS-CHIRPS precipitation data for the given basin bounds.
        2. Downloads and processes GFS data (including temperature and wind speed) for the same date.

        The processed data is saved to the specified directories.

    Logging:
        The function logs the status of the data download and processing for each date using `log.info`, 
        which is assumed to be configured in the broader application context.
    """
    # Create array of dates
    low_latency_dates = pd.date_range(start, end)
    # Getting now cast for every date, so 0 lead time
    lead_time = 0

    for date in low_latency_dates:
        #Download and process GEFS-CHIRPS data to the basin bounds (precipitation)
        try:
            log.info(f"Downloading low latency GEFS Precipitation for {date.strftime('%Y-%m-%d')}.")
            get_gefs_precip(basin_bounds=basin_bounds, forecast_raw_dir=raw_low_latency_dir, forecast_processed_dir=processed_low_latency_dir,begin= date, lead_time=lead_time, low_latency=True)
        except Exception as e:
            log.error(f"Downloading of low latency GEFS Precipitation for {date.strftime('%Y-%m-%d')} failed.")
            log.error(e)
        #Download and process GFS data (tmin, tmax, wind speed)
        try:
            log.info(f"Downloading low latency GFS data for {date.strftime('%Y-%m-%d')}.")
            get_GFS_data(basedate=date,lead_time=lead_time, basin_bounds=basin_bounds, gfs_dir=low_latency_gfs_dir, hour_of_day=12)
        except Exception as e:
            log.error(f"Downloading of low latency GFS data for {date.strftime('%Y-%m-%d')} failed.")
            log.error(e)




