import os
import math
import numpy as np
from scipy.signal import savgol_filter
from datetime import datetime
import xarray as xr
import pandas as pd

def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

def round_pixels(x):
    if((x*10)%10<5):
        x=int(x)
    else:
        x=int(x)+1
    return(x)


def create_directory(p,path_return=False):
    if not os.path.isdir(p):
        os.makedirs(p)
    if path_return:
        return p

def round_up(n, decimals=0): 
    multiplier = 10 ** decimals 
    if n>0:
        return math.ceil(n * multiplier) / multiplier
    else:
        return -math.floor(abs(n) * multiplier) / multiplier

# https://gist.github.com/pritamd47/e7ddc49f25ae7f1b06c201f0a8b98348
# Clip time-series
def clip_ts(*tss, which='left'):
    """Clips multiple time-series to align them temporally

    Args:
        which (str, optional): Defines which direction the clipping will be performed. 
                               'left' will clip the time-series only on the left side of the 
                               unaligned time-serieses, and leave the right-side untouched, and 
                               _vice versa_. Defaults to 'left'. Options can be: 'left', 'right' 
                               or 'both'

    Returns:
        lists: returns the time-series as an unpacked list in the same order that they were passed
    """
    mint = max([min(ts.index) for ts in tss])
    maxt = min([max(ts.index) for ts in tss])
    
    if mint > maxt:
        raise Exception('No overlapping time period between the time series.')

    if which == 'both':
        clipped_tss = [ts.loc[(ts.index>=mint)&(ts.index<=maxt)] for ts in tss]
    elif which == 'left':
        clipped_tss = [ts.loc[ts.index>=mint] for ts in tss]
    elif which == 'right':
        clipped_tss = [ts.loc[ts.index<=maxt] for ts in tss]
    else:
        raise Exception(f'Unknown option passed: {which}, expected "left", "right" or "both"./')

    return clipped_tss

def weighted_moving_average(data, weights, window_size):
    if window_size % 2 == 0 or window_size < 1:
        raise ValueError("Window size must be an odd positive integer.")

    data = np.array(data)
    weights = np.array(weights)

    if data.shape != weights.shape:
        raise ValueError("Data and weights must have the same shape.")

    half_window = window_size // 2
    smoothed_data = np.zeros_like(data)

    for i in range(len(data)):
        start = max(0, i - half_window)
        end = min(len(data), i + half_window + 1)

        weighted_values = data[start:end] * weights[start:end]

        # Calculate the weighted moving average
        smoothed_data[i] = np.sum(weighted_values) / np.sum(weights[start:end])

    return smoothed_data

def check_date_in_netcdf(nc_file_path, check_date):
    """
    Check if a given date is present in the 'time' dimension of a NetCDF file.

    Parameters:
    - nc_file_path (str): Path to the NetCDF file.
    - check_date (datetime.datetime): The date to check for in the NetCDF file.

    Returns:
    - bool: True if the date is present in the 'time' dimension, False otherwise.
    """
    try:
        # Open the NetCDF file
        with xr.open_dataset(nc_file_path) as ds:
            # Convert the check_date to a pandas Timestamp for comparison
            check_date = pd.Timestamp(check_date)
            
            # Check if the date is in the 'time' dimension
            if check_date in ds['time'].values:
                return True
            else:
                return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

def get_first_date_from_netcdf(nc_file_path):
    """
    Retrieve the first date from the 'time' dimension of a NetCDF file.

    Parameters:
    - nc_file_path (str): Path to the NetCDF file.

    Returns:
    - datetime.datetime: The first date in the 'time' dimension of the NetCDF file.
    """
    try:
        # Open the NetCDF file
        with xr.open_dataset(nc_file_path) as ds:
            # Extract the first date from the 'time' dimension
            first_date = ds['time'].values[0]
            
            # Convert to a datetime.datetime object if necessary
            if isinstance(first_date, np.datetime64):
                first_date = pd.to_datetime(first_date).to_pydatetime()
            return first_date
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
