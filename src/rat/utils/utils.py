import os
import math
import numpy as np
from scipy.signal import savgol_filter
from datetime import datetime

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
        return -math.ceil(abs(n) * multiplier) / multiplier

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
