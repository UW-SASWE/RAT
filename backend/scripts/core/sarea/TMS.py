import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
from scipy.stats import sigmaclip
import warnings
warnings.filterwarnings('ignore')

from utils.utils import clip_ts


class TMS():
    def __init__(self, reservoir_name, area=None, AREA_DEVIATION_THRESHOLD_PCNT=5):
        """_summary_

        Args:
            reservoir_name (_type_): _description_
            area (_type_, optional): _description_
            AREA_DEVIATION_THRESHOLD_PCNT (float, optional): _description_. Defaults to 5.

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        self.reservoir_name = reservoir_name
        self.area = area

        self.AREA_DEVIATION_THRESHOLD = self.area * AREA_DEVIATION_THRESHOLD_PCNT/100

        return self

    def tms_os(self,
            l8_dfpath: str, 
            s2_dfpath: str, 
            s1_dfpath: str, 
            CLOUD_THRESHOLD: float = 90.0,
            MIN_DATE: str = '2019-01-01'
        ):
        """Implements the TMS-OS methodology

        Args:
            l8_dfpath (string): Path of the surface area dataframe obtained using `sarea_cli_l8.py` - Landsat derived surface areas
            s2_dfpath (string): Path of the surface area dataframe obtained using `sarea_cli_s2.py` - Sentinel-2 derived surface areas
            s1_dfpath (string): Path of the surface area dataframe obtained using `sarea_cli_sar.py` - Sentinel-1  derived surface areas
            CLOUD_THRESHOLD (float): Threshold to use for cloud-masking in % (default: 90.0)
            MIN_DATE (str): Minimum date for which data to keep for all the datasets in YYYY-MM-DD or %Y-%m-%d format (default: 2019-01-01)
        """
        MIN_DATE = pd.to_datetime(MIN_DATE, format='%Y-%m-%d')

        # Read in Landsat-8
        l8df = pd.read_csv(l8_dfpath, parse_dates=['mosaic_enddate']).rename({'mosaic_enddate': 'date'}, axis=1).set_index('date')
        l8df = l8df[['water_area_cordeiro', 'non_water_area_cordeiro', 'water_area_NDWI', 'non_water_area_NDWI', 'cloud_area', 'corrected_area_cordeiro', 'corrected_area_NDWI']]
        l8df['cloud_percent'] = l8df['cloud_area']*100/(l8df['water_area_NDWI']+l8df['non_water_area_NDWI']+l8df['cloud_area'])
        l8df.replace(-1, np.nan, inplace=True)
        l8df_filtered = l8df[l8df['cloud_percent']<CLOUD_THRESHOLD]

        # Read in Sentinel-2 data
        s2df = pd.read_csv(s2_dfpath, parse_dates=['mosaic_enddate']).rename({'mosaic_enddate': 'date'}, axis=1).set_index('date')
        s2df = s2df[['water_area_cordeiro', 'non_water_area_cordeiro', 'water_area_NDWI', 'non_water_area_NDWI', 'cloud_area', 'corrected_area_cordeiro', 'corrected_area_NDWI']]
        s2df['cloud_percent'] = s2df['cloud_area']*100/(s2df['water_area_NDWI']+s2df['non_water_area_NDWI']+s2df['cloud_area'])
        s2df.replace(-1, np.nan, inplace=True)
        s2df_filtered = s2df[s2df['cloud_percent']<CLOUD_THRESHOLD]

        # Read in Sentinel-1 data
        sar = pd.read_csv(s1_dfpath, parse_dates=['time']).rename({'time': 'date'}, axis=1)
        sar['date'] = sar['date'].apply(lambda d: np.datetime64(d.strftime('%Y-%m-%d')))
        sar.set_index('date', inplace=True)
        sar.sort_index(inplace=True)
        sar = sar.loc[MIN_DATE:]

        # Combine the l8 and s2 datasets
        l8df_filtered['sat'] = 'l8'
        s2df_filtered['sat'] = 's2'

        l8df_filtered, s2df_filtered = clip_ts(l8df_filtered, s2df_filtered)
        df_filtered = pd.concat([l8df_filtered, s2df_filtered]).sort_index()   # merge and save into a dataframe called df_filtered
        df_filtered = df_filtered.loc[~df_filtered.index.duplicated(keep='last')]  # when both s2 and l8 are present, keep s2

        sarea_df = df_filtered[['corrected_area_cordeiro']].rename({'corrected_area_cordeiro': 'area'}, axis=1).dropna()
        sar = sar.rename({'sarea': 'area'}, axis=1)

        # Apply the trend based corrections
        result = trend_based_correction(sarea_df.copy(), sar.copy(), self.AREA_DEVIATION_THRESHOLD)

        return result


def deviation_from_sar(optical_areas, sar_areas, DEVIATION_THRESHOLD = 20, LOW_STD_LIM=2, HIGH_STD_LIM=2):
    """Filter out points based on deviations from SAR reported areas after correcting for bias in SAR water areas. Remove NaNs beforehand.

    Args:
        optical_areas (pd.Series): Time-series of areas obtained using an optical sensor (S2, L8, etc) on which the filtering will be applied. Must have `pd.DatetimeIndex` and corresponding areas in a column named `area`.
        sar_areas (pd.Series): Time-series of S1 surface areas. Must have `pd.DatetimeIndex` and corresponding areas in a column named `area`.
        DEVIATION_THRESHOLD (number): (Default: 20 [sq. km.]) Theshold of deviation from bias corrected SAR reported 
        LOW_STD_LIM (number): (Default: 2) Lower limit of standard deviations to use for clipping the deviations, required for calculating the bias.
        HIGH_STD_LIM (number): (Default: 2) Upper limit of standard deviations to use for clipping the deviations, required for calculating the bias.
    """
    # convert to dataframes under the hood
    optical_areas = optical_areas.to_frame()
    sar_areas = sar_areas.to_frame()

    xs = sar_areas.index.view(np.int64)//10**9  # Convert datetime to seconds from epoch
    ys = sar_areas['area']
    sar_area_func = interp1d(xs, ys, bounds_error=False)
    
    # Interpolate and calculate the sar reported areas according to the optical sensor's observation dates
    sar_sarea_interpolated = sar_area_func(optical_areas.index.view(np.int64)//10**9)
    deviations = optical_areas['area'] - sar_sarea_interpolated

    clipped = sigmaclip(deviations.dropna(), low=LOW_STD_LIM, high=HIGH_STD_LIM)
    bias = np.median(clipped.clipped)

    optical_areas['normalized_dev'] = deviations - bias
    optical_areas['flagged'] = False
    optical_areas.loc[np.abs(optical_areas['normalized_dev']) > DEVIATION_THRESHOLD, 'flagged'] = True
    optical_areas.loc[optical_areas['flagged'], 'area'] = np.nan

    return optical_areas['area']

def sar_trend(d1, d2, sar):
    subset = sar['area'].resample('1D').interpolate('linear')
    subset = subset.loc[d1:d2]
    if len(subset) == 0:
        trend = np.nan
    else:
        trend = (subset.iloc[-1]-subset.iloc[0])/((np.datetime64(d2)-np.datetime64(d1))/np.timedelta64(1, 'D'))
    
    return trend

def backcalculate(areas, trends, who_needs_correcting):
    # identify the first reliable point
    unreliable_pts_at_the_beginning = len(who_needs_correcting[:who_needs_correcting.idxmin()])-1
    corrected_areas = [np.nan] * unreliable_pts_at_the_beginning
    corrected_areas.append(areas.iloc[unreliable_pts_at_the_beginning+1])

    # # calculate previous points
    # for area, correction_required, trend in zip(areas[unreliable_pts_at_the_beginning::-1], who_needs_correcting[unreliable_pts_at_the_beginning::-1], trends[unreliable_pts_at_the_beginning::-1]):
    #     print(area, correction_required, trend)
    
    for area, correction_required, trend in zip(areas[unreliable_pts_at_the_beginning+1:], who_needs_correcting[unreliable_pts_at_the_beginning+1:], trends[unreliable_pts_at_the_beginning+1:]):
        if not correction_required:
            corrected_areas.append(area)
        else:
            corrected_area = corrected_areas[-1] + trend
            corrected_areas.append(corrected_area)
    
    return corrected_areas

def deviation_correction(area, DEVIATION_THRESHOLD, AREA_COL_NAME='area'):
    inner_area = area.copy()

    inner_area.loc[:, 'deviation'] = np.abs(inner_area['trend']-inner_area['sar_trend'])

    inner_area.loc[:, 'erroneous'] = inner_area['deviation'] > DEVIATION_THRESHOLD

    inner_area.loc[:, 'corrected_trend'] = inner_area['trend']
    inner_area.loc[inner_area['erroneous'], 'corrected_trend'] = inner_area['sar_trend']

    areas = backcalculate(inner_area[AREA_COL_NAME], inner_area['corrected_trend'], inner_area['erroneous'])
    inner_area[AREA_COL_NAME] = areas

    return inner_area

def sign_based_correction(area, AREA_COL_NAME='corrected_areas_1', TREND_COL_NAME='corrected_trend_1'):
    inner_area = area.copy()
    inner_area['sign_based_correction_reqd'] = (inner_area['trend']<0)&(inner_area['sar_trend']>0)|(inner_area['trend']>0)&(inner_area['sar_trend']<0)
    inner_area.loc[:, 'corrected_trend'] = inner_area[TREND_COL_NAME]
    inner_area.loc[inner_area['sign_based_correction_reqd'], 'corrected_trend'] = inner_area['sar_trend']

    inner_area['area'] = backcalculate(inner_area[AREA_COL_NAME], inner_area['corrected_trend'], inner_area['sign_based_correction_reqd'])

    return inner_area

def filled_by_trend(filtered_area, sar_trend, days_passed) -> pd.Series:
    """Fills in `np.nan` values of optically obtained surface area time series using SAR based time-series.

    Args:
        filtered_area (pd.Series): Optical sensor based surface areas containing `np.nan` values that will be filled in.
        sar_trend (pd.Series): SAR based surface area trends.
        days_passed (pd.Series): Days sicne last observation of optical sensor observed surface areas.

    Returns:
        pd.Series: Filled nan values
    """
    filled = [filtered_area.iloc[0]]
    for i in range(1, len(filtered_area)):
        if np.isnan(filtered_area.iloc[i]):
            a = filled[-1] + sar_trend.iloc[i] * days_passed.iloc[i]
            filled.append(a)
        else:
            filled.append(filtered_area.iloc[i])
    
    return pd.Series(filled, dtype=float, name='filled_area', index=filtered_area.index)

# Trend based correction function
def trend_based_correction(area, sar, AREA_DEVIATION_THRESHOLD=25, TREND_DEVIATION_THRESHOLD = 10):
    """Apply trend based correction on a time-series

    Args:
        area (pd.DataFrame): Pandas dataframe containing date as pd.DatetimeIndex and areas in column named `area`
        sar (pd.DataFrame): Pandas dataframe containing surface area time-series obtained from Sentinel-1 (SAR). Same format as `area`
        AREA_DEVIATION_THRESHOLD (number): (Default: 25) Threshold value of deviation of optically derived areas from SAR derived areas fro filtering.
        TREND_DEVIATION_THRESHOLD (number): (Default: 10) Threshold value of deviation in trend above which the observation is marked as erroneous and the correction step is applied
    """

    area['filtered_area'] = deviation_from_sar(area['area'], sar['area'], AREA_DEVIATION_THRESHOLD)
    area.rename({'area': 'unfiltered_area'}, axis=1, inplace=True)
    # area.rename({'filtered_area': 'area'}, axis=1, inplace=True)
    
    area_filtered = area.dropna(subset=['filtered_area'])

    area_filtered.loc[:, 'days_passed'] = area_filtered.index.to_series().diff().dt.days
    area_filtered.loc[:, 'trend'] = area_filtered['filtered_area'].diff()/area_filtered['days_passed']

    sar, area_filtered = clip_ts(sar, area_filtered)
    # sometimes the sar time-series has duplicate values which have to be removed
    sar = sar[~sar.index.duplicated(keep='first')]   # https://stackoverflow.com/a/34297689/4091712
    trend_generator = lambda arg: sar_trend(arg.index[0], arg.index[-1], sar)

    area_filtered.loc[:, 'sar_trend'] = area_filtered['filtered_area'].rolling(2).apply(trend_generator)

    deviation_correction_results = deviation_correction(area_filtered, TREND_DEVIATION_THRESHOLD, AREA_COL_NAME='filtered_area')
    area_filtered['corrected_areas_1'] = deviation_correction_results['filtered_area']
    area_filtered['corrected_trend_1'] = deviation_correction_results['corrected_trend']
    
    area['corrected_areas_1'] = area_filtered['corrected_areas_1']
    area['corrected_trend_1'] = area_filtered['corrected_trend_1']

    area.loc[:, 'sar_trend'] = area['unfiltered_area'].rolling(2).apply(trend_generator)
    area.loc[:, 'days_passed'] = area.index.to_series().diff().dt.days
    
    area, sar = clip_ts(area, sar)
    first_non_nan = area['corrected_areas_1'].first_valid_index()
    area = area.loc[first_non_nan:, :]

    # fill na based on trends
    area['filled_area'] = filled_by_trend(area['corrected_areas_1'], area['sar_trend'], area['days_passed'])

    return area
