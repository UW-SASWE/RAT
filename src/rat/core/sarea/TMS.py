import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
from scipy.stats import sigmaclip, zscore
import warnings
import os
from scipy.signal import savgol_filter
warnings.filterwarnings('ignore')

from rat.utils.utils import clip_ts
from rat.utils.utils import weighted_moving_average

class TMS():
    def __init__(self, reservoir_name, area=None, AREA_DEVIATION_THRESHOLD_PCNT=5):
        """_summary_
        Args:
            reservoir_name (_type_): _description_
            area (_type_, optional): _description_
            AREA_DEVIATION_THRESHOLD_PCNT (float, optional): _description_. Defaults to 25% for area<10 sq. km,  10% for area<100 sq. km, and 5% otherwise.
        Raises:
            Exception: _description_
        Returns:
            _type_: _description_
        """
        self.reservoir_name = reservoir_name
        self.area = area
        if self.area < 100:
            AREA_DEVIATION_THRESHOLD_PCNT=10
        elif self.area < 10:
            AREA_DEVIATION_THRESHOLD_PCNT=25

        self.AREA_DEVIATION_THRESHOLD = self.area * AREA_DEVIATION_THRESHOLD_PCNT/100

    def tms_os(self,
            l8_dfpath: str = "", 
            s2_dfpath: str = "", 
            l9_dfpath: str = "", 
            s1_dfpath: str = "", 
            CLOUD_THRESHOLD: float = 90.0,
            MIN_DATE: str = '2019-01-01'
        ):
        ## TODO: add conditional, S1 required, any one of optical datasets required
        """Implements the TMS-OS methodology
        Args:
            l8_dfpath (string): Path of the surface area dataframe obtained using `sarea_cli_l8.py` - Landsat derived surface areas
            s2_dfpath (string): Path of the surface area dataframe obtained using `sarea_cli_s2.py` - Sentinel-2 derived surface areas
            s1_dfpath (string): Path of the surface area dataframe obtained using `sarea_cli_sar.py` - Sentinel-1  derived surface areas
            CLOUD_THRESHOLD (float): Threshold to use for cloud-masking in % (default: 90.0)
            MIN_DATE (str): Minimum date for which data to keep for all the datasets in YYYY-MM-DD or %Y-%m-%d format (default: 2019-01-01)
        """
        MIN_DATE = pd.to_datetime(MIN_DATE, format='%Y-%m-%d')
        S2_TEMPORAL_RESOLUTION = 5
        S1_TEMPORAL_RESOLUTION = 12
        L8_TEMPORAL_RESOLUTION = 16
        L9_TEMPORAL_RESOLUTION = 16

        TO_MERGE = []

        if os.path.isfile(l8_dfpath):
            # Read in Landsat-8
            l8df = pd.read_csv(l8_dfpath, parse_dates=['mosaic_enddate']).rename({
                'mosaic_enddate': 'date',
                'water_area_cordeiro': 'water_area_uncorrected',
                'non_water_area_cordeiro': 'non_water_area', 
                'corrected_area_cordeiro': 'water_area_corrected'
                }, axis=1).set_index('date')
            l8df = l8df[['water_area_uncorrected', 'non_water_area', 'cloud_area', 'water_area_corrected']]
            l8df['cloud_percent'] = l8df['cloud_area']*100/(l8df['water_area_uncorrected']+l8df['non_water_area']+l8df['cloud_area'])
            l8df.replace(-1, np.nan, inplace=True)

            # QUALITY_DESCRIPTION
            #   0: Good, not interpolated either due to missing data or high clouds
            #   1: Poor, interpolated either due to high clouds
            #   2: Poor, interpolated either due to missing data
            l8df.loc[:, "QUALITY_DESCRIPTION"] = 0
            l8df.loc[l8df['cloud_percent']>=CLOUD_THRESHOLD, ("water_area_uncorrected", "non_water_area", "water_area_corrected")] = np.nan
            l8df.loc[l8df['cloud_percent']>=CLOUD_THRESHOLD, "QUALITY_DESCRIPTION"] = 1

            # in some cases l8df may have duplicated rows (with same values) that have to be removed
            if l8df.index.duplicated().sum() > 0:
                print("Duplicated labels, deleting")
                l8df = l8df[~l8df.index.duplicated(keep='last')]

            # Fill in the gaps in l8df created due to high cloud cover with np.nan values
            l8df_interpolated = l8df.reindex(pd.date_range(l8df.index[0], l8df.index[-1], freq=f'{L8_TEMPORAL_RESOLUTION}D'))
            l8df_interpolated.loc[np.isnan(l8df_interpolated["QUALITY_DESCRIPTION"]), "QUALITY_DESCRIPTION"] = 2
            l8df_interpolated.loc[np.isnan(l8df_interpolated['cloud_area']), 'cloud_area'] = max(l8df['cloud_area'])
            l8df_interpolated.loc[np.isnan(l8df_interpolated['cloud_percent']), 'cloud_percent'] = 100
            l8df_interpolated.loc[np.isnan(l8df_interpolated['non_water_area']), 'non_water_area'] = 0
            l8df_interpolated.loc[np.isnan(l8df_interpolated['water_area_uncorrected']), 'water_area_uncorrected'] = 0

            # Interpolate bad data
            l8df_interpolated.loc[:, "water_area_corrected"] = l8df_interpolated.loc[:, "water_area_corrected"].interpolate(method="linear", limit_direction="forward")
            l8df_interpolated['sat'] = 'l8'

            TO_MERGE.append(l8df_interpolated)


        # Read in Landsat-9
        if os.path.isfile(l9_dfpath):
            l9df = pd.read_csv(l9_dfpath, parse_dates=['mosaic_enddate']).rename({
                'mosaic_enddate': 'date',
                'water_area_cordeiro': 'water_area_uncorrected',
                'non_water_area_cordeiro': 'non_water_area', 
                'corrected_area_cordeiro': 'water_area_corrected'
                }, axis=1).set_index('date')
            l9df = l9df[['water_area_uncorrected', 'non_water_area', 'cloud_area', 'water_area_corrected']]
            l9df['cloud_percent'] = l9df['cloud_area']*100/(l9df['water_area_uncorrected']+l9df['non_water_area']+l9df['cloud_area'])
            l9df.replace(-1, np.nan, inplace=True)

            # QUALITY_DESCRIPTION
            #   0: Good, not interpolated either due to missing data or high clouds
            #   1: Poor, interpolated either due to high clouds
            #   2: Poor, interpolated either due to missing data
            l9df.loc[:, "QUALITY_DESCRIPTION"] = 0
            l9df.loc[l9df['cloud_percent']>=CLOUD_THRESHOLD, ("water_area_uncorrected", "non_water_area", "water_area_corrected")] = np.nan
            l9df.loc[l9df['cloud_percent']>=CLOUD_THRESHOLD, "QUALITY_DESCRIPTION"] = 1

            # in some cases l9df may have duplicated rows (with same values) that have to be removed
            if l9df.index.duplicated().sum() > 0:
                print("Duplicated labels, deleting")
                l9df = l9df[~l9df.index.duplicated(keep='last')]

            # Fill in the gaps in l9df created due to high cloud cover with np.nan values
            l9df_interpolated = l9df.reindex(pd.date_range(l9df.index[0], l9df.index[-1], freq=f'{L9_TEMPORAL_RESOLUTION}D'))
            l9df_interpolated.loc[np.isnan(l9df_interpolated["QUALITY_DESCRIPTION"]), "QUALITY_DESCRIPTION"] = 2
            l9df_interpolated.loc[np.isnan(l9df_interpolated['cloud_area']), 'cloud_area'] = max(l9df['cloud_area'])
            l9df_interpolated.loc[np.isnan(l9df_interpolated['cloud_percent']), 'cloud_percent'] = 100
            l9df_interpolated.loc[np.isnan(l9df_interpolated['non_water_area']), 'non_water_area'] = 0
            l9df_interpolated.loc[np.isnan(l9df_interpolated['water_area_uncorrected']), 'water_area_uncorrected'] = 0

            # Interpolate bad data
            l9df_interpolated.loc[:, "water_area_corrected"] = l8df_interpolated.loc[:, "water_area_corrected"].interpolate(method="linear", limit_direction="forward")
            l9df_interpolated['sat'] = 'l9'
            
            TO_MERGE.append(l9df_interpolated)

        if os.path.isfile(s2_dfpath):
            # Read in Sentinel-2 data
            s2df = pd.read_csv(s2_dfpath, parse_dates=['date']).set_index('date')
            s2df = s2df[['water_area_uncorrected', 'non_water_area', 'cloud_area', 'water_area_corrected']]
            s2df['cloud_percent'] = s2df['cloud_area']*100/(s2df['water_area_uncorrected']+s2df['non_water_area']+s2df['cloud_area'])
            s2df.replace(-1, np.nan, inplace=True)
            s2df.loc[s2df['cloud_percent']>=CLOUD_THRESHOLD, ("water_area_uncorrected", "non_water_area", "water_area_corrected")] = np.nan

            # QUALITY_DESCRIPTION
            #   0: Good, not interpolated either due to missing data or high clouds
            #   1: Poor, interpolated either due to high clouds
            #   2: Poor, interpolated either due to missing data
            s2df.loc[:, "QUALITY_DESCRIPTION"] = 0
            s2df.loc[s2df['cloud_percent']>=CLOUD_THRESHOLD, "QUALITY_DESCRIPTION"] = 1

            # in some cases s2df may have duplicated rows (with same values) that have to be removed
            if s2df.index.duplicated().sum() > 0:
                print("Duplicated labels, deleting")
                s2df = s2df[~s2df.index.duplicated(keep='last')]

            # Fill in the gaps in s2df created due to high cloud cover with np.nan values
            s2df_interpolated = s2df.reindex(pd.date_range(s2df.index[0], s2df.index[-1], freq=f'{S2_TEMPORAL_RESOLUTION}D'))
            s2df_interpolated.loc[np.isnan(s2df_interpolated["QUALITY_DESCRIPTION"]), "QUALITY_DESCRIPTION"] = 2
            s2df_interpolated.loc[np.isnan(s2df_interpolated['cloud_area']), 'cloud_area'] = max(s2df['cloud_area'])
            s2df_interpolated.loc[np.isnan(s2df_interpolated['cloud_percent']), 'cloud_percent'] = 100
            s2df_interpolated.loc[np.isnan(s2df_interpolated['non_water_area']), 'non_water_area'] = 0
            s2df_interpolated.loc[np.isnan(s2df_interpolated['water_area_uncorrected']), 'water_area_uncorrected'] = 0

            # Interpolate bad data
            s2df_interpolated.loc[:, "water_area_corrected"] = s2df_interpolated.loc[:, "water_area_corrected"].interpolate(method="linear", limit_direction="forward")
            s2df_interpolated['sat'] = 's2'

            TO_MERGE.append(s2df_interpolated)

        # If SAR file exists  
        if os.path.isfile(s1_dfpath):
            # Read in Sentinel-1 data
            sar = pd.read_csv(s1_dfpath, parse_dates=['time']).rename({'time': 'date'}, axis=1)
            # If SAR has atleast 3 data points 
            if (len(sar) >=3):
                sar['date'] = sar['date'].apply(lambda d: np.datetime64(d.strftime('%Y-%m-%d')))
                sar.set_index('date', inplace=True)
                sar.sort_index(inplace=True)

                # apply weekly area change filter
                sar = sar_data_statistical_fix(sar, self.area, 15)

                std = zscore(sar['sarea'])
                SAR_ZSCORE_LIM = 3
                sar.loc[(std > SAR_ZSCORE_LIM) | (std < -SAR_ZSCORE_LIM), 'sarea'] = np.nan
                sar['sarea'] = sar['sarea'].interpolate()
                sar = sar.loc[MIN_DATE:, :]

                # in some cases s2df may have duplicated rows (with same values) that have to be removed
                if sar.index.duplicated().sum() > 0:
                    print("Duplicated labels, deleting")
                    sar = sar[~sar.index.duplicated(keep='last')]

                # extrapolate data by 12 days (S1_TEMPORAL_RESOLUTION)
                extrapolated_date = sar.index[-1] + pd.DateOffset(S1_TEMPORAL_RESOLUTION)

                from scipy.interpolate import interp1d

                in_unix_time = lambda x: (x - pd.Timestamp("1970-01-01"))//pd.Timedelta('1s')

                extrapolated_value = interp1d(in_unix_time(sar.index[-7:]), sar['sarea'][-7:], kind='linear', fill_value="extrapolate")(in_unix_time(extrapolated_date))

                sar.loc[extrapolated_date, "sarea"] = extrapolated_value

                sar = sar.rename({'sarea': 'area'}, axis=1)
            # If SAR has less than 3 points
            else:
                sar = None
                print("Sentinel-1 SAR has less than 3 data points.")
        # If SAR file does not exist
        else:
            sar = None
            print("Sentinel-1 SAR file does not exist.")
        # combine opticals into one dataframes
        
        optical = pd.concat(TO_MERGE).sort_index()
        optical = optical.loc[~optical.index.duplicated(keep='last')] # when both s2 and l8 are present, keep s2
        optical.rename({'water_area_corrected': 'area'}, axis=1, inplace=True)


        # Apply the trend based corrections
        if(sar is not None):
            # If Optical begins before SAR and has a difference of more than 15 days
            if(sar.index[0]-optical.index[0]>pd.Timedelta(days=15)):
                # Optical without SAR
                optical_with_no_sar = optical[optical.index[0]:sar.index[0]].copy()
                optical_with_no_sar['non-smoothened optical area'] = optical_with_no_sar['area']
                optical_with_no_sar.loc[:, 'days_passed'] = optical.index.to_series().diff().dt.days.fillna(0)
                # Calculate smoothed values with moving weighted average method if more than 7 values; weights are calculated using cloud percent.
                if len(optical_with_no_sar)>7:
                    optical_with_no_sar['filled_area'] = weighted_moving_average(optical_with_no_sar['non-smoothened optical area'], weights = (101-optical_with_no_sar['cloud_percent']),window_size=3)
                # Drop 'area' column from optical_with_no_sar
                optical_with_no_sar = optical_with_no_sar.drop('area',axis=1)
                # Optical with SAR
                optical_with_sar = trend_based_correction(optical.copy(), sar.copy(), self.AREA_DEVIATION_THRESHOLD)
                # Merge both
                result = pd.concat([optical_with_no_sar,optical_with_sar],axis=0)
                # Smoothen the combined surface area estimates to avoid noise or peaks using savgol_filter if more than 9 values (to increase smoothness and include more points as we have both TMS-OS and Optical)
                if len(result)>9:    
                    result['filled_area'] = savgol_filter(result['filled_area'], window_length=7, polyorder=3)
                method = 'Combine'
            # If SAR begins before Optical
            else:
                result = trend_based_correction(optical.copy(), sar.copy(), self.AREA_DEVIATION_THRESHOLD)
                method = 'TMS-OS'
        else:
            result = optical.copy()
            result['non-smoothened optical area'] = result['area']
            result.loc[:, 'days_passed'] = optical.index.to_series().diff().dt.days.fillna(0)
            # Calculate smoothed values with Savitzky-Golay method if more than 7 values
            if len(result)>7:
                result['filled_area'] = weighted_moving_average(result['non-smoothened optical area'], weights = (101-result['cloud_percent']),window_size=3)
                result['filled_area'] = savgol_filter(result['filled_area'], window_length=7, polyorder=3)
            method = 'Optical'
        # Returning method used for surface area estimation
        return result,method

def area_change(df, date, n=14):
    """calculate the change in area in last n days"""
    start = date - pd.Timedelta(days=n)
    end = date

    # if start date is before the first date in the df, return nan
    if start < df.index[0]:
        return np.nan

    start_area = df.loc[start:end, "sarea"].iloc[0]
    end_area = df.loc[date, "sarea"]
    try:  # if end_area is a series which may happen in a SAR area dataframe (TODO: fix the cause of this issue, same area is returned twice), take the first value
        end_area = end_area.iloc[0]
    except AttributeError as AE:
        pass
    except Exception as E:
        raise E

    return end_area - start_area
    

def sar_data_statistical_fix(sar_df, nominal_area, threshold_percentage=15):
    """fix the sar data using statistical method"""
    threshold = (threshold_percentage / 100) * nominal_area

    sar_df_copy = sar_df.copy()
    sar_df_copy['date'] = sar_df_copy.index.to_series()

    sar_df_copy['area_change'] = sar_df_copy['date'].apply(lambda x: area_change(sar_df_copy, x))

    sar_df_copy.loc[(sar_df_copy['area_change'] < -threshold)|(sar_df_copy['area_change'] > threshold), 'sarea'] = np.nan
    sar_df_copy['sarea'] = sar_df_copy['sarea'].interpolate(method='time')

    return sar_df_copy.drop('area_change', axis=1).drop('date', axis=1)

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

# helper functions
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
    if(not inner_area['erroneous'].empty):
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
    
    sar, area_filtered = clip_ts(sar, area_filtered, which='left')
    # sometimes the sar time-series has duplicate values which have to be removed
    sar = sar[~sar.index.duplicated(keep='first')]   # https://stackoverflow.com/a/34297689/4091712
    trend_generator = lambda arg: sar_trend(arg.index[0], arg.index[-1], sar)

    area_filtered.loc[:, 'sar_trend'] = area_filtered['filtered_area'].rolling(2, min_periods=0).apply(trend_generator)

    deviation_correction_results = deviation_correction(area_filtered, TREND_DEVIATION_THRESHOLD, AREA_COL_NAME='filtered_area')
    area_filtered['corrected_areas_1'] = deviation_correction_results['filtered_area']
    area_filtered['corrected_trend_1'] = deviation_correction_results['corrected_trend']
    
    area['corrected_areas_1'] = area_filtered['corrected_areas_1']
    area['corrected_trend_1'] = area_filtered['corrected_trend_1']

    area.loc[:, 'sar_trend'] = area['unfiltered_area'].rolling(2, min_periods=0).apply(trend_generator)
    area.loc[:, 'days_passed'] = area.index.to_series().diff().dt.days
    
    area, sar = clip_ts(area, sar, which="left")
    first_non_nan = area['corrected_areas_1'].first_valid_index()
    area = area.loc[first_non_nan:, :]

    # fill na based on trends
    area['filled_area'] = filled_by_trend(area['corrected_areas_1'], area['sar_trend'], area['days_passed'])

    return area