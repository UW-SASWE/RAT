import pandas as pd
import numpy as np
import os
import geopandas as gpd
from logging import getLogger
from rat.utils.logging import LOG_NAME, LOG_LEVEL1_NAME

log = getLogger(f"{LOG_NAME}.{__name__}")
log_level1 = getLogger(f"{LOG_LEVEL1_NAME}.{__name__}")

def bot_filter(tmsos_gee_path,shpfile_column_dict,basin_reservoir_shpfile_path,apply,bias_threshold,outlier_threshold,trend_threshold):
    ''' 
    Applies the BOT Filter to the surface area time series,
    which filters the optical derived surface area with SAR surface area as the reference.
    
    Parameters
    ----------
    tmsos_gee_path: str
        The path to the tmsos surface area folder within the basin data directory
    shpfile_column_dict: dict
        A dictionary containing column names of unique identifier for the reservoir name and area
    basin_reservoir_shpfile_path : str
        The path to the file containing reservoir shapefile data
    apply: boolean
        Toggles whether the BOT filter is run. It is also used to reset the BOT Filtered surface area values to TMSOS if required (set to False).
        Values: True/False
    bias_threshold: float
        Controls the filtering intensity for the bias between Optical and SAR surface areas.
        Values: 0 - 9
    outlier_threshold: float
        Controls the intensity of filtering out the outliers from the Optical surface area data.
        Values: 0 - 9
    trend_threshold: float
        Controls the intensity of filtering the Optical surface area data based on the deviation in trends from the SAR surface area data.
        Values: 0 - 9
    '''
   
    #  Reading the reservoir shapefile data
    basin_reservoir_data = gpd.read_file(basin_reservoir_shpfile_path)
    
    #Check if BOT filter 'apply' is set to True.
    if(apply == True):
        log_level1.info('Running BOT filter for Surface Area')
        res_nomArea = basin_reservoir_data["AREA_SKM"]
        # Scaling filtering thresholds to actual filtering values. If threshold value is 0, they are assinged an extremely high value to essentially turn it off.
        # sigma from monthly mean; Mapped from (1-9 to 4-0.5sigma)
        filt_1_thresh = (9999 if(outlier_threshold==0) else ((3.5/8 * (9 - outlier_threshold)) + 0.5)) 
        # % of nom surface area deviation from SAR; Mapped from (1-9 to 20-5% )
        filt_2_thresh = (9999 if(bias_threshold==0) else ((15/8 * (9 - bias_threshold)) + 5)) 
        # monthly sigma deviation from SAR trend(avg of prev 2 sar trends); Mapped from (1-9 to 3-0.1sigma)
        filt_3_thresh = (9999 if(trend_threshold==0) else ((2.9/8 * (9 - trend_threshold)) + 0.1)) 
        # Initialising counter for failures
        count_failed = 0
        botFilter_status = pd.DataFrame(columns=['Reservoir', 'Status'])

       
        for curr_dam_index,reservoir in basin_reservoir_data.iterrows():   
            try:
                res_name = str(reservoir[shpfile_column_dict['unique_identifier']]).replace(" ","_")
                res_area = float(reservoir[shpfile_column_dict['area_column']])
                optical_count = 0
                sar_count = 0
                res_l8_fp = os.path.join(os.path.join(tmsos_gee_path,'l8',res_name +'.csv'))
                res_s2_fp = os.path.join(tmsos_gee_path,'s2',res_name +'.csv')
                res_l9_fp = os.path.join(tmsos_gee_path,'l9',res_name +'.csv')
                res_s1_fp = os.path.join(tmsos_gee_path,'sar',res_name +'_12d_sar.csv')
                
                #Checking if atleast 1 optical and 1 sar dataset is available to run BOT filter
                # If so,optical data is preprocessed and merged for further filtering
                res_wa_1to5day = pd.DataFrame()
                if os.path.isfile(res_l8_fp):
                    optical_count = optical_count + 1
                    res_wa_timeseries_l8 =  pd.read_csv(res_l8_fp)
                    res_wa_timeseries_l8['area'] = res_wa_timeseries_l8['corrected_area_cordeiro']
                    res_wa_timeseries_l8 = res_wa_timeseries_l8.fillna(0)
                    res_wa_timeseries_l8['time'] = pd.to_datetime(res_wa_timeseries_l8['mosaic_enddate'])
                    res_wa_timeseries_l8['date'] = res_wa_timeseries_l8['time'].dt.date
                    res_wa_1to5day = pd.concat([res_wa_1to5day,res_wa_timeseries_l8])
                    
                if os.path.isfile(res_s2_fp):
                    optical_count = optical_count + 1
                    res_wa_timeseries_s2 =  pd.read_csv(res_s2_fp)
                    res_wa_timeseries_s2['area'] = res_wa_timeseries_s2['water_area_corrected']
                    res_wa_timeseries_s2 = res_wa_timeseries_s2.fillna(0)
                    res_wa_timeseries_s2['time'] = pd.to_datetime(res_wa_timeseries_s2['date'])
                    res_wa_timeseries_s2['date'] = res_wa_timeseries_s2['time'].dt.date
                    res_wa_1to5day = pd.concat([res_wa_1to5day,res_wa_timeseries_s2])
                if os.path.isfile(res_l9_fp):
                    optical_count = optical_count + 1
                    res_wa_timeseries_l9 =  pd.read_csv(res_l9_fp)
                    res_wa_timeseries_l9['area'] = res_wa_timeseries_l9['corrected_area_cordeiro']
                    res_wa_timeseries_l9 = res_wa_timeseries_l9.fillna(0)
                    res_wa_timeseries_l9['time'] = pd.to_datetime(res_wa_timeseries_l9['mosaic_enddate'])
                    res_wa_timeseries_l9['date'] = res_wa_timeseries_l9['time'].dt.date
                    res_wa_1to5day = pd.concat([res_wa_1to5day,res_wa_timeseries_l9])
                if os.path.isfile(res_s1_fp):
                    sar_count = sar_count + 1
                    res_wa_timeseries_s1 =  pd.read_csv(res_s1_fp)
                    res_wa_timeseries_s1['time'] = pd.to_datetime(res_wa_timeseries_s1['time'])
                    res_wa_timeseries_s1['date'] = res_wa_timeseries_s1['time'].dt.date
                    
                if(optical_count == 0 and sar_count == 0):
                    raise Exception(f'Not enough optical and sar datasets available. Need atleast 1 of each')
                elif(sar_count == 0):
                    raise Exception(f'Not enough sar datasets available. Need atleast 1')
                elif(optical_count == 0):
                    raise Exception(f'Not enough optical datasets available. Need atleast 1')
 
                # Sorting data by date and creating final area field
                res_wa_1to5day = res_wa_1to5day.sort_values('date')
                res_wa_1to5day['WA_mean'] = res_wa_1to5day['area']

                #Setting date as index and creating copies for further manipulation
                pdf1 = res_wa_1to5day.copy()
                pdf2 = res_wa_timeseries_s1.copy()
                pdf1 = pdf1.set_index('date')
                pdf2 = pdf2.set_index('date')

                #Dropping duplicates and reindexing the Sentinel-2 dataset to match the frequency of the 1-5day Optical dataset.
                pdf1['date_integer'] = pdf1.index
                pdf1['date_integer'] = pd.DatetimeIndex(pdf1.index).strftime('%Y%m%d').astype(int)
                pdf2['date_integer'] = pdf2.index
                pdf2['date_integer'] = pd.DatetimeIndex(pdf2.index).strftime('%Y%m%d').astype(int)
                pdf1 = pdf1.drop_duplicates(subset = ['date_integer'], keep='first')
                pdf2 = pdf2.drop_duplicates(subset = ['date_integer'], keep='first')
                pdf2 = pdf2.reindex(pdf1.index)
                pdf2['water_area'] = pdf2['sarea'].interpolate()

                pdf2.drop('time', axis = 1, inplace = True)
                pdf2['date_integer'] = pd.DatetimeIndex(pdf2.index).strftime('%Y%m%d').astype(int)

                #pre-processing for Filter-1.
                merged_pdf = pdf2.copy()
                merged_pdf = merged_pdf.rename(columns = {'water_area':'WA_SAR'})
                merged_pdf['WA_Optical'] = pdf1['WA_mean']
                

                ## Filtering - Step 1 -- Removing outliers by considering monthly mean
                merged_pdf_2 = merged_pdf.copy()
                merged_pdf_2['Date'] = pd.to_datetime(merged_pdf_2.index)
                merged_pdf_2['YearMonth'] = merged_pdf_2['Date'].dt.year.astype(str) + merged_pdf_2['Date'].dt.month.astype(str)
                grouped_pdf = merged_pdf_2.groupby('YearMonth')

                monthly_mean_Optical = grouped_pdf['WA_Optical'].mean()
                monthly_std_Optical  = grouped_pdf['WA_Optical'].std()

                merged_pdf_2 = pd.merge(merged_pdf_2, monthly_mean_Optical, on='YearMonth', suffixes=('', '_monthly_mean'))
                merged_pdf_2 = pd.merge(merged_pdf_2, monthly_std_Optical, on='YearMonth', suffixes=('', '_monthly_std'))

                merged_pdf_2['WA_Optical_previous'] = merged_pdf_2['WA_Optical'].shift(1)  # Create a new column with the previous value of 'WA_Optical'

                for index, row in merged_pdf_2.iterrows():
                        monthly_mean_Optical = row['WA_Optical_monthly_mean']
                        monthly_std_Optical = row['WA_Optical_monthly_std']               
                        
                        if (row['WA_Optical'] < (monthly_mean_Optical - filt_1_thresh*monthly_std_Optical) or row['WA_Optical'] > (monthly_mean_Optical + filt_1_thresh*monthly_std_Optical)):
                            
                            merged_pdf_2.loc[index, 'WA_Optical'] = row['WA_Optical_previous']            
                        
                merged_pdf_2.set_index('Date', inplace = True)
                merged_pdf_remOutliers = merged_pdf_2.copy()
                merged_pdf_remOutliers['deviations'] = merged_pdf_remOutliers['WA_Optical'] - merged_pdf_remOutliers['WA_SAR']

                dev_bias = merged_pdf_remOutliers['deviations'].median()

                merged_pdf_remOutliers['norm_deviations'] = dev_bias = merged_pdf_remOutliers['deviations'] - dev_bias

                #Filtering 2 - SAR bias correction
                # res_nom_SA = res_nomArea[curr_dam_index] #km^2
                res_nom_SA = res_area
                cloud_thresh = -1 #%
                filt2_thresh_values = (-res_nom_SA*filt_2_thresh/100, res_nom_SA*filt_2_thresh/100)

                merged_pdf_filt2 = merged_pdf_remOutliers.copy()
                # startPoint = 10
                # merged_pdf_filt2 = merged_pdf_filt2[startPoint:]

                outliers = ((merged_pdf_filt2['deviations'] < filt2_thresh_values[0]) | (merged_pdf_filt2['deviations'] > filt2_thresh_values[1]))

                for i, val in merged_pdf_filt2.loc[outliers, 'deviations'].items():  
                    # mod_val = merged_pdf_filt2['WA_Optical'].loc[:i][-1] - val - res_nom_SA*filt2_thresh/100
                    
                    opt_val = merged_pdf_filt2['WA_Optical'].loc[:i][-1]    
                    sar_val = merged_pdf_filt2['WA_SAR'].loc[:i][-1]
                    opt_sar_dev = merged_pdf_filt2['deviations'].loc[:i][-1]

                    dev_sign = np.abs(merged_pdf_filt2['deviations'].loc[:i][-1]/merged_pdf_filt2['deviations'].loc[:i][-1])
                    mod_val = opt_val + dev_sign* res_nom_SA*filt_2_thresh/100 - opt_sar_dev
                    
                    merged_pdf_filt2.loc[i, 'WA_Optical'] = mod_val

                #Filtering 3 - SAR Trend correction
                merged_pdf_filt3 = merged_pdf_filt2.copy()
                merged_pdf_filt3['date_integer'] = merged_pdf_filt3.index
                merged_pdf_filt3['date_integer'] = pd.DatetimeIndex(merged_pdf_filt3.index).strftime('%Y%m%d').astype(int)
                merged_pdf_filt3['timeDiff'] = merged_pdf_filt3['date_integer'].diff()
                merged_pdf_filt3['SAR_diff'] = merged_pdf_filt3['WA_SAR'].diff()
                merged_pdf_filt3['SAR_diff'].fillna(0, inplace = True)
                merged_pdf_filt3 = merged_pdf_filt3.drop_duplicates(subset=['date_integer'], keep='first')
                merged_pdf_filt3['WA_Optical_cor'] = merged_pdf_filt3['WA_Optical']

                merged_pdf_filt3['SAR_trend'] = merged_pdf_filt3['WA_SAR'].pct_change()
                merged_pdf_filt3['WA_SAR_shift'] = merged_pdf_filt3['WA_SAR'].shift(1)
                merged_pdf_filt3['WA_Optical_cor_trend'] = merged_pdf_filt3['WA_Optical_cor'].pct_change()

                merged_pdf_filt3['SAR_trend_dev_Optical'] = np.abs(merged_pdf_filt3['WA_Optical_cor_trend'] - merged_pdf_filt3['SAR_trend'])
                grouped_pdf_2 = merged_pdf_filt3.groupby('YearMonth')
                monthly_mean_sar_trend_dev_optical = grouped_pdf_2['SAR_trend_dev_Optical'].mean()
                monthly_std_sar_trend_dev_optical = grouped_pdf_2['SAR_trend_dev_Optical'].std()

                merged_pdf_filt3['date'] = pd.to_datetime(merged_pdf_filt3.index)
                merged_pdf_filt3 = pd.merge(merged_pdf_filt3, monthly_mean_sar_trend_dev_optical, on='YearMonth', suffixes=('', '_monthly_mean'))
                merged_pdf_filt3 = pd.merge(merged_pdf_filt3, monthly_std_sar_trend_dev_optical, on='YearMonth', suffixes=('', '_monthly_std'))
                merged_pdf_filt3.set_index('date', inplace = True)

                for i, val in merged_pdf_filt3['SAR_trend'][3:].items():
                    sar_trend_weekly_av = (merged_pdf_filt3['SAR_trend'].loc[:i][-2] + merged_pdf_filt3['SAR_trend'].loc[:i][-3])/2
                    curr_sar_trend = merged_pdf_filt3['SAR_trend'].loc[:i][-1]
                    
                    curr_opt_trend = merged_pdf_filt3['WA_Optical_cor_trend'].loc[:i][-1]
                    curr_opt_trend_2 = (merged_pdf_filt3['WA_Optical_cor_trend'].loc[:i][-1] + merged_pdf_filt3['WA_Optical_cor_trend'].loc[:i][-2])/2
                    
                    # Checking if optical trend is significantly greater than sar trend 
                    if np.abs(curr_opt_trend_2 - curr_sar_trend) > filt_3_thresh* merged_pdf_filt3['SAR_trend_dev_Optical_monthly_std'].loc[:i][-1]:
                        curr = merged_pdf_filt3['WA_Optical_cor'].loc[:i][-1]
                        mod_val = (merged_pdf_filt3['WA_Optical_cor'].loc[:i][-2] + merged_pdf_filt3['WA_Optical_cor'].loc[:i][-3])/2*(1+sar_trend_weekly_av)
                        if pd.isna(mod_val):
                            mod_val = curr
                        merged_pdf_filt3['WA_Optical_cor'].loc[:i][-1] = mod_val
                        merged_pdf_filt3['WA_Optical_cor_trend'] =  merged_pdf_filt3['WA_Optical_cor'].pct_change()
                            
                merged_pdf_filt3['WA_mean_corr'] = merged_pdf_filt3['WA_Optical_cor']
                sa_save_path = os.path.join(tmsos_gee_path,res_name + '.csv')
                final_sa = pd.read_csv(sa_save_path)
                
                final_sa['date'] = pd.to_datetime(final_sa['date'])
                final_sa.set_index('date',inplace = True)
                if('area_tmsos' not in final_sa.columns):
                    final_sa['area_tmsos'] = final_sa['area']
                final_sa['area'] = merged_pdf_filt3['WA_mean_corr']            
                final_sa['days_passed'] = final_sa.index.to_series().diff().dt.days.fillna(0).astype(int)
                final_sa.to_csv(os.path.join(tmsos_gee_path,res_name + '.csv'))
                
                botFilter_status_new = pd.DataFrame({ 'Reservoir': f'{res_name}', 'Status': 1 }, index = [curr_dam_index])
                botFilter_status = pd.concat([botFilter_status, botFilter_status_new])
                
            except Exception as e:
                log.error(f'BOT Filter run failed for {res_name} due to error: {e}')
                count_failed = count_failed+1
                botFilter_status_new = pd.DataFrame({ 'Reservoir': f'{res_name}', 'Status': 0 }, index = [curr_dam_index])
                botFilter_status = pd.concat([botFilter_status, botFilter_status_new])
                
        if(count_failed > 0):    
            log_level1.info(f'BOT filter run finished with {count_failed} errors. Please check Level-2 log file for more details.')
        else:
            log_level1.info(f'BOT filter run completed succesfully')       
        log.info('BOT Filter status\n %s',botFilter_status)  
        
    else:
        # If Bot filter apply is set to False, reset the surface area to TMSOS.
        log.warning('BOT Filter toggled off. Surface Area Values will be reset to TMSOS')