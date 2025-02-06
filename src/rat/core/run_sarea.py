import os
import geopandas as gpd

from logging import getLogger
from rat.utils.logging import LOG_NAME, NOTIFICATION, LOG_LEVEL1_NAME
from rat.ee_utils.ee_utils import simplify_geometry

from rat.core.sarea.sarea_cli_s2 import sarea_s2
from rat.core.sarea.sarea_cli_l5 import sarea_l5
from rat.core.sarea.sarea_cli_l7 import sarea_l7
from rat.core.sarea.sarea_cli_l8 import sarea_l8
from rat.core.sarea.sarea_cli_l9 import sarea_l9
from rat.core.sarea.sarea_cli_sar import sarea_s1
from rat.core.sarea.bot_filter import bot_filter
from rat.core.sarea.TMS import TMS
from rat.core.sarea.multisensor_ssc_integrator import multi_sensor_ssc_integration, normalize_ssc


log = getLogger(f"{LOG_NAME}.{__name__}")
log_level1 = getLogger(f"{LOG_LEVEL1_NAME}.{__name__}")


def run_sarea(start_date, end_date, sarea_save_dir, reservoirs_shpfile, shpfile_column_dict, filt_options = None, nssc_save_dir = None):
    if isinstance(reservoirs_shpfile, gpd.GeoDataFrame):
        reservoirs_polygon = reservoirs_shpfile
    else:
        reservoirs_polygon = gpd.read_file(reservoirs_shpfile)
    no_failed_files = 0
    Optical_files = 0
    Tmsos_files = 0
    Partial_optical_tmsos_files = 0
    i = 1
    for reservoir_no,reservoir in reservoirs_polygon.iterrows():
        print(f"\n\n +++ PROCESSING RESERVOIR: {reservoir[shpfile_column_dict['id_column']]} - {reservoir[shpfile_column_dict['dam_name_column']]} ({i}/{len(reservoirs_polygon)}) +++\n\n")
        i += 1
        try:
            # Reading reservoir information
            reservoir_name = str(reservoir[shpfile_column_dict['unique_identifier']]).replace(" ","_")
            reservoir_area = float(reservoir[shpfile_column_dict['area_column']])
            reservoir_polygon = reservoir.geometry
            log.info(f"Calculating surface area for {reservoir_name}.")
            method = run_sarea_for_res(reservoir_name, reservoir_area, reservoir_polygon, start_date, end_date, sarea_save_dir, nssc_save_dir)
            log.info(f"Calculated surface area for {reservoir_name} successfully using {method} method.")
            if method == 'Optical':
                Optical_files += 1
            elif method == 'Combine':
                Partial_optical_tmsos_files +=1
            else:
                Tmsos_files += 1
        except:
            log.exception(f"Surface area calculation failed for {reservoir_name}.")
            no_failed_files += 1
    if no_failed_files:
        log_level1.warning(f"Surface area was not calculated for {no_failed_files} reservoirs.")     
    if Optical_files:
        log_level1.warning(f"Surface area was calculated using only Optical data and not TMS-OS for {Optical_files} reservoirs. It can be due to insufficient SAR data. Please refer level-2 log file for more details.")
    if Partial_optical_tmsos_files:
        log_level1.warning(f"Surface area was calculated partially using only Optical data and rest using TMS-OS for {Partial_optical_tmsos_files} reservoirs. It can be due to more Optical data than SAR data. Please refer level-2 log file for more details.")
        
    #Running Bot Filter
    if filt_options is not None:
        bot_thresholds = [filt_options['bias_threshold'],filt_options['outlier_threshold'],filt_options['trend_threshold']]
        if(None in bot_thresholds or min(bot_thresholds)<0 or max(bot_thresholds) > 9):
            if(filt_options['apply']==True):
                log_level1.error(f"BOT Filter run failed for all reservoirs.")
                log_level1.error("Filter values out of bounds. Please ensure that a value in between 0 and 9 is selected")
        else:
            bot_filter(sarea_save_dir,shpfile_column_dict,reservoirs_shpfile,**filt_options)    

def run_sarea_for_res(reservoir_name, reservoir_area, reservoir_polygon, start_date, end_date, sarea_save_dir, nssc_save_dir, simplication=True):
    
    if simplication:
        # Below function simplifies geometry with shape index (complexity) higher than a threshold, otherwise original geometry is retained
        reservoir_polygon = simplify_geometry(reservoir_polygon)
    
    # Obtain surface areas
    # Sentinel-2
    log.debug(f"Reservoir: {reservoir_name}; Downloading Sentinel-2 data from {start_date} to {end_date}")
    sarea_s2(reservoir_name, reservoir_polygon, start_date, end_date, os.path.join(sarea_save_dir, 's2'))
    s2_dfpath = os.path.join(sarea_save_dir, 's2', reservoir_name+'.csv')

    # Landsat-5
    log.debug(f"Reservoir: {reservoir_name}; Downloading Landsat-5 data from {start_date} to {end_date}")
    sarea_l5(reservoir_name, reservoir_polygon, start_date, end_date, os.path.join(sarea_save_dir, 'l5'))
    l5_dfpath = os.path.join(sarea_save_dir, 'l5', reservoir_name+'.csv')
    
    # Landsat-7
    log.debug(f"Reservoir: {reservoir_name}; Downloading Landsat-7 data from {start_date} to {end_date}")
    sarea_l7(reservoir_name, reservoir_polygon, start_date, end_date, os.path.join(sarea_save_dir, 'l7'))
    l7_dfpath = os.path.join(sarea_save_dir, 'l7', reservoir_name+'.csv')

    # Landsat-8
    log.debug(f"Reservoir: {reservoir_name}; Downloading Landsat-8 data from {start_date} to {end_date}")
    sarea_l8(reservoir_name, reservoir_polygon, start_date, end_date, os.path.join(sarea_save_dir, 'l8'))
    l8_dfpath = os.path.join(sarea_save_dir, 'l8', reservoir_name+'.csv')
    
    # Landsat-9
    log.debug(f"Reservoir: {reservoir_name}; Downloading Landsat-9 data from {start_date} to {end_date}")
    sarea_l9(reservoir_name, reservoir_polygon, start_date, end_date, os.path.join(sarea_save_dir, 'l9'))
    l9_dfpath = os.path.join(sarea_save_dir, 'l9', reservoir_name+'.csv')

    # Sentinel-1
    log.debug(f"Reservoir: {reservoir_name}; Downloading Sentinel-1 data from {start_date} to {end_date}")
    s1_dfpath = sarea_s1(reservoir_name, reservoir_polygon, start_date, end_date, os.path.join(sarea_save_dir, 'sar'))
    s1_dfpath = os.path.join(sarea_save_dir, 'sar', reservoir_name+'_12d_sar.csv')

    # Using TMSOS to ensemble surface area data
    tmsos = TMS(reservoir_name, reservoir_area)
    result,method = tmsos.tms_os(l5_dfpath=l5_dfpath, l7_dfpath=l7_dfpath, l9_dfpath=l9_dfpath, l8_dfpath=l8_dfpath,
                                  s2_dfpath=s2_dfpath, s1_dfpath=s1_dfpath)
    tmsos_savepath = os.path.join(sarea_save_dir, reservoir_name+'.csv')
    log.debug(f"Saving surface area of {reservoir_name} at {tmsos_savepath}")
    result.reset_index().rename({'index': 'date', 'filled_area': 'area'}, axis=1).to_csv(tmsos_savepath, index=False)
    
    # NSSC calculations
    if nssc_save_dir:
        ssc_components_df = multi_sensor_ssc_integration(l5_dfpath=l5_dfpath, l7_dfpath=l7_dfpath, l9_dfpath=l9_dfpath, l8_dfpath=l8_dfpath,
                                    s2_dfpath=s2_dfpath)
        nssc_df = normalize_ssc(ssc_components_df)
        nssc_savepath = os.path.join(nssc_save_dir, reservoir_name+'.csv')
        log.debug(f"Saving NSSC data of {reservoir_name} at {nssc_savepath}")
        nssc_df.to_csv(nssc_savepath, index=False)
    
    return method
