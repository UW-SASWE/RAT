import os
import geopandas as gpd

from logging import getLogger
from rat.utils.logging import LOG_NAME, NOTIFICATION, LOG_LEVEL1_NAME
from rat.utils.utils import create_directory
from rat.ee_utils.ee_utils import simplify_geometry

from rat.core.sarea.sarea_cli_s2 import sarea_s2
from rat.core.sarea.sarea_cli_l5 import sarea_l5
from rat.core.sarea.sarea_cli_l7 import sarea_l7
from rat.core.sarea.sarea_cli_l8 import sarea_l8
from rat.core.sarea.sarea_cli_l9 import sarea_l9
from rat.core.sarea.sarea_cli_sar import sarea_s1
import rat.core.sarea.sarea_elev_swot as sarea_swot
from rat.core.sarea.bot_filter import bot_filter
from rat.core.sarea.TMS import TMS
from rat.core.sarea.multisensor_ssc_integrator import multi_sensor_ssc_integration, normalize_ssc


log = getLogger(f"{LOG_NAME}.{__name__}")
log_level1 = getLogger(f"{LOG_LEVEL1_NAME}.{__name__}")


def run_sarea(start_date, end_date, sarea_save_dir, reservoirs_shpfile, shpfile_column_dict, basin_bounds, swot_run= False, swot_prior_lake_shpfile=None, swot_prior_lake_shpfile_column_dict=None, swot_save_dir= None, filt_options = None, nssc_save_dir = None):
    if isinstance(reservoirs_shpfile, gpd.GeoDataFrame):
        reservoirs_polygon = reservoirs_shpfile
    else:
        reservoirs_polygon = gpd.read_file(reservoirs_shpfile)
    
    ## Verifying if SWOT SA & Elevation needed
    pld_rat_matched_basin_gdf = None
    swot_id_column_dict = None
    if swot_run:
        log.info("Running SWOT Plugin:")
        if swot_prior_lake_shpfile and swot_prior_lake_shpfile_column_dict:
            if os.path.isfile(swot_prior_lake_shpfile):
                log.info("Reading SWOT Prior Lake Data (PLD) file ....")
                swot_pld_gdf = gpd.read_file(swot_prior_lake_shpfile, bbox=tuple(basin_bounds))
                pld_rat_matched_basin_gdf = sarea_swot.compute_swot_prior_lake_matching(rat_lakes = reservoirs_polygon,
                                            prior_lakes = swot_pld_gdf,
                                            rat_lake_id_field = shpfile_column_dict['unique_identifier'],
                                            prior_lake_id_field = swot_prior_lake_shpfile_column_dict['id_column'],
                                            drop=True)
                swot_id_column_dict = {'rat_lake_id' : shpfile_column_dict['unique_identifier'], 
                                       'prior_lake_id' : swot_prior_lake_shpfile_column_dict['id_column']}
                basin_swot_pld_dir = create_directory(os.path.join(swot_save_dir, 'basin_swot_prior_lakes'), True)
                basin_swot_pld_file = os.path.join(basin_swot_pld_dir, 'reservoirs_matched_prior_lakes.geojson')
                pld_rat_matched_basin_gdf.to_file(basin_swot_pld_file)
            else:
                log_level1.warning(f"Swot Prior Lake file was not found. Skipping extraction of Swot surface area and elevation time series.")
        else:
            if not swot_prior_lake_shpfile:
                log_level1.warning(f"Swot Prior Lake file was not provided in configuration file. Skipping extraction of Swot surface area and elevation time series.")
            else:
                log_level1.warning(f"Swot Prior Lake file's column dictionary was not provided in configuration file. Skipping extraction of Swot surface area and elevation time series.")
    else:
        pass
    
    no_failed_files = 0
    Optical_files = 0
    Tmsos_files = 0
    Partial_optical_tmsos_files = 0
    SWOT_files = 0
    i = 1
    for reservoir_no,reservoir in reservoirs_polygon.iterrows():
        # printing reservoir id & name (whatever available)
        if shpfile_column_dict.get('id_column'):
            print(f"\n\n +++ PROCESSING RESERVOIR: {reservoir[shpfile_column_dict['id_column']]} - {reservoir[shpfile_column_dict['dam_name_column']]} ({i}/{len(reservoirs_polygon)}) +++\n\n")
        else:
            print(f"\n\n +++ PROCESSING RESERVOIR: {reservoir[shpfile_column_dict['dam_name_column']]} ({i}/{len(reservoirs_polygon)}) +++\n\n")
            
        i += 1
        try:
            # Reading reservoir information
            reservoir_name = str(reservoir[shpfile_column_dict['unique_identifier']]).replace(" ","_")
            reservoir_area = float(reservoir[shpfile_column_dict['area_column']])
            reservoir_polygon = reservoir.geometry
            if pld_rat_matched_basin_gdf is not None:
                reservoir_prior_lakes_gdf = pld_rat_matched_basin_gdf[pld_rat_matched_basin_gdf[shpfile_column_dict['unique_identifier']]==reservoir[shpfile_column_dict['unique_identifier']]]
            else:
                reservoir_prior_lakes_gdf = None
            log.info(f"Calculating surface area for {reservoir_name}.")
            method = run_sarea_for_res(reservoir_name, reservoir_area, reservoir_polygon, start_date, end_date, sarea_save_dir, nssc_save_dir, 
                                       swot_run, swot_save_dir, reservoir_prior_lakes_gdf, swot_id_column_dict)
            log.info(f"Calculated surface area for {reservoir_name} successfully using {method} method.")
            if method == 'Optical':
                Optical_files += 1
            elif method == 'Combine':
                Partial_optical_tmsos_files +=1
            elif method == 'TMS-OS':
                Tmsos_files += 1
            elif method == 'SWOT':
                SWOT_files +=1
            else:
                pass
        except:
            log.exception(f"Surface area calculation failed for {reservoir_name}.")
            no_failed_files += 1
    if no_failed_files:
        log_level1.warning(f"Surface area was not calculated for {no_failed_files} reservoirs.")     
    if Optical_files:
        log_level1.warning(f"Surface area was calculated using only Optical data and not TMS-OS for {Optical_files} reservoirs. It can be due to insufficient SAR data. Please refer level-2 log file for more details.")
    if Partial_optical_tmsos_files:
        log_level1.warning(f"Surface area was calculated partially using only Optical data and rest using TMS-OS for {Partial_optical_tmsos_files} reservoirs. It can be due to more Optical data than SAR data. Please refer level-2 log file for more details.")
    if SWOT_files:
        log_level1.warning(f"Surface area was calculated using only SWOT data for {SWOT_files} reservoirs as 'only_swot' plugin was requested. Please refer level-2 log file for more details.")
        
    #Running Bot Filter
    if filt_options is not None:
        bot_thresholds = [filt_options['bias_threshold'],filt_options['outlier_threshold'],filt_options['trend_threshold']]
        if(None in bot_thresholds or min(bot_thresholds)<0 or max(bot_thresholds) > 9):
            if(filt_options['apply']==True):
                log_level1.error(f"BOT Filter run failed for all reservoirs.")
                log_level1.error("Filter values out of bounds. Please ensure that a value in between 0 and 9 is selected")
        else:
            bot_filter(sarea_save_dir,shpfile_column_dict,reservoirs_shpfile,**filt_options)    

def run_sarea_for_res(reservoir_name, reservoir_area, reservoir_polygon, start_date, end_date, sarea_save_dir, nssc_save_dir, swot_run, swot_save_dir, reservoir_prior_lakes_gdf, swot_id_column_dict, simplification=True):
    
    if simplification:
        # Below function simplifies geometry with shape index (complexity) higher than a threshold, otherwise original geometry is retained
        reservoir_polygon = simplify_geometry(reservoir_polygon)
    
    # Obtain surface areas (& elevation in case of swot)
    
    # SWOT
    if reservoir_prior_lakes_gdf is not None:
        swot_hydrocron_save_dir = create_directory(os.path.join(swot_save_dir, 'hydrocron'), True)
        log.debug(f"Reservoir: {reservoir_name}; Downloading SWOT data from {start_date} to {end_date}")
        sarea_swot.hydrocron_ts_swot(reservoir_prior_lakes_gdf, swot_id_column_dict['rat_lake_id'], swot_id_column_dict['prior_lake_id'],
                                     swot_hydrocron_save_dir,reservoir_name, start_date, end_date)
        # plot_reservoir_and_prior_lakes(
        #                     rat_reservoirs,
        #                     lake_id_gdf,
        #                     rat_lake_id_to_analyze,
        #                     rat_lake_id_field,
        #                     rat_lake_name_field,
        #                     save_path
        #                 )
    
    if swot_run == 'only_swot':
        # If only SWOT run is requested, return here
        return 'SWOT'
    
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
