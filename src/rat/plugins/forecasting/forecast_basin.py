import ruamel_yaml as ryaml
import pandas as pd
import geopandas as gpd
import numpy as np
from pathlib import Path
import shutil
import datetime
from logging import getLogger

from rat.data_processing.metsim_input_processing import CombinedNC, generate_metsim_state_and_inputs_with_multiple_nc_files
from rat.toolbox.config import update_config
from rat.utils.vic_init_state_finder import get_vic_init_state_date
from rat.utils.utils import check_date_in_netcdf, get_first_date_from_netcdf
from rat.plugins.forecasting.forecasting import get_gefs_precip, get_GFS_data, forecast_outflow
from rat.plugins.forecasting.forecasting import convert_forecast_evaporation,convert_forecast_inflow, convert_forecast_outflow_states
from rat.rat_basin import rat_basin


def forecast(config, rat_logger, low_latency_limit):
    """Function to run the forecasting plugin.

    Args:
        config (dict): Dictionary containing the configuration parameters.
        rat_logger (Logger): Logger object
        low_latency_limit (int): Maximum number of days that will be counted as low latency from today
    """

    print("Forecasting Plugin Started")
    # read necessary parameters from config
    basins_shapefile_path = config['GLOBAL']['basin_shpfile'] # Shapefile containg information of basin(s)- geometry and attributes
    basins_shapefile = gpd.read_file(basins_shapefile_path)  # Reading basins_shapefile_path to get basin polygons and their attributes
    basins_shapefile_column_dict = config['GLOBAL']['basin_shpfile_column_dict'] # Dictionary of column names in basins_shapefile, Must contain 'id' field
    region_name = config['BASIN']['region_name']  # Major basin name used to cluster multiple basins data in data-directory
    basin_name = config['BASIN']['basin_name']              # Basin name used to save basin related data
    basin_id = config['BASIN']['basin_id']                  # Unique identifier for each basin used to map basin polygon in basins_shapefile
    basin_data = basins_shapefile[basins_shapefile[basins_shapefile_column_dict['id']]==basin_id] # Getting the particular basin related information corresponding to basin_id
    basin_bounds = basin_data.bounds                          # Obtaining bounds of the particular basin
    basin_bounds = np.array(basin_bounds)[0]
    data_dir = config['GLOBAL']['data_dir']
    basin_data_dir = Path(config['GLOBAL']['data_dir']) / region_name / 'basins' / basin_name
    if config['PLUGINS'].get('forecast_rule_curve_dir'):
        rule_curve_dir = Path(config['PLUGINS'].get('forecast_rule_curve_dir'))
    else:
        rule_curve_dir = None
    reservoirs_gdf_column_dict = config['GEE']['reservoir_vector_file_columns_dict']
    forecast_reservoir_shpfile_column_dict = config['PLUGINS']['forecast_reservoir_shpfile_column_dict']
    if (config['ROUTING']['station_global_data']):
        reservoirs_gdf_column_dict['unique_identifier'] = 'uniq_id'
    else:
        reservoirs_gdf_column_dict['unique_identifier'] = reservoirs_gdf_column_dict['dam_name_column']

    # determine start date to generate forecast
    if not config['PLUGINS'].get('forecast_gen_start_date'):
        raise Exception("The start date for generating forecast is not provided. Please provide 'forecast_gen_start_date' in config file.")
    elif config['PLUGINS']['forecast_gen_start_date'] == 'end_date':
        forecast_gen_start_date = pd.to_datetime(config['BASIN']['end'])
    else:
        forecast_gen_start_date = pd.to_datetime(config['PLUGINS']['forecast_gen_start_date'])
    
    # determine end date to generate forecast if provided, otherwise same as forecast_gen_start_date
    forecast_gen_end_date = config['PLUGINS'].get('forecast_gen_end_date')
    if forecast_gen_end_date:
        forecast_gen_end_date = pd.to_datetime(forecast_gen_end_date)
    else:
        forecast_gen_end_date = forecast_gen_start_date
    
    # Verify that forecast_gen start and end dates are less than RAT's end date for last run.
    if not ((forecast_gen_end_date <= pd.to_datetime(config['BASIN']['end'])) and 
        (forecast_gen_start_date <= forecast_gen_end_date)):
        raise Exception("forecast_gen_start_date should be less than or equal to forecast_gen_end_date and both should be less than or equal to RAT's end date for last run.")
    
    # determine lead time for each day to generate forecast
    if not config['PLUGINS'].get('forecast_lead_time'):
        raise Exception("The lead time for each day to generate forecast is not provided. Please provide 'forecast_lead_time' in config file.")
    else:
        lead_time = config['PLUGINS']['forecast_lead_time']

    # Determine vic_init_state_date to be used
    if config['PLUGINS'].get('forecast_vic_init_state'):
        vic_init_state = config['PLUGINS']['forecast_vic_init_state']
    else:
        vic_init_state = get_vic_init_state_date(forecast_gen_start_date,low_latency_limit,data_dir, region_name, basin_name)
        if not vic_init_state:
            raise Exception("No vic init state was found to execute RAT. You can provide path or date using 'forecast_vic_init_state' in config file.")

    # Determine rout_init_state_date to be used
    if(isinstance(vic_init_state, datetime.date)):
        rout_init_state = vic_init_state
    else:
        if config['PLUGINS'].get('forecast_rout_init_state'):
            rout_init_state = config['PLUGINS']['forecast_rout_init_state']
        else:
            rout_init_state = None
       
    # Determine RAT start date to be used (corresponding to vic init state)
    if(isinstance(vic_init_state, datetime.date)):
        rat_start_date = vic_init_state
    # If vic init state path file is available, use forecast_gen_start_date as start date
    else:
        rat_start_date = forecast_gen_start_date

    # define and create directories
    hindcast_nc_path = basin_data_dir / 'pre_processing' / 'nc' / 'combined_data.nc'
    low_latency_nc_path = basin_data_dir / 'pre_processing' / 'nc' / 'low_latency_combined.nc'
    combined_nc_path = basin_data_dir / 'pre_processing' / 'nc' / 'forecast_combined.nc'
    metsim_inputs_dir = basin_data_dir / 'metsim' / 'metsim_inputs'
    basingridfile_path = basin_data_dir / 'basin_grid_data' / f'{basin_name}_grid_mask.tif'
    forecast_data_dir = basin_data_dir / 'forecast'
    raw_gefs_chirps_dir = forecast_data_dir / 'gefs-chirps' / 'raw'
    processed_gefs_chirps_dir = forecast_data_dir / 'gefs-chirps' / 'processed'
    gfs_dir = forecast_data_dir / 'gfs'
    raw_gfs_dir = gfs_dir / 'raw'
    extracted_gfs_dir = gfs_dir / 'extracted'
    processed_gfs_dir = gfs_dir / 'processed'
    s_area_dir = basin_data_dir / 'final_outputs' / 'sarea_tmsos'

    # List of Dates on which to generate forecast
    forecast_basedates = pd.date_range(forecast_gen_start_date, forecast_gen_end_date)

    # Get Storage scenarios
    outflow_storage_scenarios = config['PLUGINS'].get('forecast_storage_scenario')

    # Get list of storage percent left if 'ST' in scenario.
    if 'ST' in outflow_storage_scenarios:
        percent_st_change = config['PLUGINS'].get('forecast_storage_change_percent_of_smax')
        actual_storage_scenario = True
        # If list is not available raise warning and remove the 'ST' scenario.
        if not percent_st_change:
            outflow_storage_scenarios.remove('ST')
            actual_storage_scenario = False
            raise Warning("List of storage change scenarios as percent of smax is not available. Please provide 'forecast_storage_change_percent_of_smax' in the RAT config file.")
    else:
        actual_storage_scenario = False

    # For each basedate, generate forecasting results by running RAT
    for basedate in forecast_basedates:
        forecast_lead_enddate = basedate + pd.Timedelta(days=lead_time)
        forecast_lead_startdate = basedate + pd.Timedelta(days=1)

        # Determine whether to use low_latency_combined_nc ( if basedate is not in hindcast combinedNC)
        low_latency_data_need = not check_date_in_netcdf(hindcast_nc_path, basedate)

        # Define directories for basedate
        forecast_inflow_dst_dir = basin_data_dir / 'rat_outputs' / 'forecast_inflow' / f"{basedate:%Y%m%d}"
        basin_reservoir_shpfile_path = Path(basin_data_dir) / 'gee' / 'gee_basin_params' / 'basin_reservoirs.shp'
        final_inflow_out_dir = basin_data_dir / 'final_outputs' / 'forecast_inflow' / f"{basedate:%Y%m%d}"
        final_evap_out_dir = basin_data_dir / 'final_outputs' / 'forecast_evaporation' / f"{basedate:%Y%m%d}"
        evap_dir = basin_data_dir / 'rat_outputs' / 'forecast_evaporation' / f"{basedate:%Y%m%d}"
        outflow_forecast_dir = basin_data_dir / 'rat_outputs' / 'forecast_outflow' / f'{basedate:%Y%m%d}'
        final_outflow_out_dir = basin_data_dir / 'final_outputs' / 'forecast_outflow' / f'{basedate:%Y%m%d}'
        final_dels_out_dir = basin_data_dir / 'final_outputs' / 'forecast_dels' / f'{basedate:%Y%m%d}'
        final_sarea_out_dir = basin_data_dir / 'final_outputs' / 'forecast_sarea' / f'{basedate:%Y%m%d}'

        for d in [
            raw_gefs_chirps_dir, processed_gefs_chirps_dir, raw_gfs_dir, extracted_gfs_dir, processed_gfs_dir, outflow_forecast_dir,
            final_evap_out_dir, final_inflow_out_dir, final_outflow_out_dir
        ]:
            d.mkdir(parents=True, exist_ok=True)

        # cleanup previous runs
        vic_forecast_input_dir = basin_data_dir / 'vic' / 'forecast_vic_inputs'
        [f.unlink() for f in vic_forecast_input_dir.glob("*") if f.is_file()]
        vic_forecast_output_dir = basin_data_dir / 'vic' / 'forecast_vic_outputs'
        [f.unlink() for f in vic_forecast_output_dir.glob("*") if f.is_file()]
        vic_forecast_state_dir = basin_data_dir / 'vic' / 'forecast_vic_state'
        [f.unlink() for f in vic_forecast_state_dir.glob("*") if f.is_file()]
        combined_nc_path.unlink() if combined_nc_path.is_file() else None
        rout_forecast_state_dir = basin_data_dir / 'rout' / 'forecast_rout_state_file'
        [f.unlink() for f in rout_forecast_state_dir.glob("*") if f.is_file()]


        # RAT STEP-1 (Forecasting) Download and process GEFS-CHIRPS data
        get_gefs_precip(basin_bounds, raw_gefs_chirps_dir, processed_gefs_chirps_dir, basedate, lead_time)

        # RAT STEP-1 (Forecasting) Download and process GFS data
        get_GFS_data(basedate, lead_time, basin_bounds, gfs_dir)

        # RAT STEP-2 (Forecasting) make combined nc
        CombinedNC(
            basedate, forecast_lead_enddate, None,
            basingridfile_path, combined_nc_path, False,
            forecast_data_dir, basedate
        )

        # RAT STEP-2 (Forecasting) generate metsim inputs
        if low_latency_data_need:
            file_paths_to_combine = [hindcast_nc_path, low_latency_nc_path, combined_nc_path]
            low_latency_data_first_date = get_first_date_from_netcdf(low_latency_nc_path)
            start_dates_to_combine = [low_latency_data_first_date,forecast_lead_startdate.to_pydatetime()]
        else:
            file_paths_to_combine = [hindcast_nc_path, combined_nc_path]
            start_dates_to_combine = [forecast_lead_startdate.to_pydatetime()]
        generate_metsim_state_and_inputs_with_multiple_nc_files(
            nc_file_paths=file_paths_to_combine,
            start_dates= start_dates_to_combine,
            out_dir= metsim_inputs_dir,
            forcings_start_date= rat_start_date,
            forcings_end_date= forecast_lead_enddate,
            forecast_mode= True
        )

        # change config to only run metsim-routing
        config['BASIN']['vic_init_state'] = vic_init_state 
        config['BASIN']['rout_init_state'] = rout_init_state
        config['GLOBAL']['steps'] = [3, 4, 5, 6, 7, 8, 13] # only run metsim-routing and inflow file generation
        config['BASIN']['start'] = rat_start_date
        config['BASIN']['end'] = forecast_lead_enddate
        config['BASIN']['spin_up'] = False

        # run RAT with forecasting parameters
        no_errors, _ = rat_basin(config, rat_logger, forecast_mode=True, forecast_basedate=basedate)

        # generate outflow forecast
        forecast_outflow(
            basedate, lead_time, basin_data_dir, basin_reservoir_shpfile_path, reservoirs_gdf_column_dict, forecast_reservoir_shpfile_column_dict, rule_curve_dir,
            scenarios = outflow_storage_scenarios,
            st_percSmaxes = percent_st_change[:],
            actual_st_left=actual_storage_scenario
        )

        # RAT STEP-14 (Forecasting) convert forecast inflow and evaporation
        convert_forecast_inflow(forecast_inflow_dst_dir, basin_reservoir_shpfile_path, reservoirs_gdf_column_dict, final_inflow_out_dir, basedate)
        convert_forecast_evaporation(evap_dir, final_evap_out_dir)
        convert_forecast_outflow_states(outflow_forecast_dir, final_outflow_out_dir, final_dels_out_dir, final_sarea_out_dir)

    return no_errors