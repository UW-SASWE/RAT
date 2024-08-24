import ruamel_yaml as ryaml
import pandas as pd
import geopandas as gpd
import numpy as np
from pathlib import Path
import shutil

from rat.data_processing.metsim_input_processing import CombinedNC
from rat.toolbox.config import update_config
from rat.plugins.forecasting.forecasting import get_gefs_precip, get_GFS_data, generate_forecast_state_and_inputs, forecast_outflow
from rat.plugins.forecasting.forecasting import convert_forecast_evaporation,convert_forecast_inflow, convert_forecast_outflow_states
from rat.rat_basin import rat_basin


def forecast(config, rat_logger):
    """Function to run the forecasting plugin.

    Args:
        config (dict): Dictionary containing the configuration parameters.
        rat_logger (Logger): Logger object
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
    basin_data_dir = Path(config['GLOBAL']['data_dir']) / region_name / 'basins' / basin_name
    rule_curve_dir = Path(config['PLUGINS']['forecast_rule_curve_dir'])
    reservoirs_gdf_column_dict = config['GEE']['reservoir_vector_file_columns_dict']
    forecast_reservoir_shpfile_column_dict = config['PLUGINS']['forecast_reservoir_shpfile_column_dict']
    if (config['ROUTING']['station_global_data']):
        reservoirs_gdf_column_dict['unique_identifier'] = 'uniq_id'
    else:
        reservoirs_gdf_column_dict['unique_identifier'] = reservoirs_gdf_column_dict['dam_name_column']

    # determine forecast related dates - basedate, lead time and enddate
    if config['PLUGINS']['forecast_start_date'] == 'end_date':
        basedate = pd.to_datetime(config['BASIN']['end'])
    else:
        basedate = pd.to_datetime(config['PLUGINS']['forecast_start_date'])
    lead_time = config['PLUGINS']['forecast_lead_time']
    forecast_enddate = basedate + pd.Timedelta(days=lead_time)

    # define and create directories
    hindcast_nc_path = basin_data_dir / 'pre_processing' / 'nc' / 'combined_data.nc'
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
    inflow_dst_dir = basin_data_dir / 'rat_outputs' / 'forecast_inflow' / f"{basedate:%Y%m%d}"
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
        basedate, forecast_enddate, None,
        basingridfile_path, combined_nc_path, False,
        forecast_data_dir, basedate
    )

    # RAT STEP-2 (Forecasting) generate metsim inputs
    generate_forecast_state_and_inputs(
        basedate, forecast_enddate,
        hindcast_nc_path, combined_nc_path,
        metsim_inputs_dir
    )

    # change config to only run metsim-routing
    config['BASIN']['vic_init_state'] = config['BASIN']['end'] # assuming vic_init_state is available for the end date
    config['GLOBAL']['steps'] = [3, 4, 5, 6, 7, 8, 13] # only run metsim-routing and inflow file generation
    config['BASIN']['start'] = basedate
    config['BASIN']['end'] = forecast_enddate
    config['BASIN']['spin_up'] = False

    # run RAT with forecasting parameters
    no_errors, _ = rat_basin(config, rat_logger, forecast_mode=True)

    # generate outflow forecast
    forecast_outflow(
        basedate, lead_time, basin_data_dir, basin_reservoir_shpfile_path, reservoirs_gdf_column_dict, forecast_reservoir_shpfile_column_dict, rule_curve_dir,
        scenarios = ['GC', 'GO', 'RC', 'ST'],
        st_percSmaxes = [0.5, 1, 2.5]
    )

    # RAT STEP-14 (Forecasting) convert forecast inflow and evaporation
    convert_forecast_inflow(inflow_dst_dir, basin_reservoir_shpfile_path, reservoirs_gdf_column_dict, final_inflow_out_dir, basedate)
    convert_forecast_evaporation(evap_dir, final_evap_out_dir)
    convert_forecast_outflow_states(outflow_forecast_dir, final_outflow_out_dir, final_dels_out_dir, final_sarea_out_dir)

    return no_errors