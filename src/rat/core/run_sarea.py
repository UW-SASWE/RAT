import os
import geopandas as gpd

from logging import getLogger
from rat.utils.logging import LOG_NAME, NOTIFICATION, LOG_LEVEL1_NAME

from rat.core.sarea.sarea_cli_s2 import sarea_s2
from rat.core.sarea.sarea_cli_l8 import sarea_l8
from rat.core.sarea.sarea_cli_l9 import sarea_l9
from rat.core.sarea.sarea_cli_sar import sarea_s1
from rat.core.sarea.TMS import TMS

log = getLogger(f"{LOG_NAME}.{__name__}")
log_level1 = getLogger(f"{LOG_LEVEL1_NAME}.{__name__}")


def run_sarea(start_date, end_date, datadir, reservoirs_shpfile, shpfile_column_dict):
    reservoirs_polygon = gpd.read_file(reservoirs_shpfile)
    no_failed_files = 0
    
    for reservoir_no,reservoir in reservoirs_polygon.iterrows():
        try:
            # Reading reservoir information
            reservoir_name = str(reservoir[shpfile_column_dict['unique_identifier']]).replace(" ","_")
            reservoir_area = float(reservoir[shpfile_column_dict['area_column']])
            reservoir_polygon = reservoir.geometry
            log.info(f"Calculating surface area for {reservoir_name}.")
            run_sarea_for_res(reservoir_name, reservoir_area, reservoir_polygon, start_date, end_date, datadir)
            log.info(f"Calculated surface area for {reservoir_name} successfully.")
        except:
            log.exception(f"Surface area calculation failed for {reservoir_name}.")
            no_failed_files += 1
    if no_failed_files:
        log_level1.warning(f"Surface area was not calculated for {no_failed_files} reservoirs.")
        
def run_sarea_for_res(reservoir_name, reservoir_area, reservoir_polygon, start_date, end_date, datadir):

    # Obtain surface areas
    # Sentinel-2
    log.debug(f"Reservoir: {reservoir_name}; Downloading Sentinel-2 data from {start_date} to {end_date}")
    sarea_s2(reservoir_name, reservoir_polygon, start_date, end_date, os.path.join(datadir, 's2'))
    s2_dfpath = os.path.join(datadir, 's2', reservoir_name+'.csv')

    # Landsat-8
    log.debug(f"Reservoir: {reservoir_name}; Downloading Landsat-8 data from {start_date} to {end_date}")
    sarea_l8(reservoir_name, reservoir_polygon, start_date, end_date, os.path.join(datadir, 'l8'))
    l8_dfpath = os.path.join(datadir, 'l8', reservoir_name+'.csv')
    
    # Landsat-9
    log.debug(f"Reservoir: {reservoir_name}; Downloading Landsat-9 data from {start_date} to {end_date}")
    sarea_l9(reservoir_name, reservoir_polygon, start_date, end_date, os.path.join(datadir, 'l9'))
    l9_dfpath = os.path.join(datadir, 'l9', reservoir_name+'.csv')

    # Sentinel-1
    log.debug(f"Reservoir: {reservoir_name}; Downloading Sentinel-1 data from {start_date} to {end_date}")
    s1_dfpath = sarea_s1(reservoir_name, reservoir_polygon, start_date, end_date, os.path.join(datadir, 'sar'))
    s1_dfpath = os.path.join(datadir, 'sar', reservoir_name+'_12d_sar.csv')

    tmsos = TMS(reservoir_name, reservoir_area)
    result = tmsos.tms_os(l9_dfpath=l9_dfpath, l8_dfpath=l8_dfpath, s2_dfpath=s2_dfpath, s1_dfpath=s1_dfpath)

    tmsos_savepath = os.path.join(datadir, reservoir_name+'.csv')
    log.debug(f"Saving surface area of {reservoir_name} at {tmsos_savepath}")
    result.reset_index().rename({'index': 'date', 'filled_area': 'area'}, axis=1).to_csv(tmsos_savepath, index=False)
