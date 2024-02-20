import rioxarray as rxr
import xarray as xr
import numpy as np
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import networkx as nx
import os

from logging import getLogger
from rat.utils.logging import LOG_NAME, LOG_LEVEL1_NAME
from rat.utils.utils import create_directory
from rat.plugins.resorr.data_prep import generate_network
from resorr.data_prep import generate_forcings_from_rat, generate_network
from resorr.network import ReservoirNetwork

log = getLogger(f"{LOG_NAME}.{__name__}")
log_level1 = getLogger(f"{LOG_LEVEL1_NAME}.{__name__}")

def runResorr(basin_data_dir,basin_station_latlon_file,resorr_startDate,resorr_endDate):
    ''' 
    Runs the regulation algorith ResORR. 
    Dams connected to each other in a basin is identified and regulated reservoir data is computed.
    
    Parameters
    ----------
    basin_data_dir: str
        Path to the RAT basin folder 
    basin_station_latlon_file: str
        Path to file containing reservoir locations as latitude, longtitude.
    resorr_startDate : str
        Start date for ResORR calculation
    resorr_endDate : str
        End date for ResORR calculation
    
    '''
    # read in the flow direction file and reservoir location file
    flow_dir_fn = os.path.join(basin_data_dir,'ro','pars','fl.tif')
    res_location_fn = basin_station_latlon_file
    # creating directory to store the network files
    save_dir = create_directory(os.path.join(basin_data_dir,'post_processing','resorr_network'), True)
    
    # read in the reservoir location file, create a geodataframe and set the crs.  
    reservoirs = gpd.read_file(res_location_fn)
    reservoirs['geometry'] = gpd.points_from_xy(reservoirs['lon'], reservoirs['lat'])
    reservoirs.set_crs('epsg:4326', inplace=True)

    # generating the network
    try:
        G = generate_network(
            flow_dir_fn, res_location_fn, save_dir
        )
    except Exception as e:
        log.info(f'RESORR: Network generation failed due to error: {e}')
        log_level1.warning('RESORR failed to run because there are no regulated reservoirs in the basin. Please validate the flow direction file if this is unexpected.')
        return
    else:
        log.info('RESORR: Network generated')
        log.info(f'Network files stored at {save_dir}')
    
    # generating the forcings
    try:
        forcings = generate_forcings_from_rat(
        G,
        os.path.join(basin_data_dir,'rat_outputs','inflow'),
        os.path.join(basin_data_dir,'final_outputs','dels'),
        save_dir,
        rat_output_level='rat_outputs',
        aggregate_freq='daily'
        )
        
        start_time = pd.to_datetime(resorr_startDate.strftime("%Y-%m-%d"))
        end_time = pd.to_datetime(resorr_endDate.strftime("%Y-%m-%d"))
        forcings = forcings.sel(time=slice(start_time, end_time))

        reservoir_network = ReservoirNetwork(G, start_time)

        for timestep in forcings.time.values:
            dt = forcings['dt'].sel(time=timestep).values.item()
            reservoir_network.update(forcings, dt, 'wb')
        
        reservoir_network.data.to_netcdf(os.path.join(save_dir, 'resorr_outputs.nc'))

        # Saving the regulated reservoir data to csv in the RAT final_outputs folder
        filetype_list = ['inflow','outflow']
        #List of reservoirs that are part of regulated network
        reg_res_edges = list(G.edges())
        reg_res_nodes = []
        for edge in reg_res_edges:
            for res_node in edge:
                reg_res_nodes.append(res_node)
                
        for node in reservoir_network.data.node.values:
            res_name = reservoirs['name'][node]
            if node in reg_res_nodes:
                for file_type in filetype_list:
                    # Prepping the regulated data
                    reg_data = reservoir_network.data[file_type].sel(node = node)
                    reg_data = reg_data.to_dataframe()
                    reg_data.drop('node', axis = 1, inplace = True)
                    reg_data.rename(columns = {'time':'date'}, inplace = True)
                    reg_data['date'] = reg_data.index
                    reg_data.reset_index(drop=True, inplace=True)
                    reg_data = pd.DataFrame(reg_data)
                    cols = reg_data.columns.tolist()
                    cols = cols[-1:] + cols[:-1]
                    reg_data = reg_data[cols]

                    # Saving the regulated data to csv
                    filename = os.path.join(basin_data_dir,'final_outputs',file_type,'Regulated',res_name + '.csv')
                    if not os.path.exists(filename):
                        create_directory(os.path.join(basin_data_dir,'final_outputs',file_type,'Regulated'), True)
                        reg_data.to_csv(filename, index = False)
                    else:
                        file_df = pd.read_csv(filename)
                        file_df = pd.concat([file_df, reg_data], axis=0)
                        file_df.to_csv(filename, index = False)

    except Exception as e:
        log.exception(f'RESORR failed due to error: {e}')
        log_level1.error('RESORR failed to run. Please check Level-2 log file for more details.')
        return
    else:
        log_level1.info('RESORR run completed successfully')
        







