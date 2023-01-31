import configparser
import os
import pandas as pd
import geopandas as gpd
from rat.utils.utils import create_directory
from rat.data_processing import altimetry as alt


def altimeter_routine(reservoir_df, reservoir_column_dict, j3tracks, custom_reservoir_range_dict,
                                         user, password, lastcycle_no, basin_name, basin_data_dir, geoidpath, save_dirpath):
    resname = str(reservoir_df[reservoir_column_dict['unique_identifier']])
    latest_cycle = lastcycle_no         # for initializing latest cycle number

    j3_pass = alt.get_j3_tracks(reservoir_df, reservoir_column_dict, j3tracks, custom_reservoir_range_dict)
    print(j3_pass)

    if j3_pass is None:
        return (None, latest_cycle)

    # Directory to save raw downloaded altimetry data
    savedir = create_directory(os.path.join(basin_data_dir,'altimetry','raw',resname), True)

    # Directory to save extracted altimetry data from 
    extracteddir = create_directory(os.path.join(basin_data_dir,'altimetry','extracted',resname), True)
    
    # Directory to save altimetry time-series for different reservoirs 
    resultsdir = create_directory(save_dirpath, True)
    savepath = os.path.join(resultsdir, f'{resname}.csv')

    tracks = j3_pass['tracks']
    lat_ranges = j3_pass['lat_range']

    for track, lat_range in zip(tracks, lat_ranges):
        latest_cycle = alt.get_latest_cycle(user, password, lastcycle_no)
        print(f"Latest cycle: {latest_cycle}")

        ## Getting the starting cycle for downloading data
        starting_cycle = 1
        cycles_downloaded = []
        ## Replace 'gdr_f' with 'gdr_d' if downloading other than jason 3 data
        if os.path.exists(os.path.join(savedir, f'j3_{track:03}','gdr_f')):
            for f in os.listdir(os.path.join(savedir, f'j3_{track:03}','gdr_f')):
                if(f.startswith('cycle_')):
                    cycles_downloaded.append(int(f[-3:]))
            if(cycles_downloaded):
                starting_cycle = max(cycles_downloaded)

        print(f"Downloading data from cycle {starting_cycle} to {latest_cycle}")
        alt.download_data(user, password, savedir, track, starting_cycle, latest_cycle, 3)
        extractedf = alt.extract_data(savedir + f'/j3_{track:03}', extracteddir, lat_range[0], lat_range[1], track, 3, starting_cycle, latest_cycle, '_app_mod')

    ## One-reservoir has one time-series from all tracks
    print("Gathering extracted data to create time-series")
    alt.generate_timeseries(extracteddir, savepath, lat_range[0], lat_range[1], geoidpath)
    
    return (resname,latest_cycle)

def run_altimetry(config, section, res_shpfile, res_shpfile_column_dict, basin_name, basin_data_dir, save_dir):
    reservoirs_gdf = gpd.read_file(res_shpfile) 
    
    ## Declaring variables to see if only certain reservoirs needs to be processed or certain range of a reservoir is available
    to_process = None
    reservoir_latlon_ranges_dict = None

    reservoirs_csv_file_path_default = os.path.join(basin_data_dir,'altimetry','altimetry_basin_params',
                                                                            'reservoir_altimetry_basins.csv')
    if(config[section].get('reservoirs_csv_file')):
        reservoirs_csv_file_path = config[section].get('reservoirs_csv_file')
    elif(os.path.exists(reservoirs_csv_file_path_default)):
        reservoirs_csv_file_path = reservoirs_csv_file_path_default
    else:
        reservoirs_csv_file_path = None

    if(reservoirs_csv_file_path):
        ## Reading specific list of reservoirs to process
        basin_alt_reservoirs = pd.read_csv(reservoirs_csv_file_path)
        if not (config[section].get('only_for_range')):
            to_process = basin_alt_reservoirs['reservoir_uni_id'].astype(str).to_list()

        ## Reading min and max latitude if specified for any reservoir
        basin_alt_reservoirs_latrange = basin_alt_reservoirs.dropna()
        if not basin_alt_reservoirs_latrange.empty:
            reservoir_latlon_ranges_dict = basin_alt_reservoirs_latrange.set_index('reservoir_uni_id')[['min_lat','max_lat']].apply(
                                                                                                        tuple,axis=1).to_dict()

    secrets = configparser.ConfigParser()
    secrets.read(config['CONFIDENTIAL']['secrets'])
    username = secrets["aviso"]["username"]
    pwd = secrets["aviso"]["pwd"]

    lastcycle_no = config[section]['last_cycle_number']
    latest_cycle = lastcycle_no  # for initializing latest cycle number
    
    j3_tracks = config[section]['altimeter_tracks']
    j3_tracks_gdf = gpd.read_file(j3_tracks, driver='GeoJSON')

    geoidpath = config[section]['geoid_grid']
    
    if(to_process != None):
        for reservoir_no,reservoir in reservoirs_gdf.iterrows():
            # Reading reservoir information
            reservoir_name = str(reservoir[res_shpfile_column_dict['unique_identifier']])
            if (reservoir_name in to_process):
                print(f"Processing {reservoir_name}")
                output_res_name, latest_cycle = altimeter_routine(reservoir, res_shpfile_column_dict, j3_tracks_gdf, reservoir_latlon_ranges_dict, 
                                    username, pwd, lastcycle_no, 
                                    basin_name, basin_data_dir, geoidpath, save_dir)
    else:
        res_names_for_altimetry = []
        for reservoir_no,reservoir in reservoirs_gdf.iterrows():
            # Reading reservoir information
            reservoir_name = str(reservoir[res_shpfile_column_dict['unique_identifier']])
            print(f"Processing {reservoir_name}")
            output_res_name, latest_cycle = altimeter_routine(reservoir, res_shpfile_column_dict, j3_tracks_gdf, reservoir_latlon_ranges_dict, 
                                username, pwd, lastcycle_no, 
                                basin_name, basin_data_dir, geoidpath, save_dir)
            if output_res_name is not None:
                res_names_for_altimetry.append(output_res_name)
        altimetry_res_names_df = pd.DataFrame(data={'reservoir_uni_id':res_names_for_altimetry})
        altimetry_res_names_df['min_lat'] = ''
        altimetry_res_names_df['max_lat'] = ''
        create_directory(os.path.join(basin_data_dir,'altimetry','altimetry_basin_params',''))
        altimetry_res_names_df.to_csv(reservoirs_csv_file_path_default,index=False)
    
    return latest_cycle
