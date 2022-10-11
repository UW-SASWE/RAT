import subprocess
import yaml
from datetime import datetime, timedelta
from tqdm import tqdm
import os
import shutil
import tempfile
import xarray as xr
import rioxarray as rxr
import rasterio
from logging import getLogger
import pandas as pd
import configparser
import datetime
import geopandas as gpd
from shapely.geometry import Polygon,mapping
import numpy as np
import math
import glob

from utils.utils import create_directory
from utils.logging import init_logger,close_logger,NOTIFICATION
from utils.files_creator import create_basingridfile, create_basin_domain_nc_file ,create_vic_domain_param_file, create_basin_grid_flow_asc
from utils.files_creator import create_basin_station_latlon_csv, create_basin_reservoir_shpfile

from data_processing.newdata import get_newdata

from data_processing.metsim_input_processing import CombinedNC,generate_state_and_inputs
from utils.metsim_param_reader import MSParameterFile
from core.run_metsim import MetSimRunner

from utils.vic_param_reader import VICParameterFile
from core.run_vic import VICRunner

from utils.route_param_reader import RouteParameterFile
from core.run_routing import RoutingRunner

from core.run_sarea import run_sarea
from core.run_altimetry import altimeter_routine, run_altimetry

from ee_utils.ee_aec_file_creator import aec_file_creator
from core.run_postprocessing import run_postprocessing

from utils.convert_for_website import convert_sarea, convert_inflow, convert_dels, convert_evaporation, convert_outflow, convert_altimeter
from core.generate_plots import generate_plots

# Step-1: Downloading and Pre-processing of meteorolgical data
# Step-2: Pre-processing of data and preparation of MetSim Input
# Step-3: Preparation of MetSim Parameter Files
# Step-4: Running MetSim & preparation of VIC input
# Step-5: Preparation of VIC Parameter Files
# Step-6: Running of VIC and preparation of Routing input
# Step-7: Preparation of Routing Parameter Files
# Step-8: Runnning Routing and generating Inflow
# Step-9: Preparation of parameter files for Surface Area Calculation
# Step-10: TMS-OS Surface Area Calculation from GEE 
# Step-11: Elevation extraction from Altimeter
# Step-12: Generating Area Elevation Curves for reservoirs
# Step-13: Calculation of Outflow, Evaporation and Storage change

def rat(config, rat_logger, steps=[2,3,5,7,9,12,13,4,6,8,10,11]):

    rat_logger = getLogger('run_rat')
    ##--------------------- Reading and initialising global parameters ----------------------##
    # Defining resolution to run RAT
    xres=0.0625
    yres=0.0625 

    # Obtaining basin related information from RAT_Runner.yml
    basins_shapefile_path = config['GLOBAL']['basin_shpfile'] # Shapefile containg information of basin(s)- geometry and attributes
    basins_shapefile = gpd.read_file(basins_shapefile_path)  # Reading basins_shapefile_path to get basin polygons and their attributes
    basins_shapefile_column_dict = config['GLOBAL']['basin_shpfile_column_dict'] # Dictionary of column names in basins_shapefile, Must contain 'id' field
    major_basin_name = config['BASIN']['major_basin_name']  # Major basin name used to cluster multiple basins data in data-directory
    basin_name = config['BASIN']['basin_name']              # Basin name used to save basin related data
    basin_id = config['BASIN']['basin_id']                  # Unique identifier for each basin used to map basin polygon in basins_shapefile
    basin_data = basins_shapefile[basins_shapefile[basins_shapefile_column_dict['id']]==basin_id] # Getting the particular basin related information corresponding to basin_id
    basin_bounds = basin_data.bounds                          # Obtaining bounds of the particular basin
    basin_bounds = np.array(basin_bounds)[0]
    basin_geometry = basin_data.geometry                      # Obtaining geometry of the particular basin

    # Defining paths for RAT processing
    project_dir = config['GLOBAL']['project_dir']    # Directory of RAT 
    data_dir = config['GLOBAL']['data_dir']          # Data-Directory of RAT
    major_basin_data_dir = create_directory(os.path.join(data_dir,major_basin_name), True) # Major Basin data-directory within the data-directory of RAT
    basin_data_dir = create_directory(os.path.join(major_basin_data_dir,'basins',basin_name), True)  # Basin data-directory within the major basin's data-directory 
    log_dir = create_directory(os.path.join(major_basin_data_dir,'logs',basin_name,''), True)  # Log directory within the major basin's data-directory

    # Change datetimes format
    #config['BASIN']['begin'] = datetime.datetime.combine(config['BASIN']['begin'], datetime.time.min)
    config['BASIN']['start'] = datetime.datetime.combine(config['BASIN']['start'], datetime.time.min)
    config['BASIN']['end'] = datetime.datetime.combine(config['BASIN']['end'], datetime.time.min)

    if(not config['BASIN']['first_run']):
        config['BASIN']['vic_init_state_date'] = datetime.datetime.combine(config['BASIN']['vic_init_state_date'], datetime.time.min)
    
    # Changing start date if running RAT for first time for the particular basin to give VIC and MetSim to have their spin off periods
    if(config['BASIN']['first_run']):
        user_given_start = config['BASIN']['start']
        config['BASIN']['start'] = user_given_start-datetime.timedelta(days=800)  # Running RAT for extra 800 days before the user-given start date for VIC to give reliable results starting from user-given start date
        data_download_start = config['BASIN']['start']-datetime.timedelta(days=90)    # Downloading 90 days of extra meteorological data for MetSim to prepare it's initial state
        vic_init_state_date = None    # No initial state of VIC is present as running RAT for first time in this basin
    else:
        data_download_start = config['BASIN']['start']    # Downloading data from the same date as we want to run RAT from
        vic_init_state_date = config['BASIN']['vic_init_state_date'] # Date of which initial state of VIC for the particular basin exists
    
    # Defining logger
    log = init_logger(
        log_dir= log_dir,
        verbose= False,
        # notify= True,
        notify= False,
        log_level= 'DEBUG'
    )

    # Initializing Status for different models & tasks (1 for successful run & 0 for failed run)
    NEW_DATA_STATUS = 1
    METSIM_STATUS = 1
    VIC_STATUS = 1
    ROUTING_STATUS = 1
    GEE_STATUS = 1
    ALTIMETER_STATUS = 1
    DELS_STATUS = 0
    EVAP_STATUS = 0
    OUTFLOW_STATUS = 0

    ##--------------------- Read and initialised global parameters ----------------------##

    rat_logger.info(f"Running RAT from {config['BASIN']['start']} to {config['BASIN']['end']}")
    
    ######### Step-1
    if(1 in steps):
        try:
            rat_logger.info("Starting Step-1: Downloading and Pre-processing of meteorological data")
            ##----------------------- Downloading meteorological data ------------------------##
            ##--------- Download Data Begin ----------#
            get_newdata(
                basin_name= basin_name,
                basin_bounds= basin_bounds,
                data_dir= config['GLOBAL']['data_dir'],
                basin_data_dir= basin_data_dir,
                startdate= data_download_start,
                enddate= config['BASIN']['end'],
                secrets_file= config['CONFIDENTIAL']['secrets'],
                download= True,
                process= True
            )
            ##---------- Download Data End -----------# 
            NEW_DATA_STATUS = 1   #Data downloaded successfully
        except:
            rat_logger.exception("Error Executing Step-1: Downloading and Pre-processing of meteorological data")
        else:
            rat_logger.info("Finished Step-1: Downloading and Pre-processing of meteorological data")
            ##----------------------- Downloaded meteorological data ------------------------##


    ######### Step-2
    if(2 in steps):
        try:
            rat_logger.info("Starting Step-2: Pre-processing of data and preparation of MetSim Input")
            ##----------------------- Pre-processing step for METSIM ------------------------##
            #----------- Paths Necessary for creating METSIM Input Data  -----------#
            # Path of directory which will contain the combined data in nc format.
            combined_datapath = create_directory(os.path.join(basin_data_dir, 'nc', ''), True)
            combined_datapath = os.path.join(combined_datapath, 'combined_data.nc')

            # Path where the processed downloaded data is present
            processed_datadir = os.path.join(basin_data_dir, 'processed')

            # basingrid_file path to clip the global downloaded data
            basingridfile_path= create_directory(os.path.join(basin_data_dir, 'basin_grid_data',''), True)
            basingridfile_path= os.path.join(basingridfile_path, basin_name+'_grid_mask.tif')

            # Creating basinggrid_file if not exists
            if not os.path.exists(basingridfile_path):
                create_basingridfile(basin_bounds,basin_geometry,basingridfile_path,xres,yres)
            #----------- Created Paths Necessary for creating METSIM Input Data  -----------#

            #----------- Process Data Begin to combine all var data -----------#
            CombinedNC(
                start= data_download_start,
                end= config['BASIN']['end'],
                datadir= processed_datadir,
                basingridpath= basingridfile_path,
                outputdir= combined_datapath
            )
            #----------- Process Data End and combined data created -----------#

            #------ MetSim Input Data Preparation Begin ------#
            # Prepare data to metsim input format
            metsim_inputs_dir = create_directory(os.path.join(basin_data_dir, 'metsim', 'metsim_inputs', ''),True)

            ms_state, ms_input_data = generate_state_and_inputs(
                forcings_startdate= config['BASIN']['start'],
                forcings_enddate= config['BASIN']['end'],
                combined_datapath= combined_datapath, 
                out_dir= metsim_inputs_dir
            )
            #------- MetSim Input Data Preparation End -------#

            #---------- Creating metsim output directory for basin if not exist---------#
            metsim_output_path = create_directory(os.path.join(basin_data_dir, 'metsim', 'metsim_outputs',''), True)
            #----------metsim output directory created for basin if not exist---------#

            #---------- Creating vic input directory for basin if not exist---------#
            vic_input_path = create_directory(os.path.join(basin_data_dir, 'vic', 'vic_inputs',''), True)
            #----------vic input directory created for basin if not exist---------#
        except:
            rat_logger.exception("Error Executing Step-2: Pre-processing of data and preparation of MetSim Input")
        else:
            rat_logger.info("Finished Step-2: Pre-processing of data and preparation of MetSim Input")
            ##----------------------- Pre-processing step finished for METSIM ------------------------##

    ######### Step-3
    if(3 in steps):
        try:
            rat_logger.info("Starting Step-3: Preparation of MetSim Parameter Files")
            ##----------------------- Preparing parameter files for METSIM ------------------------##
            ## ---------- Creating domain.nc file for basin if not exist---------#
            domain_nc_path = os.path.join(metsim_inputs_dir,'domain.nc')
            if not os.path.exists(domain_nc_path):
                elevation_tif_filepath = config['GLOBAL']['elevation_tif_file']
                create_basin_domain_nc_file(elevation_tif_filepath,basingridfile_path,domain_nc_path)
            #----------domain.nc file created for basin if not exist ---------#
        except:
            rat_logger.exception("Error Executing Step-3: Preparation of MetSim Parameter Files")
        else:
            rat_logger.info("Finished Step-3: Preparation of MetSim Parameter Files")
            ##----------------------- Prepared parameter files for METSIM ------------------------##

    ######### Step-4
    if(4 in steps):
        try:
            if(NEW_DATA_STATUS):
                rat_logger.info("Starting Step-4: Running MetSim & preparation of VIC input")
                ##-------------- Metsim Begin & Pre-processing for VIC --------------##
                with MSParameterFile(
                    start= config['BASIN']['start'],
                    end= config['BASIN']['end'],
                    init_param= config['METSIM']['metsim_param_file'],
                    out_dir= metsim_output_path, 
                    forcings= ms_input_data, 
                    state= ms_state,
                    domain= domain_nc_path
                    ) as m:
                    
                    ms = MetSimRunner(
                        param_path= m.ms_param_path,
                        metsim_env= config['METSIM']['metsim_env'],
                        results_path= m.results,
                        multiprocessing= config['GLOBAL']['multiprocessing']
                    )
                    log.log(NOTIFICATION, f'Starting metsim from {config["BASIN"]["start"].strftime("%Y-%m-%d")} to {config["BASIN"]["end"].strftime("%Y-%m-%d")}')
                    ms.run_metsim()
                    ms.convert_to_vic_forcings(vic_input_path)
                METSIM_STATUS=1    #Metsim run successfully
            else:
                rat_logger.info("New Data Download Failed. Skipping Step-4: Running MetSim & preparation of VIC input")
        except:
            rat_logger.exception("Error Executing Step-4: Running MetSim & preparation of VIC input")
        else:
            rat_logger.info("Finished Step-4: Running MetSim & preparation of VIC input")
            ##--------------- Metsim End & Pre-processing for VIC done--------------##


     ######### Step-5
    if(5 in steps):
        try:
            rat_logger.info("Starting Step-5: Preparation of VIC Parameter Files")
            ##--------------- Preparation of Vic Parameter Files begin--------------##
            # If Vic Parameter is globally available and needs to be cropped
            if(config['VIC']['vic_global_data']):
                # Creating if not exist vic_basin_params dir 
                vic_param_dir = os.path.join(basin_data_dir,'vic', 'vic_basin_params','')
                create_directory(vic_param_dir)

                # Creating vic soil param and domain file if not present
                if not os.path.exists(os.path.join(vic_param_dir,'vic_param.nc')):
                    global_vic_param_file = os.path.join(config['VIC']['vic_global_param_dir'],config['VIC']['vic_basin_continent_param_filename'])
                    global_vic_domain_file = os.path.join(config['VIC']['vic_global_param_dir'],config['VIC']['vic_basin_continent_domain_filename'])

                    create_vic_domain_param_file(global_vic_param_file,global_vic_domain_file,basingridfile_path,vic_param_dir)
                
                # Setting vic soil_param_file and domain file paths in config, will be used in vic_params produced by VICParameterFile
                config['VIC']['vic_soil_param_file']=os.path.join(vic_param_dir,'vic_soil_param.nc')
                config['VIC']['vic_domain_file']=os.path.join(vic_param_dir,'vic_domain.nc')

            vic_input_forcing_path = os.path.join(vic_input_path,'forcing_')
            vic_output_path = create_directory(os.path.join(basin_data_dir,'vic','vic_outputs',''), True)
            rout_input_path = create_directory(os.path.join(basin_data_dir,'routing','rout_inputs',''), True)
            rout_input_state_file = create_directory(os.path.join(basin_data_dir,'routing','rout_state_file',''), True)
            rout_input_state_file = os.path.join(rout_input_state_file,'combined_input_state_file.nc')
        except:
            rat_logger.exception("Error Executing Step-5: Preparation of VIC Parameter Files")
        else:
            rat_logger.info("Finished Step-5: Preparation of VIC Parameter Files")
            ##--------------- Preparation of Vic Parameter Files end--------------##
    
    ######### Step-6
    if(6 in steps):
        try:
            if(METSIM_STATUS):
                rat_logger.info("Starting Step-6: Running of VIC and preparation of Routing input")
                ##--------------- VIC Begin & Pre-processing for Routing ----------------## 
                with VICParameterFile(
                    config = config,
                    basin_name = basin_name,
                    startdate = config['BASIN']['start'],
                    enddate = config['BASIN']['end'],
                    vic_output_path = vic_output_path,
                    forcing_prefix = vic_input_forcing_path,
                    init_state_date = vic_init_state_date
                ) as p:
                    vic = VICRunner(
                        vic_env= config['VIC']['vic_env'],
                        param_file= p.vic_param_path,
                        vic_result_file= p.vic_result_file,
                        rout_input_dir= rout_input_path
                    )
                    vic.run_vic(np=config['GLOBAL']['multiprocessing'])
                    vic.generate_routing_input_state(ndays=365, rout_input_state_file=rout_input_state_file)
                    if(config['BASIN']['first_run']):
                        vic.disagg_results(rout_input_state_file=p.vic_result_file)       # If first run, use vic result file
                    else:
                        vic.disagg_results(rout_input_state_file=rout_input_state_file)    # If not first run, use rout input state file
                    vic_startdate = p.vic_startdate
                    vic_enddate = p.vic_enddate
                VIC_STATUS=1         #Vic run successfully
            else:
                rat_logger.info("MetSim Run Failed. Skipping Step-6: Running of VIC and preparation of Routing input")
        except:
            rat_logger.exception("Error Executing Step-6: Running of VIC and preparation of Routing input")
        else:
            rat_logger.info("Finished Step-6: Running of VIC and preparation of Routing input")
            ##---------------- VIC End & Results pre-processed for Routing-----------------##


    ######### Step-7
    if(7 in steps):
        try:    
            rat_logger.info("Starting Step-7: Preparation of Routing Parameter Files")
            ##--------------- Preparation of Routing Parameter Files begin--------------##
            # Routing_input files prefix path
            rout_input_path_prefix = os.path.join(rout_input_path,'fluxes_')

            # Creating routing parameter directory
            rout_param_dir = create_directory(os.path.join(basin_data_dir,'routing','rout_basin_params',''), True)

            ### Basin Grid Flow Firection File
            # Defining path and name for basin flow direction file
            basin_flow_dir_file = os.path.join(rout_param_dir,'basin_flow_dir')
            # Creating basin grid flow diretion file if not present
            if (config['ROUTING'].get('global_flow_dir_tif_file')):
                if not os.path.exists(basin_flow_dir_file+'.asc'):
                    create_basin_grid_flow_asc(config['ROUTING']['global_flow_dir_tif_file'], basingridfile_path, basin_flow_dir_file,
                                                                    config['ROUTING'].get('replace_flow_directions'))
            basin_flow_dir_file = basin_flow_dir_file+'.asc'

            ### Basin Station File
            basin_station_latlon_file = os.path.join(rout_param_dir,'basin_station_latlon.csv')
            if (config['ROUTING']['station_global_data']):
                if not os.path.exists(basin_station_latlon_file):
                    create_basin_station_latlon_csv(basin_name, config['ROUTING']['stations_vector_file'], basin_data, 
                                                        config['ROUTING']['stations_vector_file_columns_dict'], basin_station_latlon_file)
            else:
                basin_station_latlon_file = config['ROUTING']['station_latlon_path']

            # Creating routing inflow directory
            rout_inflow_dir = create_directory(os.path.join(basin_data_dir,'routing', 'rout_inflow',''), True)
        except:
            rat_logger.exception("Error Executing Step-7: Preparation of Routing Parameter Files")
        else:    
            rat_logger.info("Finished Step-7: Preparation of Routing Parameter Files")
            ##--------------- Preparation of Routing Parameter Files end--------------##

    ######### Step-8
    if(8 in steps):
        try:
            if(VIC_STATUS):
                rat_logger.info("Starting Step-8: Runnning Routing and generating Inflow")
                #------------- Routing Begins and Pre processing for Mass Balance --------------#
                with RouteParameterFile(
                    config = config,
                    basin_name = basin_name,
                    start = config['BASIN']['start'],
                    end = config['BASIN']['end'],
                    basin_flow_direction_file = basin_flow_dir_file,
                    clean=False,
                    rout_input_path_prefix = rout_input_path_prefix
                    ) as r:
                    route = RoutingRunner(    
                        project_dir = config['GLOBAL']['project_dir'], 
                        result_dir = r.params['output_dir'], 
                        inflow_dir = rout_inflow_dir, 
                        model_path = config['ROUTING']['route_model'],
                        param_path = r.route_param_path, 
                        fdr_path = r.params['flow_direction_file'], 
                        station_path_latlon = basin_station_latlon_file,
                        station_xy = r.params['station']
                    )
                    route.create_station_file()
                    route.run_routing()
                    route.generate_inflow()
                    basin_station_xy_path = route.station_path_xy
                ROUTING_STATUS=1
            else:
                rat_logger.info("VIC Run Failed. Skipping Step-8: Runnning Routing and generating Inflow")
        except:
            rat_logger.exception("Error Executing Step-8: Runnning Routing and generating Inflow")
        else:
            rat_logger.info("Finished Step-8: Runnning Routing and generating Inflow")
            #------------- Routing Ends and Inflow pre-processed for Mass Balance --------------#

    ######### Step-9
    if(9 in steps):
        try:
            rat_logger.info("Starting Step-9: Preparation of parameter files for Surface Area Calculation")
            #------------- Selection of Reservoirs within the basin begins--------------#
            ###### Preparing basin's reservoir shapefile and it's associated column dictionary for calculating surface area #####
            ### Creating Basin Reservoir Shapefile, if not exists ###
            reservoirs_gdf_column_dict = config['GEE']['reservoir_vector_file_columns_dict']

            basin_reservoir_shpfile_path = create_directory(os.path.join(basin_data_dir,'gee','gee_basin_params',''), True)
            basin_reservoir_shpfile_path = os.path.join(basin_reservoir_shpfile_path,'basin_reservoirs.shp')
            if not os.path.exists(basin_reservoir_shpfile_path):
                create_basin_reservoir_shpfile(config['GEE']['reservoir_vector_file'], reservoirs_gdf_column_dict, basin_station_xy_path,
                                                                                config['ROUTING']['station_global_data'], basin_reservoir_shpfile_path)
            ### Creating Basin Reservoir Shapefile's column dictionary ### 
            if (config['ROUTING']['station_global_data']):
                reservoirs_gdf_column_dict['unique_identifier'] = 'uniq_id'
            else:
                reservoirs_gdf_column_dict['unique_identifier'] = reservoirs_gdf_column_dict['dam_name_column']
            ### Defining paths to save surface area from gee and heights from altimetry
            sarea_savepath = create_directory(os.path.join(basin_data_dir,'gee','gee_sarea_tmsos',''), True)
            altimetry_savepath = os.path.join(basin_data_dir,'altimetry','altimetry_timeseries')
            
            ###### Prepared basin's reservoir shapefile and it's associated column dictionary #####
        except:
            rat_logger.exception("Error Executing Step-9: Preparation of parameter files for Surface Area Calculation")
        else:
            rat_logger.info("Finished Step-9: Preparation of parameter files for Surface Area Calculation")
            #------------- Selection of Reservoirs within the basin ends--------------#

    ######### Step-10
    if(10 in steps):
        try:
            rat_logger.info("Starting Step-10: TMS-OS Surface Area Calculation from GEE")
            ##----------- Remote Sensing to estimate surface area begins -----------##
            # Get Sarea
            run_sarea(config['BASIN']['start'].strftime("%Y-%m-%d"), config['BASIN']['end'].strftime("%Y-%m-%d"), sarea_savepath, 
                                                                                    basin_reservoir_shpfile_path, reservoirs_gdf_column_dict)
            GEE_STATUS = 1
        except:
            rat_logger.exception("Error Executing Step-10: TMS-OS Surface Area Calculation from GEE")
        else:
            rat_logger.info("Finished Step-10: TMS-OS Surface Area Calculation from GEE")                                                                        
            ##----------- Remote Sensing to estimate surface area ends -----------##
    
    ######### Step-11
    if(11 in steps):
        try:
            rat_logger.info("Starting Step-11: Elevation extraction from Altimeter")
            ##----------- Altimeter height ectraction begins -----------##
            # Altimeter
            latest_altimetry_cycle = run_altimetry(config, 'ALTIMETER', basin_reservoir_shpfile_path, reservoirs_gdf_column_dict, 
                                                                                    basin_name, basin_data_dir, altimetry_savepath)
            ALTIMETER_STATUS = 1
        except:
            rat_logger.exception("Error Executing Step-11: Elevation extraction from Altimeter")
        else:
            rat_logger.info("Finished Step-11: Elevation extraction from Altimeter")                                                                        
            ##----------- Altimeter height ectraction ends -----------##

    ######### Step-12
    if(12 in steps):
        try:
            rat_logger.info("Starting Step-12: Generating Area Elevation Curves for reservoirs")
            ##--------------------------------Area Elevation Curves Extraction begins ------------------- ##
            ## Creating AEC files if not present for post-processing dels calculation
            if (config['POST_PROCESSING'].get('aec_dir')):
                aec_dir_path = config['POST_PROCESSING'].get('aec_dir')
            else:
                aec_dir_path = create_directory(os.path.join(basin_data_dir,'post_processing','post_processing_gee_aec',''), True)
            aec_file_creator(basin_reservoir_shpfile_path,reservoirs_gdf_column_dict,aec_dir_path)

            ## Paths for storing post-processed data and in webformat data
            evap_savedir = create_directory(os.path.join(basin_data_dir,'rat_outputs', "Evaporation"), True)
            dels_savedir = create_directory(os.path.join(basin_data_dir,'rat_outputs', "dels"), True)
            outflow_savedir = create_directory(os.path.join(basin_data_dir,'rat_outputs', "rat_outflow"),True)
            web_dir_path = create_directory(os.path.join(basin_data_dir,'web_format_data',''),True)
            ## End of defining paths for storing post-processed data and webformat data
        except:
            rat_logger.exception("Finished Step-12: Generating Area Elevation Curves for reservoirs")
        else:
            rat_logger.info("Finished Step-12: Generating Area Elevation Curves for reservoirs")
            ##--------------------------------Area Elevation Curves Extraction ends ------------------- ##

    ######### Step-13
    if(13 in steps):
        try:
            rat_logger.info("Starting Step-13: Calculation of Outflow, Evaporation and Storage change")
            
            ##---------- Mass-balance Approach begins and then post-processing ----------##
            DELS_STATUS, EVAP_STATUS, OUTFLOW_STATUS = run_postprocessing(basin_name, basin_data_dir, basin_reservoir_shpfile_path, reservoirs_gdf_column_dict,
                                aec_dir_path, config['BASIN']['start'], config['BASIN']['end'], evap_savedir, dels_savedir, outflow_savedir, VIC_STATUS, ROUTING_STATUS, GEE_STATUS)

            # Convert to format that is expected by the website and save in web_dir
            ## Surface Area
            if(GEE_STATUS):
                convert_sarea(sarea_savepath,web_dir_path)
                rat_logger.info("Converted Surface Area to the Output Format.")
            else:
                rat_logger.info("Could not convert Surface Area to the Output Format as GEE run failed.")
            
            ## Inflow
            if(ROUTING_STATUS):
                convert_inflow(rout_inflow_dir, basin_reservoir_shpfile_path, reservoirs_gdf_column_dict, web_dir_path)
                rat_logger.info("Converted Inflow to the Output Format.")
            else:
                rat_logger.info("Could not convert Inflow to the Output Format as Routing run failed.")
            
            ## Dels 
            if(DELS_STATUS):
                convert_dels(dels_savedir, web_dir_path)
                rat_logger.info("Converted ∆S to the Output Format.")
            else:
                rat_logger.info("Could not convert ∆S to the Output Format as GEE run failed.")
            
            ## Evaporation
            if(EVAP_STATUS):
                convert_evaporation(evap_savedir, web_dir_path)
                rat_logger.info("Converted Evaporation to the Output Format.")
            else:
                rat_logger.info("Could not convert Evaporation to the Output Format as either GEE or VIC run failed.")

            ## Outflow
            if(OUTFLOW_STATUS):
                convert_outflow(outflow_savedir, web_dir_path)
                rat_logger.info("Converted Outflow to the Output Format.")
            else:
                rat_logger.info("Could not convert Outflow to the Output Format as either GEE or Routing run failed resulting in failure of calculation of either Inflow, ∆S or Evaporation.")
            
            ## Altimeter
            if(ALTIMETER_STATUS):
                convert_altimeter(altimetry_savepath, web_dir_path)
                rat_logger.info("Converted Extracted Height from Altimeter to the Output Format.")
            else:
                rat_logger.info("Could not convert Extracted Height from Altimeter to the Output Format as Altimeter Run failed.")
        except:
            rat_logger.exception("Error Executing Step-13: Calculation of Outflow, Evaporation and Storage change")
        else:
            rat_logger.info("Finished Step-13: Calculation of Outflow, Evaporation and Storage change")
            ##---------- Mass-balance Approach ends and then post-processed outputs to obtain timeseries  -----------------##

    close_logger()