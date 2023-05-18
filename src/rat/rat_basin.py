from datetime import datetime
import os
from logging import getLogger
import datetime
import geopandas as gpd
import numpy as np
import xarray as xr
from pathlib import Path

from rat.utils.utils import create_directory
from rat.utils.logging import init_logger,close_logger,NOTIFICATION
from rat.utils.files_creator import create_basingridfile, create_basin_domain_nc_file ,create_vic_domain_param_file, create_basin_grid_flow_asc, create_basin_station_geojson
from rat.utils.files_creator import create_basin_station_latlon_csv, create_basin_reservoir_shpfile
from rat.utils.clean import Clean

from rat.data_processing.newdata import get_newdata

from rat.data_processing.metsim_input_processing import CombinedNC,generate_state_and_inputs
from rat.utils.metsim_param_reader import MSParameterFile
from rat.core.run_metsim import MetSimRunner

from rat.utils.vic_param_reader import VICParameterFile
from rat.core.run_vic import VICRunner

from rat.utils.route_param_reader import RouteParameterFile
from rat.core.run_routing import RoutingRunner, generate_inflow, run_routing

from rat.core.run_altimetry import run_altimetry

from rat.core.run_postprocessing import run_postprocessing

from rat.utils.convert_to_final_outputs import convert_sarea, convert_inflow, convert_dels, convert_evaporation, convert_outflow, convert_altimeter

# Step-(-1): Reading Configuration settings to run RAT
# Step-0: Creating required directory structure for RAT
# Step-1: Downloading and Pre-processing of meteorolgical data
# Step-2: Pre-processing of data and preparation of MetSim Input
# Step-3: Preparation of MetSim Parameter Files
# Step-4: Running MetSim & preparation of VIC input
# Step-5: Preparation of VIC Parameter Files
# Step-6: Running of VIC and preparation of Routing input
# Step-7: Preparation of Routing Parameter Files
# Step-8: Runnning Routing model
# Step-9: Preparation of parameter files for Surface Area Calculation
# Step-10: TMS-OS Surface Area Calculation from GEE 
# Step-11: Elevation extraction from Altimeter
# Step-12: Generating Area Elevation Curves for reservoirs
# Step-13: Calculation of Inflow, Outflow, Evaporation and Storage change
# Step-14: Conversion of output data to final format as time series

#TODO: Converting steps to separate modules to make RAT more robust and generalized
#module-1 step-1,2 data_preparation_vic
#module-2 step-3to8 inflow_vic
#module-3 step- NA evaporation
#module-4 step-10to12 storage_change
#module-5 step-13 outflow (mass-balance)
#RAT using all modules and step-14 to produce final outputs

def rat_basin(config, rat_logger):
    """Runs RAT as per configuration defined in `config_fn` for one single basin.

    parameters:
        config_fn (str): Path to the configuration file for one basin and not for multiple basins. To run rat for multiple basins use run_rat()
    
    returns:
        no_errors (int): Number of errors/exceptions occurred while running rat
        latest_altimetry_cycle (int): Latest altimetry jason3 cycle number if rat runs step 11
    """
    rat_logger = getLogger('run_rat')

    # Tracking number of errors to output
    no_errors = 0

    # Tracking latest altimetry cycle number if step 11 is run, otherwise return None
    latest_altimetry_cycle = None

    ######### Step -1 Mandatory Step
    try:
        rat_logger.info("Reading Configuration settings to run RAT")
        ##--------------------- Reading and initialising global parameters ----------------------##

        # Reading steps to be executed in RAT otherwise seting default value
        if config['GLOBAL'].get('steps'):
            steps = config['GLOBAL']['steps']
        else:
            steps = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]

        # Defining resolution to run RAT
        xres = 0.0625
        yres = 0.0625 

        # Obtaining basin related information from RAT_Runner.yml
        basins_shapefile_path = config['GLOBAL']['basin_shpfile'] # Shapefile containg information of basin(s)- geometry and attributes
        basins_shapefile = gpd.read_file(basins_shapefile_path)  # Reading basins_shapefile_path to get basin polygons and their attributes
        basins_shapefile_column_dict = config['GLOBAL']['basin_shpfile_column_dict'] # Dictionary of column names in basins_shapefile, Must contain 'id' field
        region_name = config['BASIN']['region_name']  # Major basin name used to cluster multiple basins data in data-directory
        basin_name = config['BASIN']['basin_name']              # Basin name used to save basin related data
        basin_id = config['BASIN']['basin_id']                  # Unique identifier for each basin used to map basin polygon in basins_shapefile
        basin_data = basins_shapefile[basins_shapefile[basins_shapefile_column_dict['id']]==basin_id] # Getting the particular basin related information corresponding to basin_id
        basin_bounds = basin_data.bounds                          # Obtaining bounds of the particular basin
        basin_bounds = np.array(basin_bounds)[0]
        basin_geometry = basin_data.geometry                      # Obtaining geometry of the particular basin

        # Defining paths for RAT processing
        project_dir = config['GLOBAL']['project_dir']    # Directory of RAT 
        data_dir = config['GLOBAL']['data_dir']          # Data-Directory of RAT
        region_data_dir = create_directory(os.path.join(data_dir,region_name), True) # Major Basin data-directory within the data-directory of RAT
        basin_data_dir = create_directory(os.path.join(region_data_dir,'basins',basin_name), True)  # Basin data-directory within the major basin's data-directory 
        log_dir = create_directory(os.path.join(region_data_dir,'logs',basin_name,''), True)  # Log directory within the major basin's data-directory

        # Change datetimes format
        #config['BASIN']['begin'] = datetime.datetime.combine(config['BASIN']['begin'], datetime.time.min)
        config['BASIN']['start'] = datetime.datetime.combine(config['BASIN']['start'], datetime.time.min)
        config['BASIN']['end'] = datetime.datetime.combine(config['BASIN']['end'], datetime.time.min)
        rout_init_state_save_date = config['BASIN']['end']

        if(not config['BASIN']['spin_up']):
            if(config['BASIN'].get('vic_init_state_date')):
                config['BASIN']['vic_init_state_date'] = datetime.datetime.combine(config['BASIN']['vic_init_state_date'], datetime.time.min)
        
        # Changing start date if running RAT for first time for the particular basin to give VIC and MetSim to have their spin off periods
        if(config['BASIN']['spin_up']):
            user_given_start = config['BASIN']['start']
            config['BASIN']['start'] = user_given_start-datetime.timedelta(days=800)  # Running RAT for extra 800 days before the user-given start date for VIC to give reliable results starting from user-given start date
            data_download_start = config['BASIN']['start']-datetime.timedelta(days=90)    # Downloading 90 days of extra meteorological data for MetSim to prepare it's initial state
            vic_init_state_date = None    # No initial state of VIC is present as running RAT for first time in this basin
            use_state = False            # Routing state file won't be used
        elif(config['BASIN'].get('vic_init_state_date')):
            data_download_start = config['BASIN']['start']    # Downloading data from the same date as we want to run RAT from
            vic_init_state_date = config['BASIN']['vic_init_state_date'] # Date of which initial state of VIC for the particular basin exists
            use_state = True           # Routing state file will be used
        else:
            data_download_start = config['BASIN']['start']-datetime.timedelta(days=90)    # Downloading 90 days of extra meteorological data for MetSim to prepare it's initial state
            vic_init_state_date = None    # No initial state of VIC is present as running RAT for first time in this basin
            use_state = False            # Routing state file won't be used

        # Defining logger
        log = init_logger(
            log_dir= log_dir,
            verbose= False,
            # notify= True,
            notify= False,
            log_level= 'DEBUG'
        )

        # Cleaning class object for removing/deleting unwanted data
        cleaner = Clean(basin_data_dir)

        # Clearing out previous rat outputs so that the new data does not gets appended.
        if(config['CLEAN_UP']['clean_previous_outputs']):
            rat_logger.info("Clearing up memory space: Removal of previous rat outputs, routing inflow, extracted altimetry data and gee extracted surface area time series")
            cleaner.clean_previous_outputs()

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
    except:
        no_errors = -1
        rat_logger.exception("Error in Configuration parameters defined to run RAT.")
        return (no_errors, latest_altimetry_cycle)
    else:
        rat_logger.info("Read Configuration settings to run RAT.")
        ##--------------------- Read and initialised global parameters ----------------------##

    rat_logger.info(f"Running RAT from {config['BASIN']['start'] } to {config['BASIN']['end']}")

    ######### Step-0 Mandatory Step
    try:
        rat_logger.info("Creating required directory structure for RAT")
        
        ##--------------------- Defining global paths and variables ----------------------##

        #----------- Paths Necessary for running of METSIM  -----------#
        # Path of directory which will contain the combined data in nc format.
        combined_datapath = create_directory(os.path.join(basin_data_dir,'pre_processing','nc', ''), True)
        combined_datapath = os.path.join(combined_datapath, 'combined_data.nc')
        # Path where the processed downloaded data is present
        processed_datadir = os.path.join(basin_data_dir,'pre_processing','processed')
        # basingrid_file path to clip the global downloaded data
        basingridfile_path= create_directory(os.path.join(basin_data_dir, 'basin_grid_data',''), True)
        basingridfile_path= os.path.join(basingridfile_path, basin_name+'_grid_mask.tif')
        #Creating metsim input directory for basin if not exist
        metsim_inputs_dir = create_directory(os.path.join(basin_data_dir, 'metsim', 'metsim_inputs', ''),True)
        #Defining paths for metsim input data, metsim state and metsim domain
        ms_state = os.path.join(metsim_inputs_dir, 'state.nc')
        ms_input_data = os.path.join(metsim_inputs_dir,'metsim_input.nc')
        domain_nc_path = os.path.join(metsim_inputs_dir,'domain.nc')
        #Creating metsim output directory for basin if not exist
        metsim_output_path = create_directory(os.path.join(basin_data_dir, 'metsim', 'metsim_outputs',''), True)
        #Creating vic input directory for basin if not exist
        vic_input_path = create_directory(os.path.join(basin_data_dir, 'vic', 'vic_inputs',''), True)
        #----------- Paths Necessary for running of METSIM  -----------#

        #----------- Paths Necessary for running of VIC  -----------#
        vic_input_forcing_path = os.path.join(vic_input_path,'forcing_')
        vic_output_path = create_directory(os.path.join(basin_data_dir,'vic','vic_outputs',''), True)
        rout_input_path = create_directory(os.path.join(basin_data_dir,'ro','in',''), True)
        rout_input_state_folder = create_directory(os.path.join(basin_data_dir,'ro','rout_state_file',''), True)
        rout_input_state_file = os.path.join(rout_input_state_folder,'ro_init_state_file_'+config['BASIN']['start'].strftime("%Y-%m-%d")+'.nc')
        rout_init_state_save_file = os.path.join(rout_input_state_folder,'ro_init_state_file_'+rout_init_state_save_date.strftime("%Y-%m-%d")+'.nc')
        #----------- Paths Necessary for running of VIC  -----------#

        #----------- Paths Necessary for running of Routing  -----------#
        # Routing_input files prefix path
        rout_input_path_prefix = os.path.join(rout_input_path,'fluxes_')
        # Creating routing parameter directory
        rout_param_dir = create_directory(os.path.join(basin_data_dir,'ro','pars',''), True)
        # routing workspace directory
        routing_wkspc_dir = Path(config['GLOBAL']['data_dir']).joinpath(config['BASIN']['region_name'], 'basins', basin_name, 'ro','wkspc')
        routing_wkspc_dir.mkdir(parents=True, exist_ok=True)
        # Creating routing inflow directory
        routing_output_dir = Path(config['GLOBAL']['data_dir']).joinpath(config['BASIN']['region_name'], 'basins', basin_name, 'ro','ou')
        routing_output_dir.mkdir(parents=True, exist_ok=True)
        inflow_dst_dir = Path(config['GLOBAL']['data_dir']).joinpath(config['BASIN']['region_name'], 'basins', basin_name, 'rat_outputs', 'inflow')
        inflow_dst_dir.mkdir(parents=True, exist_ok=True)
        # Defining path and name for basin flow direction file
        basin_flow_dir_file = os.path.join(rout_param_dir,'fl.asc')
        # Defining Basin station latlon file path
        if (config['ROUTING']['station_global_data']):
            basin_station_latlon_file = os.path.join(rout_param_dir,'basin_station_latlon.csv')
        else:
            basin_station_latlon_file = config['ROUTING']['station_latlon_path']
            basin_station_geojson_file = os.path.join(rout_param_dir,'station.geojson')
        #----------- Paths Necessary for running of Routing  -----------#

        #----------- Paths Necessary for running of Surface Area Calculation and Altimetry-----------#
        # Defining routing station file
        if(config['ROUTING PARAMETERS'].get('station_file')):
            basin_station_xy_path = config['ROUTING PARAMETERS'].get('station_file')
        else:
            basin_station_xy_path = os.path.join(basin_data_dir,'ro', 'pars','sta_xy.txt')
        # Defining path for basin reservoir shapefile
        basin_reservoir_shpfile_path = create_directory(os.path.join(basin_data_dir,'gee','gee_basin_params',''), True)
        basin_reservoir_shpfile_path = os.path.join(basin_reservoir_shpfile_path,'basin_reservoirs.shp')
        # Reading dictionary of column values for reservoir shapefile path
        reservoirs_gdf_column_dict = config['GEE']['reservoir_vector_file_columns_dict']
        # Adding key-value pair to Basin Reservoir Shapefile's column dictionary ### 
        if (config['ROUTING']['station_global_data']):
            reservoirs_gdf_column_dict['unique_identifier'] = 'uniq_id'
        else:
            reservoirs_gdf_column_dict['unique_identifier'] = reservoirs_gdf_column_dict['dam_name_column']
        # Defining paths to save surface area from gee and heights from altimetry
        sarea_savepath = create_directory(os.path.join(basin_data_dir,'gee','gee_sarea_tmsos',''), True)
        altimetry_savepath = os.path.join(basin_data_dir,'altimetry','altimetry_timeseries')
        #----------- Paths Necessary for running of Surface Area Calculation and Altimetry-----------#

        #----------- Paths Necessary for running of Post-Processing-----------#
        # Defining path for the Area Elevation curve
        if (config['POST_PROCESSING'].get('aec_dir')):
            aec_dir_path = config['POST_PROCESSING'].get('aec_dir')
        else:
            aec_dir_path = create_directory(os.path.join(basin_data_dir,'post_processing','post_processing_gee_aec',''), True)
        ## Paths for storing post-processed data and in webformat data
        evap_savedir = create_directory(os.path.join(basin_data_dir,'rat_outputs', "Evaporation"), True)
        dels_savedir = create_directory(os.path.join(basin_data_dir,'rat_outputs', "dels"), True)
        outflow_savedir = create_directory(os.path.join(basin_data_dir,'rat_outputs', "rat_outflow"),True)
        final_output_path = create_directory(os.path.join(basin_data_dir,'final_outputs',''),True)
        ## End of defining paths for storing post-processed data and webformat data
        #----------- Paths Necessary for running of Post-Processing-----------#
    except:
        no_errors = -1
        rat_logger.exception("Error in creating required directory structure for RAT")
        return (no_errors, latest_altimetry_cycle)
    else:
        rat_logger.info("Finished creating required directory structure for RAT")
        ##--------------------- Definied global paths and variables ----------------------##

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
            no_errors = no_errors+1
            rat_logger.exception("Error Executing Step-1: Downloading and Pre-processing of meteorological data")
        else:
            rat_logger.info("Finished Step-1: Downloading and Pre-processing of meteorological data")
            ##----------------------- Downloaded meteorological data ------------------------##


    ######### Step-2
    if(2 in steps):
        try:
            rat_logger.info("Starting Step-2: Pre-processing of data and preparation of MetSim Input")
            ##----------------------- Pre-processing step for METSIM ------------------------##
            #----------- Files Necessary for creating METSIM Input Data  -----------#
            # Creating basinggrid_file if not exists
            if not os.path.exists(basingridfile_path):
                create_basingridfile(basin_bounds,basin_geometry,basingridfile_path,xres,yres)
            #----------- Created Files Necessary for creating METSIM Input Data  -----------#

            #----------- Process Data Begin to combine all var data -----------#
            CombinedNC(
                start= data_download_start,
                end= config['BASIN']['end'],
                datadir= processed_datadir,
                basingridpath= basingridfile_path,
                outputdir= combined_datapath,
                use_previous= use_state,
            )
            #----------- Process Data End and combined data created -----------#

            #------ MetSim Input Data Preparation Begin ------#
            # Prepare data to metsim input format
            ms_state, ms_input_data = generate_state_and_inputs(
                forcings_startdate= config['BASIN']['start'],
                forcings_enddate= config['BASIN']['end'],
                combined_datapath= combined_datapath, 
                out_dir= metsim_inputs_dir
            )
            #------- MetSim Input Data Preparation End -------#
        except:
            no_errors = no_errors+1
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
            if(config['GLOBAL'].get('elevation_tif_file')):
                if not os.path.exists(domain_nc_path):
                    elevation_tif_filepath = config['GLOBAL']['elevation_tif_file']
                    create_basin_domain_nc_file(elevation_tif_filepath,basingridfile_path,domain_nc_path)
            else:
                domain_nc_path = config['METSIM']['metsim_domain_file']
            #----------domain.nc file created for basin if not exist ---------#
        except:
            no_errors = no_errors+1
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
            no_errors = no_errors+1
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
        except:
            no_errors = no_errors+1
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
                    vic.generate_routing_input_state(ndays=365, rout_input_state_file=rout_input_state_file, 
                                                                                        save_path=rout_init_state_save_file, use_rout_state=use_state) # Start date of routing state file will be returned
                    if(config['BASIN']['spin_up']):
                        vic.disagg_results(rout_input_state_file=p.vic_result_file)       # If spin_up, use vic result file
                    elif(config['BASIN'].get('vic_init_state_date')):                      # If vic_state file exists
                        vic.disagg_results(rout_input_state_file=rout_init_state_save_file)    # If not spin_up, use rout_init_state_save file just creates as it contains the recent vic outputs.
                    else:
                        vic.disagg_results(rout_input_state_file=p.vic_result_file)       # If spin_up is false and vic state file does not exist, use vic result file
                    vic_startdate = p.vic_startdate
                    vic_enddate = p.vic_enddate
                VIC_STATUS=1         #Vic run successfully
            else:
                rat_logger.info("MetSim Run Failed. Skipping Step-6: Running of VIC and preparation of Routing input")
        except:
            no_errors = no_errors+1
            rat_logger.exception("Error Executing Step-6: Running of VIC and preparation of Routing input")
        else:
            rat_logger.info("Finished Step-6: Running of VIC and preparation of Routing input")
            ##---------------- VIC End & Results pre-processed for Routing-----------------##


    ######### Step-7
    if(7 in steps):
        try:    
            rat_logger.info("Starting Step-7: Preparation of Routing Parameter Files")
            ##--------------- Preparation of Routing Parameter Files begin--------------##
            ### Basin Grid Flow Firection File
            # Creating basin grid flow diretion file if not present
            if (config['ROUTING'].get('global_flow_dir_tif_file')):
                if not os.path.exists(basin_flow_dir_file):
                    create_basin_grid_flow_asc(config['ROUTING']['global_flow_dir_tif_file'], basingridfile_path, basin_flow_dir_file[:-4],
                                                                    config['ROUTING'].get('replace_flow_directions'))
            ### Basin Station CSV File and Geojson File (for front-end purposes)
            if (config['ROUTING']['station_global_data']):
                if not os.path.exists(basin_station_latlon_file):
                    create_basin_station_latlon_csv(region_name,basin_name, config['ROUTING']['stations_vector_file'], basin_data, 
                                                        config['ROUTING']['stations_vector_file_columns_dict'], basin_station_latlon_file)
            else:
                if(config['ROUTING'].get('station_latlon_path')):
                    create_basin_station_geojson(region_name,basin_name,config['ROUTING']['station_latlon_path'],basin_station_geojson_file)
        except:
            no_errors = no_errors+1
            rat_logger.exception("Error Executing Step-7: Preparation of Routing Parameter Files")
        else:    
            rat_logger.info("Finished Step-7: Preparation of Routing Parameter Files")
            ##--------------- Preparation of Routing Parameter Files end--------------##

    ######### Step-8
    if(8 in steps):
        try:
            if(VIC_STATUS):
                rat_logger.info("Starting Step-8: Runnning Routing model")
                ### Extracting routing start date ###
                if(config['BASIN']['spin_up']):
                    rout_input_state_start_date = config['BASIN']['start']
                elif(config['BASIN'].get('vic_init_state_date')): 
                    rout_state_data = xr.open_dataset(rout_init_state_save_file).load()
                    rout_input_state_start_date = rout_state_data.time[0].values.astype('datetime64[us]').astype(datetime.datetime)
                    rout_state_data.close()
                else:
                    rout_input_state_start_date = config['BASIN']['start']
                ### Extracted routing start date ###
                #------------- Routing Begins and Pre processing for Mass Balance --------------#
                output_paths, basin_station_xy_path, ret_codes = run_routing(
                    config = config,
                    start = rout_input_state_start_date,
                    end = config['BASIN']['end'],
                    basin_flow_direction_file = basin_flow_dir_file,
                    rout_input_path_prefix = rout_input_path_prefix,
                    clean=False,
                    inflow_dir = inflow_dst_dir,
                    station_path_latlon = basin_station_latlon_file,
                )
                ROUTING_STATUS=1
            else:
                rat_logger.info("VIC Run Failed. Skipping Step-8: Runnning Routing and generating Inflow")
        except:
            no_errors = no_errors+1
            rat_logger.exception("Error Executing Step-8: Runnning Routing and generating Inflow")
        else:
            rat_logger.info("Finished Step-8: Runnning Routing model")
            #------------- Routing Ends --------------#

    ######### Step-9
    if(9 in steps):
        try:
            rat_logger.info("Starting Step-9: Preparation of parameter files for Surface Area Calculation")
            #------------- Selection of Reservoirs within the basin begins--------------#
            ###### Preparing basin's reservoir shapefile and it's associated column dictionary for calculating surface area #####
            ### Creating Basin Reservoir Shapefile, if not exists ###
            if not os.path.exists(basin_reservoir_shpfile_path):
                if os.path.exists(basin_station_xy_path):
                    create_basin_reservoir_shpfile(config['GEE']['reservoir_vector_file'], reservoirs_gdf_column_dict, basin_station_xy_path,
                                                                                config['ROUTING']['station_global_data'], basin_reservoir_shpfile_path)
            ###### Prepared basin's reservoir shapefile and it's associated column dictionary #####
        except:
            no_errors = no_errors+1
            rat_logger.exception("Error Executing Step-9: Preparation of parameter files for Surface Area Calculation")
        else:
            rat_logger.info("Finished Step-9: Preparation of parameter files for Surface Area Calculation")
            #------------- Selection of Reservoirs within the basin ends--------------#

    ######### Step-10
    if(10 in steps):
        try:
            from rat.core.run_sarea import run_sarea
            rat_logger.info("Starting Step-10: TMS-OS Surface Area Calculation from GEE")
            ##----------- Remote Sensing to estimate surface area begins -----------##
            if (not os.path.exists(basin_reservoir_shpfile_path)):
                if (not config['ROUTING']['station_global_data']):
                    basin_reservoir_shpfile_path = config['GEE']['reservoir_vector_file']
                else: 
                    raise Exception('There was an error in creating reservoir shapefile using spatial join for this basin from the global reservoir vector file.')
            # Get Sarea
            run_sarea(config['BASIN']['start'].strftime("%Y-%m-%d"), config['BASIN']['end'].strftime("%Y-%m-%d"), sarea_savepath, 
                                                                                    basin_reservoir_shpfile_path, reservoirs_gdf_column_dict)
            GEE_STATUS = 1
        except:
            no_errors = no_errors+1
            rat_logger.exception("Error Executing Step-10: TMS-OS Surface Area Calculation from GEE")
        else:
            rat_logger.info("Finished Step-10: TMS-OS Surface Area Calculation from GEE")                                                                        
            ##----------- Remote Sensing to estimate surface area ends -----------##
    
    ######### Step-11
    if(11 in steps):
        try:
            rat_logger.info("Starting Step-11: Elevation extraction from Altimeter")
            ##----------- Altimeter height extraction begins -----------##
            # Altimeter
            latest_altimetry_cycle = run_altimetry(config, 'ALTIMETER', basin_reservoir_shpfile_path, reservoirs_gdf_column_dict, 
                                                                                    basin_name, basin_data_dir, altimetry_savepath)
            ALTIMETER_STATUS = 1
        except:
            no_errors = no_errors+1
            rat_logger.exception("Error Executing Step-11: Elevation extraction from Altimeter")
        else:
            rat_logger.info("Finished Step-11: Elevation extraction from Altimeter")                                                                        
            ##----------- Altimeter height extraction ends -----------##

    ######### Step-12
    if(12 in steps):
        try:
            from rat.ee_utils.ee_aec_file_creator import aec_file_creator
            rat_logger.info("Starting Step-12: Generating Area Elevation Curves for reservoirs")
            ##--------------------------------Area Elevation Curves Extraction begins ------------------- ##
            ## Creating AEC files if not present for post-processing dels calculation
            aec_file_creator(basin_reservoir_shpfile_path,reservoirs_gdf_column_dict,aec_dir_path)
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
            # Generate inflow files from RAT routing outputs
            for src_dir in list(routing_wkspc_dir.glob('*')):
                if src_dir.is_dir():
                    routing_output_fn = src_dir / 'ou' / f"{str(src_dir.name)[:5] if len(str(src_dir.name)) > 5 else str(src_dir.name)}.day"
                    generate_inflow(src_dir.name, routing_output_fn, inflow_dst_dir)

            DELS_STATUS, EVAP_STATUS, OUTFLOW_STATUS = run_postprocessing(basin_name, basin_data_dir, basin_reservoir_shpfile_path, reservoirs_gdf_column_dict,
                                aec_dir_path, config['BASIN']['start'], config['BASIN']['end'], rout_init_state_save_file, use_state, evap_savedir, dels_savedir, outflow_savedir, VIC_STATUS, ROUTING_STATUS, GEE_STATUS)

        except:
            no_errors = no_errors+1
            rat_logger.exception("Error Executing Step-13: Calculation of Outflow, Evaporation and Storage change")
        else:
            rat_logger.info("Finished Step-13: Calculation of Outflow, Evaporation and Storage change")
            ##---------- Mass-balance Approach ends and then post-processed outputs to obtain timeseries  -----------------##
    
    ######### Step-14
    if(14 in steps):
        try:
            rat_logger.info("Starting Step-14: Creating final outputs in a timeseries format and cleaning up.")

            ##---------- Convert all time-series to final output csv format and clean up----------## 
            ## Surface Area
            if(GEE_STATUS):
                convert_sarea(sarea_savepath,final_output_path)
                rat_logger.info("Converted Surface Area to the Output Format.")
            else:
                rat_logger.info("Could not convert Surface Area to the Output Format as GEE run failed.")
            
            ## Inflow
            if(ROUTING_STATUS):
                convert_inflow(inflow_dst_dir, basin_reservoir_shpfile_path, reservoirs_gdf_column_dict, final_output_path)
                rat_logger.info("Converted Inflow to the Output Format.")
            else:
                rat_logger.info("Could not convert Inflow to the Output Format as Routing run failed.")
            
            ## Dels 
            if(DELS_STATUS):
                convert_dels(dels_savedir, final_output_path)
                rat_logger.info("Converted ∆S to the Output Format.")
            else:
                rat_logger.info("Could not convert ∆S to the Output Format as GEE run failed.")
            
            ## Evaporation
            if(EVAP_STATUS):
                convert_evaporation(evap_savedir, final_output_path)
                rat_logger.info("Converted Evaporation to the Output Format.")
            else:
                rat_logger.info("Could not convert Evaporation to the Output Format as either GEE or VIC run failed.")

            ## Outflow
            if(OUTFLOW_STATUS):
                convert_outflow(outflow_savedir, final_output_path)
                rat_logger.info("Converted Outflow to the Output Format.")
            else:
                rat_logger.info("Could not convert Outflow to the Output Format as either GEE or Routing run failed resulting in failure of calculation of either Inflow, ∆S or Evaporation.")
            
            ## Altimeter
            if(ALTIMETER_STATUS):
                convert_altimeter(altimetry_savepath, final_output_path)
                rat_logger.info("Converted Extracted Height from Altimeter to the Output Format.")
            else:
                rat_logger.info("Could not convert Extracted Height from Altimeter to the Output Format as Altimeter Run failed.")
            
            # Clearing out memory space as per user input 
            if(config['CLEAN_UP'].get('clean_metsim')):
                rat_logger.info("Clearing up memory space: Removal of metsim output files")
                cleaner.clean_metsim()
            if(config['CLEAN_UP'].get('clean_vic')):
                rat_logger.info("Clearing up memory space: Removal of vic input, output files and previous init_state_files")
                cleaner.clean_vic()
            if(config['CLEAN_UP'].get('clean_routing')):
                rat_logger.info("Clearing up memory space: Removal of routing input and output files")
                cleaner.clean_routing()
            if(config['CLEAN_UP'].get('clean_gee')):
                rat_logger.info("Clearing up memory space: Removal of unwanted gee extracted small chunk files")
                cleaner.clean_gee()
            if(config['CLEAN_UP'].get('clean_altimetry')):
                rat_logger.info("Clearing up memory space: Removal of raw altimetry downloaded data files.")
                cleaner.clean_altimetry()
        except:
            no_errors = no_errors+1
            rat_logger.exception("Error Executing Step-14: Creating final outputs in a timeseries format and cleaning up.")
        else:
            rat_logger.info("Finished Step-14: Creating final outputs in a timeseries format and cleaning up.")
            ##----------Converted all time-series to final output csv format and cleaned up----------## 

    close_logger()
    return (no_errors, latest_altimetry_cycle)