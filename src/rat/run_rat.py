from contextlib import redirect_stderr
from io import StringIO
import datetime
import copy
import os

import pandas as pd
import numpy as np
import yaml
import argparse
import configparser
import ee
import ruamel_yaml as ryaml
from pathlib import Path 
from dask.distributed import Client, LocalCluster

from rat.utils.logging import init_logger,close_logger,LOG_LEVEL1_NAME
from rat.utils.vic_init_state_finder import get_vic_init_state_date
import rat.ee_utils.ee_config as ee_configuration
from rat.rat_basin import rat_basin

#------------ Define Variables ------------#
def run_rat(config_fn, operational_latency=None ):
    """Runs RAT as per configuration defined in `config_fn`.

    parameters:
        config_fn (str): Path to the configuration file
        operational_latency (int): Number of days in the past from today to end RAT operational run . RAT won't run operationally if operational_latency is None. Default is None.
    """

    # IMERG Latency (in days) that works fine
    low_latency_limit = 3

    # Reading config with comments
    config_fn = Path(config_fn).resolve()
    ryaml_client = ryaml.YAML()
    config = ryaml_client.load(config_fn.read_text())

    # Logging this run
    log_dir = os.path.join(config['GLOBAL']['data_dir'],'runs','logs','')
    print(f"Logging this run at {log_dir}")
    log = init_logger(
        log_dir,
        verbose=False,
        # notify=True,
        notify=False,
        log_level='DEBUG',
        logger_name=LOG_LEVEL1_NAME,
        for_basin=False
    )

    log.debug("Initiating Dask Client ... ")
    cluster = LocalCluster(name="RAT", n_workers=config['GLOBAL']['multiprocessing'], threads_per_worker=1)
    client = Client(cluster)
    client.forward_logging(logger_name='rat-logger', level='DEBUG')

    log.debug(f"Started client with {config['GLOBAL']['multiprocessing']} workers. Dashboard link: {client.dashboard_link}")

    # Trying the ee credentials given by user
    try:
        log.info("Checking earth engine credentials")
        secrets = configparser.ConfigParser()
        secrets.read(config['CONFIDENTIAL']['secrets'])
        ee_configuration.service_account = secrets["ee"]["service_account"]
        ee_configuration.key_file = secrets["ee"]["key_file"]
        with StringIO() as fake_stderr, redirect_stderr(fake_stderr):
            ee_credentials = ee.ServiceAccountCredentials(ee_configuration.service_account,ee_configuration.key_file)
            ee.Initialize(ee_credentials)
        log.info("Connected to earth engine succesfully.")
    except Exception as e:
        log.error(f"Failed to connect to Earth Engine. RAT will not be able to use Surface Area Estimations. Error: {e}")

    ############################ ----------- Single basin run ---------------- ######################################
    if(not config['GLOBAL']['multiple_basin_run']):
        log.info('############## Starting RAT for '+config['BASIN']['basin_name']+' #################')
        
        # Checking if Rat is running operationally with some latency. If yes, update start, end and vic_init_state dates.
        if operational_latency:
            operational_latency = int(operational_latency)
            ## Calculation of gfs_days
            # If running for more than a latency of 3 days, IMERG data will be there. So data for gfs days is 0.
            if operational_latency>low_latency_limit:
                gfs_days = 0
            # If running for a lower latency of 0-3 days, GFS data will have to be used for 3-0 days.
            else:
                gfs_days = low_latency_limit-operational_latency

            try:
                # RAT has to be run for one overlapping day of IMERG(1) + one new day for IMERG(1) + GFS days
                log.info(f'Running RAT operationally at a latency of {operational_latency} day(s) from today. Updating start and end date.')
                # Record the previous end date
                previous_end_date = copy.deepcopy(config['BASIN']['end'])
                # Find vic_init_state_date from last run
                config['BASIN']['vic_init_state'] = get_vic_init_state_date(previous_end_date, low_latency_limit, config['GLOBAL']['data_dir'],
                                                                             config['BASIN']['region_name'], config['BASIN']['basin_name'])
                if not(config['BASIN']['vic_init_state']):
                    raise Exception('No vic init state file was found from last run.')
                # Start date will be same as Vic init date because RAT will start from the same date as date of VIC's initial state
                config['BASIN']['start'] = copy.deepcopy(config['BASIN']['vic_init_state'])
                config['BASIN']['rout_init_state'] = None
                # End date will be updated to today - latency 
                config['BASIN']['end'] = datetime.datetime.now().date() - datetime.timedelta(days=int(operational_latency))
            except:
                log.exception('Failed to update start and end date for RAT to run operationally. Please make sure RAT has been run atleast once before.')
                return None
        else:
            ## Calculation of gfs_days
            #If not running operationally, check end date and start date's difference from today
            end_date_diff_from_today = datetime.datetime.now().date() - config['BASIN']['end']
            start_date_diff_from_today = datetime.datetime.now().date() - config['BASIN']['start']
            # If difference is more than 3 days, gfs_days will be 0.
            if end_date_diff_from_today > datetime.timedelta(days=int(low_latency_limit)):
                gfs_days = 0
            # Else if start date and today has less than 3 days difference, gfs_days will be start-end
            elif (start_date_diff_from_today<datetime.timedelta(days=int(low_latency_limit))):
                gfs_days = (config['BASIN']['end']-config['BASIN']['start']).days
            # Else gfs_days will be low_latency_limit - difference of end_date from today
            else:
                gfs_days = low_latency_limit - end_date_diff_from_today.days


        # Running RAT (if start < end date)
        if (config['BASIN']['start'] >= config['BASIN']['end']):
            log.error('Sorry, RAT operational run for '+config['BASIN']['basin_name']+' failed.')
            log.error(f"Start date - {config['BASIN']['start']} is before the end date - {config['BASIN']['end']}")
            return None
        else:
            # Overwrite config with updated operational parameters
            ryaml_client.dump(config, config_fn.open('w'))
            # Store deep copy of config as it is mutable
            config_copy = copy.deepcopy(config)
            # Run RAT for basin
            no_errors, latest_altimetry_cycle = rat_basin(config, log, gfs_days=gfs_days)
            # Run RAT forecast for basin if forecast is True           
            if config.get('PLUGINS', {}).get('forecasting'):
                # Importing the forecast module
                try:
                    from plugins.forecasting.forecast_basin import forecast
                except:
                    log.exception("Failed to import Forecast plugin due to missing package(s).")
                log.info('############## Starting RAT forecast for '+config['BASIN']['basin_name']+' #################')
                forecast_no_errors = forecast(config, log, low_latency_limit)
                if(forecast_no_errors>0):
                    log.info('############## RAT-Forecasting run finished for '+config_copy['BASIN']['basin_name']+ ' with '+str(forecast_no_errors)+' error(s). #################')
                elif(forecast_no_errors==0):
                    log.info('############## Succesfully run RAT-Forecasting for '+config_copy['BASIN']['basin_name']+' #################')
                else:
                    log.error('############## RAT-Forecasting run failed for '+config_copy['BASIN']['basin_name']+' #################')
        # Displaying and storing RAT function outputs in the copy (non-mutabled as it was not passes to function)
        if(latest_altimetry_cycle):
            config_copy['ALTIMETER']['last_cycle_number'] = latest_altimetry_cycle
            ryaml_client.dump(config_copy, config_fn.open('w'))
        if(no_errors>0):
            log.info('############## RAT run finished for '+config['BASIN']['basin_name']+ ' with '+str(no_errors)+' error(s). #################')
        elif(no_errors==0):
            log.info('############## Succesfully run RAT for '+config['BASIN']['basin_name']+' #################')
        else:
            log.error('############## RAT run failed for '+config['BASIN']['basin_name']+' #################')

    ######################## ----------- Multiple basin run ---------------- #############################
    else:
        # Reading basins metadata
        try:
            basins_metadata = pd.read_csv(config['GLOBAL']['basins_metadata'],header=[0,1])
        except:
            raise("Please provide the proper path of a csv file in basins_metadata in the Global section of RAT's config file")
        if ('BASIN','run') in basins_metadata.columns:
            basins_metadata_filtered = basins_metadata[basins_metadata['BASIN','run']==1]
        ####### Remove in future version : Deprecation (start)########
        elif (config['GLOBAL'].get('basins_to_process')):
            DeprecationWarning("'basins_to_process' in Global section of RAT has been deprecated and will be removed in future versions. Please use 'run' in BASIN section in basins_metadata file for each basin.")
            if ('BASIN','basin_name') in basins_metadata.columns:
                basins_metadata_filtered = basins_metadata[basins_metadata['BASIN','basin_name'].isin(config['GLOBAL']['basins_to_process'])]
            else:
                raise("No column in 'basins_metadata' file corresponding to 'basin_name' in 'BASIN' section of RAT's config file.")
        ####### Remove in future version : Deprecation (end) ########
        else:
            raise ValueError("Multi-index column ['BASIN']['run'] in 'basins_metadata' file is missing. It is required and should have value either 1 or 0 corresponding to each ['BASIN']['basin_name'].")
        
        if ('BASIN','basin_name') in basins_metadata.columns:
            basins_to_process = basins_metadata_filtered['BASIN','basin_name'].tolist()
        else:
            raise("No column in 'basins_metadata' file corresponding to 'basin_name' in 'BASIN' section of RAT's config file.")

        # For each basin
        for basin in basins_to_process:
            log.info('############## Starting RAT for '+basin+' #################')
            # Checking if Rat is running operationally with some latency. If yes, update start, end and vic_init_state dates.
            if operational_latency:
                operational_latency = int(operational_latency)
                ## Calculation of gfs_days
                # If running for more than a latency of 3 days, IMERG data will be there. So data for gfs days is 0.
                if operational_latency>low_latency_limit:
                    gfs_days = 0
                # If running for a lower latency of 0-3 days, GFS data will have to be used for 3-0 days.
                else:
                    gfs_days = 3-operational_latency

                try:
                    log.info(f'Running RAT operationally at a latency of {operational_latency} day(s) from today. Updating start and end date.')
                    ## If end date is not in basins metadata.columns then it is in config file.
                    if ('BASIN','end') not in basins_metadata.columns:
                        # Record the previous end date
                        previous_end_date = copy.deepcopy(config['BASIN']['end'])
                    else:
                        previous_end_date = basins_metadata['BASIN','end'].loc[basins_metadata['BASIN','basin_name']== basin]
                    # Find vic_init_state_date from last run
                    if ('GLOBAL','data_dir') not in basins_metadata.columns:
                        data_dir = config['GLOBAL']['data_dir']
                    else:
                        data_dir = basins_metadata['GLOBAL','data_dir'].loc[basins_metadata['BASIN','basin_name']== basin]
                    if ('BASIN','region_name') not in basins_metadata.columns:
                        region_name = config['BASIN']['region_name']
                    else:
                        region_name = basins_metadata['GLOBAL','region_name'].loc[basins_metadata['BASIN','basin_name']== basin]
                    if ('BASIN','basin_name') not in basins_metadata.columns:
                        basin_name = config['BASIN']['basin_name']
                    else:
                        basin_name = basins_metadata['GLOBAL','basin_name'].loc[basins_metadata['BASIN','basin_name']== basin]

                    ## Adding [Basin][vic_init_date] to metadata.columns if not there with with None value
                    if ('BASIN','vic_init_state') not in basins_metadata.columns:
                        basins_metadata['BASIN','vic_init_state'] = None
                    # Find vic_init_state
                    vic_init_state_date = get_vic_init_state_date(previous_end_date, low_latency_limit, data_dir,region_name, basin_name)
                    # Check if vic init state is not none, else raise error
                    if not(config['BASIN']['vic_init_state']):
                        raise Exception('No vic init state file was found from last run.')
                    # Replace vic_init_state in basins metadata 
                    basins_metadata['BASIN','vic_init_state'].where(basins_metadata['BASIN','basin_name']!= basin, vic_init_state_date, inplace=True)

                     ## Adding [Basin][start] to metadata.columns if not there with with None value 
                    if ('BASIN','start') not in basins_metadata.columns:
                        basins_metadata['BASIN','start'] = None
                    # Replace start date same as vic_init_date
                    basins_metadata['BASIN','start'].where(basins_metadata['BASIN','basin_name']!= basin, vic_init_state_date, inplace=True)
                    
                    ## Adding [Basin][end] to metadata.columns if not there with with None value 
                    if ('BASIN','end') not in basins_metadata.columns:
                        basins_metadata['BASIN','end'] = None
                    # Replace end date as today - operational latency
                    operational_end_date = datetime.datetime.now().date() - datetime.timedelta(days=int(operational_latency))
                    basins_metadata['BASIN','end'].where(basins_metadata['BASIN','basin_name']!= basin, operational_end_date, inplace=True)

                    # Updating rout_init_state to None for all.
                    ## Adding [Basin][rout_init_state] to metadata.columns if not there with None value
                    if ('BASIN','rout_init_state') not in basins_metadata.columns:
                        basins_metadata['BASIN','rout_init_state'] = None
                    basins_metadata['BASIN','rout_init_state'] = None

                except:
                    log.exception('Failed to update start and end date for RAT to run operationally. Please make sure RAT has been run atleast once before.')
                    return None
            
            else:
                ## Calculation of gfs_days
                # Read start and end date from metadata if there, otherwise from config.
                if ('BASIN','start') not in basins_metadata.columns:
                    start_date_value = config['BASIN']['start']
                else:
                    start_date_value = basins_metadata['BASIN','start'].loc[basins_metadata['BASIN','basin_name']== basin]
                if ('BASIN','end') not in basins_metadata.columns:
                    end_date_value = config['BASIN']['end']
                else:
                    end_date_value = basins_metadata['BASIN','end'].loc[basins_metadata['BASIN','basin_name']== basin]
                #If not running operationally, check end date and start date's difference from today
                end_date_diff_from_today = datetime.datetime.now().date() - end_date_value
                start_date_diff_from_today = datetime.datetime.now().date() - start_date_value
                # If difference is more than 3 days, gfs_days will be 0.
                if end_date_diff_from_today > datetime.timedelta(days=int(low_latency_limit)):
                    gfs_days = 0
                # Else if start date and today has less than 3 days difference, gfs_days will be start-end
                elif (start_date_diff_from_today<datetime.timedelta(days=int(low_latency_limit))):
                    gfs_days = (end_date_value - start_date_value).days
                # Else gfs_days will be low_latency_limit - difference of end_date from today
                else:
                    gfs_days = low_latency_limit - end_date_diff_from_today.days

            
            # Extracting basin information and populating it in config if it's not NaN
            basin_info = basins_metadata[basins_metadata['BASIN']['basin_name']==basin]
            config_copy = copy.deepcopy(config)
            for col in basin_info.columns:
                if(not pd.isna(basin_info[col[0]][col[1]].values[0])):
                    config_copy[col[0]][col[1]] = basin_info[col[0]][col[1]].values[0]
            # Running RAT if start < end date
            if (config_copy['BASIN']['start'] >= config_copy['BASIN']['end']):
                log.error('Sorry, RAT operational run for '+config_copy['BASIN']['basin_name']+' failed.')
                log.error(f"Start date - {config_copy['BASIN']['start']} is before the end date - {config_copy['BASIN']['end']}")
                continue
            else:
                basins_metadata.to_csv(config['GLOBAL']['basins_metadata'], index=False)
                ryaml_client.dump(config, config_fn.open('w'))
                no_errors, latest_altimetry_cycle = rat_basin(config_copy, log, gfs_days=gfs_days)
                # Run RAT forecast for basin if forecast is True
                if config.get('PLUGINS', {}).get('forecasting'):
                    # Importing the forecast module
                    try:
                        from plugins.forecasting.forecast_basin import forecast
                    except:
                        log.exception("Failed to import Forecast plugin due to missing package(s).")
                    log.info('############## Starting RAT forecast for '+config['BASIN']['basin_name']+' #################')
                    forecast_no_errors = forecast(config, log, low_latency_limit)
                    if(forecast_no_errors>0):
                        log.info('############## RAT-Forecasting run finished for '+config_copy['BASIN']['basin_name']+ ' with '+str(forecast_no_errors)+' error(s). #################')
                    elif(forecast_no_errors==0):
                        log.info('############## Succesfully run RAT-Forecasting for '+config_copy['BASIN']['basin_name']+' #################')
                    else:
                        log.error('############## RAT-Forecasting run failed for '+config_copy['BASIN']['basin_name']+' #################')
            # Displaying and storing RAT function outputs
            if(latest_altimetry_cycle):
                # If column doesn't exist in basins_metadata, create one
                if ('ALTIMETER','last_cycle_number') not in basins_metadata.columns:
                    basins_metadata['ALTIMETER','last_cycle_number'] = None    
                basins_metadata['ALTIMETER','last_cycle_number'].where(basins_metadata['BASIN','basin_name']!= basin, latest_altimetry_cycle, inplace=True)
                basins_metadata.to_csv(config_copy['GLOBAL']['basins_metadata'], index=False)
            if(no_errors>0):
                log.info('############## RAT run finished for '+config_copy['BASIN']['basin_name']+ ' with '+str(no_errors)+' error(s). #################')
            elif(no_errors==0):
                log.info('############## Succesfully run RAT for '+config_copy['BASIN']['basin_name']+' #################')
            else:
                log.error('############## RAT run failed for '+config_copy['BASIN']['basin_name']+' #################')

    # Closing logger
    close_logger('rat_run')
    # Closing Dask workers
    try:
        client.close()
        client.retire_workers()
    except:
        print("####################### Finished executing RAT! ##########################")
        print("Please ignore any below error related to distributed.worker or closed stream.")
        print("####################### Finished executing RAT! ##########################")
        print('\n\n')
    # cluster.close()
    

def main():
    parser = argparse.ArgumentParser(description='Run RAT')
    parser.add_argument('--config', type=str, required=True,
                    help='Config file required to run RAT')
    args = parser.parse_args()

    run_rat(args.config)

if __name__ == '__main__':
    main()