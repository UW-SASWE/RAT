import pandas as pd
import numpy as np
import yaml
import os
import argparse
import configparser
import ee
import ruamel_yaml as ryaml
from pathlib import Path 
import datetime
import copy
from dask.distributed import Client, LocalCluster

from rat.utils.logging import init_logger,close_logger,LOG_LEVEL1_NAME
import rat.ee_utils.ee_config as ee_configuration
from rat.rat_basin import rat_basin

#------------ Define Variables ------------#
def run_rat(config_fn, operational_latency=None):
    """Runs RAT as per configuration defined in `config_fn`.

    parameters:
        config_fn (str): Path to the configuration file
        operational_latency (int): Number of days in the past from today to end RAT operational run . RAT won't run operationally if operational_latency is None. Default is None.
    """

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
        ee_credentials = ee.ServiceAccountCredentials(ee_configuration.service_account,ee_configuration.key_file)
        ee.Initialize(ee_credentials)
        log.info("Connected to earth engine succesfully.")
    except:
        log.info("Failed to connect to Earth engine. Wrong credentials. If you want to use Surface Area Estimations from RAT, please update the EE credentials.")

    ############ ----------- Single basin run ---------------- ################
    if(not config['GLOBAL']['multiple_basin_run']):
        log.info('############## Starting RAT for '+config['BASIN']['basin_name']+' #################')
        
        # Checking if Rat is running operationally with some latency. If yes, update start, end and vic_init_state dates.
        if operational_latency:
            try:
                log.info(f'Running RAT operationally at a latency of {operational_latency}. Updating start and end date.')
                config['BASIN']['start'] = copy.deepcopy(config['BASIN']['end'])
                config['BASIN']['vic_init_state'] = copy.deepcopy(config['BASIN']['end'])
                config['BASIN']['rout_init_state'] = None
                config['BASIN']['end'] = datetime.datetime.now().date() - datetime.timedelta(days=int(operational_latency))
            except:
                log.exception('Failed to update start and end date for RAT to run operationally. Please make sure RAT has been run atleast once before.')
                return None
        # Running RAT if start < end date
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
            no_errors, latest_altimetry_cycle = rat_basin(config, log)
            # Run RAT forecast for basin if forecast is True           
            if config.get('PLUGINS', {}).get('forecasting'):
                # Importing the forecast module
                try:
                    from rat.plugins.forecasting import forecast
                except:
                    log.exception("Failed to import Forecast plugin due to missing package(s).")
                log.info('############## Starting RAT forecast for '+config['BASIN']['basin_name']+' #################')
                forecast_no_errors = forecast(config, log)
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

    ############ ----------- Multiple basin run ---------------- ################
    else:
        basins_to_process = config['GLOBAL']['basins_to_process']
        # For each basin
        for basin in basins_to_process:
            log.info('############## Starting RAT for '+basin+' #################')
            # Reading basins metadata
            basins_metadata = pd.read_csv(config['GLOBAL']['basins_metadata'],header=[0,1])
            # Checking if Rat is running operationally with some latency. If yes, update start, end and vic_init_state dates.
            if operational_latency:
                try:
                    log.info(f'Running RAT operationally at a latency of {operational_latency}. Updating start and end date.')
                    ## If end date is not in basins metadata.columns then it is in config file.
                    if ('BASIN','end') not in basins_metadata.columns:
                        ## Adding [Basin][start] to metadata.columns if not there with with None value 
                        if ('BASIN','start') not in basins_metadata.columns:
                            basins_metadata['BASIN','start'] = None
                        ## Changning [Basin][start] in metadata.columns to previous end value from config file 
                        basins_metadata['BASIN','start'] = copy.deepcopy(config['BASIN']['end'])
                        config['BASIN']['end'] = datetime.datetime.now().date() - datetime.timedelta(days=int(operational_latency))
                    ## Else it is in metadata.columns
                    else:
                        # Updating start
                        ## Adding [Basin][start] to metadata.columns if not there with None value
                        if ('BASIN','start') not in basins_metadata.columns:
                            basins_metadata['BASIN','start'] = None    
                        ## Changning [Basin][start] in metadata.columns to previous end value from metadata
                        basins_metadata['BASIN','start'].where(basins_metadata['BASIN','basin_name']!= basin, basins_metadata['BASIN','end'], inplace=True)
                        # Updating end
                        operational_end_date = datetime.datetime.now().date() - datetime.timedelta(days=int(operational_latency))
                        basins_metadata['BASIN','end'].where(basins_metadata['BASIN','basin_name']!= basin, operational_end_date, inplace=True)
                    ### We can add vic_init_state and rout_init_state to metadata as it will override the values in config anyway.
                    # Updating vic_init_state
                    ## Adding [Basin][vic_init_state] to metadata.columns if not there with None value
                    if ('BASIN','vic_init_state') not in basins_metadata.columns:
                        basins_metadata['BASIN','vic_init_state'] = None    
                    basins_metadata['BASIN','vic_init_state'].where(basins_metadata['BASIN','basin_name']!= basin, basins_metadata['BASIN','start'], inplace=True)
                    # Updating rout_init_state to None for all.
                    ## Adding [Basin][vic_init_state] to metadata.columns if not there with None value
                    if ('BASIN','rout_init_state') not in basins_metadata.columns:
                        basins_metadata['BASIN','rout_init_state'] = None
                    basins_metadata['BASIN','rout_init_state'] = None
                except:
                    log.exception('Failed to update start and end date for RAT to run operationally. Please make sure RAT has been run atleast once before.')
                    return None
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
                no_errors, latest_altimetry_cycle = rat_basin(config_copy, log)
                # Run RAT forecast for basin if forecast is True
                if config.get('PLUGINS', {}).get('forecasting'):
                    # Importing the forecast module
                    try:
                        from rat.plugins.forecasting import forecast
                    except:
                        log.exception("Failed to import Forecast plugin due to missing package(s).")
                    log.info('############## Starting RAT forecast for '+config['BASIN']['basin_name']+' #################')
                    forecast_no_errors = forecast(config, log)
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