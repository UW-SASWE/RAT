import pandas as pd
import numpy as np
import yaml
import os
import argparse
import configparser
import ee
import ruamel_yaml as ryaml
from pathlib import Path 

from rat.utils.logging import init_logger,close_logger
import rat.ee_utils.ee_config as ee_configuration
from rat.rat_basin import rat_basin

#------------ Define Variables ------------#
def run_rat(config_fn, operational_latency=False):
    """Runs RAT as per configuration defined in `config_fn`.

    parameters:
        config_fn (str): Path to the configuration file
        operational (bool): Whether to automatically configure start and enc
    """
    config_fn = Path(config_fn).resolve()
    ryaml_client = ryaml.YAML()
    config = ryaml_client.load(config_fn.read_text())
    # config = yaml.safe_load(open(config_fn, 'r'))

    log_dir = os.path.join(config['GLOBAL']['data_dir'],'runs','logs','')
    log = init_logger(
        log_dir,
        verbose=False,
        # notify=True,
        notify=False,
        log_level='DEBUG',
        logger_name='run_rat',
        for_basin=False
    )
    # 
    # Tring the ee credentials given by user
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

    # Single basin run
    if(not config['GLOBAL']['multiple_basin_run']):
        log.info('############## Starting RAT for '+config['BASIN']['basin_name']+' #################')
        no_errors, latest_altimetry_cycle = rat_basin(config, log)
        if(latest_altimetry_cycle):
            config['ALTIMETER']['last_cycle_number'] = latest_altimetry_cycle
            ryaml_client.dump(config, config_fn.open('w'))
        if(no_errors):
            log.info('############## RAT run finished for '+config['BASIN']['basin_name']+ 'with '+str(no_errors)+' errors. #################')
        else:
            log.info('############## Succesfully run RAT for '+config['BASIN']['basin_name']+' #################')
    # Multiple basin run 
    else:
        basins_to_process = config['GLOBAL']['basins_to_process']
        basins_metadata = pd.read_csv(config['GLOBAL']['basins_metadata'],header=[0,1])

        for basin in basins_to_process:
            log.info('############## Starting RAT for '+basin+' #################')
            basin_info = basins_metadata[basins_metadata['BASIN']['basin_name']==basin]

            # Extracting basin information and populating it in config if it's not NaN
            for col in basin_info.columns:
                if(not np.isnan(basin_info[col[0]][col[1]].values[0])):
                    config[col[0]][col[1]] = basin_info[col[0]][col[1]].values[0]

            # Running rat for this basin with given configuration and extracting outputs
            no_errors, latest_altimetry_cycle = rat_basin(config, log)
            
            # Updating altimeter cycle for next run
            if(latest_altimetry_cycle):
                # If column doesn't exist in basins_metadata, create one
                if ('ALTIMETER','last_cycle_number') not in basins_metadata.columns:
                    basins_metadata['ALTIMETER','last_cycle_number'] = None    
                basins_metadata['ALTIMETER','last_cycle_number'].where(basins_metadata['BASIN','basin_name']!= basin, latest_altimetry_cycle, inplace=True)
                basins_metadata.to_csv(config['GLOBAL']['basins_metadata'], index=False)
            
            # Updating success/failure of rat run for this basin in the log file
            if(no_errors):
                log.info('############## RAT run finished for '+config['BASIN']['basin_name']+ 'with '+str(no_errors)+' errors. #################')
            else:
                log.info('############## Succesfully run RAT for '+config['BASIN']['basin_name']+' #################')

    # Clsoing logger
    close_logger('rat_run')

def main():
    parser = argparse.ArgumentParser(description='Run RAT')
    parser.add_argument('--config', type=str, required=True,
                    help='Config file required to run RAT')
    args = parser.parse_args()

    run_rat(args.config)

if __name__ == '__main__':
    main()