import pandas as pd
import yaml
import os
import argparse
import configparser
import ee

from rat.utils.logging import init_logger,close_logger
import ee_utils.ee_config as ee_configuration

#------------ Define Variables ------------#
def run_rat(config_fn):
    """Runs RAT as per configuration defined in `config_fn`.

    parameters:
        config_fn (str): Path to the configuration file
    """
    config = yaml.safe_load(open(config_fn, 'r'))

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

    from rat.rat_basin import rat

    if(not config['GLOBAL']['multiple_basin_run']):
        log.info('############## Starting RAT for '+config['BASIN']['basin_name']+' #################')
        rat(config, log)
        log.info('############## Succesfully run RAT for '+config['BASIN']['basin_name']+' #################')
    else:
        basins_to_process = config['GLOBAL']['basins_to_process']
        basins_metadata = pd.read_csv(config['GLOBAL']['basins_metadata'],header=[0,1])
        for basin in basins_to_process:
            log.info('############## Starting RAT for '+basin+' #################')
            basin_info = basins_metadata[basins_metadata['BASIN']['basin_name']==basin]
            # Extracting basin information and populating it in config
            for col in basin_info.columns:
                config[col[0]][col[1]] = basin_info[col[0]][col[1]].values[0]
            rat(config, log)
            log.info('############## Succesfully run RAT for '+basin+' #################')
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