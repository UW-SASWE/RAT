import numpy as np
from core.run_vic import VICRunner
from utils.logging import init_logger, NOTIFICATION
from core.run_metsim import MetSimRunner
from core.run_routing import RoutingRunner
from utils.vic_param_reader import VICParameterFile
import yaml


def main():
    #------------ Define Variables ------------#
    # config = configparser.ConfigParser(inline_comment_prefixes=('#', ';'))  
    # config.read("/houston2/pritam/rat_mekong_v3/backend/params/rat_mekong.conf")  # TODO Replace later with command line 
    config = yaml.safe_load(open("/houston2/pritam/rat_mekong_v3/backend/params/rat_mekong.yml", 'r'))

    param_file_path = "/houston2/pritam/rat_mekong_v3/backend/params/vic/vic_params.txt"
    #------------ Define Variables ------------#

    log = init_logger(
        None, #"/houston2/pritam/rat_mekong_v3/backend/logs",
        verbose=True,
        notify=False,
        log_level='DEBUG'
    )

    # print(config)
    # p = VICParameterFile(config, vic_param_file=config['VIC']['vic_param_file'])

    # Download Data

    # Process Data (transform, etc.)


    #-------------- Metsim Begin --------------#
    # MetSimRunner("/houston2/pritam/rat_mekong_v3/backend/params/metsim/params.yaml")
    ## Prepare MetSim data

    ## Update MetSim parameters

    ## Run MetSim and handle outputs
    #--------------- Metsim End ---------------#

    #--------------- VIC Begin ----------------#  # TODO Change to yaml from configparser
    with VICParameterFile(config) as p:
        vic = VICRunner(
            config['VIC']['vic_env'],
            p.vic_param_path,
            p.vic_result_file,
            config['ROUTING']['route_input_dir'],
            config['GLOBAL']['conda_hook']
        )

        vic.run_vic(np=config['VIC']['vic_multiprocessing'])
        vic.disagg_results()
    # #---------------- VIC End -----------------#

    # #------------- Routing Being --------------#
    # route = RoutingRunner(    # TODO create notification level of logging, where a notification is sent off to telegram/email
    #     config.get('GLOBAL', 'project_dir'), 
    #     config.get('ROUTING', 'route_param_dir'),
    #     config.get('ROUTING', 'route_result_dir'), 
    #     config.get('ROUTING', 'route_inflow_dir'), 
    #     config.get('ROUTING', 'route_model'),
    #     config.get('ROUTING', 'route_param_file'), 
    #     config.get('ROUTING', 'flow_direction_file'), 
    #     config.get('ROUTING', 'station_file')
    # )
    # route.create_station_file()
    # route.run_routing()
    # route.generate_inflow()
    # #-------------- Routing End ---------------#


if __name__ == '__main__':
    main()