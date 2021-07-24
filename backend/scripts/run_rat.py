import numpy as np
from core.run_vic import VICRunner
from utils.logging import init_logger, NOTIFICATION
from core.run_metsim import MetSimRunner
from core.run_routing import RoutingRunner
from utils.vic_param_reader import VICParameterFile
from utils.route_param_reader import RouteParameterFile
import yaml


def main():
    #------------ Define Variables ------------#
    # config = configparser.ConfigParser(inline_comment_prefixes=('#', ';'))  
    # config.read("/houston2/pritam/rat_mekong_v3/backend/params/rat_mekong.conf")  # TODO Replace later with command line 
    config = yaml.safe_load(open("/houston2/pritam/rat_mekong_v3/backend/params/rat_mekong.yml", 'r'))
    #------------ Define Variables ------------#

    log = init_logger(
        None, #"/houston2/pritam/rat_mekong_v3/backend/logs",
        verbose=True,
        notify=False,
        log_level='DEBUG'
    )

    # Download Data

    # Process Data (transform, etc.)


    #-------------- Metsim Begin --------------#
    # MetSimRunner("/houston2/pritam/rat_mekong_v3/backend/params/metsim/params.yaml")
    ## Prepare MetSim data

    ## Update MetSim parameters

    ## Run MetSim and handle outputs
    #--------------- Metsim End ---------------#

    #--------------- VIC Begin ----------------# 
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
        vic_startdate = p.vic_startdate
        vic_enddate = p.vic_enddate
    # #---------------- VIC End -----------------#

    # #------------- Routing Being --------------#
    with RouteParameterFile(config, vic_startdate, vic_enddate, clean=False) as r:
        route = RoutingRunner(    
            config['GLOBAL']['project_dir'], 
            r.params['output_dir'], 
            config['ROUTING']['route_inflow_dir'], 
            config['ROUTING']['route_model'],
            r.route_param_path, 
            r.params['flow_direction_file'], 
            config['ROUTING']['station_latlon_path'],
            r.params['station']
        )
        route.create_station_file()
        route.run_routing()
        route.generate_inflow()
    # #-------------- Routing End ---------------#


if __name__ == '__main__':
    main()