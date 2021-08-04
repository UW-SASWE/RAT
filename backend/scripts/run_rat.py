import yaml
import os
import datetime

from core.run_vic import VICRunner
from utils.logging import init_logger, NOTIFICATION
from core.run_metsim import MetSimRunner
from core.run_routing import RoutingRunner
from utils.vic_param_reader import VICParameterFile
from utils.route_param_reader import RouteParameterFile
from utils.metsim_param_reader import MSParameterFile
from data_processing.newdata import get_newdata
from data_processing.metsim_input_processing import metsim_input_processing


def main():
    #------------ Define Variables ------------#
    # config = configparser.ConfigParser(inline_comment_prefixes=('#', ';'))  
    # config.read("/houston2/pritam/rat_mekong_v3/backend/params/rat_mekong.conf")  # TODO Replace later with command line 
    config = yaml.safe_load(open("/houston2/pritam/rat_mekong_v3/backend/params/rat_mekong.yml", 'r'))

    # Change datetimes
    config['GLOBAL']['begin'] = datetime.datetime.combine(config['GLOBAL']['begin'], datetime.time.min)
    config['GLOBAL']['end'] = datetime.datetime.combine(config['GLOBAL']['end'], datetime.time.min)
    config['GLOBAL']['previous_end'] = datetime.datetime.combine(config['GLOBAL']['previous_end'], datetime.time.min)

    # # metsim results
    # ms_results = config['METSIM']['metsim_results']
    #------------ Define Variables ------------#

    log = init_logger(
        "/houston2/pritam/rat_mekong_v3/backend/logs",
        verbose=True,
        notify=True,
        log_level='DEBUG'
    )

    # Download and Process Data
    get_newdata(
        os.path.join(config['GLOBAL']['project_dir'], 'backend'),
        config['GLOBAL']['previous_end'],
        config['GLOBAL']['end']
    )

    # Process data to metsim format
    ms_state, ms_input_data = metsim_input_processing(
        os.path.join(config['GLOBAL']['project_dir'], 'backend'),
        config['GLOBAL']['begin'],
        config['GLOBAL']['end'])

    #-------------- Metsim Begin --------------#
    with MSParameterFile(config, config['METSIM']['metsim_param_file'], ms_input_data, ms_state) as m:
        ms = MetSimRunner(
            m.ms_param_path,
            config['METSIM']['metsim_env'],
            config['GLOBAL']['conda_hook'],
            m.results,
            config['GLOBAL']['multiprocessing']
        )
        ms.run_metsim()
        prefix = ms.diasgg_results(config['VIC']['vic_forcings_dir'])
    #--------------- Metsim End ---------------#

    #--------------- VIC Begin ----------------# 
    with VICParameterFile(config, prefix) as p:
        vic = VICRunner(
            config['VIC']['vic_env'],
            p.vic_param_path,
            p.vic_result_file,
            config['ROUTING']['route_input_dir'],
            config['GLOBAL']['conda_hook']
        )
        vic.run_vic(np=config['GLOBAL']['multiprocessing'])
        vic.disagg_results()
        vic_startdate = p.vic_startdate
        vic_enddate = p.vic_enddate
    #---------------- VIC End -----------------#

    #------------- Routing Being --------------#
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
    #-------------- Routing End ---------------#


if __name__ == '__main__':
    main()