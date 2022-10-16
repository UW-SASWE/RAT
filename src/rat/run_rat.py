import yaml
import os
import datetime

from rat.core.run_vic import VICRunner
from rat.utils.logging import init_logger, NOTIFICATION
from rat.core.run_metsim import MetSimRunner
from rat.core.run_routing import RoutingRunner
from rat.core.run_sarea import run_sarea
from rat.core.run_postprocessing import run_postprocessing
from rat.core.run_altimetry import run_altimetry

from rat.utils.vic_param_reader import VICParameterFile
from rat.utils.route_param_reader import RouteParameterFile
from rat.utils.metsim_param_reader import MSParameterFile

from rat.data_processing.newdata import get_newdata
from rat.data_processing.metsim_input_processing import generate_state_and_inputs
from rat.data_processing.metsim_input_processing import ForcingsNCfmt
# from rat.utils.temp_postprocessing import run_old_model, copy_generate_inflow, run_postprocess, publish
from rat.utils.convert_for_website import convert_dels_outflow, convert_sarea, convert_inflow, convert_altimeter

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
        # notify=True,
        notify=False,
        log_level='DEBUG'
    )

    #--------- Download Data Begin ----------#
    get_newdata(
        os.path.join(config['GLOBAL']['project_dir'], 'backend'),
        config['GLOBAL']['previous_end'],
        config['GLOBAL']['end']
    )
    #---------- Download Data End -----------#

    combined_datapath = os.path.join(config['GLOBAL']['project_dir'], 'backend', 'data', 'nc', 'combined_data.nc')
    #----------- Process Data Begin -----------#
    processed_datadir = os.path.join(config['GLOBAL']['project_dir'], 'backend', 'data', 'processed')
    basingridfile = os.path.join(config['GLOBAL']['project_dir'], 'backend', 'data', 'ancillary', 'MASK.tif')
    ForcingsNCfmt(
        config['GLOBAL']['previous_end'],
        config['GLOBAL']['end'],
        processed_datadir,
        basingridfile,
        combined_datapath
    )
    #------------ Process Data End ------------#


    #------ MetSim Data Processing Begin ------#
    # Process data to metsim format
    metsim_inputs_dir = os.path.join(config['GLOBAL']['project_dir'], 'backend', 'data', 'metsim_inputs')
    ms_state, ms_input_data = generate_state_and_inputs(
        config['GLOBAL']['previous_end'],
        config['GLOBAL']['end'],
        combined_datapath, 
        metsim_inputs_dir
    )
    #------- MetSim Data Processing End -------#


    #-------------- Metsim Begin --------------#
    with MSParameterFile(
        config['METSIM']['metsim_workspace'], 
        config['GLOBAL']['previous_end'],
        config['GLOBAL']['end'],
        config['METSIM']['metsim_param_file'], 
        ms_input_data, 
        ms_state
        ) as m:
        
        ms = MetSimRunner(
            m.ms_param_path,
            config['METSIM']['metsim_env'],
            config['GLOBAL']['conda_hook'],
            m.results,
            config['GLOBAL']['multiprocessing']
        )
        log.log(NOTIFICATION, f'Starting metsim from {config["GLOBAL"]["previous_end"].strftime("%Y-%m-%d")} to {config["GLOBAL"]["end"].strftime("%Y-%m-%d")}')
        ms.run_metsim()
        # # prefix = ms.diasgg_results(config['VIC']['vic_forcings_dir'])
        prefix = ms.convert_to_vic_forcings(config['VIC']['vic_forcings_dir'])
    #--------------- Metsim End ---------------#


    # --------------- VIC Begin ----------------# 
    with VICParameterFile(config) as p:
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

    # vic_startdate = config['GLOBAL']['begin'] + datetime.timedelta(days=90)
    # vic_enddate = config['GLOBAL']['end']
    #------------- Routing Being --------------#
    with RouteParameterFile(config, vic_startdate, vic_enddate, clean=False) as r:
        route = RoutingRunner(    
            config['GLOBAL']['project_dir'], 
            r.params['output_dir'], 
            # "extras/vic_cal_route_res/",
            config['ROUTING']['route_inflow_dir'], 
            config['ROUTING']['route_model'],
            r.route_param_path, 
            # 'extras/vic_cal_route_res/route_param.txt',
            r.params['flow_direction_file'], 
            config['ROUTING']['station_latlon_path'],
            r.params['station']
        )
        route.create_station_file()
        route.run_routing()
        route.generate_inflow()
    #-------------- Routing End ---------------#

    #----------- Remote Sensing Begin -----------#
    # Get Sarea
    run_sarea(config['GLOBAL']['end'].strftime("%Y-%m-%d"))

    # Altimerter
    run_altimetry("/houston2/pritam/rat_mekong_v3")
    #----------- Remote Sensing End -----------#

    #---------- Postprocessing Begin ----------#
    run_postprocessing(config['GLOBAL']['project_dir'])

    # Convert to format that is expected by the website
    convert_sarea(config['GLOBAL']['project_dir'])
    convert_inflow(config['GLOBAL']['project_dir'])
    convert_dels_outflow(config['GLOBAL']['project_dir'])
    convert_altimeter(config['GLOBAL']['project_dir'])
    # ----------- Postprocessing End -----------#

    # Publish from .sh file


if __name__ == '__main__':
    main()