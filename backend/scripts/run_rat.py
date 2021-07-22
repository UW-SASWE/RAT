from utils.logging import init_logger
from core.run_metsim import MetSimRunner
from core.run_routing import RoutingRunner
import configparser


def main():
    #------------ Define Variables ------------#
    config = configparser.ConfigParser(inline_comment_prefixes=('#', ';'))  
    config.read("/houston2/pritam/rat_mekong_v3/backend/params/rat_mekong.conf")  # TODO Replace later with command line 
    #------------ Define Variables ------------#

    log = init_logger(
        "/houston2/pritam/rat_mekong_v3/backend/logs",
        verbose=True,
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
    ## Prepare VIC input data

    ## Update VIC parameters

    ## Run VIC and handle outputa
    #---------------- VIC End -----------------#

    #------------- Routing Being --------------#
    route = RoutingRunner(    # TODO create notification level of logging, where a notification is sent off to telegram/email
        config.get('GLOBAL', 'project_dir'), 
        config.get('ROUTING', 'route_param_dir'),
        config.get('ROUTING', 'route_result_dir'), 
        config.get('ROUTING', 'route_inflow_dir'), 
        config.get('ROUTING', 'route_model'),
        config.get('ROUTING', 'route_param_file'), 
        config.get('ROUTING', 'flow_direction_file'), 
        config.get('ROUTING', 'station_file')
    )
    route.create_station_file()
    route.run_routing()
    route.generate_inflow()
    #-------------- Routing End ---------------#


if __name__ == '__main__':
    main()