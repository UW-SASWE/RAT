from utils.logging import init_logger
from core.run_metsim import MetSimRunner
from core.run_routing import RoutingRunner


def main():
    #------------ Define Variables ------------#
    fdr_path = "/houston2/pritam/rat_mekong_v3/backend/params/routing/DRT_FDR_VIC.asc"
    station_path = "/houston2/pritam/rat_mekong_v3/backend/data/ancillary/stations_latlon.csv"

    project_dir = "/houston2/pritam/rat_mekong_v3"

    route_param_dir = "/houston2/pritam/rat_mekong_v3/backend/params/routing"
    route_inflow_dir = "/houston2/pritam/rat_mekong_v3/backend/data/inflow"
    route_result_dir = "/houston2/pritam/rat_mekong_v3/backend/data/rout_results"
    route_param_path = "/houston2/pritam/rat_mekong_v3/backend/params/routing/route_param.txt"
    route_model_path = "/houston2/pritam/rat_mekong_v3/backend/models/route_model/rout"
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
    route = RoutingRunner(
        route_param_dir, 
        project_dir, 
        route_result_dir, 
        route_inflow_dir, 
        route_model_path, 
        route_param_path, 
        fdr_path, 
        station_path
    )
    route.create_station_file()
    route.run_routing()
    route.generate_inflow()
    #-------------- Routing End ---------------#


if __name__ == '__main__':
    main()