import pytest

def test_imports():
    from rat.core.run_vic import VICRunner
    from rat.utils.logging import init_logger, NOTIFICATION
    from rat.core.run_metsim import MetSimRunner
    from rat.core.run_routing import RoutingRunner
    # # from rat.core.run_sarea import run_sarea
    # from rat.core.run_postprocessing import run_postprocessing
    # from rat.core.run_altimetry import run_altimetry

    # from rat.utils.vic_param_reader import VICParameterFile
    # from rat.utils.route_param_reader import RouteParameterFile
    # from rat.utils.metsim_param_reader import MSParameterFile

    # from rat.data_processing.newdata import get_newdata
    # from rat.data_processing.metsim_input_processing import generate_state_and_inputs
    # # from rat.data_processing.metsim_input_processing import ForcingsNCfmt
    # # from rat.utils.temp_postprocessing import run_old_model, copy_generate_inflow, run_postprocess, publish
    # from rat.utils.convert_for_website import convert_dels_outflow, convert_sarea, convert_inflow, convert_altimeter