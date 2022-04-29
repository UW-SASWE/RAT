import xarray as xr
import numpy as np
import argparse
import os
import pandas as pd

from utils.metsim_param_reader import MSParameterFile


def generate_state_and_inputs_forecast(forcings_startdate, forcings_enddate, hindcast_datapath, forecast_datapath, out_dir):
    """Similar to the `generate_state_and_inputs` function in `metsim_input_processing.py` as of 20th April, 2022. 
    It will generate the state and forcings for metsim for the froecast period.

    Args:
        forcings_startdate (string): start date in YYYY-MM-DD format.
        forcings_enddate (string): end date in YYYY-MM-DD format.
        hindcast_datapath (string): path to the combined netcdf file containing hindcast data.
        forecast_datapath (string): path to the combined netcdf file containing forecast data.
        out_dir (string): path of directory to save the state and inputs
    """
    # Since we need 3 months of prior data, we'll have to get the hindcast data as well
    hindcast_data = xr.open_dataset(hindcast_datapath)
    forecast_data = xr.open_dataset(forecast_datapath)

    forcings_startdate = pd.to_datetime(forcings_startdate, format='%Y-%m-%d')
    forcings_enddate = pd.to_datetime(forcings_enddate, format='%Y-%m-%d')

    state_startdate = forcings_startdate - pd.DateOffset(90)
    state_enddate = forcings_startdate - pd.DateOffset(1)

    hindcast_subset = hindcast_data.sel(time=slice(state_startdate, state_enddate))
    combined_data = xr.combine_by_coords([hindcast_subset, forecast_data]).resample(time='1D').ffill()

    state_ds = combined_data.sel(time=slice(state_startdate, state_enddate))
    state_outpath = os.path.join(out_dir, 'state.nc')
    state_ds.to_netcdf(state_outpath)

    # Generate the metsim input
    forcings_ds = combined_data.sel(time=slice(forcings_startdate, forcings_enddate))
    forcings_outpath = os.path.join(out_dir, "metsim_input.nc")
    forcings_ds.to_netcdf(forcings_outpath)

    return state_outpath, forcings_outpath


def main():
    generate_state_and_inputs_forecast('2022-04-22', '2022-05-06', 'data/combined/combined_data.nc', 'data/forecast/2022-04-22.nc', 'data/metsim_inputs_forecast')




if __name__ == '__main__':
    main()