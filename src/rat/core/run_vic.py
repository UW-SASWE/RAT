import pandas as pd
import rasterio as rio
import os
import xarray as xr
import numpy as np
import csv
import time
import math
import datetime
from pathlib import Path
from functools import partial

from logging import getLogger
from rat.utils.logging import LOG_NAME, LOG_LEVEL, NOTIFICATION
from rat.utils.utils import create_directory
from rat.utils.run_command import run_command
from rat.utils.vic_param_reader import VICParameterFile

log = getLogger(LOG_NAME)
log.setLevel(LOG_LEVEL)

class VICRunner():
    def __init__(self, vic_env, param_file, vic_result_file, rout_input_dir, conda_hook = None) -> None:
        self.vic_env = vic_env
        self.param_file = param_file
        self.vic_result = vic_result_file
        self.rout_input = rout_input_dir
        self.conda_hook = conda_hook
        self.model_path = os.path.join(vic_env, 'vic_image.exe')

    def run_vic(self, np=16, cd=None):
        log.log(NOTIFICATION, "Running VIC Model using %s cores", np)

        if not self.conda_hook:
            arg = f'eval "$(conda shell.bash hook)" && conda activate {self.vic_env} && mpiexec -n {np} {self.model_path} -g {self.param_file}'
        else:
            arg = f"source {self.conda_hook} && conda activate {self.vic_env} && mpiexec -n  {np} {self.model_path} -g {self.param_file}"

        ret_code = run_command(arg, shell=True, executable="/bin/bash")

    def generate_routing_input_state(self, ndays, rout_input_state_file, save_path, use_rout_state):
        if os.path.isfile(rout_input_state_file) and (use_rout_state):
            print('Routing input state fle exists at '+str(rout_input_state_file))
            new_vic_output = xr.open_mfdataset(self.vic_result).load()
            first_existing_time = new_vic_output.time[0]
            new_vic_output.close()

            #Preprocessing function for merging netcdf files
            def _remove_coinciding_days(ds, cutoff_time, ndays):
                file_name = ds.encoding["source"]
                file_stem = Path(file_name).stem
                if('ro_init' in file_stem):
                    return ds.sel(time=slice(cutoff_time - np.timedelta64(ndays,'D') , cutoff_time - np.timedelta64(1,'D')))
                else:
                    return ds
            remove_coinciding_days_func = partial(_remove_coinciding_days, cutoff_time=first_existing_time, ndays=ndays)
            
            # Merging previous and new vic outputs
            try:
                save_vic_output = xr.open_mfdataset([rout_input_state_file,self.vic_result],{'time':365}, preprocess=remove_coinciding_days_func)
                save_vic_output.to_netcdf(save_path)
                print('Latest Routing input state fle saved at '+save_path)
            except:
                ## In case routing state file has dates matching with the vic output file. 
                print('Rout input state has same dates as vic_output_dates')
        else:
            new_vic_output = xr.open_mfdataset(self.vic_result,{'time':365})
            save_vic_output = new_vic_output[dict(time=slice(-ndays,None))]
            new_vic_output.close()
            save_vic_output.to_netcdf(save_path)
        

    def disagg_results(self, rout_input_state_file):
        log.log(NOTIFICATION, "Started disaggregating VIC results")
        fluxes = xr.open_dataset(rout_input_state_file).load()

        fluxes_subset = fluxes[['OUT_PREC', 'OUT_EVAP', 'OUT_RUNOFF', 'OUT_BASEFLOW']]

        nonnans = fluxes_subset.OUT_PREC.isel(time=0).values.flatten()
        nonnans = nonnans[~np.isnan(nonnans)]
        total = len(nonnans)

        log.debug("Total files to be created: %s", total)

        create_directory(self.rout_input)

        # VIC Routing doesn't round, it truncates the lat long values. Important for file names.
        lats_vicfmt = (np.floor(np.abs(fluxes_subset.lat.values)*100)/100)*np.sign(fluxes_subset.lat.values)
        lons_vicfmt = (np.floor(np.abs(fluxes_subset.lon.values)*100)/100)*np.sign(fluxes_subset.lon.values)

        s = time.time()
        for lat in range(len(fluxes_subset.lat)):
            for lon in range(len(fluxes_subset.lon)):
                if not math.isnan(fluxes_subset.OUT_PREC.isel(time=0, lat=lat, lon=lon).values):
                    fname = os.path.join(self.rout_input, f"fluxes_{lats_vicfmt[lat]:.2f}_{lons_vicfmt[lon]:.2f}")
                    # pbar.set_description(f"{fname}")

                    da = fluxes_subset.isel(lat=lat, lon=lon).to_dataframe().reset_index()

                    da.to_csv(fname, sep=' ', header=False, index=False, float_format="%.5f", quotechar="", quoting=csv.QUOTE_NONE, date_format="%Y %m %d", escapechar=" ")
                        # pbar.update(1)
        # See how many files were created
        disagg_n = len(os.listdir(self.rout_input))
        log.log(NOTIFICATION, "Finished disaggregating %s/%s files in %s seconds", disagg_n, total, f"{(time.time()-s):.3f}")
