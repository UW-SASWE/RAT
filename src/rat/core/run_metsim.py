from http.server import executable
from logging import getLogger
import numpy as np
import os
import xarray as xr
from rat.utils.logging import LOG_NAME, NOTIFICATION
from rat.utils.run_command import run_command
from rat.utils.utils import create_directory

log = getLogger(f"{LOG_NAME}.{__name__}")

class MetSimRunner():
    def __init__(self, param_path, metsim_env, results_path, multiprocessing, conda_hook=None) -> None:
        self._param_path = param_path
        self._metsim_env = metsim_env
        self._conda_hook = conda_hook
        self.results_path = results_path
        self._mp = multiprocessing

    def run_metsim(self):
        if not self._conda_hook:
            args = f'eval "$(conda shell.bash hook)" && conda activate {self._metsim_env} && ms -n {self._mp} {self._param_path}'
        else:
            args = f'source {self._conda_hook} && conda activate {self._metsim_env} && ms -n {self._mp} {self._param_path}'
        # print("will run: ", args)
        ret_code = run_command(args, metsim=True, shell=True, executable="/bin/bash")
    
    def convert_to_vic_forcings(self, forcings_dir):
        # The results have to be converted to VIC readable yearly netcdf files.
        ds = xr.open_dataset(self.results_path).load()
        
        years, dataset = zip(*ds.groupby('time.year'))
        paths = [os.path.join(forcings_dir, f'forcing_{y}.nc') for y in years]

        #Create directory if doesn't exist
        create_directory(forcings_dir)

        log.debug(f"Will create {len(years)} forcing files")
        for year, ds, p in zip(years, dataset, paths):
            if os.path.isfile(p):
                existing = xr.open_dataset(p).load()
                existing.close()

                log.debug(f"Writing file for year {year}: {p} -- Updating existing")
                # xr.merge([existing, ds], compat='override', join='outer').to_netcdf(p)
                # xr.concat([existing, ds], dim='time').to_netcdf(p)
                last_existing_time = existing.time[-1]
                log.debug("Existing data: %s", last_existing_time)
                ds = ds.sel(time=slice(last_existing_time + np.timedelta64(6,'h'), ds.time[-1]))
                #ds = ds.isel(time=slice(1, None))
                xr.merge([existing, ds]).to_netcdf(p)
            else:
                log.debug(f"Writing file for year {year}: {p} -- Updating new")
                ds.to_netcdf(p)