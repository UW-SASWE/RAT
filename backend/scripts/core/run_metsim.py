from logging import getLogger
import yaml
import os
import xarray as xr

from utils.logging import LOG_NAME, NOTIFICATION
from utils.utils import run_command

log = getLogger(f"{LOG_NAME}.{__name__}")

class MetSimRunner():
    def __init__(self, param_path, metsim_env, conda_hook, results_path, multiprocessing) -> None:
        self._param_path = param_path
        self._metsim_env = metsim_env
        self._conda_hook = conda_hook
        self.results_path = results_path
        self._mp = multiprocessing

    def run_metsim(self):
        log.log(NOTIFICATION, 'Starting metsim')
        args = f'source {self._conda_hook} && conda activate {self._metsim_env} && ms -n {self._mp} {self._param_path}'
        # print("will run: ", args)
        ret_code = run_command(args, shell=True)
    
    def diasgg_results(self, forcings_dir):
        log.debug("Disaggregating metsim results to forcings. Forcings dir: %s", forcings_dir)
        ds = xr.open_dataset(self.results_path).load()

        years, datasets = zip(*ds.groupby('time.year'))
        paths = [os.path.join(forcings_dir, f'forcing_{y}.nc') for y in years]

        log.debug("Will disaggregate to %s files: %s", len(paths), ', '.join(paths))
        xr.save_mfdataset(datasets, paths)

        return "_".join(paths[0].split("_")[:-1]) + "_"