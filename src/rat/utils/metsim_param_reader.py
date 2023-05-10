import yaml
import datetime
from logging import getLogger
import os

from rat.utils.logging import LOG_NAME, LOG_LEVEL, NOTIFICATION
from rat.utils.utils import create_directory

log = getLogger(LOG_NAME)
log.setLevel(LOG_LEVEL)

class MSParameterFile:
    def __init__(self, start, end, init_param, out_dir, forcings=None, state=None, domain=None, workspace=None, runname=None):
        self.params = yaml.safe_load(open(init_param, 'r'))   # Initialize from a parameter file
        
        self.params['MetSim']['start'] = start.strftime("%Y-%-m-%d")
        self.params['MetSim']['stop'] = end.strftime("%Y-%-m-%d")

        self.start = start
        self.end = end
        self.results = None
        self.forcings = forcings
        self.state = state
        self.domain = domain

        self.params['MetSim']['forcing'] = self.forcings
        self.params['MetSim']['state'] = self.state
        self.params['MetSim']['domain'] = self.domain

        self.params['MetSim']['out_dir']= out_dir
        
        if runname:
            self.runname = runname
        else:
            self.runname = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if workspace:
            self.workspace = create_directory(os.path.join(workspace, f'run_{self.runname}'),True)
            self.ms_param_path = os.path.join(self.workspace, 'metsim_params.yml')
        else:
            self.ms_param_path = init_param

        self._load()

    def _load(self):
        self.results = os.path.join(self.params['MetSim']['out_dir'], f"{self.params['MetSim']['out_prefix']}_{self.start.strftime('%Y%m%d')}-{self.end.strftime('%Y%m%d')}.nc")


    def __enter__(self):
        # save params
        yaml.dump(self.params, open(self.ms_param_path, 'w'))
        
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass