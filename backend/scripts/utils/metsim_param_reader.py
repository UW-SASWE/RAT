import yaml
import datetime
from logging import getLogger
import os

from utils.logging import LOG_NAME, NOTIFICATION
from utils.utils import create_directory

log = getLogger(LOG_NAME)


class MSParameterFile:
    def __init__(self, workspace, start, end, init_param, forcings=None, state=None, runname=None):
        self.params = yaml.safe_load(open(init_param, 'r'))   # Initialize from a parameter file
        
        self.params['MetSim']['start'] = start.strftime("%Y-%-m-%d")
        self.params['MetSim']['stop'] = end.strftime("%Y-%-m-%d")

        self.start = start
        self.end = end
        self.workspace = workspace
        self.ms_param_path = None
        self.results = None
        self.forcings = forcings
        self.state = state

        if runname:
            self.runname = runname
        else:
            self.runname = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        self._load()

    def _load(self):
        # Date modification
        # start = self.config['GLOBAL']['begin'] + datetime.timedelta(days=90)
        # self.params['MetSim']['start'] = start.strftime("%Y-%-m-%d")
        # end = self.config['GLOBAL']['end']
        # self.params['MetSim']['stop'] = end.strftime("%Y-%-m-%d")

        self.params['MetSim']['forcing'] = self.forcings
        self.params['MetSim']['state'] = self.state

        self.results = os.path.join(self.params['MetSim']['out_dir'], f"{self.params['MetSim']['out_prefix']}_{self.start.strftime('%Y%m%d')}-{self.end.strftime('%Y%m%d')}.nc")

        self.workspace = create_directory(os.path.join(self.workspace, f'run_{self.runname}'))

    def __enter__(self):
        # save params
        self.ms_param_path = os.path.join(self.workspace, 'metsim_params.yml')
        yaml.dump(self.params, open(self.ms_param_path, 'w'))

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass