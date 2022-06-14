import os
import datetime
from logging import getLogger
import yaml
import shutil

from utils.utils import create_directory
from utils.logging import LOG_NAME, NOTIFICATION

log = getLogger(LOG_NAME)

class RouteParameterFile:
    def __init__(self, config, basin_name, start, end, basin_flow_direction_file=None, clean=False, runname=None, rout_input_path_prefix=None,
                                config_section='ROUTING', intermediate_files=False):
        self.params = {
            'flow_direction_file': None,
            'velocity': None,
            'diff': None,
            'xmask': None,
            'fraction': None,
            'station': None,
            'input_files_prefix': None,
            'input_file_precision': None,
            'output_dir': None,
            'start_date': start.strftime("%Y %m %d"),
            'end_date': end.strftime("%Y %m %d"),
            'uh': None
        }
        self.route_param_path = None
        self.workspace = None
        self.clean = clean

        self.basin_name = basin_name
        self.intermediate_files = intermediate_files
        self.rout_input_path_prefix = rout_input_path_prefix
        self.basin_flow_direction_file = basin_flow_direction_file

        self.startdate = start
        self.enddate = end
        self.config_section = config_section

        self.config = config
        if runname is None:
            self.runname = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        else:
            self.runname = str(runname)

        self._load_from_config()

    def _load_from_config(self):
        # Determine dates
        config = self.config

        # Calculate first
        if self.params['start_date'] is None:
            self.params['start_date'] = (config['GLOBAL']['begin'] + datetime.timedelta(days=90)).strftime("%Y %m %d")
        if self.params['end_date'] is None:
            self.params['end_date'] = config['GLOBAL']['end'].strftime("%Y %m %d")

        # If passed as parameters, replace them
        if self.startdate:
            self.params['start_date'] = self.startdate.strftime('%Y %m %d')

        if self.enddate:
            self.params['end_date'] = self.enddate.strftime('%Y %m %d')

        # setup workspace
        if (config[self.config_section].get('route_workspace')):
            self.workspace = create_directory(os.path.join(config[self.config_section]['route_workspace'],
                                                                 f'run_{self.runname}'))
        else:
            self.workspace = create_directory(os.path.join(config['GLOBAL']['data_dir'],'basins',
                                                                self.basin_name,'rout_workspace',f'run_{self.runname}'))
        
        ## Route parameter file, this is where the parameter file will be saved
        if (self.intermediate_files):
            self.route_param_path = os.path.relpath(os.path.join(self.workspace, 'route_param.txt'))
        else:
            # Replacing the init_route_param_file
            if(config[self.config_section].get('route_param_file')):
                self.route_param_path = config[self.config_section].get('route_param_file')
            # Or storing it in route basin params dir and replace it from next cycle
            else:
                self.route_param_path = create_directory(os.path.join(config['GLOBAL']['data_dir'],
                                                            'basins',self.basin_name,'rout_basin_params'),True)
                self.route_param_path = os.path.join(self.route_param_path,'route_param.txt')
        
        ## flow direction file
        self.params['flow_direction_file'] = self.basin_flow_direction_file

        ## output dir
        self.params['output_dir'] = create_directory(os.path.join(config['GLOBAL']['data_dir'],
                                                            'basins',self.basin_name,'rout_outputs'),True)
        
        ## stations
        if (self.intermediate_files):
            self.params['station'] = os.path.join(self.workspace, 'stations_xy.txt')
        else:
            self.params['station'] = create_directory(os.path.join(config['GLOBAL']['data_dir'],
                                                        'basins',self.basin_name,'rout_basin_params'),True)
            self.params['station'] = os.path.join(self.params['station'],'stations_xy.txt')

        # Routing Input file prefix path   
        self.params['input_files_prefix'] = self.rout_input_path_prefix
        
        # load from config
        for key in config[f'{self.config_section} PARAMETERS']:
            val = config[f'{self.config_section} PARAMETERS'][key]
            self.params[key] = val
    
    # TODO create _load_from_param

    def _out_format_params(self):
        res = []

        res.append(f"# ROUTE PARAMETER FILE created by RouteParameterFile() on {datetime.datetime.now().strftime('%Y-%m-%d %X')} - run_{self.runname}")
        
        res.append(f"# FLOW DIRECTION FILE")
        res.append(f"{os.path.relpath(self.params['flow_direction_file'])}")
        
        res.append(f"# VELOCITY FILE")
        if not isinstance(self.params['velocity'], str): # if not a velocity file path
            res.append(f".false.")
            res.append(f"{self.params['velocity']}")
        else:
            res.append(f".true.")
            res.append(f"{os.path.relpath(self.params['velocity'])}")

        res.append(f"# DIFFUSION FILE")
        if not isinstance(self.params['diff'], str): # if not a velocity file path
            res.append(f".false.")
            res.append(f"{self.params['diff']}")
        else:
            res.append(f".true.")
            res.append(f"{os.path.relpath(self.params['diff'])}")

        res.append(f"# XMASK FILE")
        if not isinstance(self.params['xmask'], str): # if not a velocity file path
            res.append(f".false.")
            res.append(f"{self.params['xmask']}")
        else:
            res.append(f".true.")
            res.append(f"{os.path.relpath(self.params['xmask'])}")

        res.append(f"# FRACTION FILE")
        if not isinstance(self.params['fraction'], str): # if not a velocity file path
            res.append(f".false.")
            res.append(f"{self.params['fraction']}")
        else:
            res.append(f".true.")
            res.append(f"{os.path.relpath(self.params['fraction'])}")

        res.append(f"# STATION FILE")
        res.append(f"{os.path.relpath(self.params['station'])}")

        res.append(f"# INPUTS and PRECISION")
        res.append(f"{os.path.relpath(self.params['input_files_prefix'])}")
        res.append(f"{self.params['input_file_precision']}")

        res.append(f"# OUTPUT FILES")
        res.append(f"{os.path.relpath(self.params['output_dir'])}/")

        res.append(f"# START and END")
        res.append(f"{self.params['start_date']}")
        res.append(f"{self.params['end_date']}")

        res.append(f"# UH")
        res.append(f"{os.path.relpath(self.params['uh'])}")

        return '\n'.join(res)

    def _write(self):
        with open(self.route_param_path, 'w') as f:
            param = self._out_format_params()
            log.debug(param)
            f.write(param)

    def __enter__(self):
        # Save a copy of config file for record
        if(self.intermediate_files):
            config_record_path = os.path.join(self.workspace, 'config_record.yml')
            yaml.dump(self.config, open(config_record_path, 'w'))

        # TODO when defined `_load_from_param`, save the file used to initialize for record

        self._write()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.clean:
            log.debug("Removing route inputs from %s", self.params['output_dir'])
            shutil.rmtree(self.params['output_dir'])