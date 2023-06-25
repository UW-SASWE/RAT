import os
import datetime
from logging import getLogger
import yaml
import shutil
from pathlib import Path
from rat.utils.utils import create_directory
from rat.utils.logging import LOG_NAME, NOTIFICATION, LOG_LEVEL

log = getLogger(LOG_NAME)
log.setLevel(LOG_LEVEL)

class RouteParameterFile:
    def __init__(self, config, basin_name, start, end, route_param_path=None, basin_flow_direction_file=None, clean=False, runname=None, rout_input_path_prefix=None,
                                station_path=None, config_section='ROUTING', intermediate_files=False, output_dst=None, uh=None):
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
        self.route_param_path = route_param_path
        self.workspace = None
        self.clean = clean

        self.basin_name = basin_name
        self.station_path = station_path
        self.intermediate_files = intermediate_files
        self.rout_input_path_prefix = rout_input_path_prefix
        self.basin_flow_direction_file = basin_flow_direction_file
        self.project_dir = config['GLOBAL']['project_dir']
        self.output_dir = output_dst
        self.uh = uh

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
            self.params['start_date'] = (config['BASIN']['begin'] + datetime.timedelta(days=90)).strftime("%Y %m %d")
        if self.params['end_date'] is None:
            self.params['end_date'] = config['BASIN']['end'].strftime("%Y %m %d")

        # If passed as parameters, replace them
        if self.startdate:
            self.params['start_date'] = self.startdate.strftime('%Y %m %d')

        if self.enddate:
            self.params['end_date'] = self.enddate.strftime('%Y %m %d')

        # setup workspace
        if (config[self.config_section].get('route_workspace')):
            self.workspace = Path(config[self.config_section]['route_workspace']) / f'run_{self.runname}'
            self.workspace.mkdir(exist_ok=True)
        else:
            if(self.intermediate_files):
                self.workspace = Path(config['GLOBAL']['data_dir']).joinpath(config['BASIN']['region_name'],'basins',self.basin_name,'ro','rout_workspace',f'run_{self.runname}')
                self.workspace.mkdir(exist_ok=True)
        
        ## Route parameter file, this is where the parameter file will be saved
        if (self.intermediate_files):
            self.route_param_path = os.path.relpath(os.path.join(self.workspace, 'route_param.txt'),self.project_dir)
        elif self.route_param_path is None:
            # Replacing the init_route_param_file
            if(config[self.config_section].get('route_param_file')):
                self.route_param_path = os.path.relpath(config[self.config_section].get('route_param_file'),self.project_dir)
            # Or storing it in route basin params dir and replace it from next cycle
            else:
                self.route_param_path = create_directory(os.path.join(config['GLOBAL']['data_dir'],config['BASIN']['region_name'],
                                                            'basins',self.basin_name,'ro','pars'),True)
                self.route_param_path = os.path.relpath(os.path.join(self.route_param_path,'route_param.txt'),self.project_dir)
        
        ## flow direction file
        self.params['flow_direction_file'] = self.basin_flow_direction_file

        ## output dir
        if self.output_dir is not None: # if output dir is passed as parameter use it
            self.params['output_dir'] = self.output_dir
        else: # if output dir is not passed as parameter, create it in default location
            self.params['output_dir'] = create_directory(os.path.join(config['GLOBAL']['data_dir'],config['BASIN']['region_name'],
                                                            'basins',self.basin_name,'ro','ou'),True)
        
        ## stations
        if self.station_path: # if station path is passed as parameter
            self.params['station'] = self.station_path # use it
        elif (self.intermediate_files): # if intermediate files are to be generated (on-the-fly workspace folder creation)
            self.params['station'] = os.path.join(self.workspace, 'sta_xy.txt') # use the workspace folder
        else:
            self.params['station'] = create_directory(os.path.join(config['GLOBAL']['data_dir'],config['BASIN']['region_name'],
                                                        'basins',self.basin_name,'ro','pars'),True) # else use the default folder
            self.params['station'] = os.path.join(self.params['station'],'sta_xy.txt')

        # Routing Input file prefix path   
        self.params['input_files_prefix'] = self.rout_input_path_prefix
        
        # load from config
        for key in config[f'{self.config_section} PARAMETERS']:
            val = config[f'{self.config_section} PARAMETERS'][key]
            self.params[key] = val

        # if uh is passed as parameter (override the value from rat_config)
        if self.uh:
            self.params['uh'] = self.uh
        # if basin_flow_direction file is passed as parameter (override the value from rat_config)
        if self.basin_flow_direction_file:
            self.params['flow_direction_file'] = self.basin_flow_direction_file
    
    # TODO create _load_from_param

    def _out_format_params(self):
        res = []

        res.append(f"# ROUTE PARAMETER FILE created by RouteParameterFile() on {datetime.datetime.now().strftime('%Y-%m-%d %X')} - run_{self.runname}")
        
        res.append(f"# FLOW DIRECTION FILE")
        res.append(f"{self.params['flow_direction_file']}")
        
        res.append(f"# VELOCITY FILE")
        if not isinstance(self.params['velocity'], str): # if not a velocity file path
            res.append(f".false.")
            res.append(f"{self.params['velocity']}")
        else:
            res.append(f".true.")
            res.append(f"{self.params['velocity']}")

        res.append(f"# DIFFUSION FILE")
        if not isinstance(self.params['diff'], str): # if not a velocity file path
            res.append(f".false.")
            res.append(f"{self.params['diff']}")
        else:
            res.append(f".true.")
            res.append(f"{self.params['diff']}")

        res.append(f"# XMASK FILE")
        if not isinstance(self.params['xmask'], str): # if not a velocity file path
            res.append(f".false.")
            res.append(f"{self.params['xmask']}")
        else:
            res.append(f".true.")
            res.append(f"{self.params['xmask']}")

        res.append(f"# FRACTION FILE")
        if not isinstance(self.params['fraction'], str): # if not a velocity file path
            res.append(f".false.")
            res.append(f"{self.params['fraction']}")
        else:
            res.append(f".true.")
            res.append(f"{self.params['fraction']}")

        res.append(f"# STATION FILE")
        res.append(f"{self.params['station']}")

        res.append(f"# INPUTS and PRECISION")
        res.append(f"{self.params['input_files_prefix']}")
        res.append(f"{self.params['input_file_precision']}")

        res.append(f"# OUTPUT FILES")
        res.append(f"{self.params['output_dir']}")

        res.append(f"# START and END")
        res.append(f"{self.params['start_date']}")
        res.append(f"{self.params['end_date']}")

        res.append(f"# UH")
        res.append(f"{self.params['uh']}")

        return '\n'.join(res)

    def _write(self):
        with open(os.path.join(self.project_dir,self.route_param_path), 'w') as f:
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