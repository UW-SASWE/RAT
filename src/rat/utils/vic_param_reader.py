import datetime
import os
from logging import getLogger
from pathlib import Path

import yaml

from rat.utils.logging import LOG_NAME, NOTIFICATION, LOG_LEVEL
from rat.utils.utils import create_directory

log = getLogger(LOG_NAME)
log.setLevel(LOG_LEVEL)

class VICParameterFile:
    def __init__(self, config, basin_name, startdate=None, enddate=None, vic_output_path=None, vic_section='VIC',
                                     forcing_prefix=None, runname=None, init_state=None, save_init_state=True, init_state_dir=None,
                                      init_state_out_dir=None, intermediate_files= False):
        
        self.params = {
            'steps': {
                'MODEL_STEPS_PER_DAY'   : None,
                'SNOW_STEPS_PER_DAY'    : None,
                'RUNOFF_STEPS_PER_DAY'  : None
            },
            'dates': {
                'STARTYEAR'     : None,
                'STARTMONTH'    : None,
                'STARTDAY'      : None,
                'ENDYEAR'       : None,
                'ENDMONTH'      : None,
                'ENDDAY'        : None,
                'CALENDAR'      : None,
            },
            'domain': {
                'DOMAIN'        : None,
                'DOMAIN_TYPE'   : {
                    'LAT': None,
                    'LON': None,
                    'MASK': None,
                    'AREA': None,
                    'FRAC': None,
                    'YDIM': None,
                    'XDIM': None
                },
            },
            'state_file_params': {
                'INIT_STATE'    : None,
                'STATENAME'     : None,
                'STATEYEAR'     : None,
                'STATEMONTH'    : None,
                'STATEDAY'      : None,
                'STATESEC'      : 0,
                'STATE_FORMAT'  : 'NETCDF4',
            },
            'forcings': {
                'FORCING1'      : None,
                'FORCE_TYPE': {
                    'AIR_TEMP'      : None,
                    'PREC'          : None,
                    'PRESSURE'      : None,
                    'SWDOWN'        : None,
                    'LWDOWN'        : None,
                    'VP'            : None,
                    'WIND'          : None
                },
                'WIND_H'        : 10.0
            },
            'parameters': {
                'PARAMETERS'        : None,
                'VEGPARAM_LAI'      : 'TRUE',
                'LAI_SRC'           : 'FROM_VEGPARAM',
                'VEGPARAM_FCAN'     : 'TRUE',
                'FCAN_SRC'          : 'FROM_VEGPARAM',
                'VEGPARAM_ALBEDO'   : 'TRUE',
                'ALB_SRC'           : 'FROM_VEGPARAM',
                'NODES'             :  2,
                'SNOW_BAND'         : 'FALSE'
            },
            'results': {
                'RESULT_DIR'    : None,
                'LOG_DIR'       : None,
                'OUTFILE'       : None,
                'COMPRESS'      : 'FALSE',
                'OUT_FORMAT'    : 'NETCDF4',
                'AGGFREQ'       : 'NDAYS 1',
                'OUTVAR'        : ['OUT_PREC', 'OUT_EVAP', 'OUT_RUNOFF', 'OUT_BASEFLOW', 'OUT_SOIL_LIQ', 'OUT_SOIL_MOIST', 'OUT_EVAP_CANOP', 'OUT_EVAP_BARE', 'OUT_SWE', 'OUT_LAI']
            },
            'model_decisions': {
                'FULL_ENERGY'   : 'FALSE',
                'QUICK_FLUX'    : 'TRUE',
                'FROZEN_SOIL'   : 'FALSE'
            },
            'extras': {}
        }
        self.config = config
        self.basin_name = basin_name
        # self.forcing = forcing
        self.init_param_file = self.config[vic_section].get('vic_param_file', None)
        self.workspace = self.config[vic_section].get('vic_workspace', None)
        self.vic_param_path = None
        self.vic_result_file = None
        self.vic_startdate = None
        self.vic_enddate = None
        self.fn_param_vic_startdate = startdate if startdate else None
        self.fn_param_vic_enddate = enddate if enddate else None
        self.init_state = init_state if init_state else None

        self.vic_output_path = vic_output_path
        self.intermediate_files = intermediate_files
        self.straight_from_metsim = False
        self.save_init_state = save_init_state
        #VIC State Save Date
        self.vic_init_state_save_date = config['BASIN']['end']
        if init_state_dir:
            self.init_state_dir = init_state_dir
        else:
            self.init_state_dir = 'vic_init_states'
        if init_state_out_dir:
            self.init_state_out_dir = init_state_out_dir
        else:
            self.init_state_out_dir = 'vic_init_states'

        if runname is None:
            self.runname = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        else:
            self.runname = str(runname)
        
        if (self.intermediate_files):
            if self.workspace:
                self.workspace = create_directory(os.path.join(self.workspace, f'run_{self.runname}'),True)
            else :
                self.workspace = create_directory(os.path.join(config['GLOBAL']['data_dir'],config['BASIN']['region_name'],'basins',basin_name,'vic','vic_workspace',f'run_{self.runname}'),True)

        if self.init_param_file:
            self._load_from_vic_param()
        
        if forcing_prefix:
            self.straight_from_metsim = True
            self.forcing_prefix = forcing_prefix

        self._load_from_config()

        # self._write()

    def _load_from_vic_param(self):
        log.debug("Reading VIC parameters from %s", self.init_param_file)
        with open(self.init_param_file, 'r') as f:
            params = []
            lines = f.readlines()

            for line in lines:
                line = line.strip()
                if not line.startswith("#") and not len(line) == 0:
                    line = line.split()
                    params.append(line)

        for line in params:
            log.debug("Initializing: %s", ' '.join(line))
            # Domain Vars
            if line[0] == 'DOMAIN_TYPE':
                self.params['domain']['DOMAIN_TYPE'][line[1]] = line[2]
            
            # Forcings
            elif line[0] == 'FORCE_TYPE':
                self.params['forcings']['FORCE_TYPE'][line[1]] = line[2]
            
            # Outvars. Add if not present in default list
            elif line[0] == 'OUTVAR':
                if line[1] not in self.params['results']['OUTVAR']:
                    self.params['results']['OUTVAR'].append(line[1])

            # Others
            elif line[0] in self.params['steps']:
                self.params['steps'][line[0]] = ' '.join(line[1:])

            elif line[0] in self.params['dates']:
                self.params['dates'][line[0]] = ' '.join(line[1:])

            elif line[0] in self.params['domain']:
                self.params['domain'][line[0]] = ' '.join(line[1:])

            elif line[0] in self.params['state_file_params']:
                self.params['state_file_params'][line[0]] = ' '.join(line[1:])

            elif line[0] in self.params['forcings']:
                self.params['forcings'][line[0]] = ' '.join(line[1:])

            elif line[0] in self.params['parameters']:
                self.params['parameters'][line[0]] = ' '.join(line[1:])

            elif line[0] in self.params['results']:
                self.params['results'][line[0]] = ' '.join(line[1:])

            elif line[0] in self.params['model_decisions']:
                self.params['model_decisions'][line[0]] = ' '.join(line[1:])

            # if not found in pre-defined variables, output a warning
            else:
                self.params['extras'][line[0]] = ' '.join(line[1:])
    
    def _load_from_config(self):
        config = self.config

        # Start and End date
        # Initialize by calculating first. Will get overriden later if specified in configuration
        if (config['BASIN'].get('begin',None)):
            self.params['dates']['STARTYEAR'] = (config['BASIN']['begin'] + datetime.timedelta(days=90)).strftime('%Y')
            self.params['dates']['STARTMONTH'] = (config['BASIN']['begin'] + datetime.timedelta(days=90)).strftime('%m')
            self.params['dates']['STARTDAY'] = (config['BASIN']['begin'] + datetime.timedelta(days=90)).strftime('%d')
        self.params['dates']['ENDYEAR'] = config['BASIN']['end'].strftime('%Y')
        self.params['dates']['ENDMONTH'] = config['BASIN']['end'].strftime('%m')
        self.params['dates']['ENDDAY'] = config['BASIN']['end'].strftime('%d')

        # Rename forcing file
        if self.straight_from_metsim:
            self.params['forcings']['FORCING1'] = self.forcing_prefix
        
        # Puttting the path of vic_domain and vic_soil_param NC files in global vic param file
        self.params['parameters']['PARAMETERS'] = config['VIC']['vic_soil_param_file']
        self.params['domain']['DOMAIN'] = config['VIC']['vic_domain_file']
 
        # Save vic run logs and parameters in `vic_workspace` if intermediate_files is true 
        # else just replacing the init_param_file if given in config or will update(or create) it in vic_basin_params 
        # and log file will be update(or created) in data_dir/vic_logs/basin_name/
        if (self.intermediate_files):
            self.vic_param_path = os.path.join(self.workspace, 'vic_param.txt')    # Paramter file will be saved here
            self.params['results']['LOG_DIR'] = create_directory(os.path.join(self.workspace, 'logs',''),True)
            
        else:
            # Replacing the init_param_file if given
            if self.init_param_file:
                self.vic_param_path = self.init_param_file   # Paramter file will be replaced
            # Or storing it in vic basin params dir and replace it from next cycle
            else:
                self.vic_param_path = create_directory(os.path.join(config['GLOBAL']['data_dir'],config['BASIN']['region_name'],
                                                            'basins',self.basin_name,'vic','vic_basin_params'),True)
                self.vic_param_path = os.path.join(self.vic_param_path,'vic_param.txt')
            self.params['results']['LOG_DIR'] = create_directory(os.path.join(config['GLOBAL']['data_dir'],config['BASIN']['region_name'],
                                                             'vic_logs',self.basin_name,''),True) 
        log.debug("VIC Logs Directory: %s ", self.params['results']['LOG_DIR'])
        # if not self.params['results']['LOG_DIR'].endswith(os.sep):
        #     self.params['results']['LOG_DIR'] = self.params['results']['LOG_DIR'] + f'{os.sep}'

            

        # Reading vic parameters directly from rat_runner config file
        if config['VIC PARAMETERS'] is not None:
            if 'STARTYEAR' in config['VIC PARAMETERS']:
                log.debug("Updating from config: %s", 'MODEL RUN PERIOD')
                self.params['dates']['STARTYEAR'] = config['VIC PARAMETERS']['STARTYEAR']
                self.params['dates']['STARTMONTH'] = config['VIC PARAMETERS']['STARTMONTH']
                self.params['dates']['STARTDAY'] = config['VIC PARAMETERS']['STARTDAY']
                self.params['dates']['ENDYEAR'] = config['VIC PARAMETERS']['ENDYEAR']
                self.params['dates']['ENDMONTH'] = config['VIC PARAMETERS']['ENDMONTH']
                self.params['dates']['ENDDAY'] = config['VIC PARAMETERS']['ENDDAY']
            
            if 'DOMAIN_TYPE' in config['VIC PARAMETERS']:
                log.debug("Updating from config: %s", 'DOMAIN')
                for key in config['VIC PARAMETERS']['DOMAIN_TYPE']:
                    log.debug("Updating from config: %s %s", key, val)
                    val = config['VIC PARAMETERS']['DOMAIN_TYPE'][key]
                    self.params['domain']['DOMAIN_TYPE'][key] = val
            
            if 'FORCE_TYPE' in config['VIC PARAMETERS']:
                log.debug("Updating from config: %s", 'FORCINGS')
                for key in config['VIC PARAMETERS']['FORCE_TYPE']:
                    val = config['VIC PARAMETERS']['FORCE_TYPE'][key]
                    log.debug("Updating from config: %s %s", key, val)
                    self.params['forcings']['FORCE_TYPE'][key] = val
            
            if 'OUTVAR' in config['VIC PARAMETERS']:
                log.debug("Updating from config: %s", 'OUTPUT VARIABLES')
                for var in config['VIC PARAMETERS']['OUTVAR']:
                    if var not in self.params['results']['OUTVAR']:
                        log.debug("Updating from config: %s", var)
                        self.params['results']['OUTVAR'].append(var)
            
            # Handle rest of the options.
            for key in config['VIC PARAMETERS']:
                if key not in ('STARTYEAR', 'STARTMONTH', 'STARTDAY', 'ENDYEAR', 'ENDMONTH', 'ENDDAY', 'DOMAIN_TYPE', 'FORCE_TYPE', 'OUTVAR'):
                    log.debug("Updating from config: %s %s", key, config['VIC PARAMETERS'][key])
                    if key in self.params['steps']:
                        self.params['steps'][key] = config['VIC PARAMETERS'][key]
                    elif key in self.params['dates']:
                        self.params['dates'][key] = config['VIC PARAMETERS'][key]
                    elif key in self.params['forcings']:
                        self.params['forcings'][key] = config['VIC PARAMETERS'][key]
                    elif key in self.params['state_file_params']:
                        self.params['state_file_params'][key] = config['VIC PARAMETERS'][key]
                    elif key in self.params['parameters']:
                        self.params['parameters'][key] = config['VIC PARAMETERS'][key]
                    elif key in self.params['results']:
                        self.params['results'][key] = config['VIC PARAMETERS'][key]
                    elif key in self.params['model_decisions']:
                        self.params['model_decisions'][key] = config['VIC PARAMETERS'][key]
                    else:
                        self.params['extras'][key] = config['VIC PARAMETERS'][key]

        # if start and enddates were passed using the constructor parameters, override the dates with them
        if self.fn_param_vic_startdate:
            self.params['dates']['STARTYEAR'] = self.fn_param_vic_startdate.strftime('%Y')
            self.params['dates']['STARTMONTH'] = self.fn_param_vic_startdate.strftime('%m')
            self.params['dates']['STARTDAY'] = self.fn_param_vic_startdate.strftime('%d')
        
        if self.fn_param_vic_enddate:
            self.params['dates']['ENDYEAR'] = self.fn_param_vic_enddate.strftime('%Y')
            self.params['dates']['ENDMONTH'] = self.fn_param_vic_enddate.strftime('%m')
            self.params['dates']['ENDDAY'] = self.fn_param_vic_enddate.strftime('%d')
        
        # If vic_output_path is passed as constructor parameter, override it
        if self.vic_output_path:
            self.params['results']['RESULT_DIR'] = self.vic_output_path
        
        # Saving initital state file for vic's next run  if save_init_state is True else deleting STATENAME from state_file_params
        if (self.save_init_state):
            create_directory(os.path.join(config['GLOBAL']['data_dir'],config['BASIN']['region_name'],'basins',self.basin_name,'vic', self.init_state_out_dir),False)
            self.params['state_file_params']['STATENAME'] = os.path.join(config['GLOBAL']['data_dir'],config['BASIN']['region_name'],'basins',self.basin_name,'vic', self.init_state_out_dir,'state_')
            self.params['state_file_params']['STATEYEAR'] = self.vic_init_state_save_date.strftime('%Y')
            self.params['state_file_params']['STATEMONTH'] = self.vic_init_state_save_date.strftime('%m')
            self.params['state_file_params']['STATEDAY'] = self.vic_init_state_save_date.strftime('%d')
        else:
            del self.params['state_file_params']['STATENAME']

        # Using initial state file for this run if init_state is not None else deleting INIT_STATE from state_file_params
        if (self.init_state):
            if(isinstance(self.init_state, datetime.date)):
                init_state_date_str = str(self.init_state.strftime('%Y'))+str(self.init_state.strftime('%m'))+\
                                    str(self.init_state.strftime('%d'))
                
                self.params['state_file_params']['INIT_STATE'] = os.path.join(config['GLOBAL']['data_dir'],config['BASIN']['region_name'],
                                                            'basins',self.basin_name,'vic',self.init_state_dir,
                                                            'state_.'+init_state_date_str+'_00000.nc')
            elif(isinstance(self.init_state, str)):
                init_state_file_path = Path(self.init_state).resolve()
                self.params['state_file_params']['INIT_STATE'] = init_state_file_path
        else:
            del self.params['state_file_params']['INIT_STATE']

    def _out_format_params(self): # return a VIC compatible string of paramters
        header = '\n'.join([
            f'#------------------------- VIC Parameter File -------------------------#',
            f'### VIC Parameter file created by VICParamerFile() on {datetime.datetime.now().strftime("%Y-%m-%d %X")}'
        ])
        model_steps = '\n'.join([
            '# Model Steps',
            f'MODEL_STEPS_PER_DAY   {self.params["steps"]["MODEL_STEPS_PER_DAY"]}',
            f'SNOW_STEPS_PER_DAY    {self.params["steps"]["SNOW_STEPS_PER_DAY"]}',
            f'RUNOFF_STEPS_PER_DAY  {self.params["steps"]["RUNOFF_STEPS_PER_DAY"]}'
        ])
        simulation_period = '\n'.join([
            '# Simulation Period',
            f'STARTYEAR             {self.params["dates"]["STARTYEAR"]}',
            f'STARTMONTH            {self.params["dates"]["STARTMONTH"]}',
            f'STARTDAY              {self.params["dates"]["STARTDAY"]}',
            f'ENDYEAR               {self.params["dates"]["ENDYEAR"]}',
            f'ENDMONTH              {self.params["dates"]["ENDMONTH"]}',
            f'ENDDAY                {self.params["dates"]["ENDDAY"]}',
            f'CALENDAR              {self.params["dates"]["CALENDAR"]}'
        ])
        model_decisions = '\n'.join([
            '# Model decisions',
            f'FULL_ENERGY           {self.params["model_decisions"]["FULL_ENERGY"]}',
            f'QUICK_FLUX            {self.params["model_decisions"]["QUICK_FLUX"]}',
            f'FROZEN_SOIL           {self.params["model_decisions"]["FROZEN_SOIL"]}',
        ])
        domain = [
            '# Domain',
            f'DOMAIN                {self.params["domain"]["DOMAIN"]}',
        ]
        for key in self.params['domain']['DOMAIN_TYPE']:
            val = self.params['domain']['DOMAIN_TYPE'][key]
            domain.append(f'DOMAIN_TYPE           {key}\t{val}')
        domain = '\n'.join(domain)

        if self.init_state:
            init_state_prefix=''
        else:
            init_state_prefix='#'
        if self.save_init_state:
            statename_prefix=''
        else:
            statename_prefix='#'
        state_file_parameters = '\n'.join([
            '# State Files and Parameters',
            f"{init_state_prefix}INIT_STATE           {self.params['state_file_params'].get('INIT_STATE','')}",
            f"{statename_prefix}STATENAME             {self.params['state_file_params'].get('STATENAME','')}",
            f"STATEYEAR             {self.params['state_file_params'].get('STATEYEAR','')}",
            f"STATEMONTH            {self.params['state_file_params'].get('STATEMONTH','')}",
            f"STATEDAY              {self.params['state_file_params'].get('STATEDAY','')}",
            f"STATESEC              {self.params['state_file_params'].get('STATESEC','')}",
            f"STATE_FORMAT          {self.params['state_file_params'].get('STATE_FORMAT','')}"
        ])

        forcings = '\n'.join([
            '# Forcings',
            f'FORCING1              {self.params["forcings"]["FORCING1"]}',
            f'FORCE_TYPE            AIR_TEMP        {self.params["forcings"]["FORCE_TYPE"]["AIR_TEMP"]}',
            f'FORCE_TYPE            PREC            {self.params["forcings"]["FORCE_TYPE"]["PREC"]}',
            f'FORCE_TYPE            PRESSURE        {self.params["forcings"]["FORCE_TYPE"]["PRESSURE"]}',
            f'FORCE_TYPE            SWDOWN          {self.params["forcings"]["FORCE_TYPE"]["SWDOWN"]}',
            f'FORCE_TYPE            LWDOWN          {self.params["forcings"]["FORCE_TYPE"]["LWDOWN"]}',
            f'FORCE_TYPE            VP              {self.params["forcings"]["FORCE_TYPE"]["VP"]}',
            f'FORCE_TYPE            WIND            {self.params["forcings"]["FORCE_TYPE"]["WIND"]}',
            f'WIND_H                {self.params["forcings"]["WIND_H"]}'
        ])

        parameters = '\n'.join([
            f'# Parameters',
            f'PARAMETERS            {self.params["parameters"]["PARAMETERS"]}',
            f'LAI_SRC               {self.params["parameters"]["LAI_SRC"]}',
            f'FCAN_SRC              {self.params["parameters"]["FCAN_SRC"]}',
            f'ALB_SRC               {self.params["parameters"]["ALB_SRC"]}',
            f'NODES                 {self.params["parameters"]["NODES"]}',
            f'SNOW_BAND             {self.params["parameters"]["SNOW_BAND"]}',
        ])

        results = [
            f'# Results',
            f'RESULT_DIR            {self.params["results"]["RESULT_DIR"]}',
            f'OUTFILE               {self.params["results"]["OUTFILE"]}',
            f'COMPRESS              {self.params["results"]["COMPRESS"]}',
            f'OUT_FORMAT            {self.params["results"]["OUT_FORMAT"]}',
            f'AGGFREQ               {self.params["results"]["AGGFREQ"]}',
        ]
        if self.params["results"]["LOG_DIR"]:
            results.append(f'LOG_DIR               {self.params["results"]["LOG_DIR"]}')
        results = '\n'.join(results)

        outvars = [
            f'# Output Variables'
        ]
        for var in self.params['results']['OUTVAR']:
            outvars.append(f'OUTVAR                {var}')
        outvars = '\n'.join(outvars)

        # Any Remaining Extras
        extras = [
            f"# Extra options"
        ]
        if len(self.params['extras']) > 0:
            for key in self.params['extras']:
                val = self.params["extras"][key]
                extras.append(f'{key}           {val}')
        extras = '\n'.join(extras)

        res = '\n\n'.join([
            header, 
            model_steps, 
            simulation_period, 
            model_decisions, 
            domain, 
            state_file_parameters,
            forcings, 
            parameters,
            extras, 
            results,
            outvars
        ])

        return res

    def _write(self):
        with open(self.vic_param_path, 'w') as f:
            param = self._out_format_params()
            log.debug(param)
            f.write(param)

    def __enter__(self):
        if (self.intermediate_files):
            # Save config file used to run the program too
            config_record_path = os.path.join(self.workspace, 'config_record.yml')
            yaml.dump(self.config, open(config_record_path, 'w'))
            # Save copy of global vic parameter file used to initialize parameters
            if self.init_param_file:
                with open(os.path.join(self.workspace, 'init_global_params.txt'), 'w') as dst:
                    with open(self.init_param_file, 'r') as src:
                        dst.writelines(src.readlines())

        # Determine output file
        # Assuming only one file is generated
        startddate_str = f'{self.params["dates"]["STARTYEAR"]}-{self.params["dates"]["STARTMONTH"]}-{self.params["dates"]["STARTDAY"]}'
        self.vic_startdate = datetime.datetime.strptime(startddate_str, '%Y-%m-%d')
        enddate_str = f'{self.params["dates"]["ENDYEAR"]}-{self.params["dates"]["ENDMONTH"]}-{self.params["dates"]["ENDDAY"]}'
        self.vic_enddate = datetime.datetime.strptime(enddate_str, '%Y-%m-%d')
        self.vic_result_file = os.path.join(self.params['results']['RESULT_DIR'], f'{self.params["results"]["OUTFILE"]}.{startddate_str}.nc')

        self._write()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass
