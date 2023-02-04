from datetime import datetime

GUNNISON_PATHS = {
    'GLOBAL': {
        'data_dir': 'data/test_output',
        'basin_shpfile': 'data/test_data/gunnison/gunnison_boundary/gunnison_boundary.shp',
    },

    'METSIM':{
        'metsim_env': 'models/metsim',
        'metsim_param_file': 'params/metsim/params.yaml',
        'metsim_domain_file': 'data/test_data/gunnison/metsim_inputs/domain.nc'
    },

    'VIC': {
        'vic_env': 'models/vic',
        'vic_param_file': 'params/vic/vic_params.txt',
    },

    'ROUTING': {
        'route_model': 'models/routing/rout',
        'route_param_file': 'params/routing/route_param.txt',
        'route_flow_dir': 'data/test_data/gunnison/fl/fl.asc',
        'station_latlon_path': 'data/test_data/gunnison/gunnison_reservoirs/gunnison_reservoirs_locations.csv'
    },

    'ROUTING PARAMETERS':{
        'uh': 'params/routing/uh.txt'
    },

    'GEE': {
        'reservoir_vector_file': 'data/test_data/gunnison/gunnison_reservoirs/gunnison_reservoirs_named.geojson',
    },

    'ALTIMETER': {
        'altimeter_tracks': 'global_data/global_altimetry/j3_tracks.geojson',
        'geoid_grid': 'global_data/global_altimetry/geoidegm2008grid.mat'
    },
}

GUNNISON_PARAMS = {
    'GLOBAL': {
        'basin_shpfile_column_dict': {'id': 'gridcode'},
        'multiple_basin_run': False,
    },
    
    'BASIN': {
        'region_name': 'colorado',
        'basin_name': 'gunnison',
        'basin_id': 0,
        'spin_up': False,
        'start': datetime(2022, 1, 1),
        'end': datetime(2022, 1, 31),
    },

    'VIC': {
        'vic_global_data': True,
        'vic_soil_param_file': 'data/test_data/gunnison/vic_basin_params/vic_domain.nc',
        'vic_domain_file': 'data/test_data/gunnison/vic_basin_params/vic_soil_param.nc',
    },

    'ROUTING': {
        'station_global_data': False
    }
}