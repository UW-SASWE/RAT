from datetime import datetime

DOWNLOAD_LINK_DROPBOX = {
    'test_data': "https://www.dropbox.com/scl/fi/f1pnyz9mo178kweh3agtf/test_data.zip?dl=1&rlkey=qxvsfrhc2li55dh4yj4a03bq2"
}
DOWNLOAD_LINK_GOOGLE = {
    'test_data': "https://drive.google.com/uc?id=1F5Z6f4rX7nBSXXo7XYGK_XC0i5-G6zBF"
}

PATHS = {
    'GUNNISON' : {
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
            'vic_soil_param_file': 'data/test_data/gunnison/vic_basin_params/vic_soil_param.nc',
            'vic_domain_file': 'data/test_data/gunnison/vic_basin_params/vic_domain.nc'
        },

        'ROUTING': {
            'route_model': 'models/routing/rout',
            'route_param_file': 'params/routing/route_param.txt',
            'station_latlon_path': 'data/test_data/gunnison/gunnison_reservoirs/gunnison_reservoirs_locations.csv'
        },

        'ROUTING PARAMETERS':{
            'flow_direction_file': 'data/test_data/gunnison/fl/fl.asc',
            'uh': 'params/routing/uh.txt'
        },

        'GEE': {
            'reservoir_vector_file': 'data/test_data/gunnison/gunnison_reservoirs/gunnison_reservoirs_named.geojson',
        },

    },
    'NUECES':{
        'GLOBAL': {
            'data_dir': 'data/test_output',
            'basin_shpfile': 'data/test_data/Nueces/basin_shapefile/Nueces_polygon.json',
        },

        'METSIM':{
            'metsim_env': 'models/metsim',
            'metsim_param_file': 'params/metsim/params.yaml',
            'metsim_domain_file': 'data/test_data/Nueces/metsim_inputs/domain.nc'
        },

        'VIC': {
            'vic_env': 'models/vic',
            'vic_param_file': 'params/vic/vic_params.txt',
            'vic_soil_param_file': 'data/test_data/Nueces/vic_basin_params/vic_soil_param.nc',
            'vic_domain_file': 'data/test_data/Nueces/vic_basin_params/vic_domain.nc'
        },

        'ROUTING': {
            'route_model': 'models/routing/rout',
            'route_param_file': 'params/routing/route_param.txt',
            'station_latlon_path': 'data/test_data/Nueces/reservoirs/basin_station_latlon.csv'
        },

        'ROUTING PARAMETERS':{
            'flow_direction_file': 'data/test_data/Nueces/fl/fl.asc',
            'uh': 'params/routing/uh.txt'
        },

        'GEE': {
            'reservoir_vector_file': 'data/test_data/Nueces/reservoirs/basin_reservoirs.shp',
        },
        
        }
}

PARAMS = {
    'GUNNISON':{
        'GLOBAL': {
            'steps':[1,2,3,4,5,6,7,8,9,10,12,13,14],
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
            'vic_global_data': False,
        },

        'GEE': {
            'reservoir_vector_file_columns_dict': {
                'id_column': None, 
                'dam_name_column': 'DAM_NAME',
                'area_column': 'area'}
        },

        'ROUTING': {
            'station_global_data': False
        }
    },
    'NUECES':{
        'GLOBAL': {
            'steps':[1,2,3,4,5,6,7,8,9,10,12,13,14],
            'basin_shpfile_column_dict': {'id': 'MRBID'},
            'multiple_basin_run': False,
        },
        
        'BASIN': {
            'region_name': 'Texas',
            'basin_name': 'Nueces',
            'basin_id': 4223,
            'spin_up': False,
            'start': datetime(2022, 8, 1),
            'end': datetime(2022, 8, 31),
        },

        'VIC': {
            'vic_global_data': False,
        },

        'GEE': {
            'reservoir_vector_file_columns_dict': 
                                    {'id_column' : 'GRAND_ID',
                                    'dam_name_column' : 'uniq_id',
                                    'area_column'     : 'AREA_SKM'} 
        },

        'ROUTING': {
            'station_global_data': False
        }
    }
}

TEST_PATHS = {
    'GUNNISON':{
        'expected_outputs': 'data/test_data/gunnison/expected_outputs',
        'rat_produced_outputs': 'data/test_output/colorado/basins/gunnison/final_outputs'
    },
    'NUECES':{
        'expected_outputs': 'data/test_data/Nueces/expected_outputs',
        'rat_produced_outputs': 'data/test_output/Texas/basins/Nueces/final_outputs'
    }
}
