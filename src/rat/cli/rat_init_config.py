DOWNLOAD_LINKS = {
    'route_model': "https://www.dropbox.com/s/9jwep2g5pyj8sni/routing.zip?dl=1",
    'params': "https://www.dropbox.com/s/obb5bd24eb8u12h/params.zip?dl=1",
    'global_data': "https://www.dropbox.com/s/u8vc3oxujmaak97/global_data.zip?dl=1",
    'global_vic_params': "https://www.dropbox.com/s/jsg2wu62qi2ltwz/global_vic_params.zip?dl=1",
}

SUFFIXES_GLOBAL = {
    'GLOBAL': {
        'project_dir': '',
        'data_dir': 'data',
        'basin_shpfile': 'global_data/global_basin_data/shapefiles/mrb_basins.json',
        'elevation_tif_file': 'global_data/global_elevation_data/World_e-Atlas-UCSD_SRTM30-plus_v8.tif',
        'basins_metadata': 'params/basins_metadata.csv'
    },

    'METSIM':{
        'metsim_env': 'models/metsim',
        'metsim_param_file': 'params/metsim/params.yaml'
    },

    'VIC': {
        'vic_env': 'models/vic',
        'vic_param_file': 'params/vic/vic_params.txt',
        'vic_global_param_dir': 'global_data/global_vic_params'
    },

    'ROUTING': {
        'route_model': 'models/routing/rout',
        'route_param_file': 'params/routing/route_param.txt',
        'global_flow_dir_tif_file': 'global_data/global_drt_flow_file/global_drt_flow_16th.tif',
        'stations_vector_file': 'global_data/global_dam_data/GRanD_dams_v1_3_filtered.shp'
    },

    'ROUTING PARAMETERS':{
        'uh': 'params/routing/uh.txt'
    },

    'GEE': {
        'reservoir_vector_file': 'global_data/global_reservoir_data/GRanD_reservoirs_v1_3.shp'
    },

    'ALTIMETER': {
        'altimeter_tracks': 'global_data/global_altimetry/j3_tracks.geojson',
        'geoid_grid': 'global_data/global_altimetry/geoidegm2008grid.mat'
    },
}

SUFFIXES_NOTGLOBAL = {
    'GLOBAL': {
        'project_dir': '',
        'data_dir': 'data',
        'basins_metadata': 'params/basins_metadata.csv'
    },

    'METSIM':{
        'metsim_env': 'models/metsim',
        'metsim_param_file': 'params/metsim/params.yaml'
    },

    'VIC': {
        'vic_env': 'models/vic',
        'vic_param_file': 'params/vic/vic_params.txt',
    },

    'ROUTING': {
        'route_model': 'models/routing/rout',
        'route_param_file': 'params/routing/route_param.txt',
    },

    'ROUTING PARAMETERS':{
        'uh': 'params/routing/uh.txt'
    }
}

