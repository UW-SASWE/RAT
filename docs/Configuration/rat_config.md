# RAT Configuration File 

RAT Configuration settings are defined by a 'yaml' file. `rat init` command provides a `rat_config.yaml` file which is partially auto-filled for user's convenience and `rat_config_template.yaml` file which is just a template for the required rat configuration settings. User can use the latter to manually fill configuration file if they choose/feel not to use the auto-partially-filled configuration file. 

!!! note
    `rat_config.yaml` and `rat_config_template.yaml` files are present inside `params` directory at `./rat_project/params/`.

RAT runs over one single river-basin at a time. But RAT-3 has the flexibility to run over multiple river-basins using a single configuration file and thus avoids the need to have one configuration file for each river-basin.

!!! tip_note "Tip"
    Though time taken by RAT to operate over all the basins will be same if you use `rat run` command individually for each basin's configuration file or if you use that command for the single configuration file representing multiple basins, It will be convenient to have one single configuration file rather than multiple configuration files.

RAT config file has 12 major sections that defines several parameters which are needed to run rat. Each section parameters are indented to right by 4 spaces as compared to section heading. Parameters of each section are described below. 

!!! note
    For RAT Configuration File section, Default value is the auto-filled value of a parameter inserted by `rat init` command in the `rat_config` or `rat_config_template` file.

### Global

* <h6 class="parameter_heading">*`steps`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: List of steps that you want RAT to run using `rat run` command.  There are total 14 steps that RAT can run, details of which can be found here.

    <span class="parameter_property">Default </span>: `[1,2,3,4,5,6,7,8,9,10,11,12,13,14]`

    <span class="parameter_property">Syntax </span>: If you want to run RAT's step 1 and 2 only, then
    ```
    GLOBAL:
        steps: [1,2]
    ```
    or
    ```
    GLOBAL:
        steps: 
            -1
            -2
    ```
    
* <h6 class="parameter_heading">*`project_dir`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of RAT project directory. 

    <span class="parameter_property">Default </span>: Specified by `- d` or `--dir` option of `rat init` command. 

    <span class="parameter_property">Syntax </span>: If RAT project directory has the name *'rat_project'* and has the path *'/Cheetah/rat_project'*, then
    ```
    GLOBAL:
        project_dir: /Cheetah/rat_project/
    ```

* <h6 class="parameter_heading">*`data_dir`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of RAT output data directory. It can be an empty directory which will be used by RAT to store log files, intermediate and final outputs.

    <span class="parameter_property">Default </span>: A directory named *'data'* inside `project_dir` 

    <span class="parameter_property">Syntax </span>: If RAT data directory has the name *'data'* and has the path *'/Cheetah/rat_project/data'*, then
    ```
    GLOBAL:
        data_dir: /Cheetah/rat_project/data
    ```

    !!! tip_note "Tip"
        `data_dir` does not necessarily have to be inside `project_dir` even though it is recommended to do so. In case you don't have enough memory in `project_dir`, this tip can be handy. 

* <h6 class="parameter_heading">*`basin_shpfile`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the shapefile containing the polygons of the basin on which you want to run RAT. The shapefile must have a primary key column (unique id) for each basin in the shapefile. Currently 'json' and shapefile ('shp') are the supported formats. 

    <span class="parameter_property">Default </span>: If `-g` or `--global_data` option has been provided with `rat init` command, then the default path is *'`project_dir`/global_data/global_basin_data/shapefiles/mrb_basins.json'*, otherwise left blank. 

    <span class="parameter_property">Syntax </span>: If basin shapefile has the path *'/Cheetah/rat_project/global_data/global_basin_data /shapefiles/mrb_basins.json'*, then
    ```
    GLOBAL:
        basin_shpfile: /Cheetah/rat_project/global_data/global_basin_data/shapefiles/mrb_basins.json
    ```    
    !!! note
        Default shapefile is downloaded along with global-database and is provided by [Global Runoff Data Centre (GRDC)](https://www.bafg.de/GRDC/EN/02_srvcs/22_gslrs/221_MRB/riverbasins.html?nn=201274#doc2731742bodyText2). It has all the major river basins of the world.
    
* <h6 class="parameter_heading">*`basin_shpfile_column_dict`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Dictionary of column names for `basin_shpfile`. The dictionary must have a key 'id' and it's value should be the primary key (unique-id) column name.

    <span class="parameter_property">Default </span>: If `-g` or `--global_data` option has been provided with `rat init` command, then the default value is {'id':'MRBID'} for the default `basin_shpfile`.

    <span class="parameter_property">Syntax </span>: If elevation ras has a column named 'Basin-ID' where each basin has a unique id, then
    ```
    GLOBAL:
        basin_shpfile_column_dict: {'id':'Basin-ID'}
    ```
    or
    ```
    GLOBAL:
        basin_shpfile_column_dict: 
            id: Basin-ID
    ```

* <h6 class="parameter_heading">*`elevation_tif_file`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of a raster file having the global elevation data in 'geotif' format. Elevation should be in meters and the crs should be 'WGS84'.

    <span class="parameter_property">Default </span>: If `-g` or `--global_data` option has been provided with `rat init` command, path is *'`project_dir`/global_data/global_elevation_data/World_e-Atlas-UCSD_SRTM30-plus_v8.tif'*, otherwise left blank. 

    <span class="parameter_property">Syntax </span>: If basin shapefile has a path *'/Cheetah/rat_project/global_data/global_elevation_data/elevation.tif'*, then
    ```
    GLOBAL:
        elevation_tif_file: /Cheetah/rat_project/global_data/global_elevation_data/elevation.tif
    ```
    !!! note
        1. If this parameter is not provided, then `metsim_domain_file` must be provided in `METSIM` section of the config file.
        2. Default elevation raster 'geotif' file is downloaded along with global-database and uses SRTM-30_Plus version-8 data product which is provided by [University of California San Diego (UCSD)](https://eatlas.org.au/data/uuid/80301676-97fb-4bdf-b06c-e961e5c0cb0b). This dataset is a 30-arc second resolution global topography/bathymetry grid developed from a wide variety of data sources. 