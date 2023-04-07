# RAT Configuration File 

RAT Configuration settings are defined by a 'yaml' file. `rat init` command provides a `rat_config.yaml` file which is partially auto-filled for user's convenience and `rat_config_template.yaml` file which is just a template for the required rat configuration settings. User can use the latter to manually fill configuration file if they choose/feel not to use the auto-partially-filled configuration file. 

!!! note
    `rat_config.yaml` and `rat_config_template.yaml` files are present inside `params` directory at `./rat_project/params/`.

RAT runs over one single river-basin at a time. But RAT {{rat_version.major}}.{{rat_version.minor}} has the flexibility to run over multiple river-basins using a single configuration file and thus avoids the need to have one 
configuration file for each river-basin.

!!! tip_note "Tip"
    Though time taken by RAT to operate over all the basins will be same if you use `rat run` command individually for each basin's configuration file or if you use that command for the single configuration file representing multiple basins, It will be convenient to have one single configuration file rather than multiple configuration files.

RAT config file has 12 major sections that defines several parameters which are needed to run rat. Each section parameters are indented to right by 4 spaces as compared to section heading. Parameters of each section are described below. 

!!! note
    For RAT Configuration File section, Default value is the auto-filled value of a parameter inserted by `rat init` command in the `rat_config` or `rat_config_template` files.

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

    <span class="parameter_property">Default </span>: Specified by `-d` or `--dir` option of `rat init` command. 

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

    <span class="parameter_property">Default </span>: If `-g` or `--global_data` option has been provided with `rat init` command, then the default value is {'id':'MRBID'} for the default `basin_shpfile`.Otherwise, left blank to be filled by the user.

    <span class="parameter_property">Syntax </span>: If `basin_shpfile` has a column named 'Basin-ID' where each basin has a unique id, then
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

    <span class="parameter_property">Syntax </span>: If elevation raster file has a path *'/Cheetah/rat_project/global_data/global_elevation_data/elevation.tif'*, then
    ```
    GLOBAL:
        elevation_tif_file: /Cheetah/rat_project/global_data/global_elevation_data/elevation.tif
    ```
    !!! note
        1. If this parameter is not provided, then `metsim_domain_file` must be provided in `METSIM` section of the config file.
        2. Default elevation raster 'geotif' file is downloaded along with global-database and uses SRTM-30_Plus version-8 data product which is provided by [University of California San Diego (UCSD)](https://eatlas.org.au/data/uuid/80301676-97fb-4bdf-b06c-e961e5c0cb0b). This dataset is a 30-arc second resolution global topography/bathymetry grid developed from a wide variety of data sources. 

* <h6 class="parameter_heading">*`multiple_basin_run`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: `True` if you want to run RAT for multiple basins using `rat run` command according to the parameters `basins_metadata` and `basins_to_process`. `False` otherwise. 

    <span class="parameter_property">Default </span>: `False`

    <span class="parameter_property">Syntax </span>: If you want to run RAT for just one basin, then
    ```
    GLOBAL:
        multiple_basin_run: False
    ```

### Basin 

* <h6 class="parameter_heading">*`region_name`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Name of the region in which the basin is located. It is used only to store RAT outputs in a directory named `region_name`. Multiple basins can have a same `region_name`.

    <span class="parameter_property">Default </span>: It is blank by default and should be filled by the user.

    <span class="parameter_property">Syntax </span>: If you want to run RAT for, say 'Sabine' basin, then the `region_name` can be 'Texas' as the basin lies in Texas state or 'North_America'.
    ```
    BASIN:
        region_name: Texas
    ```

* <h6 class="parameter_heading">*`basin_name`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Name of the basin. It is used only to store RAT outputs in a directory named `basin_name` inside a directory named `region_name`. Each basins should have a unique `basin_name`.

    <span class="parameter_property">Default </span>: It is blank by default and should be filled by the user.

    <span class="parameter_property">Syntax </span>: If you want to run RAT for, say 'Sabine' basin, then the `basin_name` can be 'Sabine'.
    ```
    BASIN:
        basin_name: Sabine
    ```
    !!! warning_note "Caution"
        Do not use blank spaces in `region_name` and `basin_name`. Instead, use underscore (_).

* <h6 class="parameter_heading">*`basin_id`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Unique ID of the basin. It is value of the column 'id' defined in `basin_shpfile_column_dict` in Global section. It is used to map the basin in the `basin_shpfile` over which you want to run RAT for.

    <span class="parameter_property">Default </span>: It is blank by default and should be filled by the user.

    <span class="parameter_property">Syntax </span>: If you want to run RAT for, say 'Sabine' basin, and the 'unique-id' column has a value of, say 2341, then
    ```
    BASIN:
        basin_id: 2341
    ```

* <h6 class="parameter_heading">*`spin_up`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: True, if you want RAT to run VIC with a spin up period, recommended if you are running RAT first time for a particular basin and you don't have VIC State File for `start` date. Otherwise, False.

    <span class="parameter_property">Default </span>: `True`

    <span class="parameter_property">Syntax </span>: Suppose, if you have run RAT for the period 2022-03-01 to 2022-08-01 and want to run RAT for second time for a period 2022-08-01 to 2022-08-31, then
    ```
    BASIN:
        spin_up: False
    ```    

* <h6 class="parameter_heading">*`start`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Start date of the time period for which you want to run RAT in the format `yyyy-mm-dd`.

    <span class="parameter_property">Default </span>: It is blank by default and should be filled by the user.

    <span class="parameter_property">Syntax </span>: Suppose, if you want to run RAT for a period 2022-08-01 to 2022-08-31, then
    ```
    BASIN:
        start: 2022-08-01
    ```

* <h6 class="parameter_heading">*`end`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: End date of the time period for which you want to run RAT in the format `yyyy-mm-dd`.

    <span class="parameter_property">Default </span>: It is blank by default and should be filled by the user.

    <span class="parameter_property">Syntax </span>: Suppose, if you want to run RAT for a period 2022-08-01 to 2022-08-31, then
    ```
    BASIN:
        end: 2022-08-31
    ```

* <h6 class="parameter_heading">*`vic_init_state_date`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: It is the date (in format `yyyy-mm-dd`) for which you have the initial state VIC parameters available so that `spin_up` is not required. VIC use these soil state parameters to initialize. 

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: Suppose, if you have run RAT for the period 2022-03-01 to 2022-08-01 and want to run RAT for second time for a period 2022-08-01 to 2022-08-31, then
    ```
    BASIN:
        vic_init_state_date: 2022-08-01
    ```

    !!! note
        1. If `spin_up` is True, the value of `vic_init_state_date` is ignored.
        2. Every time you run RAT successfully, it by default saves a VIC state file for the `end` date.
        3. Generally, the value of `vic_init_state_date` should be same (or as close as possible) to the vic_init_state_date. Since VIC initializes with the soil parameters of that date, it is best practice to start running RAT from the same date.  

    !!! warning_note "Caution"
        If `spin_up` is False and `vic_init_state_date` is blank or not provided, the RAT results will not have any significance for the initial 24-28 months of the simulated time period as they were produced using VIC computed inflow without considering any spin up.

### Metsim 
    
* <h6 class="parameter_heading">*`metsim_env`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the directory in which virtual metsim python environment has been installed. 

    <span class="parameter_property">Default </span>: *'`project_dir`/models/metsim'* 

    <span class="parameter_property">Syntax </span>: If metsim python environment has the path *'/Cheetah/rat_project/models/metsim'*, then
    ```
    GLOBAL:
        metsim_env: /Cheetah/rat_project/models/metsim
    ``` 
    !!! reminder_note "Reminder"
        MetSim gets automatically installed at the above mentioned default path, once you use `rat_init` command.  

* <h6 class="parameter_heading">*`metsim_param_file`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the parameter file required by MetSim in 'yaml' format. Details about the parameter file is available [here](https://metsim.readthedocs.io/en/develop/configuration.html). It can also be the path to sample metsim parameter file as RAT automatically updates it during `rat run` command. For further information, see the tip below.

    <span class="parameter_property">Default </span>: *'`project_dir`/params/metsim/params.yaml'* 

    <span class="parameter_property">Syntax </span>: If metsim parameter file has the path *'/Cheetah/rat_project/params/metsim/params.yaml'*, then
    ```
    GLOBAL:
        metsim_param_file: /Cheetah/rat_project/params/metsim/params.yaml
    ```
    !!! tip_note "Tip"
        1. RAT {{rat_version.major}}.{{rat_version.minor}} automatically downloads a sample of `metsim_param_file` once you use  `rat init` command. 
        2. You do not need to manually update `metsim_param_file` as RAT automatically updates it with the known parameters (which usually includes input and output paths, data formats, start and end dates, etc.). If there is any **constant** parameter that you want to update (not necessary), you can do it in the default sample copy.

* <h6 class="parameter_heading">*`metsim_domain_file`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the domain parameter file required by MetSim in NetCDF format. Details about the domain parameter file is available [here](https://metsim.readthedocs.io/en/develop/data.html#domain-file).

    <span class="parameter_property">Default </span>:  It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If metsim domain parameter file has the path *'/Cheetah/rat_project/custom_files/metsim_domain.nc'*, then
    ```
    GLOBAL:
        metsim_domain_file: /Cheetah/rat_project/custom_files/metsim_domain.nc
    ```
    !!! note
        `metsim_domain_file` is only required if `elevation_tif_file` in Global section is not provided, otherwise it is ignored. RAT {{rat_version.major}}.{{rat_version.minor}} automatically prepares a metsim domain file using an elevation raster and basin polygon. 

* <h6 class="parameter_heading">*`historical_precipitation`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the historical precipitation data (>= 3years) that extends atleast over the polygon of `basin_name` in NetCDF format. If provided, it will be used by RAT for climatology based correction of satellite precipitation

    <span class="parameter_property">Default </span>:  It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If historical precipitation file has the path *'/Cheetah/rat_project/custom_files/ historical_precipitation.nc'*, then
    ```
    GLOBAL:
        historical_precipitation: /Cheetah/rat_project/custom_files/historical_precipitation.nc
    ```

### Vic

* <h6 class="parameter_heading">*`vic_env`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the directory in which virtual vic python environment has been installed. 

    <span class="parameter_property">Default </span>: *'`project_dir`/models/vic'* 

    <span class="parameter_property">Syntax </span>: If vic python environment has the path *'/Cheetah/rat_project/models/vic'*, then
    ```
    GLOBAL:
        vic_env: /Cheetah/rat_project/models/vic
    ``` 
    !!! reminder_note "Reminder"
        VIC gets automatically installed at the above mentioned default path, once you use `rat_init` command.  

* <h6 class="parameter_heading">*`vic_param_file`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the vic's global parameter file required by vic in 'txt' format. Details about the parameter file is available [here](https://vic.readthedocs.io/en/master/Documentation/Drivers/Image/GlobalParam/). It can also be the path to sample vic global parameter file as RAT automatically updates it during `rat run` command. For further information, see the tip below.

    <span class="parameter_property">Default </span>: *'`project_dir`/params/vic/vic_params.txt'* 

    <span class="parameter_property">Syntax </span>: If vic parameter file has the path *'/Cheetah/rat_project/params/vic/vic_params.txt'*, then
    ```
    GLOBAL:
        vic_param_file: /Cheetah/rat_project/params/vic/vic_params.txt
    ```
    !!! tip_note "Tip"
        1. RAT {{rat_version.major}}.{{rat_version.minor}} automatically downloads a sample of `vic_param_file` once you use `rat init` command. 
        2. You do not need to manually update `vic_param_file` as RAT automatically updates it with the known parameters (which usually includes input and output paths, data formats, start and end dates, etc.). If there is any **constant** parameter that you want to update (not necessary), you can do it in the default sample copy.


