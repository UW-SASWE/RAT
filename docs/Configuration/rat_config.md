# RAT Configuration File 

RAT Configuration settings are defined by a 'yaml'/'yml' file. `rat init` command provides a `rat_config.yaml` file which is partially auto-filled for user's convenience and `rat_config_template.yaml` file which is just a template for the required rat configuration settings. User can use the latter to manually fill configuration file if they choose/feel not to use the auto-partially-filled configuration file. 

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

    <span class="parameter_property">Description </span>: `True` if you want to run RAT for multiple basins using `rat run` command according to the parameters `basins_metadata` and `basins_to_process`. `False` otherwise. For more information, please look [multiple basin run](../basins_metadata).

    <span class="parameter_property">Default </span>: `False`

    <span class="parameter_property">Syntax </span>: If you want to run RAT for just one basin, then
    ```
    GLOBAL:
        multiple_basin_run: False
    ```

* <h6 class="parameter_heading">*`basins_metadata`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the basins metadata file in csv format. It is a csv file that is multi-indexed and have same index as this configuration file. For more information, please look [multiple basin run](../basins_metadata).

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If `basins_metadata` file has a path *'/Cheetah/rat_project/params/ basins_metadata_sample.csv'*, then
    ```
    GLOBAL:
        basins_metadata: /Cheetah/rat_project/params/basins_metadata_sample.csv
    ```

* <h6 class="parameter_heading">*`basins_to_process`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: List of basins to run RAT for within the `basins_metadata`. The list values must match with the values of `basin_name` in `BASIN` section in `basins_metadata`.  For more information, please look [multiple basin run](../basins_metadata).

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If you want to run RAT for basins Sabine and Nueces, then
    ```
    GLOBAL:
        basins_to_process: ['Sabine','Nueces']
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

    <span class="parameter_property">Description </span>: It is the date (in format `yyyy-mm-dd`) for which you have the initial state VIC pARAMETERS available so that `spin_up` is not required. VIC use these soil state parameters to initialize. 

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
    METSIM:
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
    METSIM:
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
    METSIM:
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
    METSIM:
        historical_precipitation: /Cheetah/rat_project/custom_files/historical_precipitation.nc
    ```

### VIC

* <h6 class="parameter_heading">*`vic_env`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the directory in which virtual vic python environment has been installed. 

    <span class="parameter_property">Default </span>: *'`project_dir`/models/vic'* 

    <span class="parameter_property">Syntax </span>: If vic python environment has the path *'/Cheetah/rat_project/models/vic'*, then
    ```
    VIC:
        vic_env: /Cheetah/rat_project/models/vic
    ``` 
    !!! reminder_note "Reminder"
        VIC gets automatically installed at the above mentioned default path, once you use `rat_init` command.  

* <h6 class="parameter_heading">*`vic_param_file`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the vic's global parameter file required by vic in 'txt' format. Details about the parameter file is available [here](https://vic.readthedocs.io/en/master/Documentation/Drivers/Image/GlobalParam/). It is used as a sample vic global parameter file as RAT automatically updates it during `rat run` command. For further information, see the tip below.

    <span class="parameter_property">Default </span>: *'`project_dir`/params/vic/vic_params.txt'* 

    <span class="parameter_property">Syntax </span>: If vic parameter file has the path *'/Cheetah/rat_project/params/vic/vic_params.txt'*, then
    ```
    VIC:
        vic_param_file: /Cheetah/rat_project/params/vic/vic_params.txt
    ```
    !!! note
        `vic_param_file` is ***optional*** and can be left blank if all the required parameters are defined in `VIC PARAMETERS` section. 
    !!! tip_note "Tip"
        1. RAT {{rat_version.major}}.{{rat_version.minor}} automatically downloads a sample of `vic_param_file` once you use `rat init` command. 
        2. You do not need to manually update `vic_param_file` as RAT automatically updates it with the known parameters (which usually includes input and output paths, data formats, start and end dates, etc.). If there is any parameter value that you want to define and don't want RAT to update it, you can do it in the `VIC PARAMETERS` section.

* <h6 class="parameter_heading">*`vic_global_data`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: `True` if vic "global" (relative to basin) soil and domain parameter information is available and needs to be cropped for the basin. `False` otherwise. If False, you should have vic soil and domain parameter files that can be used **"as it is"** by VIC. For more information about vic soil paramater file, click [here](https://vic.readthedocs.io/en/master/Documentation/Drivers/Image/Params/) and for vic domain parameter file, click [here](https://vic.readthedocs.io/en/master/Documentation/Drivers/Image/Domain/).

    <span class="parameter_property">Default </span>: `True`

    <span class="parameter_property">Syntax </span>: If you have vic soil and domain parameter files for the whole continent or the country in which the basin lies, then
    ```
    VIC:
        vic_global_data: True
    ```

    !!! note
        1. Default "global" vic soil parameter and domain files for each continent is downloaded along with global-database and was prepared by  [Jacob et al.(2021)](https://doi.org/10.1038/s41597-021-00999-4).
        2. If `vic_global_data` is **True**, `vic_global_param_dir`,`vic_basin_continent_param_filename` and `vic_basin_continent_domain_filename` are the required parameters.  `vic_soil_param_file` and `vic_domain_file` are ignored.
        3. If `vic_global_data` is **False**, `vic_soil_param_file` and `vic_domain_file` are required and `vic_global_param_dir`,`vic_basin_continent_param_filename` and `vic_basin_continent_domain_filename` are ignored.
    
* <h6 class="parameter_heading">*`vic_global_param_dir`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the directory contining the vic's global soil parameter and domain files required by vic. 

    <span class="parameter_property">Default </span>: *'`project_dir`/global_data/global_vic_params'* 

    <span class="parameter_property">Syntax </span>: If you want to run VIC for a basin in North America and you have the vic soil parameter and domain files for the North America in the directory *'/Cheetah/rat_project/global_data/global_vic_params'*, then
    ```
    VIC:
        vic_global_param_dir: /Cheetah/rat_project/global_data/global_vic_params
    ```

* <h6 class="parameter_heading">*`vic_basin_continent_param_filename`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Name of the [vic soil parameter file](https://vic.readthedocs.io/en/master/Documentation/Drivers/Image/Params/) in 'NetCDF' format which must have parameter information of a larger extent as compared to the basin so that it can be cropped (i.e. global relative to basin). 

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If you want to run VIC for a basin in North America and you have the vic soil parameter file for the North America in `vic_global_param_dir` by the name *'namerica_params.nc'*, then
    ```
    VIC:
        vic_basin_continent_param_filename: namerica_params.nc
    ```

* <h6 class="parameter_heading">*`vic_basin_continent_domain_filename`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Name of the [vic domain parameter file](https://vic.readthedocs.io/en/master/Documentation/Drivers/Image/Domain/) in 'NetCDF' format which must have parameter information of a larger extent as compared to the basin so that it can be cropped (i.e. global relative to basin). 

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If you want to run VIC for a basin in North America and you have the vic domain parameter file for the North America in `vic_global_param_dir` by the name *'namerica_domain.nc'*, then
    ```
    VIC:
        vic_basin_continent_domain_filename: namerica_domain.nc
    ```

* <h6 class="parameter_heading">*`vic_soil_param_file`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the [vic soil parameter file](https://vic.readthedocs.io/en/master/Documentation/Drivers/Image/Params/) in 'NetCDF' format which could be used "as it is" by VIC. `vic_global_data` must be **False** to use this parameter, otherwise ignored.

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If you want to run VIC for a basin whose vic soil parameter file is at the path *'/Cheetah/rat_project/custom_files/basin_vic_soil_params.nc'*, then
    ```
    VIC:
        vic_soil_param_file: /Cheetah/rat_project/custom_files/basin_vic_soil_params.nc
    ```

* <h6 class="parameter_heading">*`vic_domain_file`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the [vic soil parameter file](https://vic.readthedocs.io/en/master/Documentation/Drivers/Image/Params/) in 'NetCDF' format which could be used "as it is" by VIC. `vic_global_data` must be **False** to use this parameter, otherwise ignored.

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If you want to run VIC for a basin whose vic soil parameter file is at the path *'/Cheetah/rat_project/custom_files/basin_vic_domain.nc'*, then
    ```
    VIC:
        vic_domain_file: /Cheetah/rat_project/custom_files/basin_vic_domain.nc
    ```

### VIC Parameters

This section is ***optional*** and describes the parameters defined by `vic_param_file`. As `vic_param_file` is used as a template and RAT {{rat_version.major}}.{{rat_version.minor}} automatically updates all the parameter values, this section can be used by you to define any parameter's value that you don't want to get update automatically in `vic_param_file`. To know about the available parameters, click [here](https://vic.readthedocs.io/en/master/Documentation/Drivers/Image/GlobalParam/). 

For instance, if you want to update the number of moisture layers (`NLAYER`) used by the VIC model and want to define the name of forcing type for air temperature (`AIR_TEMP`) as *'temp'* to be read from the forcing file:
    ```
    VIC PARAMETERS:
        NLAYER: 2
        FORCE_TYPE:
            AIR_TEMP: temp
    ```

!!! tip_note "Tip"
    Parameter keywords are **case sensitive** and please refer [this](https://vic.readthedocs.io/en/master/Documentation/Drivers/Image/GlobalParam/) page for this section to have a look what case you should use for a particular parameter.

### Routing
* <h6 class="parameter_heading">*`route_model`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the routing model. 

    <span class="parameter_property">Default </span>: *'`project_dir`/models/routing/rout'* 

    <span class="parameter_property">Syntax </span>: If routing model has the path *'/Cheetah/rat_project/models/routing/rout'*, then
    ```
    ROUTING:
        route_model: /Cheetah/rat_project/models/routing/rout
    ``` 
    !!! reminder_note "Reminder"
        Routing model gets automatically installed and compiled at the above mentioned default path, once you use `rat_init` command.  

* <h6 class="parameter_heading">*`route_param_file`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the routing parameter file required by routing in 'txt' format. Details about the parameter file is available [here](https://vic.readthedocs.io/en/vic.4.2.d/Documentation/Routing/RoutingInput/). It is used as a sample parameter file as RAT automatically updates it during `rat run` command. For further information, see the tip below.

    <span class="parameter_property">Default </span>: *'`project_dir`/params/routing/route_param.txt'* 

    <span class="parameter_property">Syntax </span>: If routing parameter file has the path *'/Cheetah/rat_project/params/routing/route_param.txt'*, then
    ```
    ROUTING:
        route_param_file: /Cheetah/rat_project/params/routing/route_param.txt
    ```
    !!! note
        `route_param_file` is ***optional*** and can be left blank if all the required parameters are defined in `ROUTING PARAMETERS` section. 
    !!! tip_note "Tip"
        1. RAT {{rat_version.major}}.{{rat_version.minor}} automatically downloads a sample of `route_param_file` once you use `rat init` command. 
        2. You do not need to manually update `route_param_file` as RAT automatically updates it with the known parameters (which usually includes input and output paths, data formats, start and end dates, etc.). If there is any parameter value that you want to define and don't want RAT to update it, you can do it in the `ROUTING PARAMETERS` section.

* <h6 class="parameter_heading">*`global_flow_dir_tif_file`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the flow direction raster file in 'geotif' format. The provided raster file must have flow direction information of a larger or equal extent as compared to the basin in WGS84 projection and the resolution of this file must be 0.0625&deg;. This file is used to create another file in a format required by Routing, details of which is available [here](https://vic.readthedocs.io/en/vic.4.2.d/Documentation/Routing/RoutingInput/#flow-direction-file).

    <span class="parameter_property">Default </span>: *'`project_dir`/global_data/global_drt_flow_file/global_drt_flow_16th.tif'* 

    <span class="parameter_property">Syntax </span>: If routing parameter file has the path *'/Cheetah/rat_project/global_data/global_drt_flow_file/global_drt_flow_16th.tif'*, then
    ```
    ROUTING:
        global_flow_dir_tif_file: /Cheetah/rat_project/global_data/global_drt_flow_file/global_drt_flow_16th.tif
    ```
    !!! note
        1. RAT {{rat_version.major}}.{{rat_version.minor}} requires the resolution of the flow direction file as 1/16<sup>th</sup> or 0.0625&deg;.
        2. Default flow direction raster 'tif' file with the required resolution of 0.0625&deg; in WGS84 projection is downloaded along with global-database and is provided by [NTSG Group at University of Montana](https://www.umt.edu/numerical-terradynamic-simulation-group/project/drt.php).
        3. Numbers that represnt flow directions in the default flow direction file is as follows: <br>
        &nbsp; 1   = east <br>
        &nbsp; 2   = southeast <br>
        &nbsp; 4   = south <br>
        &nbsp; 8   = southwest <br>
        &nbsp; 16  = west <br>
        &nbsp; 32  = northwest <br>
        &nbsp; 64  = north <br>
        &nbsp; 128 = northeast <br>
        &nbsp; 255 = no flow


* <h6 class="parameter_heading">*`replace_flow_directions`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Dictionary of key value pairs where key represents the flow direction in `global_flow_dir_tif_file` and that flow direction number will then be replaced by the value of that key. 

    <span class="parameter_property">Default </span>: `{ 1 : 3, 4 : 5, 2 : 4, 8 : 6, 16 : 7, 32 : 8, 64 : 1,128 : 2, 255 : 0 }`

    <span class="parameter_property">Syntax </span>: If you want to replace flow directions 2 and 4 by 4 and 5 respectively in `global_flow_dir_tif_file`, then
    ```
    ROUTING:
        replace_flow_directions: { 4 : 5,    # first replace 4 by 5
                                   2 : 4,    # and then replace 2 by 4.
                                    }
    ```
    or
    ```
    ROUTING:
        replace_flow_directions:  
            4 : 5    # first replace 4 by 5
            2 : 4    # and then replace 2 by 4.
    ```
    !!! note
        Flow direction numbers as required by Routing in each grid cell is as follows: <br>
        &nbsp; 0   = no flow <br>
        &nbsp; 1   = north <br>
        &nbsp; 2   = northeast <br>
        &nbsp; 3   = east <br>
        &nbsp; 4   = southeast <br>
        &nbsp; 5   = south <br>
        &nbsp; 6   = southwest <br>
        &nbsp; 7   = west <br>
        &nbsp; 8   = northwest 

    !!! tip_note "Tip"
        Replacing of directions takes place sequentially in the order provided. So if you want to replace 2 by 4 and 4 by 5 then first replace 4 by 5 and then replace 2 by 4. If you do the other way round, then there will be no directions with value 4. 
    
* <h6 class="parameter_heading">*`station_global_data`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: `True` if  reservoir information is available in a vector file. It can be "global" (relative to basin) and will be automatically filtered for the basin.`False` if you don't have reservoir information in a vector file. For more information about routing station file, click [here](https://vic.readthedocs.io/en/vic.4.2.d/Documentation/Routing/RoutingInput/#station-location-file).

    <span class="parameter_property">Default </span>: `True`

    <span class="parameter_property">Syntax </span>: If you have reservoir information in a shapefile, then
    ```
    ROUTING:
        station_global_data: True
    ```

    !!! note
        1. Default "global" reservoirs and dam data is downloaded along with global-database in the form of shapefiles and uses the [Global Reservoir and Dam (GRanD) database version 1.3](https://www.globaldamwatch.org/grand) 
        2. If `station_global_data` is **True**, `stations_vector_file` and `stations_vector_file_columns_dict` are the required parameters and `station_latlon_path` is ignored.
        3. If `station_global_data` is **False**, `station_latlon_path` is required whereas `stations_vector_file` and `stations_vector_file_columns_dict` are ignored.

* <h6 class="parameter_heading">*`stations_vector_file`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the dam/station vector file where the geometry is represented by point location of dams and there must be unique id, name, longitude and latitude columns. It can be "global" (relative to basin) and will be automatically filtered for the basin. It is used to create routing station file. For more information about routing station file, click [here](https://vic.readthedocs.io/en/vic.4.2.d/Documentation/Routing/RoutingInput/#station-location-file).

    <span class="parameter_property">Default </span>: *'`project_dir`/global_data/global_dam_data/GRanD_dams_v1_3_filtered.shp'* 

    <span class="parameter_property">Syntax </span>: If reservoir station file has the path *'/Cheetah/rat_project/global_data/global_dam_data/GRanD_dams_v1_3_filtered.shp'*, then
    ```
    ROUTING:
        stations_vector_file : Cheetah/rat_project/global_data/global_dam_data/GRanD_dams_v1_3_filtered.shp
    ```
    !!! tip_note "Tip"
        To use this parameter, make sure `station_global_data` is `True`.

* <h6 class="parameter_heading">*`stations_vector_file_columns_dict:`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Dictionary of column names for `stations_vector_file`. The dictionary must have keys 'id_column', 'name_column', 'lon_column' and 'lat_column'  and their values should be the actual name of the corresponding columns respectively. The 'id_column' value should be unique for all stations in the vector file. 

    <span class="parameter_property">Default </span>: `{id_column: GRAND_ID, name_column: DAM_NAME, lon_column: LONG_DD, lat_column: LAT_DD}`

    <span class="parameter_property">Syntax </span>: If `stations_vector_file` has column names 'GRAND_ID', 'DAM_NAME', 'LONG_DD' and 'LAT_DD', then 
    ```
    ROUTING:
        stations_vector_file_columns_dict: {id_column: GRAND_ID, name_column: DAM_NAME, lon_column: LONG_DD, lat_column: LAT_DD}
    ```
    or
    ```
    ROUTING:
        stations_vector_file_columns_dict: 
            id_column: GRAND_ID
            name_column: DAM_NAME
            lon_column: LONG_DD
            lat_column: LAT_DD
    ```
    !!! tip_note "Tip"
        1. To use this parameter, make sure `station_global_data` is `True`.
        2. The default value is only useful if you are using the GRanD dam shapefile provided with global database of RAT 3.0.

* <h6 class="parameter_heading">*`station_latlon_path:`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of station file in csv format with columns 'run', 'name', 'lon' and 'lat'. The 'run' column values should be 1 for  stations for which routing should be executed otherwise 0.

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If station csv file has the path *'/Cheetah/rat_project/custom_files/station_lat_lon.csv'*, then 
    ```
    ROUTING:
        station_latlon_path: /Cheetah/rat_project/custom_files/station_lat_lon.csv
    ```
    !!! tip_note "Tip"
        To use this parameter, make sure `station_global_data` is `False`.

### Routing Parameters

This section of the configuration file describes the parameters defined by `route_param_file`. As `route_param_file` is used as a template and RAT {{rat_version.major}}.{{rat_version.minor}} automatically updates all the parameter values, this section can be used by you to define any parameter's value that you don't want to get update automatically in `route_param_file`. To know about the available parameters, click [here](https://vic.readthedocs.io/en/vic.4.2.d/Documentation/Routing/RoutingInput/#input-file-setup).

!!! note
    By default, RAT {{rat_version.major}}.{{rat_version.minor}} uses a standard unit hydrograph file which is downloaded when RAT is initialized using `rat init` command. To know more about the unit hydrograph file, click [here](https://vic.readthedocs.io/en/vic.4.2.d/Documentation/Routing/RoutingInput/#uh-file).

!!! tip_note "Tip"
    To improve accuracy of inflow values for a river basin, VIC and routing models can be calibrated. To know more, click [here](https://vic.readthedocs.io/en/vic.4.2.d/Documentation/Calibration/). 

### GEE

* <h6 class="parameter_heading">*`reservoir_vector_file`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the reservoir vector file where the geometry is represented by reservoir polygons. It can be "global" (relative to basin) and will be automatically filtered for the basin. It can have unique id column, dam name column and surface area columns. 

    <span class="parameter_property">Default </span>: *'`project_dir`/global_data/global_reservoir_data/GRanD_reservoirs_v1_3.shp'* 

    <span class="parameter_property">Syntax </span>: If reservoir station file has the path *'/Cheetah/rat_project/global_data/global_reservoir_data/GRanD_reservoirs_v1_3.shp'*, then
    ```
    GEE:
        reservoir_vector_file : Cheetah/rat_project/global_data/global_reservoir_data/GRanD_reservoirs_v1_3.shp
    ```

* <h6 class="parameter_heading">*`reservoir_vector_file_columns_dict`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Dictionary of column names for `reservoir_vector_file`. The dictionary must have keys 'id_column', 'dam_name_column' and 'area_column' and their values should be the actual name of the corresponding columns respectively. 'area' column should have value of average surface area of reservoirs in square Kilometers.   

    <span class="parameter_property">Default </span>: `{id_column : 'GRAND_ID', dam_name_column : 'DAM_NAME', area_column : 'AREA_SKM'}` 

    <span class="parameter_property">Syntax </span>: If `reservoir_vector_file` has column names 'GRAND_ID', 'DAM_NAME' and 'AREA_SKM', then 
    ```
    ROUTING:
        reservoir_vector_file_columns_dict: {id_column : 'GRAND_ID', dam_name_column : 'DAM_NAME', area_column : 'AREA_SKM'}
    ```
    or
    ```
    ROUTING:
        reservoir_vector_file_columns_dict: 
            id_column: GRAND_ID
            dam_name_column: DAM_NAME
            area_column: AREA_SKM
    ```
    !!!note
        1.  If `station_global_data` is `True`, the values of 'id_column' and 'dam_name_column' in `reservoir_vector_file` should match with that of 'id_column' and 'name_column' in `stations_vector_file`.
        <br><br>
        2. If `station_global_data` is `False`, the values of 'dam_name_column' in `reservoir_vector_file` should match with that of 'name' column in `station_latlon_path`. 'id_column' is not required in this case and will be ignored if provided.

* <h6 class="parameter_heading">*`bot_filter`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: The BOT (Bias Outlier Trend) Filter can be used to have granular control over the filtering applied to surface area (SA) time series. It corrects the Optical satellite based surface area results using SAR (Synthetic Aperture Radar) data and a set of three filters. To use the filter, set 'apply' to 'true' and set the three thresholds.</br> Bias_threshold: The intensity of filtering out optical SA values that has a bias from SAR value. Outlier_threshold: The intensity of filtering out outlier values in the Optical SA time series </br> Trend_threshold: The intensity of filtering out optical SA values whose trend differs from SAR trend </br> Threshold ranges: (Off: 0 - 9: MAX) Eg. Relatively Aggressive filtering set: [8,8,8]  

    <span class="parameter_property">Default </span>: `{apply: false, bias_threshold: 9, outlier_threshold: 8, trend_threshold: 9}` 

    <span class="parameter_property">Syntax </span>: If filtering for surface area is required to be run, then 
    ```
    GEE:
        bot_filter: {apply: true, bias_threshold: 0-9, outlier_threshold: 0-9, trend_threshold: 0-9}
    ```
    !!!note
        Setting `bot_filter` `apply` to `false` in a subsequent run after it was made `true` will result in Surface Area reverting to `TMSOS` based values.

### Post Processing

* <h6 class="parameter_heading">*`aec_dir`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the directory containing area elevation curve (AEC) files in csv format for all reservoirs. If for any reservoir AEC file is missing, it will be automatically created using digital elevation model inside this directory. 

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If `aec_dir` has the path *'/Cheetah/rat_project/custom_files/aec/'*, then
    ```
    POST_PROCESSING:
        aec_dir : /Cheetah/rat_project/custom_files/aec/
    ```
    !!! note
    1. If `station_global_data` is `True`, AEC file names should be <'id_column' value>_<'dam_name_column' value where spaces are replaced by '_'>. For example, the file name for a reservoir with 'dam_name' as 'Tehri Dam' and 'id' as 115 will be '115_Tehri_Dam.csv'.
    <br><br>
    2. If `station_global_data` is `False`, AEC file names should be <'dam_name_column' value where spaces are replaced by '_'>. For example, the file name for a reservoir with 'dam_name' as 'Tehri Dam' will be 'Tehri_Dam.csv'.
    3. Each AEC file should have two columns with headers as 'Elevation' and 'CumArea'. 'Elevation' should be in meters and 'CumArea' should be in square Kilometers.

### Clean Up

* <h6 class="parameter_heading">*`clean_preprocessing`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: `True` if  you want to delete intermediate pre-processed data for a river basin except global raw data downloaded from servers after the RAT run. Otherwise, `False`.

    <span class="parameter_property">Default </span>: `False`

    <span class="parameter_property">Syntax </span>: If you want to delete intermediate pre-processed data for a river basin, 
    ```
    CLEAN_UP:
        clean_preprocessing: True
    ```

* <h6 class="parameter_heading">*`clean_metsim`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: `True` if  you want to delete intermediate metsim outputs for a river basin after the RAT run. Otherwise, `False`.

    <span class="parameter_property">Default </span>: `False`

    <span class="parameter_property">Syntax </span>: If you want to delete intermediate metsim outputs for a river basin, 
    ```
    CLEAN_UP:
        clean_metsim: True
    ```

* <h6 class="parameter_heading">*`clean_vic`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: `True` if  you want to delete intermediate vic inputs and outputs, and any vic initial soil state file that is older than 15 days, for a river basin after the RAT run. Otherwise, `False`.

    <span class="parameter_property">Default </span>: `False`

    <span class="parameter_property">Syntax </span>: If you want to delete intermediate vic inputs and outputs, and any vic initial soil state file that is older than 15 days for a river basin, 
    ```
    CLEAN_UP:
        clean_vic: True
    ```

* <h6 class="parameter_heading">*`clean_routing`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: `True` if  you want to delete intermediate routing inputs and outputs, and any routing initial state file that is older than 15 days, for a river basin after the RAT run. Otherwise, `False`.

    <span class="parameter_property">Default </span>: `False`

    <span class="parameter_property">Syntax </span>: If you want to delete intermediate routing inputs and outputs, and any routing initial state file that is older than 15 days for a river basin, 
    ```
    CLEAN_UP:
        clean_routing: True
    ```

* <h6 class="parameter_heading">*`clean_gee`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: `True` if  you want to delete gee produced small chunk files of surface area time series for a river basin after the RAT run. Otherwise, `False`.

    <span class="parameter_property">Default </span>: `False`

    <span class="parameter_property">Syntax </span>: If you want to delete gee produced small chunk files of surface area time series for a river basin, 
    ```
    CLEAN_UP:
        clean_gee: True
    ```
    !!!note
        If `clean_gee` is `True`, it will not delete the final gee outputs that will be appended with new data in next RAT run. To delete that, use `clean_previous_outputs`.

* <h6 class="parameter_heading">*`clean_altimetry`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: `True` if  you want to delete raw altimetry data that takes a lot of time to download for a river basin after the RAT run. Otherwise, `False`.

    <span class="parameter_property">Default </span>: `False`

    <span class="parameter_property">Syntax </span>: If you want to delete raw altimetry data for a river basin, 
    ```
    CLEAN_UP:
        clean_altimetry: True
    ```
    !!!note
        If `clean_altimetry` is `True`, it will not delete the extracted altimetry data that will be appended with new data in next RAT run. To delete that, use `clean_previous_outputs`.

* <h6 class="parameter_heading">*`clean_previous_outputs`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: `True` if  you want to delete previous outputs, gee extracted surface area time series and altimetry extracted height data produced by last RAT run. Otherwise, `False`.

    <span class="parameter_property">Default </span>: `False`

    <span class="parameter_property">Syntax </span>: If you want to delete raw altimetry data that takes a lot of time to download for a river basin, 
    ```
    CLEAN_UP:
        clean_previous_outputs: True
    ```
    !!!note
         If `clean_previous_outputs` is `True`, the previous outputs are cleaned before executing any step in `steps`.
         
    !!! tip_note "Tip"
        You should use `clean_previous_outputs` if you want to have fresh outputs of RAT for a river basin. Otherwise, by default RAT will keep appending the new outputs to the same files and will concatenate data by calendar dates.

### Confidential
* <h6 class="parameter_heading">*`secrets:`* :</h6> 
    <span class="requirement">Reuired parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of secrets file in ini format with credentials for IMERG, AVISO and GEE as mentioned [here](../../QuickStart/UserGuide/#requirements).

    <span class="parameter_property">Default </span>: Specified by `-s` or `--secrets` option of `rat init` command.

    <span class="parameter_property">Syntax </span>: If secrets file has the path *'/Cheetah/rat_project/secrets/secrets.ini'*, then 
    ```
    CONFIDENTIAL:
        secrets: /Cheetah/rat_project/secrets/secrets.ini
    ```
    !!! note 
        It will be left blank if '-s' argument is not provided in `rat init` command.