# Command Line Interface Functionality

RAT {{rat_version.major}}.{{rat_version.minor}} offers command line interface functionality to run RAT along with features like to test its installation, to initialize (which downloads dependent hydrological models and global database) and creates directory structure making it super easy for new users to use RAT. Below are the command line interface (CLI) commands that can be used by a user.

## Initialization
`init` command initializes RAT {{rat_version.major}}.{{rat_version.minor}} which is required only once after the installation of RAT. RAT depends on models like MetSim, VIC and Routing to produce inflow and thus this command removes the hassle for a user to install them separately as it takes care of all the downloading and installation. It also provides the flexibility to download [global database](../RAT_Data/GlobalDatabase/#global-database) at a desired location.

It has the following parameters:

* <h6 class="parameter_heading">*`-d or --dir`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute/Relative path of RAT project directory. 

    <span class="parameter_property">Default </span>: If left blank, user will be prompt to enter the project directory path.

* <h6 class="parameter_heading">*`-g or --global_data`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Flag if given, the global database is downloaded, otherwise it will not be downloaded. 

    <span class="parameter_property">Default </span>: It is not included by default so the global database will not be downloaded.

* <h6 class="parameter_heading">*`-gp or --global_data_dir`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute/Relative path of the already existing global database if `-g or --global_data` is not provided, assuming you have downloaded it before. If `-g or --global_data` is provided then the global database will be downloaded at the provided path.

    <span class="parameter_property">Default </span>: Default path if `-g or --global_data` is provided is `project_dir`/global_data. 

* <h6 class="parameter_heading">*`-s or --secrets`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute/Relative path of the [secrets file](../../Configuration/secrets).

    <span class="parameter_property">Default </span>: If left blank, it will not be automatically populated in the RAT configuration file. You can later use [`rat configure`](#configuring) command described below. 

* <h6 class="parameter_heading">*`-dr or --drive`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Drive to download the routing model, parameter files and global database from. Acceptable choices are google and dropbox.

    <span class="parameter_property">Default </span>: If left blank, data will be downloaded from google drive.

Usage example of the command:
    ```
        rat init -d <PATH_OF_RAT_PROJECT_DIRECTORY> -g -gp <PATH_TO_DOWNLOAD/OF_DOWNLOADED_GLOBAL_DATA> -s <PATH_TO_SECRETS_FILE> -dr google
    ```

!!! warning_note "Warning"
    Default global-database is more than 129 GB in size and therefore it is recommended to have at least 140GB disc space in rat_project directory. If you don't have enough space in rat_project directory, you can use `-gp or --global_data_dir` or initialize without downloading global-database using `rat init -d ./rat_project/`.

!!! note
    A ‘rat_config.yaml’ file is prepared using ‘rat_config_template’ in `project_dir`>‘Params’ which is created after initializing RAT. 

## Configuring
`configure` command updates RAT's configuration file by automatically updating important paths related to global database, secrets and project directory. User can also specify the cores to use to execute RAT.

This command has the following parameters:

* <h6 class="parameter_heading">*`-d or --dir`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute/Relative path of RAT project directory. 

    <span class="parameter_property">Default </span>: No default value as it is required parameter.

* <h6 class="parameter_heading">*`-p or --param`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute/Relative path of RAT configuration file that you want to update. 

    <span class="parameter_property">Default </span>: No default value as it is required parameter.

* <h6 class="parameter_heading">*`-gp or --global_data_dir`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute/Relative path of the already existing global database, assuming you have downloaded it before. If not downloaded leave it blank.

    <span class="parameter_property">Default </span>: If not provided, the paths related to global database are not updated. 

* <h6 class="parameter_heading">*`-s or --secrets`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Absolute/Relative path of the [secrets file](../../Configuration/secrets).

    <span class="parameter_property">Default </span>: If left blank, it will not be automatically updated in the RAT configuration file.

* <h6 class="parameter_heading">*`-nc or --n_cores`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Number of cores to be used to execute RAT and therefore MetSim, VIC and routing.

    <span class="parameter_property">Default </span>: By default, RAT uses maximum cores to be executed.  

Usage example of the command:
    ```
        rat configure -d <PATH_OF_RAT_PROJECT_DIRECTORY> -p <PATH_OF_CONFIGURATION_FILE_TO_UPDATE> -gp <PATH_OF_DOWNLOADED_GLOBAL_DATA> -s <PATH_TO_SECRETS_FILE> -nc 8
    ```

## Testing
`test` command tests functioning of RAT {{rat_version.major}}.{{rat_version.minor}} to make sure it has been correctly installed and initialized. Please make sure RAT has been initialized before testing. Currently user has the option to choose between two test basins. Also, please note that a user does not need global database in order to test RAT functioning.

This coomand has the following parameters:

* <h6 class="parameter_heading">*`-d or --dir`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute/Relative path of RAT project directory. 

    <span class="parameter_property">Default </span>: No default value as it is required parameter.

* <h6 class="parameter_heading">*`-b or --basin`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: River basin for which RAT will be executed for testing. Acceptable values are NUECES and GUNNISON.

    <span class="parameter_property">Default </span>: No default value as it is required parameter.

* <h6 class="parameter_heading">*`-s or --secrets`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute/Relative path of the [secrets file](../../Configuration/secrets).

    <span class="parameter_property">Default </span>: No default value as it is required parameter.

* <h6 class="parameter_heading">*`-dr or --drive`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Drive to download the test data for the selected river basin from. Acceptable choices are google and dropbox.

    <span class="parameter_property">Default </span>: If left blank, data will be downloaded from google drive.

Usage example of the command:
    ```
        rat test -d <PATH_OF_RAT_PROJECT_DIRECTORY> -b NUECES -s <PATH_TO_SECRETS_FILE>
    ```
!!! note
    A ‘test_config.yaml’ file is prepared using ‘rat_config_template’ in `project_dir`>‘Params’ which is created after initializing RAT. 

## Execution/Operationalization 
`run` command executes RAT {{rat_version.major}}.{{rat_version.minor}}. You can use this command to run RAT according to the configuration file as it is or by updating the last configuration file in an inteligent manner.

It has the following parameters:

* <h6 class="parameter_heading">*`-p or --param`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute/Relative path of RAT configuration file that you want to use to run RAT. 

    <span class="parameter_property">Default </span>: No default value as it is required parameter.

* <h6 class="parameter_heading">*`-o or --operational`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Specifies the desired latency to operationalize RAT. While the typical latency of meteorological data ranges from 1-2 days, it is advised to set a latency of 3 days when operationalizing RAT. This ensures that RAT{{rat_version.major}}.{{rat_version.minor}} obtains all the necessary input data from various satellite data servers and executes smoothly, without encountering any errors. By setting a latency of 3 days, RAT {{rat_version.major}}.{{rat_version.minor}} will initiate execution from the end of the previous RAT {{rat_version.major}}.{{rat_version.minor}} run and cover the duration up to three days prior to the current day. Also, please note that the configuration file's start and end date will be updated and overwritten in the same file.

    <span class="parameter_property">Default </span>: If not provided, the configuration file will not be updated before executing RAT and RAT will run for the duration specified in the configuration file.

Usage example of the command to execute RAT:
    ```
        rat run -p <PATH_OF_CONFIGURATION_FILE> 
    ```
Usage example of the command to operationalize RAT:
    ```
        rat run -p <PATH_OF_CONFIGURATION_FILE> -o 3
    ```

!!! warning_note "Warning"
    RAT operational feature should only be used if RAT {{rat_version.major}}.{{rat_version.minor}} has already been executed once before, for that particular river basin.

!!! tip_note "Tip"
    1. To operationalize RAT, set a daily cron job for the following command:
    ```
        rat run -p <PATH_OF_CONFIGURATION_FILE> -o 3
    ```
    2. Now RAT {{rat_version.major}}.{{rat_version.minor}} can run operationally in [0 latency mode](../Plugins/0_latency_mode) using the following command:
    ```
        rat run -p <PATH_OF_CONFIGURATION_FILE> -o 0
    ```