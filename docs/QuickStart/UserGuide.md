# User-Guide

RAT {{rat_version.major}}.{{rat_version.minor}} can be easily installed and set up for monitoring of any reservoir in the world. It can be used to monitor multiple reservoirs located over different river-basins. A user can set up RAT {{rat_version.major}}.{{rat_version.minor}}, on any machine (having linux operating system), to run operationally at a delay of a specified number of days (3 days-recommended). 

RAT {{rat_version.major}}.{{rat_version.minor}} comes with default global database which makes it easy for a user to start and set up RAT for any river-basin. The user has to just specify the id of the river-basin in the defualt global river-basin database and provide `start-date` and `end-date` for `RAT run`. RAT will automatically run for all the reservoirs in the default global reservoir database that lies within the specified river-basin and will generate Inflow, Outflow, Storage-change, Surface Area and Evaporation time-series for each reservoir from `start-date` to `end-date`. It is the most straightforward and convenient way to start using RAT but different users might have different interests like a smaller sub-basin or a selective list of reservoirs and thus RAT comes with a lot more flexibility where users can provide custom inputs based on their interests.

Essentially, RAT {{rat_version.major}}.{{rat_version.minor}} requires only the boundary polygon of a reservoir(s) in the form of a shapefile, the geospatial coordinates (latitude, longitude) of that reservoir(s) as a shapefile or a csv-file and a river-basin polygon(s), in which the reservoir(s) is located, in another shapefile.      

## Requirements

+ Linux based operating system with [miniconda(recommended)/anaconda] (https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html) installed. 
+ Login credentials for [AVISO user-account](https://www.aviso.altimetry.fr/en/data/data-access/registration-form.html)(for reservoir height data)
+ Login credentials for [IMERG user-account](https://registration.pps.eosdis.nasa.gov/registration/)
(for accessing precipitation data for hydrologic model)
+ Login credentials for [Earth Engine](https://developers.google.com/earth-engine/cloud/earthengine_cloud_project_setup) using service account(for reservoir storage change calculation)


## Installation

1. Create an empty project directory. 
    ```
    mkdir ./rat_project
    ```
2. Create a conda environment using directory inside the project directory.
    ```
    conda create --prefix ./rat_project/.rat_env
    ```
3. Activate this environment using conda.
    ```bash
    conda activate ./rat_project/.rat_env
    ```
4. Install RAT {{rat_version.major}}.{{rat_version.minor}} using conda
    ```
    conda install rat –c conda-forge
    ```

!!! success_note "Congratulations"
    RAT {{rat_version.major}}.{{rat_version.minor}} is successfully installed. You can now use RAT {{rat_version.major}}.{{rat_version.minor}} from command line using `rat` command or can do `import rat` in python. It is necessary to initialize RAT ater installation (once) before you can start using it for generating reservoir time-series data.

## Initialization

RAT depends on [MetSim](https://metsim.readthedocs.io/en/develop/), [VIC](https://vic.readthedocs.io/en/master/) and [routing](https://vic.readthedocs.io/en/vic.4.2.d/Documentation/Routing/RoutingInput/) models for computing inflow and therefore these models need to be separately installed in order to use RAT. Don't worry, you will not have to install all these models manually as thankfully RAT {{rat_version.major}}.{{rat_version.minor}} can automatically download all these models along with the required parameter files. Also, RAT {{rat_version.major}}.{{rat_version.minor}} provides an option to automatically download the default global database (recommended for first time users).  

RAT {{rat_version.major}}.{{rat_version.minor}} can be initialized using `rat init` command.

```
rat init –d ./rat_project/ –g 
```
!!! warning_note "Warning"
    Default global-database is 129 GB in size and therefore it is recommended to have at least 140GB disc space in rat_project directory. If you don't have enough space initialize without downloading global-database using `rat init -d ./rat_project/`

!!! note
    Fill the [login credentials](#Requirements) for AVISO, IMERG and Earth-Engine accounts in the `secrets` file downloaded using `rat init` at the path `./rat_project/params/secrets_template.ini` and rename the file as `secrets.ini`.

## Testing 

Once RAT has been installed and initialized, you can test if RAT is working properly using `rat test` command. To test RAT, `secrets` file is required but default global-database is **not**.
```
rat test –d ./rat_project/ –b GUNNISON –s ./rat_project/params/secrets.ini
```
!!! note 
    If the above command runs successfully, it means RAT has been installed and initialized successfully. RAT test output data can be found at the path `./rat_project/data/test_output/basins/gunnison/final_outputs`.  
    <br> If the above command fails, RAT has not been installed successfully. Try to reinstall and reinitialize RAT in a new directory.
