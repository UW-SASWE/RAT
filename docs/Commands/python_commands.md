# Python Functionality

RAT {{rat_version.major}}.{{rat_version.minor}} can be executed and implemented in python but it needs to be initialized, tested and configured using [command line interface](../cli_commands/#command-line-interface-functionality). Since it can be executed in python, one can use jupyter notebook environments and python scripts to automatically update configuration file and [calibrate models](https://vic.readthedocs.io/en/vic.4.2.d/Documentation/Calibration/#routcal) like VIC. Please note that it will be easier to calibrate VIC using RAT {{rat_version.major}}.{{rat_version.minor}} rather than just using VIC.

## Execution/Operationalization

`run_rat` function is used to execxute RAT {rat_version.major}}.{{rat_version.minor}} in python. It can also be used to operationalize RAT. This function has the following paramters:

* <h6 class="parameter_heading">*`config_fn`* :</h6> 
    <span class="requirement">Required parameter</span>

    <span class="parameter_property">Description </span>: Absolute/Relative path of RAT configuration file that you want to use to run RAT. 

    <span class="parameter_property">Default </span>: No default value as it is a required parameter.

* <h6 class="parameter_heading">*`operational_latency`* :</h6> 
    <span class="requirement">Optional parameter</span>

    <span class="parameter_property">Description </span>: Specifies the desired latency to operationalize RAT (needs to be numeric whole number). While the typical latency of meteorological data ranges from 1-2 days, it is advised to set a latency of 3 days when operationalizing RAT. This ensures that RAT{{rat_version.major}}.{{rat_version.minor}} obtains all the necessary input data from various satellite data servers and executes smoothly, without encountering any errors. By setting a latency of 3 days, RAT {{rat_version.major}}.{{rat_version.minor}} will initiate execution from the end of the previous RAT {{rat_version.major}}.{{rat_version.minor}} run and cover the duration up to three days prior to the current day. Also, please note that the configuration file's start and end date will be updated and overwritten in the same file.

    <span class="parameter_property">Default </span>: Default value is None. The configuration file will not be updated before executing RAT and RAT will run for the duration specified in the configuration file.

Usage example of the command to execute RAT:
    ```
        from rat.run_rat import run_rat
        run_rat(config_fn = <PATH_OF_CONFIGURATION_FILE>, operational_latency=None)
    ```
Usage example of the command to operationalize RAT:
    ```
        from rat.run_rat import run_rat
        run_rat(config_fn = <PATH_OF_CONFIGURATION_FILE>, operational_latency=3)
    ```

!!! warning_note "Warning"
    RAT operational parameter should only be used if RAT {{rat_version.major}}.{{rat_version.minor}} has already been executed once before, for that particular river basin.

!!! tip_note "Tip"
    To operationalize RAT, set a daily cron job for the following python script:
    ```python
        from rat.run_rat import run_rat
        run_rat(config_fn = <PATH_OF_CONFIGURATION_FILE>, operational_latency=3)
    ``` 