# Patch Notes

### v3.0.13
In this release, we have:

1. Updated Forecasting plugin so that it can now be used to estimate reservoir outflow as well along with reservoir inflow depending on the scenarios provided by the user.
2. Updated RAT Documentation for ResORR plugin, BOT Filter and Forecasting plugin.
3. Updated the IMERG Precipitation web link to download IMERG data as the IMERG product versions were changed on 1st June, 2024.
4. Updated RAT Documentation to have sections for Patch Notes and Recent Adjustments to highlight the recent changes. 
5. Updated RAT Documentation in 'Getting Ready' section in 'GEE Credentials'.
6. Updated RAT Documentation and RAT Github to show users what papers to cite in their work if they use this software.


### v3.0.12
In this release, we have:

1. Added Forecasting plugin that can be used to forecast inflow in a reservoir upto 15 days in the future. 
2. Added RAT Tutorial in RAT Documentation.
3. Fixed bugs related to ResORR plugin, errors in RAT Documentation and solved dependency issues for plugins.


### v3.0.9

In this release, we have:

1. Added visualization feature to easily create interactive plots for inflow, outflow, storage change, evaporation and surface area of a reservoir along with its Area Elevation Curve.
2. Provided functionality to easily convert relative paths to absolute paths for updating configuration file.
3. Updated the IMERG Precipitation web link to download IMERG data. (web link change was done on 8th November.)

### v3.0.8

In this release, we have added rat.toolbox module to contain helpful and utility functions of user. Right now it has one function related to config and that is to update an existing config. Future work will be to add more functions to this module like to create config, or to plot outputs etc.

### v3.0.7

In this release, we have added the BOT Filter for surface area time series, which filters the optical derived surface area with SAR surface area as the reference.
The BOT Filter can be used to have granular control over the filtering applied to surface area (SA) time series.
To use the filter, set 'apply' in the config file to 'true' and set the three thresholds.
Bias_threshold: The intensity of filtering out optical SA values that has a bias from SAR value.
Outlier_threshold: The intensity of filtering out outlier values in the Optical SA time series
Trend_threshold: The intensity of filtering out optical SA values whose trend differs from SAR trend
Threshold ranges: (Off: 0 - 9: MAX)
Eg. Relatively Aggressive filtering set: [8,8,8]

### v3.0.6

Bug Fixes:

TMS-OS will work after 2019
TMS-OS will make use of Optical data to estimate surface area of reservoirs if SAR is not available or if Optical is much more than SAR.
To further smoothen and remove the erroneous peaks rising due to use of optical data, Savitzky-Golay filter is applied for cases mentioned in [2].
TMS-OS will not produce error if end date is before the start of Landsat-9 mission.

Improvements:
Tests will now be matched for numeric data and will be considered as match if the error percent between each value is less than threshold (default - 5) %.
State init files (vic and Routing) can now be provided as optional to dates. Earlier RAT used to assume that the state init files will be existing in a particular directory structure. This will not be true if someone is working on tutorial or want to use state files not produced by RAT.

### v3.0.2
First stable release version of RAT 3.0.