# Reservoir Assessment Tool 2.0

The Reservoir Assessment Tool (RAT) Framework was originally developed by [Biswas et al. (2021)](https://doi.org/10.1016/j.envsoft.2021.105043) at [SASWE](https://saswe.net/), [University of Washington](https://www.washington.edu/). RAT uses satellite remote sensing data to monitor water surface area and water level changes in artificial reservoirs. It uses this information, along with topographical information (either derived from satellite data, or in-situ topo maps) to estimate the **Storage Change (∆S)** in the reservoirs. Additionally, RAT models the **Inflow (I)** and the **Evaporation (E)** of each reservoir. Finally, RAT uses the modeled I, and E, and estimated ∆S, to estimate the **Outflow (O)** from reservoirs. 

Since the RAT framework developed by [Biswas et al. (2021)](https://doi.org/10.1016/j.envsoft.2021.105043) was a first-of-its-kind open-source reservoir monitoring tool, it is reffered to as the version 1 of RAT, or [RAT 1.0](http://depts.washington.edu/saswe/rat_beta/). It currently runs for 3 regions - (1) South and South East Asia, (2) Africa, and (3) South America, and can be accessed [here](http://depts.washington.edu/saswe/rat_beta/). 

The Reservoir Assessment Tool (RAT) 2.0 introduces numerous improvements over the RAT 1.0. The improvements in RAT 2.0 are as follows:
1. A robust multi-sensor approach to surface area estimation is implemnted, called, Tiered Multi-Sensor algorithm (Optical and SAR), abbreviated as **TMS-OS**. It uses a combination of various satellite sensors, such as Sentinel-2, Landsat-8, and Sentinel-1. This allows us to obtain highly accurate surface area estimates even during challenging conditions, such as during high cloud cover.
2. The temporal frequency of surface area, ∆S and Outflow monitoring was increased from a monthly resolution (in RAT 1.0) to 1-5 day frequency. This allows RAT to quantify sub-weekly reservoir operations.
3. In comparison to using only Landsat 7 and Landsat 8 in RAT 1.0, RAT 2.0 uses a total of 4 satellite sensors as of now, and more to be added. RAT 2.0 uses two optical sensors (Sentinel-2 and Landsat-8) to estiamte the surface area of the reservoirs; one SAR sensor (Sentinel-1) to estimate the trends in surface area changes; and one altimeter (Jason-3) to estiamte the water level heights with very high accuracy.
4. The latest version of the [VIC hydrological model](https://github.com/UW-Hydro/VIC) (VIC 5, [Hamman et al. (2018)](https://doi.org/10.5194/gmd-11-3481-2018)) and [MetSim](https://github.com/UW-Hydro/MetSim) are used in parallel computation mode. This allows for rapid model runs, and more efficient data storage options in the form of NetCDF files. 
5. Evaporation is modeled explicitly using the Penman Combination method, allowing for more control over the parameters used to calculate evaporation.

The [RAT 2.0](https://depts.washington.edu/saswe/mekong/) was developed using the Mekong basin as the test-bed. Hence, RAT 2.0 currently monitors 36 reservoirs in the Mekong Basin, and can be accessed [here](https://depts.washington.edu/saswe/mekong/). A guide on how to interact with the RAT 2.0 interface can be found [here](https://depts.washington.edu/saswe/mekong/howtouse.html).

## The RAT 2.0 GitHub repository

This GitHub repository contains the backend code of RAT 2.0, which performs the following tasks:
1. Downloads, Processes and Stores meteorological data for Hydrological modeling.
2. Prepares input data and runs the [MetSim](https://github.com/UW-Hydro/MetSim) meteorological disaggregator, followed by the [VIC hydrological model](https://github.com/UW-Hydro/VIC) for the Mekong basin.
3. Routes the surface runoff using the [VIC Routing model](https://vic.readthedocs.io/en/vic.4.2.d/Documentation/Routing/RoutingInput/) at the reservoir locations to obtain Inflow.
4. Processes Optical and SAR data using GEE to obtain surface areas and trends in surface area changes. This data is fed into the TMS-OS algorithm to obtain surface area time-series.
5. Obtains the water level time-series from the Jason-3 altimeter using the [Okeowo et al. (2017)](https://doi.org/10.1109/JSTARS.2017.2684081) methodology.
6. Using the surface area and water level time-series, estimates the ∆S and Outflow.
7. Calulates the Evaporation over the reservoir using the Penman Combination method.
8. Using the modeled Inflow, ∆S and Evaporation, calculates the Outflow for the reservoirs.