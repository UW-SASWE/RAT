# RESERVOIR ASSESSMENT TOOL (RAT)

<h5>A scalable and easy-to-apply python based software architecture to empower the global water community</h5>

The [Reservoir Assessment Tool (RAT)](https://depts.washington.edu/saswe/rat) uses satellite remote sensing data to monitor water surface area and water level changes in artificial reservoirs. It uses this information, along with topographical information (either derived from satellite data, or in-situ topo maps) to estimate the Storage Change (∆S) in the reservoirs. Additionally, RAT models the Inflow (I) and the Evaporation (E) of each reservoir. Finally, RAT uses the modeled I, and E, and estimated ∆S, to estimate the Outflow (O) from reservoirs.

!!! tip_note "Tip"
    If RAT was working fine previously but you've recently encountered errors, please check out [Recent Adjustments](../../docs/Development/RecentAdjustments) for potential issues and their solutions.

!!! note
    You are reading documentation for RAT version 3 (RAT-3). RAT 3.0 makes numerous improvements to the code structure, performance optimizations, added configurations, ability to run RAT for multiple basins, among some introduced features. It also introduces packaging of RAT as a conda package, allowing for quick and easy installation.

It was originally developed by Biswas et al. (2021) at SASWE, University of Washington. The RAT framework developed by Biswas et al. (2021) was a first-of-its-kind open-source reservoir monitoring tool, it is referred to as the version 1 of RAT, or RAT 1.0. It currently runs for 3 regions - (1) South and South East Asia, (2) Africa, and (3) South America, and can be accessed [here](http://depts.washington.edu/saswe/rat_retired).

The Reservoir Assessment Tool (RAT) 2.0 was introduced with numerous improvements over the RAT 1.0. Such as weekly satellite observations (every 1-5 days) using a combination of multiple satellites (Sentinel-2, landsat-8, landsat-9 and Sentinel-1), usage of VIC hydrological model (VIC 5, Hamman et al. (2018)) and MetSim in parallel computation mode, data storage using NetCDF format and explicit representation of Evaporation using the Penman Combination method.

!!! note
    If you use this software, please cite the following depending on the context:

    1. Minocha, S., Hossain, F., Das, P., Suresh, S., Khan, S., Darkwah, G., Lee, H., Galelli, S., Andreadis, K. and Oddo, P., 2023. Reservoir Assessment Tool version 3.0: a scalable and user-friendly software platform to mobilize the global water management community. Geoscientific Model Development Discussions, 2023, pp.1-23.

    2. Das, P., Hossain, F., Khan, S., Biswas, N.K., Lee, H., Piman, T., Meechaiya, C., Ghimire, U. and Hosen, K., 2022. Reservoir Assessment Tool 2.0: Stakeholder driven improvements to satellite remote sensing based reservoir monitoring. Environmental Modelling & Software, 157, p.105533.

    3. Biswas, N.K., Hossain, F., Bonnema, M., Lee, H. and Chishtie, F., 2021. Towards a global Reservoir Assessment Tool for predicting hydrologic impacts and operating patterns of existing and planned reservoirs. Environmental Modelling & Software, 140, p.105043.

