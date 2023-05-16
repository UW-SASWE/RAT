![Reservoir Assessment Tool](docs/logos/Rat_Logo_black_github.png)
# Reservoir Assessment Tool 3.0
[![Documentation Status](https://readthedocs.org/projects/rat-satellitedams/badge/?version=latest)](https://rat-satellitedams.readthedocs.io/en/latest/?badge=latest)

The Reservoir Assessment Tool (RAT) uses satellite remote sensing data to monitor water surface area and water level changes in artificial reservoirs. It uses this information, along with topographical information (either derived from satellite data, or in-situ topo maps) to estimate the **Storage Change (∆S)** in the reservoirs. Additionally, RAT models the **Inflow (I)** and the **Evaporation (E)** of each reservoir. Finally, RAT uses the modeled I, and E, and estimated ∆S, to estimate the **Outflow (O)** from reservoirs.

RAT 3.0 makes numerous improvements to the code structure, performance optimizations, added configurations, ability to run RAT for multiple basins, among some introduced features. It also introduces packaging of RAT as a conda package, allowing for quick and easy installation.

It was originally developed by [Biswas et al. (2021)](https://doi.org/10.1016/j.envsoft.2021.105043) at [SASWE](https://saswe.net/), [University of Washington](https://www.washington.edu/). The RAT framework developed by [Biswas et al. (2021)](https://doi.org/10.1016/j.envsoft.2021.105043) was a first-of-its-kind open-source reservoir monitoring tool, it is reffered to as the version 1 of RAT, or [RAT 1.0](http://depts.washington.edu/saswe/rat_beta/). It currently runs for 3 regions - (1) South and South East Asia, (2) Africa, and (3) South America, and can be accessed [here](http://depts.washington.edu/saswe/rat_beta/).

The [Reservoir Assessment Tool (RAT) 2.0](https://depts.washington.edu/saswe/mekong/) was introduced with numerous improvements over the RAT 1.0. Such as weekly satellite observations (every 1-5 days) using a combination of multiple satellites (Sentinel-2, landsat-8, landsat-9 and Sentinel-1), usage of [VIC hydrological model](https://github.com/UW-Hydro/VIC) (VIC 5, [Hamman et al. (2018)](https://doi.org/10.5194/gmd-11-3481-2018)) and [MetSim](https://github.com/UW-Hydro/MetSim) in parallel computation mode, data storage using NetCDF format and explicit representation of Evaporation using the Penman Combination method.

## LICENSE
RAT 3.0 is distributed under the GPL v3 license. You may copy, distribute and modify the software as long as you track changes/dates in sourcefiles. Any modifications to or software including GPL-licensed code must also be made available under the GPL along with build & install instructions.
For more information, please see [LICENSE](./LICENSE).
