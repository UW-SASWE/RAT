![Reservoir Assessment Tool](docs/logos/Rat_Logo_black_github.png)
# Reservoir Assessment Tool (RAT) 3.0

***A scalable and easy-to-apply python based software architecture to empower the global water community***

The Reservoir Assessment Tool (RAT) uses satellite remote sensing data to monitor water surface area and water level changes in artificial reservoirs. It uses this information, along with topographical information (either derived from satellite data, or in-situ topo maps) to estimate the **Storage Change (∆S)** in the reservoirs. Additionally, RAT models the **Inflow (I)** and the **Evaporation (E)** of each reservoir. Finally, RAT uses the modeled I, and E, and estimated ∆S, to estimate the **Outflow (O)** from reservoirs.

## Current release info

| Name | Downloads | Version | Platforms | Documentation | Release Date |
| --- | --- | --- | --- | --- | --- |
| [![Conda Package](https://img.shields.io/badge/package-rat-51A1B0.svg)](https://anaconda.org/conda-forge/rat) | [![Conda Downloads](https://img.shields.io/conda/dn/conda-forge/rat.svg)](https://anaconda.org/conda-forge/rat) | [![Conda Version](https://img.shields.io/conda/vn/conda-forge/rat.svg)](https://anaconda.org/conda-forge/rat) | [![Conda Platforms](https://img.shields.io/conda/pn/conda-forge/rat.svg)](https://anaconda.org/conda-forge/rat) | [![Documentation Status](https://readthedocs.org/projects/rat-satellitedams/badge/?version=latest)](https://rat-satellitedams.readthedocs.io/en/latest/?badge=latest) | [![Anaconda-Server Badge](https://anaconda.org/conda-forge/rat/badges/latest_release_date.svg)](https://anaconda.org/conda-forge/rat) |

## Installing RAT

Installing `rat` from the `conda-forge` channel can be achieved by adding `conda-forge` to your channels with:

```
conda config --add channels conda-forge
conda config --set channel_priority strict
```

Once the `conda-forge` channel has been enabled, `rat` can be installed with `conda`:

```
conda install rat
```

or with `mamba`:

```
mamba install rat
```

It is possible to list all of the versions of `rat` available on your platform with `conda`:

```
conda search rat --channel conda-forge
```

or with `mamba`:

```
mamba search rat --channel conda-forge
```

Alternatively, `mamba repoquery` may provide more information:

```
# Search all versions available on your platform:
mamba repoquery search rat --channel conda-forge

# List packages depending on `rat`:
mamba repoquery whoneeds rat --channel conda-forge

# List dependencies of `rat`:
mamba repoquery depends rat --channel conda-forge
```


## About RAT
[RAT 3.0](https://doi.org/10.5194/gmd-17-3137-2024) makes numerous improvements to the code structure, performance optimizations, added configurations, ability to run RAT for multiple basins, among some introduced features. It also introduces packaging of RAT as a conda package, allowing for quick and easy installation.

It was originally developed by [Biswas et al. (2021)](https://doi.org/10.1016/j.envsoft.2021.105043) at [SASWE](https://saswe.net/), [University of Washington](https://www.washington.edu/). The RAT framework developed by [Biswas et al. (2021)](https://doi.org/10.1016/j.envsoft.2021.105043) was a first-of-its-kind open-source reservoir monitoring tool, it is reffered to as the version 1 of RAT, or [RAT 1.0](http://depts.washington.edu/saswe/rat_retired/). It currently runs for 3 regions - (1) South and South East Asia, (2) Africa, and (3) South America, and can be accessed [here](http://depts.washington.edu/saswe/rat_beta/).

The [Reservoir Assessment Tool (RAT) 2.0](https://depts.washington.edu/saswe/mekong/) was introduced with numerous improvements over the RAT 1.0. Such as weekly satellite observations (every 1-5 days) using a combination of multiple satellites (Sentinel-2, landsat-8, landsat-9 and Sentinel-1), usage of [VIC hydrological model](https://github.com/UW-Hydro/VIC) (VIC 5, [Hamman et al. (2018)](https://doi.org/10.5194/gmd-11-3481-2018)) and [MetSim](https://github.com/UW-Hydro/MetSim) in parallel computation mode, data storage using NetCDF format and explicit representation of Evaporation using the Penman Combination method.

## CITATION
If you use this software, please cite the following depending on the context of the work:

1. [Minocha, S., Hossain, F., Das, P., Suresh, S., Khan, S., Darkwah, G., Lee, H., Galelli, S., Andreadis, K., and Oddo, P.: Reservoir Assessment Tool version 3.0: a scalable and user-friendly software platform to mobilize the global water management community, Geosci. Model Dev., 17, 3137–3156, https://doi.org/10.5194/gmd-17-3137-2024, 2024.](https://doi.org/10.5194/gmd-17-3137-2024)  <br><br>

2. [Das, P., Hossain, F., Khan, S., Biswas, N.K., Lee, H., Piman, T., Meechaiya, C., Ghimire, U. and Hosen, K., 2022. Reservoir Assessment Tool 2.0: Stakeholder driven improvements to satellite remote sensing based reservoir monitoring. Environmental Modelling & Software, 157, p.105533.](https://doi.org/10.1016/j.envsoft.2022.105533)  <br><br>

3. [Biswas, N.K., Hossain, F., Bonnema, M., Lee, H. and Chishtie, F., 2021. Towards a global Reservoir Assessment Tool for predicting hydrologic impacts and operating patterns of existing and planned reservoirs. Environmental Modelling & Software, 140, p.105043.](https://doi.org/10.1016/j.envsoft.2021.105043) 

## LICENSE
RAT 3.0 is distributed under the GPL v3 license. You may copy, distribute and modify the software as long as you track changes/dates in sourcefiles. Any modifications to or software including GPL-licensed code must also be made available under the GPL along with build & install instructions.
For more information, please see [LICENSE](./LICENSE).
