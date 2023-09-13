---
title: 'Reservoir Assessment Tool (RAT): A Python Package for monitoring the dynamic state of reservoirs and analyzing dam operations'
tags:
  - Reservoir Monitoring
  - Remote Sensing
  - Python
  - Hydrology
authors:
  - name: Sanchit Minocha
    orcid: 0009-0007-1969-7521
    affiliation: 1
    corresponding: true 
  - name: Pritam Das
    orcid: 0000-0003-4795-4736
    affiliation: 1
  - name: Faisal Hossain 
    affiliation: 1
affiliations:
 - name: Department of Civil and Environmental Engineering, University of Washington, USA
   index: 1
date: 10 September 2023
bibliography: paper.bib
---


# Summary 

Rivers, Earth's circulatory system, akin to human arteries, ensure water reaches every corner of the planet. While these river networks are natural, human intervention has led to the construction of dams, which store substantial water volumes in reservoirs for human consumption, such as, domestic water supply, irrigation, and hydropower generation [@jackson2001water]. Although dams with the reservoirs behind them, offer benefits, they can also have adverse effects, such as exacerbating downstream droughts, choking the natural flow of sediments and ecological disturbance. Such effects emphasize the necessity for consistent reservoir monitoring [@poff2002dams]. Amidst today’s ever-increasing advancing technological landscape, real-time reservoir monitoring through satellite data is now a reality [@bonnema2017inferring] [@eldardiry2019understanding]. Reservoir Assessment Tool (RAT), is a Python package that seamlessly integrates cloud computing, satellite imagery processing, information technology, and hydrological modeling to provide estimation of critical reservoir metrics, encompassing surface area, inflow, outflow, and storage changes.

# Statement of need

Data regarding inland water quantities that are regulated by dams with reservoirs, hold profound significance, not only for water scientists, but also for diverse fields closely intertwined with water, such as food production, energy generation, and climate modeling. The availability of surface water in a region is no longer solely determined by natural factors like precipitation; human activities, notably dam operations, now exert significant influence over reservoir levels [@adrian2009lakes]. Satellite remote sensing offers a viable means of monitoring these dynamic reservoir conditions. However, most studies and currently available tools are constrained to specific regions due to challenges in accessing and integrating real-time meteorological data and the complexities associated with implementing hydrological models for inflow estimation [@bonnema2016understanding][@muala2014estimation]. The intricacies involved in processing satellite imagery to extract water quantity data present an additional hurdle, creating difficulties for the diverse stakeholders as they endeavor to independently apply these techniques within their respective areas of need. This reliance on specialized expertise creates a barrier, hindering broader accessibility to this invaluable information on how dams are regulating surface water.

To lower this barrier and democratize the access to dynamic information on reservoirs operated by dams, we introduce RAT, an open-source Python package. It functions as an all-in-one tool for investigating near real-time reservoir dynamics. This versatile suite effortlessly integrates various functions, including real-time meteorological data retrieval, hydrological modeling, cloud computing for satellite image processing through the Google Earth Engine, and the use of parallel processing for computing reservoir parameters. While other Python packages can measure water body surface areas, they often overlook the potential of cloud computing for satellite image processing, do not integrate distributed physical modeling of the landscape, or are unsuitable for automating workflows [@owusu2022pygee] [@cordeiro2021automatic]. To the best of our knowledge, no other Python package offers such a comprehensive and complete view of dynamic state vector of reservoirs (comprising inflow, outflow, surface area, elevation and evaporation).

RAT is user-friendly and simple to set up, requiring minimal preparation of outside input for the first-time user. Its automated self-installation design is tailored to make RAT  useable even to those with limited knowledge of hydrology or remote sensing. While RAT is a comprehensive model offering data on inflow, storage changes, evaporation, outflow dynamic surface area and elevation of a reservoir. Additionally, by running RAT only daily by utilizing the previous run's state, one can efficiently monitor in real-time reservoir dynamics while minimizing computational burden. These attributes position RAT as a tool for enabling breakthroughs across hydrology-related fields. For detailed information on the architecture of version 3.0 of RAT, which has been instrumental in the development of this Python package, please consult the companion paper [@minocha2023reservoir].

# Command Line Functionality 

## Initialization

RAT relies on models like MetSim, VIC, and routing to estimate inflow through the execution of hydrological models [@bennett2020metsim] [@hamman2018variable]. While both MetSim and VIC are Python packages, they require separate Python environments for their operation. Additionally, the Fortran script used for routing must be downloaded and compiled independently. To simplify this process, the CLI (command-line interface) command 'rat init' streamlines RAT's initialization. It accomplishes this by automatically downloading and installing the required Python environments for MetSim and VIC, as well as fetching and compiling the routing scripts. Moreover, it acquires several parameter files necessary for VIC, MetSim, Routing, or RAT. This command also offers users the option to download a global database, encompassing multiple global datasets, which can serve as the default for RAT execution. Notably, the initialization of RAT is a one-time requirement following the installation of the RAT conda package.

## Testing

After RAT is installed and initialized, users can test it using 'rat test.' This command downloads a test dataset with parameter files for test river basins and expected results. It runs RAT for a test basin selected by user, comparing outputs to expectations across six test cases: inflow, outflow, surface area, storage change, evaporation, and area elevation curve. It also validates user provided credentials.

## Execution

To run RAT through the command-line interface, you can utilize the 'rat run' command by specifying its configuration file. It also offers users the option to run RAT in an operational mode, which means that RAT will resume from the last state of a previous run, commencing execution from the end date of that prior run. Moreover, executing and operationalizing RAT can also be accomplished through Python scripts for more flexibility and customization.

## Configure

Given that the RAT configuration file contains numerous parameters, many of which may prove perplexing for beginners while being useful for advanced users, there exists a 'rat configure' command. This command automatically fills in specific parameter values in the configuration file using user-provided options and, when available, the default global database, enhancing convenience and clarity.

# Input requirements

At the least, execution of RAT requires a river basin shapefile, point locations of dams (or stations) where to calculate inflow, and a shapefile containing geometry polygons of all reservoirs to track storage change, evaporation and outflow from the dams \autoref{fig:1}. For rest of the required parameters, default global database can be used for initial run. After that, the default datasets can be calibrated using the actual observed data to achieve better accuracy. 

# Applications and Ongoing Research

RAT has the ability to offer a global perspective on reservoir dynamics, revealing insights into how governments manage dams and alter natural river flows. It's already adopted by organizations like the Asian Disaster Preparedness Center and the Mekong River Commission for operational use [@das2022reservoir]. It can acquire setting reservoir data in transboundary river basins like Mekong and Tigris-Euphrates where the data is not available due to logistical or political barriers [@hossain2023restoring]. Additionally, RAT has been employed in studying flood tracking in Kerala [@suresh2023satellite] and is being utilized to analyze the hydro-thermal history of the Columbia River basin. Ongoing research aims to expand RAT's capabilities, including features related to regulated river inflow, forecasted inflow and improving accuracy of storage change using new satellite missions like SWOT.

# Developer Notes

RAT is developed on [GitHub](https://github.com/UW-SASWE/RAT) as an open-source package and is available only for Linux OS to download from [anaconda.org](https://anaconda.org/conda-forge/rat). The authors encourage contributions from the community and report issues faced at [https://github.com/UW-SASWE/RAT/issues](https://github.com/UW-SASWE/RAT/issues). The discussion forum dedicated to support the user community of RAT can be accessed at [https://github.com/UW-SASWE/RAT/discussions](https://github.com/UW-SASWE/RAT/discussions). Complete documentation of the python package is available at [https://rat-satellitedams.readthedocs.io/en/latest/](https://rat-satellitedams.readthedocs.io/en/latest/). 

# Figures

![RAT python package minimal input requirements are (a)river basin shapefile (example- Tigris Euphrates river basin), (b) dam location (example- Tabqa dam) and (c) reservoir shapefile (example- Tabqa reservoir) and it will give output (d) area-elevation curve and time series for storage change, evaporation, inflow, outflow and surface area.  \label{fig:1}](figure_1.jpg)

# Acknowledgements

This work was supported by the NASA Applied Science Program for Water 80NSSC22K0918. Additional support to the first author from the NSF-supported Graduate Training program called Future Rivers and the Ivanhoe Foundation is gratefully acknowledged.

# References
