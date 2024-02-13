# RAT-Forecasting

The forecasting plugin adds functionality to generate short-term forecasts of the reservoir state for up to 15 days. It uses forecasted weather data to forecast the inflow to reservoirs. Other Reservoir operations related fluxes, specifically, the storage change in the forecast window and the resulting outflow from the reservoir are estimated as different scenarios, which are described in detail below. 

## How to Use
To run the forecast plugin, set the value of the `forecast` option in the PLUGINS section of the configuration file to True.

```
PLUGINS: 
	forecast: True 
	forecast_lead_time: 15
	forecast_start_time: end_date     # can either be “end_date” or a date in YYYY-MM-DD format 
```

The `forecast_start_time` option controls when the forecast will begin. If the value is set to `end_date`, the forecast will begin on the end date of RAT’s normal mode of running, i.e., in nowcast mode, which are controlled by `start_date` and `end_date` options in the BASIN section. Alternatively, a date in the YYYY-MM-DD format can also be provided to start the forecast from that date. The forecast window or the number of days ahead for which the forecast is generated is controlled by the `forecast_lead_time` option, with a maximum of 15 days ahead. 

## Forecasted inflow and evaporation 
- The inflow to the reservoir is simulated using forecasted precipitation from Climate Hazards Center InfraRed Precipitation with Stations-Global Ensemble Forecasting System [(CHIRPS-GEFS)](https://chc.ucsb.edu/data/chirps-gefs) and forecasted temperature and wind data from Global Forecasting System [(GFS)](https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast). CHIRPS-GEFS uses satellite and observations of precipitation (CHIRPS) for bias correction and downscaling of the Global Ensemble Forecasting System (GEFS) precipitation forecasts. The Global Forecasting System (GFS) is a Numerical Weather Prediction system for operational weather prediction which forecasts meteorological variables, including temperature and wind.
- The forecasted meteorology is used to run the hydrological model component of RAT (MetSim + VIC + VIC Routing) to obtain forecasted streamflow and evaporation for each reservoir.

## Reservoir Storage Change and Outflow Scenarios 
The reservoir state – storage change, outflow, water surface elevation, and the water surface area – is estimated based on different scenarios of possible reservoir operations in the forecasting window. The scenarios used to estimate reservoir storage change are as follows -  

- Target reservoir water level – Maintains a target water level by storing and releasing the necessary amount of water. 

- Fraction of maximum reservoir storage – Storage change is estimated as a fraction of the maximum reservoir storage. 

- Historical operations-based outflow scenarios – Storage change is estimated based on historical operations of the reservoir. Historical observations of the reservoir are obtained from Biswas et al. (2021), which infers the reservoir operations using long-term observations of the reservoir surface area. 

- Gates Closed/Open - Simulation of the reservoir state by considering the dam gates to be either fully closed or fully open. 

- User defined storage change – Users can directly input the expected volume of storage change in the forecasting window to simulate the reservoir states.

## Publication

We used this forecasting plugin to perform a forensic study of devastating floods due to extreme precipitation that caused havoc in the entire state of Kerala, India in 2018. The forecasted data generated for the study can be accessed here - [RAT-Forecasting-Kerala-2018](forecasting-data.zip). It contains inflow forecast, inflow nowcast and release scenarios.