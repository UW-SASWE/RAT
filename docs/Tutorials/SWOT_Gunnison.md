# Tutorial: Using the RAT Framework with the SWOT Plugin

This tutorial demonstrates how to set up, run, and analyze results from the Reservoir Assessment Tool (RAT) with the SWOT plugin enabled. The workflow covers configuration, execution, and visualization for reservoir monitoring.

---

## 1. Prerequisites

- **RAT installed** in a conda environment (see [User Guide](../QuickStart/UserGuide.md))
- **Required credentials** for AVISO, IMERG, and Google Earth Engine ([Getting Ready](../QuickStart/GettingReady.md))
- **Project directory structure** as described in [Directory Structure](../RAT_Data/DirectoryStructure.md)

---

## 2. Import RAT and Other Modules

```python
import pandas as pd
import plotly.express as px
import os
from datetime import date

from rat.toolbox import config
from rat.toolbox.MiscUtil import rel2abs
from rat.run_rat import run_rat
from rat.toolbox.visualize import plot_reservoir_and_prior_lakes, RAT_RESERVOIR
```

---

## 3. Change to RAT Project Directory

```python
cd /tiger1/msanchit/research/tmsos_update/rat_jpl
```

---

## 4. Update RAT Configuration File

Set up all required parameters, including those for the SWOT plugin.

```python
# Define the parameters to update in the configuration file of rat
## Dams
dam_file = '/tiger1/msanchit/research/tmsos_update/rat_jpl/global_data/global_dam_data/GRanD_dams_v1_3_filtered.shp'
dam_file_col_dict = {
                      'id_column'      : 'GRAND_ID',
                      'name_column'    : "DAM_NAME",
                      'lon_column'     : 'LONG_DD', 
                      'lat_column'     : 'LAT_DD', 
                    }

## Reservoir  
reservoir_file = "/tiger1/msanchit/research/tmsos_update/rat_jpl/global_data/global_grand_data/GRanD_Version_1_3/GRanD_reservoirs_v1_3.shp"
reservoir_file_col_dict = {
                            'id_column'      : 'GRAND_ID',
                            'dam_name_column': "DAM_NAME",
                            'area_column'    : "AREA_SKM",
                            'dam_lat'        : 'LAT_DD', 
                            'dam_lon'        : 'LONG_DD', 
                            'dam_height'     : 'DAM_HGT_M'
                          }

## River Basin
basin_file = "/tiger1/msanchit/research/tmsos_update/rat_jpl/custom_data/gunnison/gunnison_boundary/gunnison_boundary.shp"
basin_file_col_dict = {
                        'id': "gridcode",
                      }
region_name = "Colorado" # or Karnataka 
basin_name = "gunnison"
basin_id = 0
spin_up = True

## Specifying Run Parameters
steps = [9,10,12,13,14] #Not running altimeter
start_date = date(2024,1,1)  
end_date = date(2024,12,31)

## Specifying VIC Parameters
vic_parameter_file = 'namerica_params.nc'
vic_domain_file = 'namerica_domain.nc'    

## Secrets & Configuration File
secrets_file = "extras/confidential/data_download/secrets.ini"
config_file = "params/rat_config.yaml"

## SWOT Plugins Parameters
swot_run = True
swot_prior_lake_shapefile= "global_data/swot_lakes/SWOT_PRIOR_LAKE_DATABASE/PLD_continents/PLD_SWOT_LakeDatabaseSci_NA.geojson" 
swot_prior_lake_shapefile_column_dict = {'id_column': 'lake_id'}
```

Prepare a dictionary of the above defined parameters to update the configuration file.

```python
params_to_update={

'GLOBAL': {
            'steps'                    : steps,
            'basin_shpfile'            : rel2abs(basin_file), 
            'basin_shpfile_column_dict': basin_file_col_dict
          },

'BASIN' :{
            'region_name'    : region_name,
            'basin_name'     : basin_name,
            'basin_id'       : basin_id,
            'spin_up'        : spin_up,
            'start'          : start_date,
            'end'            : end_date,
         },

'VIC': {
            'vic_basin_continent_param_filename': vic_parameter_file,
            'vic_basin_continent_domain_filename': vic_domain_file
},

'ROUTING': {
            'stations_vector_file': rel2abs(dam_file),
            'stations_vector_file_columns_dict': dam_file_col_dict
        },

'GEE':  {
            'reservoir_vector_file':rel2abs(reservoir_file),
            'reservoir_vector_file_columns_dict': reservoir_file_col_dict
        },

'CONFIDENTIAL':{
            'secrets': rel2abs(secrets_file)
               },

'PLUGINS':{
        'swot': swot_run,
        'swot_prior_lake_shpfile': rel2abs(swot_prior_lake_shapefile),
        'swot_prior_lake_shpfile_column_dict': swot_prior_lake_shapefile_column_dict,
        }
}
```
Update the configuration file.

```python
config.update_config(config_file,params_to_update)
```

---

## 5. Run RAT

```python
run_rat(config_fn=config_file)
```
```console
####################### Finished executing RAT! ##########################  
Please ignore any below error related to distributed.worker or closed stream.  
####################### Finished executing RAT! ##########################
```
---

## 6. Visualize & Analyze Results

### Define Reservoirs and Methods

```python
rat_outputs_path = '/tiger1/msanchit/research/tmsos_update/rat_jpl/data/Colorado/basins/gunnison/final_outputs'
observed_data_path = '/tiger1/msanchit/research/tmsos_update/rat_jpl/ground_data/gunisson_reservoirs'
methods = ['sarea_based', 'elevation_based', 'elevation_sarea_based']

blue_mesa = RAT_RESERVOIR(file_name='541_Blue_Mesa.csv', reservoir_name='Blue Mesa',
                          final_outputs=rat_outputs_path, observed_data_path=observed_data_path)
taylor_park = RAT_RESERVOIR(file_name='536_Taylor_Park.csv', reservoir_name='Taylor Park',
                          final_outputs=rat_outputs_path, observed_data_path=observed_data_path)
silver_jack = RAT_RESERVOIR(file_name='549_Silver_Jack.csv', reservoir_name='Silver Jack',
                          final_outputs=rat_outputs_path, observed_data_path=observed_data_path)
morrow_point = RAT_RESERVOIR(file_name='542_Morrow_Point.csv', reservoir_name='Morrow Point',
                          final_outputs=rat_outputs_path, observed_data_path=observed_data_path)
reservoirs = {
    'Blue Mesa': blue_mesa,
    'Taylor Park': taylor_park,
    'Silver Jack': silver_jack,
    'Morrow Point': morrow_point
}
```

### Plotting Configuration

```python
colors = {
    "tmsos": "blue",
    "methods": ["#9AC607", "#11A8A8", "#0A7102"],
    "observed": "red"
}
```

---

### Surface Area

Plotting:
```python
common_args = dict(
    var_to_observe="Surface Area",
    xlabel="time", ylabel="Surface Area",
    x_axis_units="", y_axis_units="Km^2",
    x_scaling_factor=1,
    xaxis_range=["2024-01-01", "2025-01-01"]
)

for reservoir_name in reservoirs.keys():
    # RAT Tmsos
    fig = reservoirs[reservoir_name].plot_var(
        title_for_plot=f"{common_args['var_to_observe']} at {reservoir_name}",
        y_scaling_factor=1, new_plot=True,
        color=colors["tmsos"], observed_data=False, line_label="RAT-Tmsos",
        **common_args
    )
    # SWOT methods
    for method, color in zip(methods, colors["methods"]):
        fig = reservoirs[reservoir_name].plot_var(
            title_for_plot=f"{common_args['var_to_observe']} at {reservoir_name}",
            y_scaling_factor=1, new_plot=False,
            color=color, swot_method=method, observed_data=False,
            line_label=f"SWOT-{method}", **common_args
        )
    # Observed data (acre-feet to Km^2)
    fig = reservoirs[reservoir_name].plot_var(
        title_for_plot=f"{common_args['var_to_observe']} at {reservoir_name}",
        y_scaling_factor=0.00405, new_plot=False,
        x_col="datetime", y_col="area",
        color=colors["observed"], observed_data=True, line_label="Observed",
        savepath=f"/tiger1/msanchit/research/tmsos_update/rat_jpl/data/plots/{reservoir_name}_{common_args['var_to_observe']}.html".replace(" ", "_"),
        **common_args
    )
    fig.show()
```
Outputs:
<iframe src="../../images/tutorials/SWOT_Gunnison/Blue_Mesa_Surface_Area.html" width="100%" height="400px"></iframe>
<iframe src="../../images/tutorials/SWOT_Gunnison/Taylor_Park_Surface_Area.html" width="100%" height="400px"></iframe>
<iframe src="../../images/tutorials/SWOT_Gunnison/Silver_Jack_Surface_Area.html" width="100%" height="400px"></iframe>
<iframe src="../../images/tutorials/SWOT_Gunnison/Morrow_Point_Surface_Area.html" width="100%" height="400px"></iframe>


Compute Correlations:

```python
for reservoir_name in reservoirs.keys():
    corrs = reservoirs[reservoir_name].compute_correlation("Surface Area",
                                                           swot=True, observed_y_col='area',
                                                           start_date="2024-01-01", end_date="2024-12-31")
    print(f"Correlations for {reservoir_name}: {corrs}")
```
Output:
```console
Correlations for Blue Mesa: {'RAT-Tmsos': 0.538, 'swot_sarea_based': 0.01, 'swot_elevation_based': 0.804, 'swot_elevation_sarea_based': 0.01}
Correlations for Taylor Park: {}
Correlations for Silver Jack: {'RAT-Tmsos': 0.926, 'swot_sarea_based': 0.716, 'swot_elevation_based': 0.823, 'swot_elevation_sarea_based': 0.716}
Correlations for Morrow Point: {'RAT-Tmsos': 0.054, 'swot_sarea_based': 0.232, 'swot_elevation_based': 0.389, 'swot_elevation_sarea_based': 0.232}
```
---

### Elevation

Plotting:
```python
common_args = dict(
    var_to_observe="Elevation",
    xlabel="time", ylabel="Elevation",
    x_axis_units="", y_axis_units="m",
    x_scaling_factor=1,
    xaxis_range=["2024-01-01", "2025-01-01"]
)

for reservoir_name in reservoirs.keys():
    fig = reservoirs[reservoir_name].plot_var(
        title_for_plot=f"{common_args['var_to_observe']} at {reservoir_name}",
        y_scaling_factor=1, new_plot=True,
        color=colors["tmsos"], observed_data=False, line_label="RAT-Tmsos",
        **common_args
    )
    for method, color in zip(methods, colors["methods"]):
        fig = reservoirs[reservoir_name].plot_var(
            title_for_plot=f"{common_args['var_to_observe']} at {reservoir_name}",
            y_scaling_factor=1, new_plot=False,
            color=color, swot_method=method, observed_data=False,
            line_label=f"SWOT-{method}", **common_args
        )
    fig = reservoirs[reservoir_name].plot_var(
        title_for_plot=f"{common_args['var_to_observe']} at {reservoir_name}",
        y_scaling_factor=0.3048, new_plot=False,
        x_col="datetime", y_col="pool elevation",
        color=colors["observed"], observed_data=True, line_label="Observed",
        savepath=f"/tiger1/msanchit/research/tmsos_update/rat_jpl/data/plots/{reservoir_name}_{common_args['var_to_observe']}.html".replace(" ", "_"),
        **common_args
    )
    fig.show()
```
Outputs:
<iframe src="../../images/tutorials/SWOT_Gunnison/Blue_Mesa_Elevation.html" width="100%" 
height="400px"></iframe>
<iframe src="../../images/tutorials/SWOT_Gunnison/Taylor_Park_Elevation.html" width="100%"
 height="400px"></iframe>
<iframe src="../../images/tutorials/SWOT_Gunnison/Silver_Jack_Elevation.html" width="100%"
 height="400px"></iframe> 
<iframe src="../../images/tutorials/SWOT_Gunnison/Morrow_Point_Elevation.html" width="100%"
 height="400px"></iframe>

Compute Correlations:

```python
for reservoir_name in reservoirs.keys():
    corrs = reservoirs[reservoir_name].compute_correlation("Elevation", swot=True,
                                                           observed_y_col='pool elevation',
                                                           start_date="2024-01-01", end_date="2024-12-31")
    print(f"Correlations for {reservoir_name}: {corrs}")
```
Output:
```console
Correlations for Blue Mesa: {'RAT-Tmsos': 0.538, 'swot_sarea_based': 0.009, 'swot_elevation_based': 0.804, 'swot_elevation_sarea_based': 0.804}
Correlations for Taylor Park: {'RAT-Tmsos': 0.78, 'swot_sarea_based': 0.159, 'swot_elevation_based': 0.229, 'swot_elevation_sarea_based': 0.229}
Correlations for Silver Jack: {'RAT-Tmsos': 0.926, 'swot_sarea_based': 0.716, 'swot_elevation_based': 0.823, 'swot_elevation_sarea_based': 0.823}
Correlations for Morrow Point: {'RAT-Tmsos': 0.054, 'swot_sarea_based': 0.226, 'swot_elevation_based': 0.377, 'swot_elevation_sarea_based': 0.377}
```
---

### Storage Change

Plotting:
```python
common_args = dict(
    var_to_observe="Storage Change",
    xlabel="time", ylabel="Storage Change",
    x_axis_units="", y_axis_units="m3/day",
    x_scaling_factor=1,
    xaxis_range=["2024-01-01", "2025-01-01"]
)

for reservoir_name in reservoirs.keys():
    fig = reservoirs[reservoir_name].plot_var(
        title_for_plot=f"{common_args['var_to_observe']} at {reservoir_name}",
        y_scaling_factor=1, new_plot=True, y_col='ds (m3/day)',
        color=colors["tmsos"], observed_data=False, line_label="RAT-Tmsos",
        **common_args
    )
    for method, color in zip(methods, colors["methods"]):
        fig = reservoirs[reservoir_name].plot_var(
            title_for_plot=f"{common_args['var_to_observe']} at {reservoir_name}",
            y_scaling_factor=1, new_plot=False, y_col='ds (m3/day)',
            color=color, swot_method=method, observed_data=False,
            line_label=f"SWOT-{method}", **common_args
        )
    fig = reservoirs[reservoir_name].plot_var(
        title_for_plot=f"{common_args['var_to_observe']} at {reservoir_name}",
        y_scaling_factor=1233.48, new_plot=False,
        x_col="datetime", y_col="delta storage",
        color=colors["observed"], observed_data=True, line_label="Observed",
        savepath=f"/tiger1/msanchit/research/tmsos_update/rat_jpl/data/plots/{reservoir_name}_{common_args['var_to_observe']}.html".replace(" ", "_"),
        **common_args
    )
    fig.show()
```
Outputs:
<iframe src="../../images/tutorials/SWOT_Gunnison/Blue_Mesa_Storage_Change.html" width="100%" 
height="400px"></iframe>
<iframe src="../../images/tutorials/SWOT_Gunnison/Taylor_Park_Storage_Change.html" width="100%"
 height="400px"></iframe>
<iframe src="../../images/tutorials/SWOT_Gunnison/Silver_Jack_Storage_Change.html" width="100%" height="400px"></iframe> 
<iframe src="../../images/tutorials/SWOT_Gunnison/Morrow_Point_Storage_Change.html" width="100%" height="400px"></iframe>
Compute Correlations:

```python
for reservoir_name in reservoirs.keys():
    corrs = reservoirs[reservoir_name].compute_correlation("Storage Change", swot=True,
                                                           observed_y_col='delta storage',
                                                           start_date="2024-01-01", end_date="2024-12-31")
    print(f"Correlations for {reservoir_name}: {corrs}")
```
Output:
```console
Correlations for Blue Mesa: {'RAT-Tmsos': 0.157, 'swot_sarea_based': 0.021, 'swot_elevation_based': 0.418, 'swot_elevation_sarea_based': 0.409}
Correlations for Taylor Park: {'RAT-Tmsos': 0.323, 'swot_sarea_based': 0.06, 'swot_elevation_based': 0.414, 'swot_elevation_sarea_based': 0.416}
Correlations for Silver Jack: {'RAT-Tmsos': 0.757, 'swot_sarea_based': 0.018, 'swot_elevation_based': 0.441, 'swot_elevation_sarea_based': 0.415}
Correlations for Morrow Point: {'RAT-Tmsos': -0.12, 'swot_sarea_based': 0.429, 'swot_elevation_based': -0.485, 'swot_elevation_sarea_based': -0.498}
```
---

## Summary

You have now learned how to configure and run RAT with the SWOT plugin, and how to visualize and analyze reservoir results. For more details, refer to the [RAT's SWOT Plugin](../../Plugins/Swot).

---