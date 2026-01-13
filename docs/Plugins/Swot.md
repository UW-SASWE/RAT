# RAT-SWOT  

The **SWOT Plugin** integrates with RAT to enable estimation of reservoir storage change, evaporation, and outflow using SWOT-derived surface water observations. It leverages the [**Hydrocron API**](https://podaac.github.io/hydrocron/intro.html) to extract **surface area** and **water surface elevation** time series for reservoirs and lakes.  

The plugin requires a set of parameters in the configuration file. Outputs are organized methodically into sub-directories inside the `final_outputs/swot/` directory.  

!!! note
    The SWOT plugin currently supports three estimation methods:  
    - **elevation_based**  
    - **sarea_based**  
    - **elevation_sarea_based**  

Each method differs in how SWOT data (area/elevation) and AEC (Area–Elevation Curve) are utilized to estimate storage change.  

---

### Parameters 

The following parameters are required to enable the SWOT plugin in the [PLUGINS section](../../Configuration/rat_config/#plugins) of the configuration file. The requirement is specified with regard to the running of SWOT plugin. If you don't want to run SWOT plugin, then leave the parameters blank or comment them.

* <h6 class="parameter_heading">*`swot`* :</h6>  
    <span class="requirement">Required parameter</span>  

    <span class="parameter_property">Description </span>:  Flag that specifies whether to run SWOT plugin during RAT run.  Accepted values are `True` or `False` or `only_swot`. If `only_swot` is specified, then only the SWOT plugin will run and not the default TMSOS.

    <span class="parameter_property">Syntax </span>:  
    ```yaml
    PLUGINS:
        swot: True
    ```

---

* <h6 class="parameter_heading">*`swot_prior_lake_shpfile`* :</h6>  
    <span class="requirement">Required parameter</span>  

    <span class="parameter_property">Description </span>: Path to the shapefile containing SWOT prior lakes. It should have unique ID field corresponding to the SWOT lake ID.

    <span class="parameter_property">Syntax </span>:  
    ```yaml
    PLUGINS:
        swot_prior_lake_shpfile: absolute/path/to/data/lakes/swot_prior_lakes.shp
    ```

---

* <h6 class="parameter_heading">*`swot_prior_lake_shpfile_column_dict`* :</h6>  
    <span class="requirement">Required parameter</span>  

    <span class="parameter_property">Description </span>: Dictionary specifying column mapping for unique ID of lake/reservoir identifiers. The unique ID must match with SWOT lake ID as it is used by hydrocron API to extract SWOT data.  

    <span class="parameter_property">Syntax </span>:  
    ```yaml
    PLUGINS:
        swot_prior_lake_shpfile_column_dict:
            id_column: lake_id
    ```

---

### Estimation Methods  

The plugin computes **storage change**, **evaporation**, and **outflow** using one of the following methods:  

1. **elevation_based**:  
Uses SWOT **elevation** data and the **AEC (Area–Elevation Curve)** to estimate storage change.  

2. **sarea_based**:  
Uses SWOT **surface area** data and the **AEC (Area–Elevation Curve)** to estimate storage change.  

3. **elevation_sarea_based**:  
Uses both SWOT **area** and **elevation** data to estimate storage change, without relying on AEC.  

---

### Outputs  

The plugin writes results to:  

```bash
final_outputs/
└── swot/
    ├── elevation_based/
    │   ├── dels/
    │   ├── elevation/
    │   ├── sarea_swot/
    │   ├── evaporation/
    │   └── outflow/
    ├── sarea_based/
    │   ├── dels/
    │   ├── elevation/
    │   ├── sarea_swot/
    │   ├── evaporation/
    │   └── outflow/
    └── elevation_sarea_based/
        ├── dels/
        ├── elevation/
        ├── sarea_swot/
        ├── evaporation/
        └── outflow/
```

- **dels/** → Storage change time series of reservoirs  
- **elevation/** →  Water Surface Elevation time series of reservoirs
- **sarea_swot/** → Water Surface Area time series of reservoirs 
- **evaporation/** → Evaporation time series of reservoirs  
- **outflow/** → Outflow time series of reservoirs  

!!! tip_note "Tip"
    Each method’s directory contains parallel outputs, enabling easy comparison between methods.  

---

### Example Configuration Block  

```yaml
PLUGINS:
    swot: True
    swot_prior_lake_shpfile: absolute/path/to/data/lakes/swot_prior_lakes.shp
    swot_prior_lake_shpfile_column_dict:
        id_column: lake_id
```