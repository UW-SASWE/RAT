import os
import pandas as pd
import numpy as np
import xarray as xr
import geopandas as gpd
import warnings
import datetime

warnings.filterwarnings("ignore")

from logging import getLogger
from rat.utils.logging import LOG_NAME,LOG_LEVEL1_NAME,NOTIFICATION
from rat.utils.science import penman

log = getLogger(f"{LOG_NAME}.{__name__}")
log_level1 = getLogger(f"{LOG_LEVEL1_NAME}.{__name__}")


def calc_dels(aecpath, sareapath, savepath):
    aec = pd.read_csv(aecpath)
    df = pd.read_csv(sareapath, parse_dates=['date'])

    df = df.drop_duplicates('date')
    area_column = 'area' if 'area' in aec.columns else 'CumArea'  # patch to handle either CumArea or area as column name. 
    elevation_column = 'elevation' if 'elevation' in aec.columns else 'Elevation'
    get_elev = lambda area: np.interp(area, aec[area_column], aec[elevation_column])

    df['wl (m)'] = df['area'].apply(get_elev)

    # Even after filtering, use the rolling mean
    # df['wl (m)'] = df['wl (m)'].rolling(5).mean()
    # df['area'] = df['area'].rolling(5).mean()

    # Convert area from km^2 to m^2
    df['area'] = df['area'] * 1e6

    A0 = df['area'].iloc[:-1]
    A1 = df['area'].iloc[1:]

    h0 = df['wl (m)'].iloc[:-1]
    h1 = df['wl (m)'].iloc[1:]

    S = (h1.values - h0.values)*(A1.values + A0.values)/2
    S = np.insert(S, 0, np.nan)

    S =  S * 1e-9                               # Convert to BCM

    df['dS'] = S

    df.to_csv(savepath, index=False)

def calc_E(res_data, start_date, end_date, forcings_path, vic_res_path, sarea, savepath, forecast_mode=False):
    ds = xr.open_dataset(vic_res_path)
    forcings_ds = xr.open_mfdataset(forcings_path, engine='netcdf4')
    ## Slicing the latest run time period
    ds = ds.sel(time=slice(start_date, end_date))
    forcings_ds = forcings_ds.sel(time=slice(start_date, end_date))

    # create buffer to get required bounds
    res_geom = res_data.geometry
    res_buf = res_geom.buffer(0.1)

    minx, miny, maxx, maxy = res_buf.bounds

    log.debug(f"Bounds: {res_buf.bounds}")
    log.debug("Clipping forcings")
    forcings_ds_clipped = forcings_ds['air_pressure'].sel(lon=slice(minx, maxx), lat=slice(maxy, miny))
    forcings = forcings_ds_clipped.load()
    
    log.debug("Clipping VIC results")
    ds_clipped = ds.sel(lon=slice(minx, maxx), lat=slice(maxy, miny))
    reqvars_clipped = ds_clipped[['OUT_EVAP', 'OUT_R_NET', 'OUT_VP', 'OUT_WIND', 'OUT_AIR_TEMP']]
    reqvars = reqvars_clipped.load()

    # get sarea - if string, read from file, else use same surface area value for all time steps
    log.debug(f"Getting surface areas - {sarea}")
    if isinstance(sarea, str):
        sarea = pd.read_csv(sarea, parse_dates=['date']).rename({'date': 'time'}, axis=1)[['time', 'area']]
        sarea = sarea.set_index('time')
        upsampled_sarea = sarea.resample('D').mean()
        sarea_interpolated = upsampled_sarea.interpolate(method='linear')
        
        ## Slicing the latest run time period
        first_obs = sarea_interpolated.index[0]
        if forecast_mode: # forecast mode. extrapolate using forward fill.
            ix = pd.date_range(start=first_obs, end=end_date, freq='D')
            sarea_interpolated = sarea_interpolated.reindex(ix).fillna(method='ffill')
        sarea_interpolated = sarea_interpolated[start_date:end_date]
    
    log.debug("Checking if grid cells lie inside reservoir")
    last_layer = reqvars.isel(time=-1).to_dataframe().reset_index()
    temp_gdf = gpd.GeoDataFrame(last_layer, geometry=gpd.points_from_xy(last_layer.lon, last_layer.lat))
    points_within = temp_gdf[temp_gdf.within(res_geom)]['geometry']

    if len(points_within) == 0:
        log.debug("No points inside reservoir, using nearest point to centroid")
        centroid = res_geom.centroid

        data = reqvars.sel(lat=centroid.y, lon=centroid.x, method='nearest')
        data = data.to_dataframe().reset_index()[1:].set_index('time')

        P = forcings.sel(lat=centroid.y, lon=centroid.x, method='nearest')
        P = P.to_dataframe().reset_index().set_index('time')['air_pressure'].resample('1D').mean()[1:]
        P.head()

    else:
        # print(f"[!] {len(points_within)} Grid cells inside reservoir found, averaging their values")
        data = reqvars.sel(lat=np.array(points_within.y), lon=np.array(points_within.x), method='nearest').to_dataframe().reset_index().groupby('time').mean()[1:]

        P = forcings.sel(lat=np.array(points_within.y), lon=np.array(points_within.x), method='nearest').resample({'time':'1D'}).mean().to_dataframe().groupby('time').mean()[1:]

    if isinstance(sarea, pd.DataFrame):
        data['area'] = sarea_interpolated['area']
    else:
        data['area'] = sarea
    data['P'] = P
    data = data.dropna()
    if (data.empty):
        print('After removal of NAN values, no data left to calculate evaporation.')
        return None
    else:
        data['penman_E'] = data.apply(lambda row: penman(row['OUT_R_NET'], row['OUT_AIR_TEMP'], row['OUT_WIND'], row['OUT_VP'], row['P'], row['area']), axis=1)
        data = data.reset_index()

        # Save (Writing new file if not exist otherwise append)
        if os.path.isfile(savepath):
            existing_data = pd.read_csv(savepath, parse_dates=['time'])
            new_data = data[['time', 'penman_E']].rename({'penman_E': 'OUT_EVAP'}, axis=1)
            # Concat the two dataframes into a new dataframe holding all the data (memory intensive):
            complement = pd.concat([existing_data, new_data], ignore_index=True)
            # Remove all duplicates:
            complement.drop_duplicates(subset=['time'],inplace=True, keep='first')
            complement.to_csv(savepath, index=False)
        else:
            data[['time', 'penman_E']].rename({'penman_E': 'OUT_EVAP'}, axis=1).to_csv(savepath, index=False)


def calc_outflow(inflowpath, dspath, epath, area, savepath):
    if os.path.isfile(inflowpath):
        inflow = pd.read_csv(inflowpath, parse_dates=["date"])
    else:
        raise Exception('Inflow file does not exist. Outflow cannot be calculated.')
    if os.path.isfile(epath):
        E = pd.read_csv(epath, parse_dates=['time'])
    else:
        raise Exception('Evaporation file does not exist. Outflow cannot be calculated.')

    if isinstance(dspath, str):
        if os.path.isfile(dspath):
            df = pd.read_csv(dspath, parse_dates=['date'])
        else:
            raise Exception('Storage Change file does not exist. Outflow cannot be calculated.')
    else:
        df = dspath
    
    inflow = inflow[inflow['date']>=df['date'].iloc[0]]
    inflow = inflow[inflow['date']<=df['date'].iloc[-1]]
    inflow = inflow.set_index('date')

    inflow['streamflow'] = inflow['streamflow'] * (60*60*24)

    E = E[E['time']>=df['date'].iloc[0]]
    E = E[E['time']<=df['date'].iloc[-1]]
    E = E.set_index('time')

    E['OUT_EVAP'] = E['OUT_EVAP'] * (0.001 * area * 1000*1000)  # convert mm to m3. E in mm, area in km2

    last_date = df['date'][:-1]
    df = df.iloc[1:,:]
    
    df['last_date'] = last_date.values
    
    df['inflow_vol'] = df.apply(lambda row: inflow.loc[row['last_date']:row['date'], 'streamflow'].sum(), axis=1)
    df['evap_vol'] = df.apply(lambda row: E.loc[(E.index > row['last_date'])&(E.index <= row['date']), 'OUT_EVAP'].sum(), axis=1)
    df['outflow_vol'] = df['inflow_vol'] - (df['dS']*1e9)# - df['evap_vol']
    df['days_passed'] = (df['date'] - df['last_date']).dt.days
    df['outflow_rate'] = df['outflow_vol']/(df['days_passed']*24*60*60)   # cumecs

    df.to_csv(savepath, index=False)


def run_postprocessing(basin_name, basin_data_dir, reservoir_shpfile, reservoir_shpfile_column_dict, aec_dir_path, start_date, end_date, rout_init_state_save_file, use_rout_state,
                            evap_datadir, dels_savedir, outflow_savedir, vic_status, routing_status, gee_status, forecast_mode=False):
    # read file defining mapped resrvoirs
    # reservoirs_fn = os.path.join(project_dir, 'backend/data/ancillary/RAT-Reservoirs.geojson')
    reservoirs = gpd.read_file(reservoir_shpfile)
    start_date_str = start_date.strftime("%Y-%m-%d")
    if(use_rout_state):
        start_date_str_evap = (start_date-datetime.timedelta(days=100)).strftime("%Y-%m-%d")
    else:
        start_date_str_evap = start_date_str
    end_date_str = end_date.strftime("%Y-%m-%d")
    # Defining flags
    EVAP_STATUS = 0
    DELS_STATUS = 0
    OUTFLOW_STATUS = 0

    # SArea
    sarea_raw_dir = os.path.join(basin_data_dir,'gee', "gee_sarea_tmsos")

    ## No of failed files (no_failed_files) is tracked and used to print a warning message in log level 1 file.
    # DelS
    if(gee_status):
        log.debug("Calculating ∆S")
        no_failed_files = 0
        aec_dir = aec_dir_path

        for reservoir_no,reservoir in reservoirs.iterrows():
            try:
                # Reading reservoir information
                reservoir_name = str(reservoir[reservoir_shpfile_column_dict['unique_identifier']])
                sarea_path = os.path.join(sarea_raw_dir, reservoir_name + ".csv")
                savepath = os.path.join(dels_savedir, reservoir_name + ".csv")
                aecpath = os.path.join(aec_dir, reservoir_name + ".csv")

                if os.path.isfile(sarea_path):
                    log.debug(f"Calculating ∆S for {reservoir_name}, saving at: {savepath}")
                    calc_dels(aecpath, sarea_path, savepath)
                else:
                    raise Exception("Surface area file not found; skipping ∆S calculation")
            except:
                log.exception(f"∆S for {reservoir_name} could not be calculated.")
                no_failed_files += 1 
        DELS_STATUS=1
        if no_failed_files:
            log_level1.warning(f"∆S was not calculated for {no_failed_files} reservoir(s). Please check Level-2 log file for more details.")
    else:
        log.debug("Cannot Calculate ∆S because GEE Run Failed.")
        

    # Evaporation
    if(vic_status and gee_status):
        log.debug("Retrieving Evaporation")
        no_failed_files = 0
        if(use_rout_state):
            vic_results_path = rout_init_state_save_file
        else:
            if forecast_mode:
                vic_results_path = os.path.join(basin_data_dir,'vic', 'forecast_vic_outputs', "nc_fluxes."+start_date_str+".nc")
            else:
                vic_results_path = os.path.join(basin_data_dir,'vic', "vic_outputs/nc_fluxes."+start_date_str+".nc")
        if forecast_mode:
            forcings_path = os.path.join(basin_data_dir,'vic', 'forecast_vic_inputs/*.nc')
        else:
            forcings_path = os.path.join(basin_data_dir,'vic', "vic_inputs/*.nc")

        for reservoir_no,reservoir in reservoirs.iterrows():
            try:
                # Reading reservoir information
                reservoir_name = str(reservoir[reservoir_shpfile_column_dict['unique_identifier']])
                sarea_path = os.path.join(sarea_raw_dir, reservoir_name + ".csv")
                if not os.path.isfile(sarea_path):
                    sarea = float(reservoir[reservoir_shpfile_column_dict['area_column']])
                else:
                    sarea = sarea_path
                e_path = os.path.join(evap_datadir, reservoir_name + ".csv")
                
                if os.path.isfile(sarea) or isinstance(sarea, float):
                    log.debug(f"Calculating Evaporation for {reservoir_name}")
                    calc_E(reservoir, start_date_str_evap, end_date_str, forcings_path, vic_results_path, sarea, e_path, forecast_mode=forecast_mode)
                else:
                    raise Exception("Surface area file not found; skipping evaporation calculation")          
            except:
                log.exception(f"Evaporation for {reservoir_name} could not be calculated.")
                no_failed_files +=1
        EVAP_STATUS = 1
        if no_failed_files:
            log_level1.warning(f"Evapotration was not calculated for {no_failed_files} reservoir(s). Please check Level-2 log file for more details.")
    elif((not vic_status) and (not gee_status)):
        log.debug("Cannot Retrieve Evaporation because both VIC and GEE Run Failed.")
    elif(vic_status):
        log.debug("Cannot Retrieve Evaporation because VIC Run Failed.")
    else:
        log.debug("Cannot Retrieve Evaporation because GEE Run Failed.")

    # Outflow
    if((routing_status) and (EVAP_STATUS) and (DELS_STATUS)):
        log.debug("Calculating Outflow")
        no_failed_files = 0
        inflow_dir = os.path.join(basin_data_dir, "rat_outputs", "inflow")

        for reservoir_no,reservoir in reservoirs.iterrows():
            try:
                # Reading reservoir information
                reservoir_name = str(reservoir[reservoir_shpfile_column_dict['unique_identifier']])
                deltaS = os.path.join(dels_savedir, reservoir_name + ".csv")
                inflowpath = os.path.join(inflow_dir, reservoir_name + ".csv")
                epath = os.path.join(evap_datadir, reservoir_name + ".csv")
                a = float(reservoir[reservoir_shpfile_column_dict['area_column']])

                savepath = os.path.join(outflow_savedir, reservoir_name + ".csv")
                log.debug(f"Calculating Outflow for {reservoir_name} saving at: {savepath}")
                calc_outflow(inflowpath, deltaS, epath, a, savepath)
            except:
                log.exception(f"Outflow for {reservoir_name} could not be calculated")
                no_failed_files+=1
        OUTFLOW_STATUS = 1
        if no_failed_files:
            log_level1.warning(f"Outflow was not calculated for {no_failed_files} reservoir(s). Please check Level-2 log file for more details.")
    else:
        log.debug("Cannot Calculate Outflow because either evaporation, ∆S or Inflow is missing.")
    
    return (DELS_STATUS, EVAP_STATUS, OUTFLOW_STATUS)
def main():
    pass


if __name__ == '__main__':
    main()