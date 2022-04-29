import os
import datetime
import subprocess
import pandas as pd
import numpy as np
import xarray as xr
import geopandas as gpd
import warnings

warnings.filterwarnings("ignore")


from logging import getLogger
from utils.logging import LOG_NAME, NOTIFICATION
from utils.utils import run_command
from utils.science import penman

log = getLogger(f"{LOG_NAME}.{__name__}")


def sarea_postprocessor(dfpath, savepath):
    df = pd.read_csv(dfpath, parse_dates=['mosaic_enddate'])

    # Filter out drastic changes
    df['std'] = df['corrected_area'].rolling(5).std()
    df['mean'] = df['corrected_area'].rolling(5).mean()
    df = df[(df['corrected_area'] <= df['mean']+1.68*df['std']) & (df['corrected_area'] >= df['mean']-1.68*df['std'])]

    log.debug(f"Filtering out spikes from {dfpath}; Saving at -> {savepath}")
    df.to_csv(savepath, index=False)


def calc_dels(aecpath, sareapath, savepath):
    aec = pd.read_csv(aecpath)
    df = pd.read_csv(sareapath, parse_dates=['mosaic_enddate']).rename({'mosaic_enddate': 'date'}, axis=1)

    df = df.drop_duplicates('date')

    get_elev = lambda area: np.interp(area, aec['CumArea'], aec['Elevation'])

    df['wl (m)'] = df['corrected_area'].apply(get_elev)

    # Even after filtering, use the rolling mean
    df['wl (m)'] = df['wl (m)'].rolling(5).mean()
    df['corrected_area'] = df['corrected_area'].rolling(5).mean()

    # Convert area from km^2 to m^2
    df['corrected_area'] = df['corrected_area'] * 1e6

    A0 = df['corrected_area'].iloc[:-1]
    A1 = df['corrected_area'].iloc[1:]

    h0 = df['wl (m)'].iloc[:-1]
    h1 = df['wl (m)'].iloc[1:]

    S = (h1.values - h0.values)*(A1.values + A0.values)/2
    S = np.insert(S, 0, np.nan)

    S =  S * 1e-9                               # Convert to BCM

    df['dS'] = S

    df.to_csv(savepath, index=False)

def calc_E(res_path, forcings_path, vic_res_path, sarea_path, savepath):
    ds = xr.open_dataset(vic_res_path)#.load()
    forcings_ds = xr.open_mfdataset(forcings_path, engine='netcdf4')

    res_name = res_path.split(os.sep)[-1]

    res = gpd.read_file(res_path)
    # create buffer to get required bounds
    res_buf = res.buffer(0.1)

    minx, miny, maxx, maxy = res_buf.iloc[0].bounds

    log.debug(f"Bounds: {res_buf.iloc[0].bounds}")
    log.debug("Clipping forcings")
    forcings_ds_clipped = forcings_ds['air_pressure'].sel(lon=slice(minx, maxx), lat=slice(maxy, miny))
    forcings = forcings_ds_clipped.load()
    
    log.debug("Clipping VIC results")
    ds_clipped = ds.sel(lon=slice(minx, maxx), lat=slice(maxy, miny))#.isel(time=slice(100, 200))
    reqvars_clipped = ds_clipped[['OUT_EVAP', 'OUT_R_NET', 'OUT_VP', 'OUT_WIND', 'OUT_AIR_TEMP']]
    reqvars = reqvars_clipped.load()

    # get sarea
    log.debug("Getting surface areas")
    sarea = pd.read_csv(sarea_path, parse_dates=['mosaic_enddate']).rename({'mosaic_enddate': 'time'}, axis=1)[['time', 'corrected_area']]
    sarea = sarea.set_index('time')
    upsampled_sarea = sarea.resample('D').mean()
    sarea_interpolated = upsampled_sarea.interpolate(method='linear')
    
    log.debug("Checking if grid cells lie inside reservoir")
    last_layer = reqvars.isel(time=-1).to_dataframe().reset_index()
    temp_gdf = gpd.GeoDataFrame(last_layer, geometry=gpd.points_from_xy(last_layer.lon, last_layer.lat))

    res_geom = res.iloc[0].geometry
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
        data = reqvars.sel(lat=points_within.y, lon=points_within.x, method='nearest').to_dataframe().reset_index().groupby('time').mean()[1:]

        # res_geom = res.iloc[0].geometry
        P = forcings.sel(lat=points_within.y, lon=points_within.x, method='nearest').resample({'time':'1D'}).mean().to_dataframe().groupby('time').mean()[1:]

    data['area'] = sarea_interpolated['corrected_area']
    data['P'] = P
    data = data.dropna()
    data['penman_E'] = data.apply(lambda row: penman(row['OUT_R_NET'], row['OUT_AIR_TEMP'], row['OUT_WIND'], row['OUT_VP'], row['P'], row['area']), axis=1)
    data = data.reset_index()

    # Save 
    data[['time', 'penman_E']].rename({'penman_E': 'OUT_EVAP'}, axis=1).to_csv(savepath, index=False)


def calc_outflow(inflowpath, dspath, epath, area, savepath):
    inflow = pd.read_csv(inflowpath, parse_dates=["date"])
    E = pd.read_csv(epath, parse_dates=['time'])

    if isinstance(dspath, str):
        df = pd.read_csv(dspath, parse_dates=['date'])
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


def run_postprocessing(project_dir):
    # SArea
    log.debug("Started Postprocessing Surface Area")
    sarea_raw_dir = os.path.join(project_dir, "backend/data/sarea")
    sarea_postprocessing_dir = os.path.join(sarea_raw_dir, "postprocessed")

    raw_sareas = [os.path.join(sarea_raw_dir, f) for f in os.listdir(sarea_raw_dir) if f.endswith('.csv')]
    for raw_sarea in raw_sareas:
        sarea_name = os.path.split(raw_sarea)[-1]
        savepath = os.path.join(sarea_postprocessing_dir, sarea_name)
        
        log.debug(f"Postprocessing SArea {sarea_name}")
        sarea_postprocessor(raw_sarea, savepath)
    
    # DelS
    log.debug("Calculating ∆S")
    dels_savedir = os.path.join(project_dir, "backend/data/dels")
    aec_dir = os.path.join(project_dir, "backend/data/aec")

    aec_mapping = {
        'Lam_Pao': '5150',
        'Lower_Sesan_2': '7303',
        'Nam_Ngum_1': '5136', 
        'Sesan_4': '7203',
        'Sirindhorn': '5796',
        'Ubol_Ratana': '5149',
        'Nam_Theun_2': '6999',
        'Xe_Kaman_1': '7003'
    }

    postprocessed_sareas = [os.path.join(sarea_postprocessing_dir, f) for f in os.listdir(sarea_postprocessing_dir) if f.endswith('.csv')]
    for postprocessed_sarea in postprocessed_sareas:
        sarea_name = os.path.split(postprocessed_sarea)[-1]
        savepath = os.path.join(dels_savedir, sarea_name)

        aecpath = os.path.join(aec_dir, sarea_name.replace(".csv", ".txt"))
        print(aecpath)
        if not os.path.isfile(aecpath):
            if sarea_name.replace(".csv", "") in aec_mapping.keys():
                aecpath = os.path.join(aec_dir, f"{aec_mapping[sarea_name.replace('.csv', '')]}.txt")
            else:
                log.debug(f"∆S will not be calculated for {sarea_name} It is not mapped in RAT yet")
                continue

        savepath = os.path.join(dels_savedir, sarea_name)
        log.debug(f"Calculating ∆S for {sarea_name}, saving at: {savepath}")
        calc_dels(aecpath, postprocessed_sarea, savepath)

    # Evaporation
    log.debug("Retrieving Evaporation")
    evap_datadir = os.path.join(project_dir, "backend/data/E")
    res_dir = os.path.join(project_dir, "backend/data/ancillary/reservoirs")
    vic_results_path = os.path.join(project_dir, "backend/data/vic_results/nc_fluxes.2001-04-01.nc")
    sarea_dir = os.path.join(project_dir, "backend/data/sarea")
    forcings_path = os.path.join(project_dir, "backend/data/forcings/*.nc")

    reservoirs = [os.path.join(res_dir, f) for f in os.listdir(res_dir) if f.endswith(".shp")]

    # hotfix
    patch = {
        'Siridhorn.csv': 'Sirindhorn.csv'
    }

    for respath in reservoirs:
        _, resname = os.path.split(respath)
        # using hotfix; isn't ideal, fix during reorganizing
        if not resname.replace(".shp", ".csv") in patch.keys():
            sarea_path = os.path.join(sarea_dir, resname.replace(".shp", ".csv"))
            e_path = os.path.join(evap_datadir, resname.replace(".shp", '.csv'))
        else:
            sarea_path = os.path.join(sarea_dir, patch[resname.replace(".shp", ".csv")])
            e_path = os.path.join(evap_datadir, patch[resname.replace(".shp", '.csv')])

        if os.path.isfile(sarea_path):
            log.debug(f"Calculating Evaporation for {resname}")
            # calc_E(e_path, respath, vic_results_path)
            calc_E(respath, forcings_path, vic_results_path, sarea_path, e_path)
        else:
            log.debug(f"{sarea_path} not found; skipping")

    # Outflow
    log.debug("Calculating Outflow")
    outflow_savedir = os.path.join(project_dir, "backend/data/outflow")
    inflow_dir = os.path.join(project_dir, "backend/data/inflow")
    evap_dir = os.path.join(project_dir, "backend/data/E")

    names_to_ids = {
        "5136": "Nam_N",
        "5149": "Ubol_",
        "5150": "Lam_P",
        "5796": "Sirid",
        "6999": "Nam_T",
        "7003": "Xe_Ka",
        "7203": "Sesan",
        "7303": "Lower",
        "5117": "5117 ",
        "5138": "5138 ",
        "5143": "5143 ",
        "5147": "5147 ",
        "5148": "5148 ",
        "5151": "5151 ",
        "5152": "5152 ",
        "5155": "5155 ",
        "5156": "5156 ",
        "5160": "5160 ",
        "5162": "5162 ",
        "5795": "5795 ",
        "5797": "5797 ",
        "7000": "7000 ",
        "7001": "7001 ",
        "7002": "7002 ",
        "7004": "7004 ",
        "7037": "7037 ",
        "7087": "7087 ",
        "7158": "7158 ",
        "7159": "7159 ",
        "7164": "7164 ",
        "7181": "7181 ",
        "7201": "7201 ",
        "7232": "7232 ",
        "7284": "7284 ",
        "7303": "7303 "
    }

    areas = {                   # Areas in km2, from GRAND if available, or calcualted
        "5136": 436.9299,
        "5149": 313.38,
        "5150": 202.51,
        "5796": 235.58,
        "6999": 414.34,
        "7003": 101.43,
        "7203": 53.08,
        "7303": 332.96,
        "5117": 36.91,
        "5138": 9.23,
        "5143": 38.43999,
        "5147": 73.22,
        "5148": 15.79,
        "5151": 6.96,
        "5152": 1.84,
        "5155": 4.78,
        "5156": 26.89,
        "5160": 9.69,
        "5162": 11.57,
        "5795": 86.9,
        "5797": 31,
        "7000": 10.4,
        "7001": 93.77,
        "7002": 4.42,
        "7004": 4.12,
        "7037": 20.46,
        "7087": 9.64,
        "7158": 31.76,
        "7159": 6.82,
        "7164": 12.85,
        "7181": 24.91,
        "7201": 43.95,
        "7232": 246.16,
        "7284": 154.34,
        "7303": 332.96
    }

    deltaSs = [os.path.join(dels_savedir, f) for f in os.listdir(dels_savedir) if f.endswith('.csv')]
    for deltaS in deltaSs:
        deltaSname = os.path.split(deltaS)[-1]
        deltaSname_withoutext = os.path.splitext(deltaSname)[0]
        if not deltaSname_withoutext.isdigit():
            grandid = aec_mapping[deltaSname_withoutext]   # If it is not grand id, copy grand id from the aecmapping dict
        else:
            grandid = deltaSname_withoutext
        
        inflowpath = os.path.join(inflow_dir, f"{names_to_ids[grandid]}.csv")
        epath = os.path.join(evap_dir, deltaSname)
        a = areas[grandid]

        savepath = os.path.join(outflow_savedir, deltaSname)
        log.debug(f"Calculating Outflow for {deltaSname_withoutext} saving at: {savepath}")
        calc_outflow(inflowpath, deltaS, epath, a, savepath)

def main():
    pass


if __name__ == '__main__':
    main()