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


# def sarea_postprocessor(dfpath, savepath):
    # df = pd.read_csv(dfpath, parse_dates=['date'])

    # # Filter out drastic changes
    # df['std'] = df['area'].rolling(5).std()
    # df['mean'] = df['area'].rolling(5).mean()
    # df = df[(df['area'] <= df['mean']+1.68*df['std']) & (df['area'] >= df['mean']-1.68*df['std'])]

    # log.debug(f"Filtering out spikes from {dfpath}; Saving at -> {savepath}")
    # df.to_csv(savepath, index=False)


def calc_dels(aecpath, sareapath, savepath):
    aec = pd.read_csv(aecpath)
    df = pd.read_csv(sareapath, parse_dates=['date'])

    df = df.drop_duplicates('date')

    get_elev = lambda area: np.interp(area, aec['CumArea'], aec['Elevation'])

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
    sarea = pd.read_csv(sarea_path, parse_dates=['date']).rename({'date': 'time'}, axis=1)[['time', 'area']]
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

    data['area'] = sarea_interpolated['area']
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
    # read file defining mapped resrvoirs
    reservoirs_fn = os.path.join(project_dir, 'backend/data/ancillary/RAT-Reservoirs.geojson')
    reservoirs = gpd.read_file(reservoirs_fn)
    ids = reservoirs['RAT_ID'].tolist()

    # define mappings according to RAT_ID
    aec_names = {
        1: "Sre_Pok_4",
        2: "Phumi_Svay_Chrum",
        3: "Battambang_1",
        4: "5117",
        5: "5136",
        6: "5138",
        7: "5143",
        8: "5147",
        9: "5148",
        10: "5149",
        11: "5150",
        12: "5151",
        13: "5152",
        14: "5155",
        15: "5156",
        16: "5160",
        17: "5162",
        18: "5795",
        19: "5796",
        20: "5797",
        21: "6999",
        22: "7000",
        23: "7001",
        24: "7002",
        25: "7003",
        26: "7004",
        27: "7037",
        28: "7159",
        29: "7164",
        30: "7181",
        31: "7201",
        32: "7203",
        33: "7232",
        34: "7284",
        35: "7303",
        36: "Yali",
        37: "Nam_Ton"
    }
    sarea_names = {
        1: "Sre_Pok_4",
        2: "Phumi_Svay_Chrum",
        3: "Battambang_1",
        4: "5117",
        5: "Nam_Ngum_1",
        6: "5138",
        7: "5143",
        8: "5147",
        9: "5148",
        10: "Ubol_Ratana",
        11: "Lam_Pao",
        12: "5151",
        13: "5152",
        14: "5155",
        15: "5156",
        16: "5160",
        17: "5162",
        18: "5795",
        19: "Sirindhorn",
        20: "5797",
        21: "Nam_Theun_2",
        22: "7000",
        23: "7001",
        24: "7002",
        25: "Xe_Kaman_1",
        26: "7004",
        27: "7037",
        28: "7159",
        29: "7164",
        30: "7181",
        31: "7201",
        32: "Sesan_4",
        33: "7232",
        34: "7284",
        35: "Lower_Sesan_2",
        36: "Yali",
        37: "Nam_Ton"
    }
    dels_names = {
        1: "Sre_Pok_4",
        2: "Phumi_Svay_Chrum",
        3: "Battambang_1",
        4: "5117",
        5: "Nam_Ngum_1",
        6: "5138",
        7: "5143",
        8: "5147",
        9: "5148",
        10: "Ubol_Ratana",
        11: "Lam_Pao",
        12: "5151",
        13: "5152",
        14: "5155",
        15: "5156",
        16: "5160",
        17: "5162",
        18: "5795",
        19: "Sirindhorn",
        20: "5797",
        21: "Nam_Theun_2",
        22: "7000",
        23: "7001",
        24: "7002",
        25: "Xe_Kaman_1",
        26: "7004",
        27: "7037",
        28: "7159",
        29: "7164",
        30: "7181",
        31: "7201",
        32: "Sesan_4",
        33: "7232",
        34: "7284",
        35: "Lower_Sesan_2",
        36: "Yali",
        37: "Nam_Ton"
    }
    res_shp_names = {
        1: "Sre_Pok_4",
        2: "Phumi_Svay_Chrum",
        3: "Battambang_1",
        4: "5117",
        5: "Nam_Ngum_1",
        6: "5138",
        7: "5143",
        8: "5147",
        9: "5148",
        10: "Ubol_Ratana",
        11: "Lam_Pao",
        12: "5151",
        13: "5152",
        14: "5155",
        15: "5156",
        16: "5160",
        17: "5162",
        18: "5795",
        19: "Siridhorn",
        20: "5797",
        21: "Nam_Theun_2",
        22: "7000",
        23: "7001",
        24: "7002",
        25: "Xe_Kaman_1",
        26: "7004",
        27: "7037",
        28: "7159",
        29: "7164",
        30: "7181",
        31: "7201",
        32: "Sesan_4",
        33: "7232",
        34: "7284",
        35: "Lower_Sesan_2",
        36: "Yali",
        37: "Nam_Ton"
    }
    outflow_names = {
        1: "Sre_Pok_4",
        2: "Phumi_Svay_Chrum",
        3: "Battambang_1",
        4: "5117",
        5: "Nam_Ngum_1",
        6: "5138",
        7: "5143",
        8: "5147",
        9: "5148",
        10: "Ubol_Ratana",
        11: "Lam_Pao",
        12: "5151",
        13: "5152",
        14: "5155",
        15: "5156",
        16: "5160",
        17: "5162",
        18: "5795",
        19: "Sirindhorn",
        20: "5797",
        21: "Nam_Theun_2",
        22: "7000",
        23: "7001",
        24: "7002",
        25: "Xe_Kaman_1",
        26: "7004",
        27: "7037",
        28: "7159",
        29: "7164",
        30: "7181",
        31: "7201",
        32: "Sesan_4",
        33: "7232",
        34: "7284",
        35: "Lower_Sesan_2",
        36: "Yali",
        37: "Nam_Ton"
    }
    areas = {                   # Areas in km2, from GRAND if available, or calcualted
        1: 3.4,
        2: 0.7,
        3: 15,
        4: 36.91,
        5: 436.93,
        6: 9.23,
        7: 38.44,
        8: 73.22,
        9: 15.79,
        10: 313.38,
        11: 202.51,
        12: 6.96,
        13: 1.84,
        14: 4.78,
        15: 26.89,
        16: 9.69,
        17: 11.57,
        18: 86.9,
        19: 235.58,
        20: 31,
        21: 414.34,
        22: 10.4,
        23: 93.77,
        24: 4.42,
        25: 101.43,
        26: 4.12,
        27: 20.46,
        28: 6.82,
        29: 12.85,
        30: 24.91,
        31: 43.95,
        32: 53.08,
        33: 246.16,
        34: 154.34,
        35: 332.96,
        36: 45,
        37: 7.5
    }
    inflow_names = {
        1: "Sre_P",
        2: "Phumi",
        3: "Batta",
        4: "5117 ",
        5: "Nam_N",
        6: "5138 ",
        7: "5143 ",
        8: "5147 ",
        9: "5148 ",
        10: "Ubol_",
        11: "Lam_P",
        12: "5151 ",
        13: "5152 ",
        14: "5155 ",
        15: "5156 ",
        16: "5160 ",
        17: "5162 ",
        18: "5795 ",
        19: "Sirid",
        20: "5797 ",
        21: "Nam_T",
        22: "7000 ",
        23: "7001 ",
        24: "7002 ",
        25: "Xe_Ka",
        26: "7004 ",
        27: "7037 ",
        28: "7159 ",
        29: "7164 ",
        30: "7181 ",
        31: "7201 ",
        32: "Sesan",
        33: "7232 ",
        34: "7284 ",
        35: "Lower",
        36: "Yali ",
        37: "Nam_T"
    }

    # SArea
    sarea_raw_dir = os.path.join(project_dir, "backend/data/sarea_tmsos")

    # DelS
    log.debug("Calculating ∆S")
    dels_savedir = os.path.join(project_dir, "backend/data/dels")
    aec_dir = os.path.join(project_dir, "backend/data/aec")

    for RAT_ID in ids:
        sarea_path = os.path.join(sarea_raw_dir, sarea_names[RAT_ID] + ".csv")
        savepath = os.path.join(dels_savedir, dels_names[RAT_ID] + ".csv")
        aecpath = os.path.join(aec_dir, aec_names[RAT_ID] + ".txt")

        log.debug(f"Calculating ∆S for {sarea_names[RAT_ID]}, saving at: {savepath}")
        calc_dels(aecpath, sarea_path, savepath)

    # Evaporation
    log.debug("Retrieving Evaporation")
    evap_datadir = os.path.join(project_dir, "backend/data/E")
    res_dir = os.path.join(project_dir, "backend/data/ancillary/reservoirs")
    vic_results_path = os.path.join(project_dir, "backend/data/vic_results/nc_fluxes.2001-04-01.nc")
    sarea_dir = os.path.join(project_dir, "backend/data/sarea_tmsos")
    forcings_path = os.path.join(project_dir, "backend/data/forcings/*.nc")

    for RAT_ID in ids:
        respath = os.path.join(res_dir, res_shp_names[RAT_ID] + ".shp")
        _, resname = os.path.split(respath)
        sarea_path = os.path.join(sarea_dir, resname.replace(".shp", ".csv"))
        e_path = os.path.join(evap_datadir, resname.replace(".shp", '.csv'))
        
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

    for RAT_ID in ids:
        respath = os.path.join(res_dir, res_shp_names[RAT_ID] + ".csv")
        deltaS = os.path.join(dels_savedir, dels_names[RAT_ID] + ".csv")
        _, resname = os.path.split(deltaS)
        inflowpath = os.path.join(inflow_dir, inflow_names[RAT_ID] + ".csv")
        epath = os.path.join(evap_datadir, resname)
        a = areas[RAT_ID]

        savepath = os.path.join(outflow_savedir, outflow_names[RAT_ID] + ".csv")
        log.debug(f"Calculating Outflow for {resname} saving at: {savepath}")
        calc_outflow(inflowpath, deltaS, epath, a, savepath)

def main():
    pass


if __name__ == '__main__':
    main()