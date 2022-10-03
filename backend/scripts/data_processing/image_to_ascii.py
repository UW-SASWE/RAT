import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import rasterio as rio
import xarray as xr
from tqdm import tqdm
import csv
import math
import os
import pandas as pd
import time


fluxes = xr.open_dataset("/houston2/pritam/rat_mekong_v3/backend/data/vic_results/nc_fluxes.2001-04-01.nc").load()

fluxes_subset = fluxes[['OUT_PREC', 'OUT_EVAP', 'OUT_RUNOFF', 'OUT_BASEFLOW', 'OUT_WDEW', 'OUT_SOIL_LIQ', 'OUT_SOIL_MOIST']]
fluxes_subset

# fluxes_subset['lat'] = fluxes_subset.lat.values.round(2)
# fluxes_subset['lon'] = fluxes_subset.lon.values.round(2)

mask = fluxes_subset.OUT_PREC.isel(time=0)
mask.plot()


d = "/houston2/pritam/rat_mekong_v3/backend/data/rout_input"

YEAR = np.array([int(pd.to_datetime(t).strftime("%Y")) for t in fluxes_subset.time.values])
MONTH = np.array([int(pd.to_datetime(t).strftime("%m")) for t in fluxes_subset.time.values])
DAY = np.array([int(pd.to_datetime(t).strftime("%d")) for t in fluxes_subset.time.values])


nonnans = fluxes_subset.OUT_PREC.isel(time=0).values.flatten()
nonnans = nonnans[~np.isnan(nonnans)]
total = len(nonnans)
i = 1

# VIC Routing doesn't round, it truncates the lat long values. Important for file names.
lats_vicfmt = np.array(list(map(lambda x: math.floor(x * 10 ** 2) / 10 ** 2, fluxes_subset.lat.values)))
lons_vicfmt = np.array(list(map(lambda x: math.floor(x * 10 ** 2) / 10 ** 2, fluxes_subset.lon.values)))

with tqdm(total=total) as pbar:
    for lat in range(len(fluxes_subset.lat)):
        for lon in range(len(fluxes_subset.lon)):
            if not math.isnan(fluxes_subset.OUT_PREC.isel(time=0, lat=lat, lon=lon).values):
                s = time.time()
                fname = os.path.join(d, f"fluxes_{lats_vicfmt[lat]:.2f}_{lons_vicfmt[lon]:.2f}")
                pbar.set_description(f"{fname}")

                da = fluxes_subset.isel(lat=lat, lon=lon, nlayer=0).to_dataframe().reset_index()
                # da['time'] = da['time'].apply(lambda x: x.strftime("%Y\t%m\t%d"))

                da.to_csv(fname, sep=' ', header=False, index=False, float_format="%.5f", quotechar="", quoting=csv.QUOTE_NONE, date_format="%Y %m %d", escapechar=" ")
                # print(f"{lat}, {lon} - in {time.time()-s:.3f} seconds. {total-i} remaining")
                # i += 1
                # csv.to_csv(da, fname, nogil=True, sep='\t', header=False, index=False, date_format="%Y\t%m\t%d", float_format="%.5f", quoting=False)

                # # print(TIME.apply(lambda x: x.strftime("%Y\t%m\t%d")))
                # OUT_PREC = fluxes_subset.OUT_PREC.isel(lat=lat, lon=lon).values
                # OUT_EVAP = fluxes_subset.OUT_EVAP.isel(lat=lat, lon=lon).values
                # OUT_RUNOFF = fluxes_subset.OUT_RUNOFF.isel(lat=lat, lon=lon).values
                # OUT_BASEFLOW = fluxes_subset.OUT_BASEFLOW.isel(lat=lat, lon=lon).values
                # OUT_WDEW = fluxes_subset.OUT_WDEW.isel(lat=lat, lon=lon).values
                # OUT_SOIL_LIQ = fluxes_subset.OUT_SOIL_LIQ.isel(lat=lat, lon=lon).values
                # OUT_SOIL_MOIST = fluxes_subset.OUT_SOIL_MOIST.isel(lat=lat, lon=lon).values
                # 
                # data = np.array([YEAR, MONTH, DAY, OUT_PREC, OUT_EVAP, OUT_RUNOFF, OUT_BASEFLOW, OUT_WDEW, OUT_SOIL_LIQ, OUT_SOIL_MOIST])
                # print(data)
                # with open(fname, "w") as f:
                #     np.savetxt(f, data )
                # input()
                pbar.update(1)
