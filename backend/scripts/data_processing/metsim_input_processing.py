import xarray as xr
import datetime
import os

import os
from tqdm import tqdm
import configparser
import numpy as np
import rasterio as rio
import numpy as np
import xarray as xr
import os
from tqdm import tqdm
import itertools
import pandas as pd


class ForcingsNCfmt:
    def __init__(self, start, end, datadir, basingridpath, outputdir):
        """
        Parameters:
            start: Start date in YYYY-MM-DD format
            :
            :
            datadir: Directory path of ascii format files
        """
        self._start = start
        self._end = end
        self._total_days = (self._end - self._start).days
        self._datadir = datadir
        self._outputdir = outputdir

        self._rast = rio.open(basingridpath)
        self._ar = self._rast.read(1, masked=True)
        self._gridvalue = self._ar.flatten()

        self._longitudes1d, self._latitudes1d = self._get_lat_long_1d()
        self._latitudes, self._longitudes = self._get_lat_long_meshgrid()

        self.precips = np.zeros((self._total_days+1, self._rast.height, self._rast.width))
        self.tmaxes = np.zeros((self._total_days+1, self._rast.height, self._rast.width))
        self.tmins = np.zeros((self._total_days+1, self._rast.height, self._rast.width))
        self.winds = np.zeros((self._total_days+1, self._rast.height, self._rast.width))
        self.dates = pd.date_range(start, end)

        self._read()
        self._write()


    def _get_lat_long_1d(self):
        x_res, y_res = self._rast.res
        r_lon_0, r_lat_0 = self._rast.xy(self._rast.height-1, 0)
        longitudes1d = (np.arange(0, self._rast.shape[1])*x_res + r_lon_0).round(5)
        latitudes1d = (np.arange(0, self._rast.shape[0])*y_res + r_lat_0).round(5)

        return (longitudes1d, latitudes1d)
    
    def _get_lat_long_meshgrid(self):
        xy = np.meshgrid(self._latitudes1d, self._longitudes1d, indexing='ij')
        longitudes, latitudes = xy[1].flatten(), xy[0].flatten()
        # Need to flip latutudes on horizontal axis
        latitudes = np.flip(latitudes, 0)
        
        return latitudes, longitudes

    def _read(self):
        with tqdm(total=len(self.dates)) as pbar:
            for day, date in enumerate(self.dates):
                fileDate = date
                reqDate = fileDate.strftime("%Y-%m-%d")
                # pbar.set_description(reqDate)
                
                precipfilepath = os.path.join(self._datadir, f'precipitation/{reqDate}_IMERG.asc')
                precipitation = rio.open(precipfilepath).read(1, masked=True).astype(np.float32).filled(np.nan)#.flatten()[self.gridvalue==0.0]
                
                #Reading Maximum Temperature ASCII file contents
                tmaxfilepath = os.path.join(self._datadir, f'tmax/{reqDate}_TMAX.asc')
                tmax = rio.open(tmaxfilepath).read(1, masked=True).astype(np.float32).filled(np.nan)#.flatten()[self.gridvalue==0.0]

                #Reading Minimum Temperature ASCII file contents
                tminfilepath = os.path.join(self._datadir, f'tmin/{reqDate}_TMIN.asc')
                tmin = rio.open(tminfilepath).read(1, masked=True).astype(np.float32).filled(np.nan)#.flatten()[self.gridvalue==0.0]

                #Reading Average Wind Speed ASCII file contents
                uwndfilepath = os.path.join(self._datadir, f'uwnd/{reqDate}_UWND.asc')
                uwnd = rio.open(uwndfilepath).read(1, masked=True).astype(np.float32).filled(np.nan)
                
                # #Reading Average Wind Speed ASCII file contents
                vwndfilepath = os.path.join(self._datadir, f'vwnd/{reqDate}_VWND.asc')
                vwnd = rio.open(vwndfilepath).read(1, masked=True).astype(np.float32).filled(np.nan)
                wind = (0.75*np.sqrt(uwnd**2 + vwnd**2))#.flatten()[self.gridvalue==0.0]
                
                # self.dates.append(fileDate)
                self.precips[day, :, :] = precipitation
                self.tmaxes[day, :, :] = tmax
                self.tmins[day, :, :] = tmin
                self.winds[day, :, :] = wind
                pbar.update(1)

    def _write(self):
        precip_da = xr.DataArray(
            data = self.precips,
            coords=[self.dates, np.flip(self._latitudes1d), self._longitudes1d],
            dims=['time', 'lat', 'lon']
        )

        tmax_da = xr.DataArray(
            data = self.tmaxes,
            coords=[self.dates, np.flip(self._latitudes1d), self._longitudes1d],
            dims=['time', 'lat', 'lon']
        )

        tmin_da = xr.DataArray(
            data = self.tmins,
            coords=[self.dates, np.flip(self._latitudes1d), self._longitudes1d],
            dims=['time', 'lat', 'lon']
        )

        wind_da = xr.DataArray(
            data = self.winds,
            coords=[self.dates, np.flip(self._latitudes1d), self._longitudes1d],
            dims=['time', 'lat', 'lon']
        )

        extent_da = xr.DataArray(
            data = self._ar,
            coords=[np.flip(self._latitudes1d), self._longitudes1d],
            dims=['lat', 'lon']
        )

        ds = xr.Dataset(
            data_vars=dict(
                precip = precip_da,
                tmax = tmax_da,
                tmin = tmin_da,
                wind = wind_da,
                extent = extent_da
            )
        )

        # # Groupby and save as multiple datasets
        # years, datasets = zip(*ds.groupby("date.year"))
        # paths = [os.path.join(self._outputdir, f"{year}.nc") for year in years]
        ds.to_netcdf(self._outputdir)
        # print(f"Saving {len(paths)} files at {self._outputdir}")
        # xr.save_mfdataset(datasets, paths)


def date_range(start, end):
    # start = datetime.datetime.strptime(start,strfmt)
    # end = datetime.datetime.strptime(end,strfmt)

    start_year = start.year
    end_year = end.year
    
    if not start_year == end_year:
        res = [(start, datetime.datetime(start_year, 12, 31))]

        for year in range(start_year+1, end_year):
            res.append((datetime.datetime(year, 1, 1), datetime.datetime(year, 12, 31)))
        
        res.append((datetime.datetime(end_year, 1, 1), end))
    else:
        res = [(start, end)]

    return res


def main():
    project_base = "/houston2/pritam/rat_mekong_v3/backend"

    basingridfile = os.path.join(project_base, "data", "ancillary", "MASK.tif")
    datadir = os.path.join(project_base, "data", "processed")
    nc_outputdir = os.path.join(project_base, "data", "nc")
    metsim_inputdir = os.path.join(project_base, "data", "metsim_inputs")
    state_dir = os.path.join(project_base, "data", "metsim_inputs", "state")
    # vicfmt_outputdir = os.path.join(project_base, "data", "forcings")

    startdate = datetime.datetime.strptime("2001-01-01", "%Y-%m-%d")
    enddate = datetime.datetime.strptime("2001-12-31", "%Y-%m-%d")

    dateranges = date_range(startdate, enddate)
    print(dateranges)
    # for start, end in tqdm(dateranges):
    #     ForcingsNCfmt(start, end, datadir, basingridfile, os.path.join(metsim_inputdir, f"{start.year}.nc"))

    ## Create State file
    # Take first year's forcing file, and section off 90 days of data as state.nc
    first_year_dspath = os.path.join(metsim_inputdir, f"{startdate.year}.nc")
    print(f"Modifying {first_year_dspath}")

    first_year_ds = xr.open_dataset(first_year_dspath).load()   # Load to read off of disk. Allows for writing back to same name https://github.com/pydata/xarray/issues/2029#issuecomment-377572708
    print(first_year_ds)
    state_enddate = startdate + datetime.timedelta(days=89)
    forcings_startdate = startdate + datetime.timedelta(days=90)
    
    state_ds = first_year_ds.sel(time=slice(startdate, state_enddate))
    print(state_ds)
    stateoutpath = os.path.join(state_dir, "state.nc")
    print(f"Saving state at: {stateoutpath}")
    state_ds.to_netcdf(stateoutpath)

    # Save rest of the file as forcing file
    forcings_enddate = first_year_ds.time[-1]
    forcing_ds = first_year_ds.sel(time=slice(forcings_startdate, forcings_enddate))
    first_year_ds.close()

    outpath = os.path.join(metsim_inputdir, f"{forcings_startdate.strftime('%Y')}.nc")
    print(f"Saving forcings: {outpath}")

    forcing_ds.to_netcdf(os.path.join(metsim_inputdir, f"{forcings_startdate.strftime('%Y')}.nc"))


if __name__ == '__main__':
    main()