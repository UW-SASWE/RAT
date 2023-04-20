import xarray as xr
import datetime
import os
from logging import getLogger
import numpy as np
import rasterio as rio
import numpy as np
import xarray as xr
import os
import pandas as pd

from rat.utils.logging import LOG_NAME, NOTIFICATION

log = getLogger(LOG_NAME)

class CombinedNC:
    def __init__(self, start, end, datadir, basingridpath, outputdir, use_previous):
        """
        Parameters:
            start: Start date in YYYY-MM-DD format
            :
            :
            datadir: Directory path of ascii format files
        """
        log.debug("Started combining forcing data for MetSim processing")
        self._start = start
        self._end = end
        self._total_days = (self._end - self._start).days
        self._datadir = datadir
        self._outputpath = outputdir
        self._use_previous = use_previous

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
        for day, date in enumerate(self.dates):
            fileDate = date
            reqDate = fileDate.strftime("%Y-%m-%d")
            log.debug("Combining data: %s", reqDate)
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
            # pbar.update(1)

    def _impute_basin_missing_data(self, combined_data):
        combine_nomiss_data = combined_data.where(combined_data['extent']==1,-9999)
        try:
            combine_nomiss_data = combine_nomiss_data.interpolate_na(dim="time", method="linear", fill_value="extrapolate")
        except:
            try:
                combine_nomiss_data = combine_nomiss_data.interpolate_na(dim="lon", method="linear", fill_value="extrapolate")
            except:
                print("No inter or extra polation can be done.")
        combine_nomiss_data = combine_nomiss_data.where(combine_nomiss_data!=-9999,combined_data)
        return combine_nomiss_data

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

        if self._use_previous:
            if os.path.isfile(self._outputpath):
                log.debug(f"Found existing file at {self._outputpath} -- Updating in-place")
                # Assuming the existing file structure is same as the one generated now. Basically
                #   assuming that the previous file was also created by MetSimRunner
                existing = xr.open_dataset(self._outputpath).load()
                existing.close()
                last_existing_time = existing.time[-1]
                log.debug("Existing data: %s", last_existing_time)
                existing_to_append = existing.sel(time=slice(ds.time[0] - np.timedelta64(120,'D') , last_existing_time))
                ds = ds.sel(time=slice(last_existing_time + np.timedelta64(1,'D') , ds.time[-1]))
                # ds = ds.isel(time=slice(1, None))
                write_ds = xr.merge([existing_to_append, ds])
                ds = self._impute_basin_missing_data(write_ds)
            else:
                raise Exception('Previous combined dataset not found. Please run RAT without state files first.')
        else:
            log.debug(f"Creating new file at {self._outputpath}")
            ds = self._impute_basin_missing_data(ds)
        ds.to_netcdf(self._outputpath)
        # log.debug(f"Saving {len(paths)} files at {self._outputdir}")
        # xr.save_mfdataset(datasets, paths)

def generate_state_and_inputs(forcings_startdate, forcings_enddate, combined_datapath, out_dir):
    # Generate state. Assuming `nc_fmt_data` contains all the data, presumably containing enough data
    # to create state file (upto 90 days prior data from forcings_startdate)
    combined_data = xr.open_dataset(combined_datapath)

    state_startdate = forcings_startdate - datetime.timedelta(days=90)
    state_enddate = forcings_startdate - datetime.timedelta(days=1)

    state_ds = combined_data.sel(time=slice(state_startdate, state_enddate))
    state_outpath = os.path.join(out_dir, "state.nc")
    log.debug(f"Saving state at: {state_outpath}")
    state_ds.to_netcdf(state_outpath)

    # Generate the metsim input
    forcings_ds = combined_data.sel(time=slice(forcings_startdate, forcings_enddate))
    forcings_outpath = os.path.join(out_dir, "metsim_input.nc")
    log.debug(f"Saving forcings: {forcings_outpath}")
    forcings_ds.to_netcdf(forcings_outpath)

    return state_outpath, forcings_outpath