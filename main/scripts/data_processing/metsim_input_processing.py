import xarray as xr
import datetime
import os
from logging import getLogger
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

from utils.logging import LOG_NAME, NOTIFICATION

log = getLogger(LOG_NAME)


class ForcingsNCfmt:
    def __init__(self, start, end, datadir, basingridpath, outputdir):
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
        # with tqdm(total=len(self.dates)) as pbar:
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

        if os.path.isfile(self._outputpath):
            log.debug(f"Found existing file at {self._outputpath} -- Updating in-place")
            # Assuming the existing file structure is same as the one generated now. Basically
            #   assuming that the previous file was also created by MetSimRunner
            existing = xr.open_dataset(self._outputpath).load()
            existing.close()
            # xr.merge([existing, ds]).to_netcdf(self._outputpath)
            # xr.merge([existing, ds], compat='override', join='outer').to_netcdf(self._outputpath)
            # xr.concat([existing, ds], dim='time').to_netcdf(self._outputpath)
            # existing.combine_first(ds).to_netcdf(self._outputpath)
            last_existing_time = existing.time[-1]
            log.debug("Existing data: %s", last_existing_time)
            ds = ds.sel(time=slice(last_existing_time, ds.time[-1]))
            ds = ds.isel(time=slice(1, None))
            xr.merge([existing, ds]).to_netcdf(self._outputpath)
        else:
            log.debug(f"Creating new file at {self._outputpath}")
            ds.to_netcdf(self._outputpath)
        # log.debug(f"Saving {len(paths)} files at {self._outputdir}")
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


# def metsim_input_processing(project_base, start, end):
#     basingridfile = os.path.join(project_base, "data", "ancillary", "MASK.tif")
#     datadir = os.path.join(project_base, "data", "processed")
#     nc_outputdir = os.path.join(project_base, "data", "nc")
#     metsim_inputdir = os.path.join(project_base, "data", "metsim_inputs")
#     # state_dir = os.path.join(project_base, "data", "metsim_inputs", "state")

#     startdate = start
#     enddate = end

#     startdate_str = start.strftime("%Y-%m-%d")
#     enddate_str = end.strftime("%Y-%m-%d")

#     log.log(NOTIFICATION, "Starting metsim input processing from %s to %s", startdate_str, enddate_str)

#     # ForcingsNCfmt(startdate, enddate, datadir, basingridfile, os.path.join(nc_outputdir, "forcings_all.nc"))

#     # nc_fmt_data = xr.open_dataset(os.path.join(nc_outputdir, "forcings_all.nc")).load()
#     # state_outpath, forcings_outpath = generate_state_and_inputs(startdate, enddate, nc_fmt_data, metsim_inputdir)

#     # ## Create State file
#     # # Take first year's forcing file, and section off 90 days of data as state.nc
#     # forcings_dspath = os.path.join(metsim_inputdir, "forcings_all.nc")
#     # log.debug('Creating statefile from: %s', forcings_dspath)

#     # first_year_ds = xr.open_dataset(forcings_dspath).load()   # Load to read off of disk. Allows for writing back to same name https://github.com/pydata/xarray/issues/2029#issuecomment-377572708
#     # state_startdate = startdate - datetime.timedelta(days=90)
#     # state_enddate = startdate - datetime.timedelta(days=1)
#     # forcings_startdate = startdate
#     # forcings_enddate = pd.to_datetime(first_year_ds.time[-1].values).to_pydatetime()

#     # log.debug("Creating state file using data from %s to %s", startdate, state_enddate.strftime('%Y-%m-%d'))
#     # log.debug("Creating MS input file using data from %s to %s", forcings_startdate.strftime('%Y-%m-%d'), forcings_enddate.strftime('%Y-%m-%d'))
    
#     # state_ds = first_year_ds.sel(time=slice(startdate, state_enddate))
#     # stateoutpath = os.path.join(metsim_inputdir, "state.nc")
#     # log.debug(f"Saving state at: {stateoutpath}")
#     # state_ds.to_netcdf(stateoutpath)

#     # # Save rest of the file as forcing file
#     # forcing_ds = first_year_ds.sel(time=slice(forcings_startdate, forcings_enddate))
#     # first_year_ds.close()

#     # outpath = os.path.join(metsim_inputdir, f"{forcings_startdate.strftime('%Y')}.nc")
#     # log.debug(f"Saving forcings: {outpath}")

#     # ms_input_path = os.path.join(metsim_inputdir, f"{forcings_startdate.strftime('%Y')}.nc")
#     # forcing_ds.to_netcdf(ms_input_path)

#     return (state_outpath, forcings_outpath)


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
    log.debug(dateranges)
    # for start, end in tqdm(dateranges):
    #     ForcingsNCfmt(start, end, datadir, basingridfile, os.path.join(metsim_inputdir, f"{start.year}.nc"))

    ## Create State file
    # Take first year's forcing file, and section off 90 days of data as state.nc
    first_year_dspath = os.path.join(metsim_inputdir, f"{startdate.year}.nc")
    log.debug(f"Modifying {first_year_dspath}")

    first_year_ds = xr.open_dataset(first_year_dspath).load()   # Load to read off of disk. Allows for writing back to same name https://github.com/pydata/xarray/issues/2029#issuecomment-377572708
    log.debug(first_year_ds)
    state_enddate = startdate + datetime.timedelta(days=89)
    forcings_startdate = startdate + datetime.timedelta(days=90)
    
    state_ds = first_year_ds.sel(time=slice(startdate, state_enddate))
    log.debug(state_ds)
    stateoutpath = os.path.join(state_dir, "state.nc")
    log.debug(f"Saving state at: {stateoutpath}")
    state_ds.to_netcdf(stateoutpath)

    # Save rest of the file as forcing file
    forcings_enddate = first_year_ds.time[-1]
    forcing_ds = first_year_ds.sel(time=slice(forcings_startdate, forcings_enddate))
    first_year_ds.close()

    outpath = os.path.join(metsim_inputdir, f"{forcings_startdate.strftime('%Y')}.nc")
    log.debug(f"Saving forcings: {outpath}")

    forcing_ds.to_netcdf(os.path.join(metsim_inputdir, f"{forcings_startdate.strftime('%Y')}.nc"))


if __name__ == '__main__':
    main()