import xarray as xr
import datetime
import os
from logging import getLogger
import numpy as np
import rasterio as rio
import rioxarray as rxr
import numpy as np
import os
import pandas as pd
import shutil
import dask
from pathlib import Path
from dateutil.relativedelta import relativedelta

from rat.utils.logging import LOG_NAME, LOG_LEVEL, NOTIFICATION

log = getLogger(LOG_NAME)
log.setLevel(LOG_LEVEL)

class CombinedNC:
    def __init__(self, start, end, datadir, basingridpath, outputdir, use_previous, forecast_dir=None, forecast_basedate=None, climatological_data=None, z_lim=3):
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

        if forecast_dir:
            self.read_forecast(forecast_dir, forecast_basedate)
            self._write()
        else:
            self._read_and_write_in_chunks()
            # self._read()
            # self._write()
        
        # If climatological data is passed, then run the climatological data correction
        if climatological_data:
            self._climatological_data = climatological_data
            self._z_lim = z_lim
            log.debug(f"Running climatological corection of satellite precipitation")
            self.satellite_precip_correction()

    def satellite_precip_correction(self):
        # UPDATE (2023-06-02): Seems like this function isn't able to filter out some very high precipitation values (Jan 6, Jan 7 2022).
        # from https://github.com/pritamd47/2022_05_04-extreme_precip/blob/1baa1319513abbecfb89f7a7269a1214b50cdca0/notebooks/Extreme-Precip.ipynb
        daily_precip = xr.open_dataset(self._climatological_data)
        log_precip = daily_precip.copy()
        # Take the natural log to convert to normal from log-normal distribution
        log_precip['tp'] = np.log(log_precip['tp'], where=log_precip['tp']>0)

        weekly_log_precip_mean = log_precip.groupby(log_precip['time'].dt.isocalendar().week).mean()
        weekly_log_precip_std = log_precip.groupby(log_precip['time'].dt.isocalendar().week).std()

        forcings = xr.open_dataset(self._outputpath)
        forcings_precip = forcings[['precip']]

        log_forcings_precip = forcings_precip.copy()
        log_forcings_precip['precip'] = np.log(forcings_precip['precip'], where=forcings_precip['precip']>0)

        # Align both the datasets, so that the final z-scores, means and std devs can be calculated and compared easily.
        weekly_log_precip_mean_sampled = weekly_log_precip_mean.sel(longitude=log_forcings_precip.lon, latitude=log_forcings_precip.lat, method='nearest')
        weekly_log_precip_std_sampled = weekly_log_precip_std.sel(longitude=log_forcings_precip.lon, latitude=log_forcings_precip.lat, method='nearest')

        log_forcings_precip['weekly_mean_precip'] = weekly_log_precip_mean_sampled['tp']
        log_forcings_precip['weekly_std_precip'] = weekly_log_precip_std_sampled['tp']

        # build daily clim precip
        times = []
        clim_precip_mean = []
        clim_precip_std = []

        for t in log_forcings_precip.time:
            time = pd.to_datetime(t.values)
            times.append(time)
            # clim_precip_std.append(log_forcings_precip['weekly_std_precip'].sel(week=time.weekofyear))
            clim_precip_mean.append(log_forcings_precip['weekly_mean_precip'].sel(week=time.weekofyear).values)
            clim_precip_std.append(log_forcings_precip['weekly_std_precip'].sel(week=time.weekofyear).values)

        mean = xr.DataArray(np.array(clim_precip_mean), coords=({'time': times, 'lon': log_forcings_precip.lon, 'lat': log_forcings_precip.lat}), dims=["time", "lat", "lon"])
        std = xr.DataArray(np.array(clim_precip_std), coords=({'time': times, 'lon': log_forcings_precip.lon, 'lat': log_forcings_precip.lat}), dims=["time", "lat", "lon"])

        log_forcings_precip['climatological_precip_mean'] = mean
        log_forcings_precip['climatological_precip_std'] = std

        # calculate z-scores
        z_scores = (log_forcings_precip['precip'] - log_forcings_precip['climatological_precip_mean'])/log_forcings_precip['climatological_precip_std']

        high_precip = log_forcings_precip['climatological_precip_mean']
        precip_extremes_handled = np.where((z_scores.data>self._z_lim)|(z_scores.data<-self._z_lim), high_precip.data, log_forcings_precip['precip'].data)
        log_forcings_precip['precip'].data = precip_extremes_handled
        forcings['precip'].data = np.exp(log_forcings_precip['precip'].data)  # Convert to precipitation 
        forcings.attrs['precip_filtering'] = f">{self._z_lim}sigma | <-{self._z_lim}sigma"

        forcings.to_netcdf(self._outputpath.replace(".nc", "_precip_handled.nc"))
        forcings.close()
        daily_precip.close()
        shutil.move(self._outputpath.replace(".nc", "_precip_handled.nc"), self._outputpath)

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

    def read_forecast(self, forecast_dir, basedate):
        forecast_dir = Path(forecast_dir)
        basedate = pd.to_datetime(basedate)

        # define data arrays
        self.precips = np.zeros((15, self._rast.height, self._rast.width))
        self.tmaxes = np.zeros((15, self._rast.height, self._rast.width))
        self.tmins = np.zeros((15, self._rast.height, self._rast.width))
        self.winds = np.zeros((15, self._rast.height, self._rast.width))

        self.dates = pd.date_range(basedate + datetime.timedelta(days=1), basedate + datetime.timedelta(days=15))

        for day, date in enumerate(self.dates):
            fileDate = date
            reqDate = fileDate.strftime("%Y-%m-%d")
            # pbar.set_description(reqDate)

            precipfilepath = forecast_dir / f'gefs-chirps/processed' / f'{basedate:%Y%m%d}' / f'{date:%Y%m%d}.asc'
            precipitation = rio.open(precipfilepath).read(1, masked=True).astype(np.float32).filled(np.nan)#.flatten()[self.gridvalue==0.0]

            #Reading Maximum Temperature ASCII file contents
            tmaxfilepath = forecast_dir / f'gfs/processed/{basedate:%Y%m%d}/tmax/{date:%Y%m%d}.asc'
            tmax = rio.open(tmaxfilepath).read(1, masked=True).astype(np.float32).filled(np.nan)#.flatten()[self.gridvalue==0.0]

            #Reading Minimum Temperature ASCII file contents
            tminfilepath = forecast_dir / f'gfs/processed/{basedate:%Y%m%d}/tmin/{date:%Y%m%d}.asc'
            tmin = rio.open(tminfilepath).read(1, masked=True).astype(np.float32).filled(np.nan)#.flatten()[self.gridvalue==0.0]

            #Reading Average Wind Speed ASCII file contents
            uwndfilepath = forecast_dir / f'gfs/processed/{basedate:%Y%m%d}/uwnd/{date:%Y%m%d}.asc'
            uwnd = rio.open(uwndfilepath).read(1, masked=True).astype(np.float32).filled(np.nan)

            # #Reading Average Wind Speed ASCII file contents
            vwndfilepath = forecast_dir / f'gfs/processed/{basedate:%Y%m%d}/vwnd/{date:%Y%m%d}.asc'
            vwnd = rio.open(vwndfilepath).read(1, masked=True).astype(np.float32).filled(np.nan)
            wind = (0.75*np.sqrt(uwnd**2 + vwnd**2))#.flatten()[self.gridvalue==0.0]

            # self.dates.append(fileDate)
            self.precips[day, :, :] = precipitation
            self.tmaxes[day, :, :] = tmax
            self.tmins[day, :, :] = tmin
            self.winds[day, :, :] = wind
            # pbar.update(1)

    def _read(self):
        self.precips = np.zeros((self._total_days+1, self._rast.height, self._rast.width))
        self.tmaxes = np.zeros((self._total_days+1, self._rast.height, self._rast.width))
        self.tmins = np.zeros((self._total_days+1, self._rast.height, self._rast.width))
        self.winds = np.zeros((self._total_days+1, self._rast.height, self._rast.width))
        self.dates = pd.date_range(self._start, self._end)

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

    # Imputes missing data by interpolation in the order of dimensions time, lon, lat.
    def _impute_basin_missing_data(self, combined_data):
        combine_nomiss_data = combined_data
        try:
            #Interpolation using time dimension - if unsuccesful values will still be NaN
            combine_nomiss_data = combine_nomiss_data.interpolate_na(dim="time", method="linear", fill_value="extrapolate", limit = 30)
            #Interpolation using lon dimension - if unsuccesful values will still be NaN
            combine_nomiss_data = combine_nomiss_data.interpolate_na(dim="lon", method="linear", fill_value="extrapolate")
            #Interpolation using lat dimension - if unsuccesful values will still be NaN
            combine_nomiss_data = combine_nomiss_data.interpolate_na(dim="lat", method="linear", fill_value="extrapolate", use_coordinate=False)
        except:
            print("No inter or extra polation is possible.")
        #Restoring original values outside basin extent. This ensures that ocean tiles remain to be NaN/-9999
        combine_nomiss_data = combine_nomiss_data.where(combined_data['extent']==1,combined_data)
        return combine_nomiss_data
    
    def _min_max_temperature_check(self, combined_data):
        # Create a mask where tmax is less than tmin
        mask = combined_data['tmax'] < combined_data['tmin']

        # Use the mask to interchange tmax and tmin values efficiently
        # Apply the swap using xarray's where function to avoid temporary arrays

        tmax_swapped = xr.where(mask, combined_data['tmin'], combined_data['tmax'])
        tmin_swapped = xr.where(mask, combined_data['tmax'], combined_data['tmin'])

        # Assign the swapped arrays back to the original dataset
        combined_data['tmax'] = tmax_swapped
        combined_data['tmin'] = tmin_swapped

        return combined_data

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
                existing = xr.open_dataset(self._outputpath)#.load()
                existing.close()
                last_existing_time = existing.time[-1]
                log.debug("Existing data: %s", last_existing_time)
                existing_to_append = existing.sel(time=slice(ds.time[0] - np.timedelta64(120,'D') , last_existing_time))
                ds = ds.sel(time=slice(last_existing_time + np.timedelta64(1,'D') , ds.time[-1]))
                # ds = ds.isel(time=slice(1, None))
                write_ds = xr.merge([existing_to_append, ds])
                ds = self._impute_basin_missing_data(write_ds)
                ds = self._min_max_temperature_check(ds)
            else:
                raise Exception('Previous combined dataset not found. Please run RAT without state files first.')
        else:
            log.debug(f"Creating new file at {self._outputpath}")
            ds = self._impute_basin_missing_data(ds)
            ds = self._min_max_temperature_check(ds)
        ds.to_netcdf(self._outputpath)
        # log.debug(f"Saving {len(paths)} files at {self._outputdir}")
        # xr.save_mfdataset(datasets, paths)


    def _read_chunk(self,chunk_start,chunk_end):
        dates = pd.date_range(chunk_start, chunk_end)

        ds_list=[]

        for day, date in enumerate(dates):
            fileDate = date
            reqDate = fileDate.strftime("%Y-%m-%d")
            log.debug("Combining data: %s", reqDate)
            # pbar.set_description(reqDate)

            precipfilepath = os.path.join(self._datadir, f'precipitation/{reqDate}_IMERG.asc')
            precipitation = rxr.open_rasterio(precipfilepath,masked=True).sel(band=1, drop=True).astype(np.float32)
            # Replace no-data values with NaN
            precipitation = precipitation.where(precipitation != precipitation.rio.nodata, np.nan)
            # Create additional dimension to concatenate
            precip_da = precipitation.expand_dims(time=[date])

            #Reading Maximum Temperature ASCII file contents
            tmaxfilepath = os.path.join(self._datadir, f'tmax/{reqDate}_TMAX.asc')
            tmax = rxr.open_rasterio(tmaxfilepath,masked=True).sel(band=1, drop=True).astype(np.float32)
            # Replace no-data values with NaN
            tmax = tmax.where(tmax != tmax.rio.nodata, np.nan)
            # Create additional dimension to concatenate
            tmax_da = tmax.expand_dims(time=[date])

            #Reading Minimum Temperature ASCII file contents
            tminfilepath = os.path.join(self._datadir, f'tmin/{reqDate}_TMIN.asc')
            tmin = rxr.open_rasterio(tminfilepath,masked=True).sel(band=1, drop=True).astype(np.float32)
            # Replace no-data values with NaN
            tmin = tmin.where(tmin != tmin.rio.nodata, np.nan)
            # Create additional dimension to concatenate
            tmin_da = tmin.expand_dims(time=[date])

            #Reading Average Wind Speed ASCII file contents
            uwndfilepath = os.path.join(self._datadir, f'uwnd/{reqDate}_UWND.asc')
            uwnd = rxr.open_rasterio(uwndfilepath,masked=True).sel(band=1, drop=True).astype(np.float32)

            # #Reading Average Wind Speed ASCII file contents
            vwndfilepath = os.path.join(self._datadir, f'vwnd/{reqDate}_VWND.asc')
            vwnd = rxr.open_rasterio(vwndfilepath,masked=True).sel(band=1, drop=True).astype(np.float32)
            wind = (0.75*np.sqrt(uwnd**2 + vwnd**2))
            # Replace no-data values with NaN
            wind = wind.where(wind != wind.rio.nodata, np.nan)
            # Create additional dimension to concatenate
            wind_da = wind.expand_dims(time=[date])

            ## Making an xarray dataset with all the above variables
            ds_day = xr.Dataset(
                data_vars={
                    'precip': precip_da,
                    'tmax': tmax_da,
                    'tmin': tmin_da,
                    'wind': wind_da
                }
            )
            ds_list.append(ds_day)

        return ds_list 

    def _write_chunk(self, ds_list, existing_to_append=None, last_existing_time=None, first_loop=False):
        
        ds_chunk = xr.concat(ds_list, dim='time')
        # Rename dimensions & coordinates from 'x' and 'y' to 'lon' and 'lat'
        ds_chunk = ds_chunk.rename({'x': 'lon', 'y': 'lat'})
        # Set latitude and longitude as required by metsim
        ds_chunk.coords['lat'] = np.flip(self._latitudes1d) 
        ds_chunk.coords['lon'] = self._longitudes1d
        # Drop unwanted coordinates or variables
        ds_chunk = ds_chunk.drop('spatial_ref')

        # If existing data is there  and we're not starting from scratch, append data to the existing data 
        if existing_to_append:
            ## Select dates starting from last existing time and write it after merging with existing data
            ds_chunk_sel = ds_chunk.sel(time=slice(last_existing_time + np.timedelta64(1,'D') , ds_chunk.time[-1]))
            # Separate the non-time-dependent variable
            if 'extent' in existing_to_append.data_vars:
                # Drop the extent variable from existing_to_append if it already exists in the file for concat operation with other chunks. It will be added after writing all chunks in _apply_dataset_operations
                ds_chunk = ds_chunk.drop_vars('extent')
            write_ds_chunk = xr.concat([existing_to_append, ds_chunk_sel], dim='time')
            write_ds_chunk.to_netcdf(self._outputpath, mode='w', unlimited_dims=['time']) 
        # Else just write the chunk in file (by creating new if first chunk, otherwise append)
        else:
            ## Write the chunk
            if first_loop:
                write_ds_chunk = ds_chunk
                write_ds_chunk.to_netcdf(self._outputpath, mode='w', unlimited_dims=['time'])
            else:
                appending_ds = xr.open_dataset(self._outputpath, mode='a')
                # Concatenate the existing and new data along the time dimension
                combined_ds = xr.concat([appending_ds, ds_chunk], dim='time')
                appending_ds.close()
                # Write the combined dataset back to the file
                combined_ds.to_netcdf(self._outputpath, mode='w', unlimited_dims=['time'])
    
    def _read_and_write_in_chunks(self):

        start_date = pd.to_datetime(self._start).to_pydatetime()
        start_date_pd = pd.to_datetime(self._start)
        end_date = pd.to_datetime(self._end).to_pydatetime()
        
        chunk_start = start_date
        # use_previous = False

        loop_number = 0

        while chunk_start < end_date:
            # Calculate the chunk end date (5 years later)
            chunk_end = min(chunk_start + relativedelta(years=5), end_date)

            # Convert chunk dates back to string format for passing to CombinedNC
            chunk_start_str = chunk_start.strftime("%Y-%m-%d")
            chunk_end_str = chunk_end.strftime("%Y-%m-%d")
            print(f"Running combinedNC for {chunk_start_str} and {chunk_end_str}")
            
            # read chunk data
            ds_list = self._read_chunk(chunk_start,chunk_end)
            # For first chunk
            if loop_number==0:
                # If using previously stored data, merege last 120 days from the start date to the chunk_data
                if self._use_previous:
                    if os.path.isfile(self._outputpath):
                        log.debug(f"Found existing file at {self._outputpath} -- Updating in-place")
                        # Assuming the existing file structure is same as the one generated now. Basically
                        #   assuming that the previous file was also created by MetSimRunner
                        existing = xr.open_dataset(self._outputpath)
                        existing.close()
                        last_existing_time = existing.time[-1]
                        log.debug("Existing data: %s", last_existing_time)
                        existing_to_append = existing.sel(time=slice(start_date_pd.to_datetime64() - np.timedelta64(120,'D') , last_existing_time))
                        self._write_chunk(ds_list, existing_to_append, last_existing_time, first_loop=True)
                    else:
                        raise Exception('Previous combined dataset not found. Please run RAT with spin-up or use state file paths.')
                # Else simply write a chunk in write mode
                else:
                    self._write_chunk(ds_list, first_loop=True)
            # For all other chunks, just write them in append mode
            else:
                self._write_chunk(ds_list)
            
            # Increase loop number
            loop_number += 1
            # Move to the next chunk
            chunk_start = chunk_end + relativedelta(days=1)
            print("Chunk written.")
        
        print("Written complete CombinedNC and now applying data imputation and other operations like min-max temperature check.")
        self._apply_dataset_operations()
    
    def _apply_dataset_operations(self):
        da = xr.open_dataset(self._outputpath)
        ### create extent variable to be added in the dataset
        extent_da = xr.DataArray(
                data=self._ar,
                coords={'lat': np.flip(self._latitudes1d), 'lon': self._longitudes1d},
                dims=['lat', 'lon'],
                name='extent'  
            )
        ### Add extent DataArray to dataset as a variable
        da['extent'] = extent_da
        clean_da = self._impute_basin_missing_data(da)
        clean_da = self._min_max_temperature_check(clean_da)
        da.close()
        clean_da.to_netcdf(self._outputpath)

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