import xarray as xr
import rioxarray as rxr
import numpy as np
import requests
import dask
import pandas as pd
import geopandas as gpd
from dask.distributed import LocalCluster, Client
from pathlib import Path
import tempfile
import os
import subprocess
import shutil
import numpy as np
from datetime import date
from rat.utils.run_command import run_command
import ruamel_yaml as ryaml
# import cfgrib
from rat.data_processing.metsim_input_processing import CombinedNC
from rat.rat_basin import rat_basin
from rat.toolbox.config import update_config


# Obtain GEFS precip data
def download_gefs_chirps(basedate, lead_time, save_dir):
    """Download forecast precipitation from GEFS-CHIRPS.

    Args:
        basedate (pd.Timestamp): Data starting from next day will be downloaded.
        lead_time (int): Lead time in days.
        save_dir (Path): Directory to save data.
    """
    assert lead_time <= 15, "Maximum lead time is 15 days for GEFS-CHIRPS."

    forecast_dates = pd.date_range(basedate, basedate + pd.Timedelta(days=lead_time))

    for forecast_date in forecast_dates[1:]: # Skip the first day, download forecast from next day
        by = basedate.year
        bm = basedate.month
        bd = basedate.day
        fy = forecast_date.year
        fm = forecast_date.month
        fd = forecast_date.day

        save_fp = save_dir / f"{basedate:%Y%m%d}" / f"{forecast_date:%Y%m%d}.tif"
        save_fp.parent.mkdir(parents=True, exist_ok=True)
        prefix = f"https://data.chc.ucsb.edu/products/EWX/data/forecasts/CHIRPS-GEFS_precip_v12/daily_16day/{by}/{bm:02}/{bd:02}/"
        url = prefix + f"data.{fy}.{fm:02}{fd:02}.tif"

        r = requests.get(url)
        if r.status_code == 200:
            with open(save_fp, 'wb') as f:
                f.write(r.content)
        else:
            print(f"Could not download {url}")


def process_gefs_chirps(
        basin_bounds, 
        srcpath, 
        dstpath, 
        temp_datadir=None,
    ):
    """For any IMERG Precipitation file located at `srcpath` is clipped, scaled and converted to
    ASCII grid file and saved at `dstpath`. All of this is done in a temporarily created directory
    which can be controlled by the `datadir` path
    """
    src_fn = Path(srcpath)
    dst_fn = Path(dstpath)
    dst_fn.parent.mkdir(parents=True, exist_ok=True) # Create parent directory if it doesn't exist

    date = pd.to_datetime(src_fn.stem.split('_')[0])
    if temp_datadir is not None and not os.path.isdir(temp_datadir):
        STATUS='FAILED'
        return date, 'Precipitaion', STATUS
    
    if not(os.path.exists(dstpath)):
        # log.debug("Processing Precipitation file: %s", srcpath)
        print(f"Processing Precipitation file: {srcpath}")
        STATUS = 'STARTED'
        with tempfile.TemporaryDirectory(dir=temp_datadir) as tempdir:
            clipped_temp_file = os.path.join(tempdir, 'clipped.tif')
            cmd = [
                "gdalwarp",
                "-dstnodata", 
                "-9999.0",
                "-tr",
                "0.0625",
                "0.0625",
                "-te",
                str(basin_bounds[0]),
                str(basin_bounds[1]),
                str(basin_bounds[2]),
                str(basin_bounds[3]),
                '-of',
                'GTiff',
                '-overwrite', 
                f'{srcpath}',
                clipped_temp_file
            ]
            run_command(cmd)

            # Change format, and save as processed file
            aai_temp_file = os.path.join(tempdir, 'processed.tif')
            cmd = [
                'gdal_translate',
                '-of', 
                'aaigrid',
                clipped_temp_file,
                aai_temp_file
            ]
            run_command(cmd)

            # Move to destination
            shutil.move(aai_temp_file, dstpath)
            STATUS = 'SUCCESS'
    else:
        STATUS = 'SKIPPED'
    return date, 'Precipitaion', STATUS


def get_gefs_precip(basin_bounds, forecast_raw_dir, forecast_processed_dir, begin, lead_time, temp_datadir=None):
    """Download and process forecast data for a day."""
    # download data
    download_gefs_chirps(begin, lead_time, forecast_raw_dir)

    # process data
    futures = []
    forecast_raw_date_dir = forecast_raw_dir / f"{begin:%Y%m%d}"
    raw_files = sorted(list(forecast_raw_date_dir.glob("*.tif")))
    for raw_file in raw_files:
        date = pd.to_datetime(raw_file.stem.split('_')[0])
        dstpath = forecast_processed_dir / f"{begin:%Y%m%d}" / f"{date:%Y%m%d}.asc"
        dstpath.parent.mkdir(parents=True, exist_ok=True)

        print(raw_file, dstpath)

        future = dask.delayed(process_gefs_chirps)(basin_bounds, raw_file, dstpath, temp_datadir=temp_datadir)
        futures.append(future)

    processed_statuses = dask.compute(*futures)
    processed_statuses = pd.DataFrame(processed_statuses, columns=['Date','Variable','Status'])
    processed_statuses.sort_values(by=["Date","Variable"], inplace=True)
    return processed_statuses


def download_GFS_files(
        basedate, lead_time, save_dir
    ):
    """Download GFS files for a day and saves it with a consistent naming format for further processing."""
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    hours = range(24, 16*24+1, 24)

    # determine where to download from. nomads has data for the last 10 days
    if basedate > pd.Timestamp(date.today()) - pd.Timedelta(days=10):
        links = [f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2Fgfs.{basedate:%Y%m%d}%2F00%2Fatmos&file=gfs.t00z.pgrb2.0p25.f{h:03}&var_TMAX=on&var_TMIN=on&var_UGRD=on&var_VGRD=on&lev_2_m_above_ground=on&lev_10_m_above_ground=on" for h in hours]
    else:
        links = [f"https://data.rda.ucar.edu/ds084.1/{basedate:%Y}/{basedate:%Y%m%d}/gfs.0p25.{basedate:%Y%m%d}00.f{h:03}.grib2" for h in hours]
    
    raw_savefns = [save_dir / f"{basedate:%Y%m%d}/{basedate+pd.Timedelta(h, 'hours'):%Y%m%d}.grib2" for h in hours]

    for link, savefn in zip(links, raw_savefns):
        savefn.parent.mkdir(parents=True, exist_ok=True)
        r = requests.get(link)
        with open(savefn, 'wb') as f:
            f.write(r.content)


def aggregate_ncs(ncs, fn, var=None):
    """Will return a single dataset same as ncs[0] with the underlying values being equal to the result of the `fn` aggregation function

    Args:
        ncs (list[DataArray/DataSet]): List of datarrays or datasets to aggregate. If a dataset is passed, `var` must be passed
        fn (function): Aggregation function
        var (str): Variable name 
    """
    if isinstance(ncs[0], xr.Dataset):
        ncs_dataarrays = [nc[var] for nc in ncs]
    else:
        ncs_dataarrays = ncs
    
    res = ncs_dataarrays[0].copy()

    res.values = fn([nc.values for nc in ncs_dataarrays], axis=0)

    return res


def extract_gribs(gfs_dir, date):
    """Extracts GRIB files to NetCDF files and returns the paths to the extracted files."""
    gfs_dir = Path(gfs_dir)
    date = pd.to_datetime(date)
    raw_dir = gfs_dir / "raw" / f"{date:%Y%m%d}"
    save_dir = gfs_dir / "extracted" / f"{date:%Y%m%d}"
    save_dir.mkdir(parents=True, exist_ok=True)

    filepaths = sorted(list(raw_dir.glob("*.grib2")))

    extracted_dir = {
        "tmax": save_dir / f"tmax",
        "tmin": save_dir / f"tmin",
        "uwnd": save_dir / f"uwnd",
        "vwnd": save_dir / f"vwnd"
    }

    extraction = {
        "tmax": lambda ds: (ds['tmax']-273.15).sel(heightAboveGround=2).rename('tmax').drop_vars('heightAboveGround'),
        "tmin": lambda ds: (ds['tmin']-273.15).sel(heightAboveGround=2).rename('tmin').drop_vars('heightAboveGround'),
        "uwnd": lambda ds: ds['u10'].sel(heightAboveGround=10).rename('uwnd').drop_vars('heightAboveGround'),
        "vwnd": lambda ds: ds['v10'].sel(heightAboveGround=10).rename('vwnd').drop_vars('heightAboveGround'),
    }

    extracted_fps = {
        "tmax": [],
        "tmin": [],
        "uwnd": [],
        "vwnd": []
    }

    for fn in filepaths:
        basedate = pd.to_datetime(fn.parent.name)
        forecasted_date = pd.to_datetime(fn.stem)

        # cfgrib_datasets = cfgrib.open_datasets(str(fn))
        tempds = xr.open_dataset(
            str(fn), engine="cfgrib",
            backend_kwargs={
                'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 2},
                "indexpath": ""
            }
        )
        windds = xr.open_dataset(
            str(fn), engine="cfgrib",
            backend_kwargs={
                'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 10},
                "indexpath": ""
            }
        )

        ds = xr.merge([
            d.expand_dims("heightAboveGround") 
            for d in [tempds, windds]
        ])
        ds = ds.assign_coords(longitude=((360 + (ds.longitude % 360)) % 360))
        ds = ds.roll(longitude=int(len(ds['longitude']) / 2), roll_coords=True)

        for var in ["tmax", "tmin", "uwnd", "vwnd"]:
            savedir = extracted_dir[var]
            savedir.mkdir(parents=True, exist_ok=True)

            # (1) Read in with xarray and only save the bare minimum
            modified = extraction[var](ds)

            savep = savedir / f"{forecasted_date:%Y%m%d}.nc"
            modified.to_netcdf(savep)
            print(f"{basedate} ({forecasted_date} hours) - {var} - (1) Subsetted nc file")
            extracted_fps[var].append(savep)


def process_GFS_file(fn, basin_bounds, gfs_dir):
    # date = pd.to_datetime(fn.split(os.sep)[-1].split('.')[0], format='%Y%m%d')
    fn = Path(fn)
    gfs_dir = Path(gfs_dir)

    basedate = pd.to_datetime(fn.parent.parent.name)
    forecasted_date = pd.to_datetime(fn.stem)
    var = fn.parent.name
    
    processed_savedirs = {
        "tmax": gfs_dir / f"processed/{basedate:%Y%m%d}/tmax",
        "tmin": gfs_dir / f"processed/{basedate:%Y%m%d}/tmin",
        "uwnd": gfs_dir / f"processed/{basedate:%Y%m%d}/uwnd",
        "vwnd": gfs_dir / f"processed/{basedate:%Y%m%d}/vwnd"
    }
    for v in ["tmax", "tmin", "uwnd", "vwnd"]:
        processed_savedirs[v].mkdir(parents=True, exist_ok=True)

    # (1) Convert to GTiff
    in_fn = fn
    out_fn = fn.with_name(f'{forecasted_date:%Y%m%d}_converted_1.tif')
    cmd = ['gdal_translate', '-of', 'Gtiff', '-a_ullr', "-180", "90", "180", "-90", str(in_fn), str(out_fn)]
    res = run_command(cmd)  #, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # print(f"{basedate:%Y-%m-%d} - {var} - (2) Converted to Geotiff")

    # (2)Change scale and clip
    in_fn = out_fn
    out_fn = in_fn.with_name(f'{forecasted_date:%Y%m%d}_clipped_2.tif')
    cmd = ['gdalwarp', '-dstnodata', '-9999.0', '-tr', '0.0625', '0.0625', '-te', str(basin_bounds[0]), str(basin_bounds[1]), str(basin_bounds[2]), str(basin_bounds[3]), "-of", "GTiff", "-overwrite", str(in_fn), str(out_fn)]
    res = run_command(cmd)  #, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # print(f"{basedate:%Y-%m-%d} - {var} - (3) Scaled and clipped")

    # (3) Change format to AAIgrid
    in_fn = out_fn
    outdir = processed_savedirs[var]
    outdir.mkdir(parents=True, exist_ok=True)

    out_fn = outdir / f"{forecasted_date:%Y%m%d}.asc"
    cmd = ['gdal_translate', '-of', 'aaigrid', str(in_fn), str(out_fn)]
    res = run_command(cmd)#, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # print(f"{basedate:%Y-%m-%d} - {var} - (4) Converted to .asc")


def process_GFS_files(basedate, lead_time, basin_bounds, gfs_dir):
    """Extracts only the required meteorological variables and converts from GRIB2 format to netcdf

    Args:
        fns (string): Path of GRIB2 GFS file
    """
    gfs_dir = Path(gfs_dir)
    hours = range(24, lead_time*24+1, 24)
    forecasted_dates = [basedate + pd.Timedelta(h, 'hours') for h in hours]
    
    in_fns = [
        gfs_dir / f"extracted/{basedate:%Y%m%d}/{var}/{forecasted_date:%Y%m%d}.nc" 
        for forecasted_date in forecasted_dates 
        for var in ["tmax", "tmin", "uwnd", "vwnd"]
    ]
    for i, in_fn in enumerate(in_fns):
        process_GFS_file(in_fn, basin_bounds, gfs_dir)


def get_GFS_data(basedate, lead_time, basin_bounds, gfs_dir):
    """Extracts only the required meteorological variables and converts from GRIB2 format to netcdf

    Args:
        fns (string): Path of GRIB2 GFS file
    """
    download_GFS_files(
        basedate,
        lead_time=lead_time,
        save_dir=gfs_dir / "raw",
    )

    extract_gribs(gfs_dir, basedate)

    process_GFS_files(
        basedate,
        lead_time,
        basin_bounds,
        gfs_dir
    )

def generate_forecast_state_and_inputs(
        forecast_startdate, # forecast start date
        forecast_enddate, # forecast end date
        hindcast_combined_datapath, 
        forecast_combined_datapath, 
        out_dir
    ):
    out_dir = Path(out_dir)
    hindcast_combined_data = xr.open_dataset(hindcast_combined_datapath)
    forecast_combined_data = xr.open_dataset(forecast_combined_datapath)
    
    # check if tmin < tmax. If not, set tmin = tmax
    forecast_combined_data['tmin'] = xr.where(
        forecast_combined_data['tmin'] < forecast_combined_data['tmax'],
        forecast_combined_data['tmin'],
        forecast_combined_data['tmax']
    )

    combined_data = xr.concat([
        hindcast_combined_data, forecast_combined_data
    ], dim="time")

    state_startdate = forecast_startdate - pd.Timedelta(days=90)
    state_enddate = forecast_startdate - pd.Timedelta(days=1)

    state_ds = combined_data.sel(time=slice(state_startdate, state_enddate))
    state_outpath = out_dir / "forecast_state.nc"
    state_ds.to_netcdf(state_outpath)

    # # Generate the metsim input
    forcings_ds = combined_data.sel(time=slice(forecast_startdate, forecast_enddate))
    forcings_outpath = out_dir / "forecast_metsim_input.nc"
    print(f"Saving forcings: {forcings_outpath}")
    forcings_ds.to_netcdf(forcings_outpath)

    return state_outpath, forcings_outpath


def forecast(config, rat_logger):
    """Function to run the forecasting plugin.

    Args:
        config (dict): Dictionary containing the configuration parameters.
        rat_logger (Logger): Logger object
    """
    print("Forecasting Plugin Started")
    # read necessary parameters from config
    basins_shapefile_path = config['GLOBAL']['basin_shpfile'] # Shapefile containg information of basin(s)- geometry and attributes
    basins_shapefile = gpd.read_file(basins_shapefile_path)  # Reading basins_shapefile_path to get basin polygons and their attributes
    basins_shapefile_column_dict = config['GLOBAL']['basin_shpfile_column_dict'] # Dictionary of column names in basins_shapefile, Must contain 'id' field
    region_name = config['BASIN']['region_name']  # Major basin name used to cluster multiple basins data in data-directory
    basin_name = config['BASIN']['basin_name']              # Basin name used to save basin related data
    basin_id = config['BASIN']['basin_id']                  # Unique identifier for each basin used to map basin polygon in basins_shapefile
    basin_data = basins_shapefile[basins_shapefile[basins_shapefile_column_dict['id']]==basin_id] # Getting the particular basin related information corresponding to basin_id
    basin_bounds = basin_data.bounds                          # Obtaining bounds of the particular basin
    basin_bounds = np.array(basin_bounds)[0]
    basin_data_dir = Path(config['GLOBAL']['data_dir']) / region_name / 'basins' / basin_name

    # determine forecast related dates - basedate, lead time and enddate
    if config['PLUGINS']['forecast_start_date'] == 'end_date':
        basedate = pd.to_datetime(config['BASIN']['end'])
    else:
        basedate = pd.to_datetime(config['PLUGINS']['forecast_start_date'])
    lead_time = config['PLUGINS']['forecast_lead_time']
    forecast_enddate = basedate + pd.Timedelta(days=lead_time)

    # define and create directories
    hindcast_nc_path = basin_data_dir / 'pre_processing' / 'nc' / 'combined_data.nc'
    combined_nc_path = basin_data_dir / 'pre_processing' / 'nc' / 'forecast_combined.nc'
    metsim_inputs_dir = basin_data_dir / 'metsim' / 'metsim_inputs'
    basingridfile_path = basin_data_dir / 'basin_grid_data' / f'{basin_name}_grid_mask.tif'
    forecast_data_dir = basin_data_dir / 'forecast'
    raw_gefs_chirps_dir = forecast_data_dir / 'gefs-chirps' / 'raw'
    processed_gefs_chirps_dir = forecast_data_dir / 'gefs-chirps' / 'processed'
    gfs_dir = forecast_data_dir / 'gfs'
    raw_gfs_dir = gfs_dir / 'raw'
    extracted_gfs_dir = gfs_dir / 'extracted'
    processed_gfs_dir = gfs_dir / 'processed'

    for d in [raw_gefs_chirps_dir, processed_gefs_chirps_dir, raw_gfs_dir, extracted_gfs_dir, processed_gfs_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # cleanup previous runs
    vic_forecast_input_dir = basin_data_dir / 'vic' / 'forecast_vic_inputs'
    [f.unlink() for f in vic_forecast_input_dir.glob("*") if f.is_file()]
    vic_forecast_state_dir = basin_data_dir / 'vic' / 'forecast_vic_state'
    [f.unlink() for f in vic_forecast_state_dir.glob("*") if f.is_file()]
    combined_nc_path.unlink() if combined_nc_path.is_file() else None
    rout_forecast_state_dir = basin_data_dir / 'rout' / 'forecast_rout_state_file'
    [f.unlink() for f in rout_forecast_state_dir.glob("*") if f.is_file()]


    # RAT STEP-1 (Forecasting) Download and process GEFS-CHIRPS data
    get_gefs_precip(basin_bounds, raw_gefs_chirps_dir, processed_gefs_chirps_dir, basedate, lead_time)

    # RAT STEP-1 (Forecasting) Download and process GFS data
    get_GFS_data(basedate, lead_time, basin_bounds, gfs_dir)

    # RAT STEP-2 (Forecasting) make combined nc
    CombinedNC(
        basedate, forecast_enddate, None,
        basingridfile_path, combined_nc_path, False,
        forecast_data_dir, basedate
    )

    # RAT STEP-2 (Forecasting) generate metsim inputs
    generate_forecast_state_and_inputs(
        basedate, forecast_enddate,
        hindcast_nc_path, combined_nc_path,
        metsim_inputs_dir
    )

    # change config to only run metsim-routing
    config['BASIN']['vic_init_state'] = config['BASIN']['end'] # assuming vic_init_state is available for the end date
    config['GLOBAL']['steps'] = [3, 4, 5, 6, 7, 8, 13] # only run metsim-routing and inflow file generation
    config['BASIN']['start'] = basedate
    config['BASIN']['end'] = forecast_enddate
    config['BASIN']['spin_up'] = False

    # Run RAT with forecasting parameters
    no_errors, _ = rat_basin(config, rat_logger, forecast_mode=True)

    return no_errors