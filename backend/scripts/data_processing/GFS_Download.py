import argparse
from email import parser
from multiprocessing import process
import pandas as pd
from datetime import datetime
import numpy as np
from tempfile import TemporaryDirectory
import os
import subprocess
import xarray as xr
import sys
import shutil
from data_processing.newdata import run_command
from data_processing.metsim_input_processing import ForcingsNCfmt
from collections import OrderedDict



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


def extract_gribs(fps, datadir):
    fps = fps['fp']
    filepaths = fps.values
    
    varnames = {
        "tmax": "TMAX_P8_L103_GLL0_max6h",
        "tmin": "TMIN_P8_L103_GLL0_min6h",
        "precip": "PRATE_P0_L1_GLL0",
        "uwnd": "UGRD_P0_L103_GLL0,lv_HTGL2",
        "vwnd": "VGRD_P0_L103_GLL0,lv_HTGL2"
    }
    extracted_dir = {
        "tmax": os.path.join(datadir, f"extracted/tmax"),
        "tmin": os.path.join(datadir, f"extracted/tmin"),
        "precip": os.path.join(datadir, f"extracted/precipitation"),
        "uwnd": os.path.join(datadir, f"extracted/uwnd"),
        "vwnd": os.path.join(datadir, f"extracted/vwnd")
    }

    extraction = {
        "precip": lambda ds: (ds['PRATE_P0_L1_GLL0']*24*60*60).rename('precip').rename({'lat_0': 'lat', 'lon_0': 'lon'}),
        "tmax": lambda ds: (ds['TMAX_P8_L103_GLL0_max6h']-273.15).rename('tmax').rename({'lat_0': 'lat', 'lon_0': 'lon'}),
        "tmin": lambda ds: (ds['TMIN_P8_L103_GLL0_min6h']-273.15).rename('tmin').rename({'lat_0': 'lat', 'lon_0': 'lon'}),
        "uwnd": lambda ds: ds['UGRD_P0_L103_GLL0'].sel(lv_HTGL2=10).rename('uwnd').rename({'lat_0': 'lat', 'lon_0': 'lon'}),
        "vwnd": lambda ds: ds['VGRD_P0_L103_GLL0'].sel(lv_HTGL2=10).rename('vwnd').rename({'lat_0': 'lat', 'lon_0': 'lon'}),
    }
    
    extracted_fps = {
        "tmax": [],
        "tmin": [],
        "precip": [],
        "uwnd": [],
        "vwnd": []
    }

    for fn in filepaths:
        date = pd.to_datetime(fn.split('_')[-2].split('.')[0], format='%Y%m%d')
        hour = fn.split('_')[-1].split('.')[0]
        name = date.strftime('%Y%m%d')
        for var in varnames.keys():
            nc_var = varnames[var]
            arg = ["ncl_convert2nc", fn, '-v', nc_var]

            savedir = extracted_dir[var]
            if not os.path.isdir(savedir):
                os.makedirs(savedir)

            # (0) Convert from GRIB to NC uncleanly
            res = subprocess.run(arg, cwd=savedir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(f"{name} ({hour} hours) - {var} - (0) Converted GRIB file to NC")
            extracted_nc_fn = os.path.join(savedir, fn.split(os.sep)[-1].replace('grib2', 'nc'))

            # (1) Read in with xarray and only save the bare minimum
            ds = xr.open_dataset(extracted_nc_fn, engine="netcdf4")
            modified = extraction[var](ds)
            ds.close()

            savep = os.path.join(savedir, f"{name}_{hour}") + '.nc'
            modified.to_netcdf(savep)
            print(f"{name} ({hour} hours) - {var} - (1) Subsetted nc file")
            # if os.path.isfile(extracted_nc_fn):
            #     os.remove(extracted_nc_fn)
            extracted_fps[var].append(savep)

    # Now that all the GRIB2 files have been converted to NC files, we'll have to combine them
    save_dirs = {
        "tmax": os.path.join(datadir, f"raw/tmax"),
        "tmin": os.path.join(datadir, f"raw/tmin"),
        "precip": os.path.join(datadir, f"raw/precipitation"),
        "uwnd": os.path.join(datadir, f"raw/uwnd"),
        "vwnd": os.path.join(datadir, f"raw/vwnd")
    }
    agg_func = {
        "tmax": np.max,
        "tmin": np.min,
        "precip": np.mean,
        "uwnd": np.mean,
        "vwnd": np.mean
    }
    save_paths = {
        "tmax": None,
        "tmin": None,
        "precip": None,
        "uwnd": None,
        "vwnd": None
    }

    for var in ['tmax', 'tmin', 'precip', 'uwnd', 'vwnd']:
        extracted_ncs = extracted_fps[var]

        print(f"Merging: {var}")

        datasets = []
        times = []
        for extracted_nc in extracted_ncs:
            datasets.append(xr.open_dataset(extracted_nc))
            times.append(pd.to_datetime(extracted_nc.split(os.sep)[-1].split('.')[0], format='%Y%m%d_%H'))

        merged = aggregate_ncs(datasets, agg_func[var], var)
        # time_index = pd.DatetimeIndex(times, name='time')

        # merged = xr.concat(datasets, time_index)
        # merged = merged.apply(agg_func[var], axis=2)

        if not os.path.isdir(save_dirs[var]):
            os.makedirs(save_dirs[var])
        savep = os.path.join(save_dirs[var], times[0].strftime("%Y%m%d") + ".nc")
        save_paths[var] = savep
        merged.to_netcdf(savep)

    return pd.Series(save_paths)


def process_GFS_file(fn, datadir):
    date = pd.to_datetime(fn.split(os.sep)[-1].split('.')[0], format='%Y%m%d')
    name = date.strftime('%Y%m%d')
    var = fn.split(os.sep)[-2]
    if var == 'precipitation':
        var = 'precip'    # :) quick and easy way patch

    # folder structure
    # raw_savedirs = {
    #     "tmax": os.path.join(datadir, f"raw/tmax"),
    #     "tmin": os.path.join(datadir, f"raw/tmin"),
    #     "precip": os.path.join(datadir, f"raw/precipitation"),
    #     "uwnd": os.path.join(datadir, f"raw/uwnd"),
    #     "vwnd": os.path.join(datadir, f"raw/vwnd")
    # }
    
    processed_savedirs = {
        "tmax": os.path.join(datadir, f"processed/tmax"),
        "tmin": os.path.join(datadir, f"processed/tmin"),
        "precip": os.path.join(datadir, f"processed/precipitation"),
        "uwnd": os.path.join(datadir, f"processed/uwnd"),
        "vwnd": os.path.join(datadir, f"processed/vwnd")
    }

    suffix = {
        "tmax": f"_TMAX",
        "tmin": f"_TMIN",
        "precip": f"_IMERG",
        "uwnd": f"_UWND",
        "vwnd": f"_VWND"
    }

    out_paths = {
        "tmax": None,
        "tmin": None,
        "precip": None,
        "uwnd": None,
        "vwnd": None
    }

    # (2) Convert to GTiff
    in_fn = fn
    out_fn = fn.replace('.nc', '_converted_1.tif')
    cmd = ['gdal_translate', '-of', 'Gtiff', '-a_ullr', "0", "90", "360", "-90", in_fn, out_fn]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(f"{name} - {var} - (2) Converted to Geotiff")

    # (3)Change scale and clip
    in_fn = out_fn
    out_fn = in_fn.replace('_converted_1.tif', '_clipped_2.tif')
    cmd = ['gdalwarp', '-dstnodata', '-9999.0', '-tr', '0.0625', '0.0625', '-te', "93.875", "9.5625", "108.6875", "33.8125", "-of", "GTiff", "-overwrite", in_fn, out_fn]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(f"{name} - {var} - (3) Scaled and clipped")

    # (4) Change format to AAIgrid
    in_fn = out_fn
    # outdir = os.path.join(datadir, f'processed/{var}')
    outdir = processed_savedirs[var]
    if not os.path.isdir(outdir):
        os.makedirs(outdir)
    # out_fn = os.path.join(outdir, fn.split('_')[-1].split('.')[0] + suffix[var] + '.asc')
    out_fn = os.path.join(outdir, date.strftime('%Y-%m-%d') + suffix[var] + '.asc')
    cmd = ['gdal_translate','-of', 'aaigrid', in_fn, out_fn]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(f"{name} - {var} - (4) Converted to .asc")

    # # (5) Create wind
    # vwnd_fn = os.path.join(datadir, f"raw/vwnd/{fn.split('_')[-1].split('.')[0]}_clipped_2.tif")
    # uwnd_fn = os.path.join(datadir, f"raw/uwnd/{fn.split('_')[-1].split('.')[0]}_clipped_2.tif")
    # outdir = os.path.join(datadir,f"raw/wind" )
    # if not os.path.isdir(outdir):
    #     os.makedirs(outdir)
    # outpath = os.path.join(outdir, f"{fn.split('_')[-1].split('.')[0]}.tif")
    # cmd = ["gdal_calc.py", "-A", uwnd_fn, "-B", vwnd_fn, f"--calc=\"numpy.sqrt(numpy.power(A, 2) + numpy.power(B, 2))\"", '--overwrite', f"--outfile={outpath}", "--NoDataValue=-9999", "--format=GTiff"]
    # res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # print(f"{name} - wind - (5) Created wind file")

    # # (4) Change format of wind to AAIgrid
    # in_fn = outpath
    # outdir = os.path.join(datadir, f'processed/wind')
    # if not os.path.isdir(outdir):
    #     os.makedirs(outdir)
    # out_fn = os.path.join(outdir, date.strftime('%Y-%m-%d') + suffix[var] +'.asc')
    # cmd = ['gdal_translate','-of', 'aaigrid', in_fn, out_fn]
    # res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # print(f"{name} - wind - (6) Converted wind file to .asc")


def process_GFS_files(fns, datadir):
    """Extracts only the required meteorological variables and converts from GRIB2 format to netcdf

    Args:
        fns (string): Path of GRIB2 GFS file
    """
    download_dir = fns[0].split(os.sep)[0]
    base_date = pd.to_datetime(fns[0].split(os.sep)[-1].split('_')[0], format="%Y%m%d")
    forecasted_dates = [pd.to_datetime("_".join(fn.split(os.sep)[-1].split('.')[0].split('_')[1:]), format="%Y%m%d_%H") for fn in fns]
    processing_df_6h = pd.DataFrame({
        'fp': fns,
        'time': forecasted_dates
    }).set_index('time')

    # Extract these files and combine into a single nc file for the day
    # processing_df_6h[['precip', 'tmin', 'tmax', 'uwnd', 'vwnd']] = processing_df_6h.resample('1D').agg(lambda grouped: extract_gribs(grouped, datadir), )
    processing_df_1d = processing_df_6h.groupby(pd.Grouper(freq='1D')).apply(lambda grouped: extract_gribs(grouped, datadir))

    print(processing_df_1d)

    for idx, row in processing_df_1d.iterrows():
        process_GFS_file(row['tmax'], datadir)
        process_GFS_file(row['tmin'], datadir)
        process_GFS_file(row['uwnd'], datadir)
        process_GFS_file(row['vwnd'], datadir)
        process_GFS_file(row['precip'], datadir)



def download_GFS_files(links, savefns, wget_path='/usr/bin/wget'):
    # Using parallel subprocesses
    processes = []
    logs = []
    for link, savefn in zip(links, savefns):
        log_fn = f"{savefn}.log"
        print(f"Downloading {savefn}")
        p = subprocess.Popen([wget_path, link, "-O", savefn, '-o', log_fn], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        processes.append((p, savefn))
        # logs.append(log_fn)
    
    for p, savefn in processes:
        p.wait()
        print(f"Downloaded {savefn}")


def combine_processed(start, end, processed_dir, basin_grid, savefn):
    ForcingsNCfmt(
        start,
        end,
        processed_dir,
        basin_grid,
        savefn
    )
    return savefn


def get_forecast(start_date, basin_file_fn, data_dir='./data', savedir="./data/forecast", lead_time=15):
    """Downloads, processes and combines forecast data from GFS

    Args:
        start_date (string): Start date for data download in %Y-%m-%d format. Usually today's date 
            will be `today`.
        data_dir (string, default: ./data): Path of directory that'll be used to store the data. Temporary 
            directories will be created in this directory while processing. 
        lead_time (int, default: 15): Number of days in the future to download data for. Max: 16
    """
    assert lead_time <= 16, f"Maximum possible lead time is 16 days, and {lead_time} was passed"
    fmt = '%Y-%m-%d'
    aws_fmt = '%Y%m%d'
    date = pd.to_datetime(start_date, format=fmt)

    aws_date_str = date.strftime(aws_fmt)
    hours = np.arange(12, 24*lead_time, 6)
    dates = np.array([date + pd.DateOffset(hours=int(h)) for h in hours])

    # with TemporaryDirectory(dir=data_dir, delete=False) as td:
    td = "/houston2/pritam/rat_mekong_v3/extras/2022_04_12-forecasting/data/temp_dir_mock"
    if isinstance(td, TemporaryDirectory):
        td_name = td.name
    else:
        td_name = td

    # links = [f"https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{aws_date_str}/00/atmos/gfs.t00z.pgrb2.0p25.f{h:03}" for h in hours]
    links = [f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t00z.pgrb2.0p25.f{h:03}&all_lev=on&lev_10_m_above_ground=on&lev_2_m_above_ground=on&lev_surface=on&var_PRATE=on&var_TMAX=on&var_TMIN=on&var_UGRD=on&var_VGRD=on&dir=%2Fgfs.20220418%2F00%2Fatmos" for h in hours]
    if not os.path.isdir(os.path.join(td_name, 'downloaded')):
        os.makedirs(os.path.join(td, 'downloaded'))
    raw_savefns = [f"{os.path.join(td_name, 'downloaded', aws_date_str)}_{d.strftime('%Y%m%d_%H')}.grib2" for d in dates]

    # # download
    # download_GFS_files(links, raw_savefns)

    # # process
    # process_GFS_files(raw_savefns, td)

    # combine and save
    savep = os.path.join(savedir, date.strftime(fmt) + '.nc')
    if not os.path.isdir(savedir):
        os.makedirs(savedir)
    start = pd.to_datetime(dates[0].strftime('%Y%m%d'), format='%Y%m%d')
    end = pd.to_datetime(dates[-1].strftime('%Y%m%d'), format='%Y%m%d')
    print(f"Combining data - {savep} from {start} to {end}")
    combine_processed(start, end, os.path.join(td_name, 'processed'), basin_file_fn, savep)


def main():
    parser = argparse.ArgumentParser(description='Download and process forecasted meteorological variables from GFS')
    parser.add_argument('start_date', type=str, help='Start date for data download in YYYY-MM-DD format. Usually today\'s date')
    parser.add_argument('basin_grid', type=str, help='Path of the basin\'s grid file. It is the "mask" file, a raster containing the mask of the basin')
    parser.add_argument('--lead_time', type=int, default=15, help='Number of days in the future to download data for. Default: 15 days, Max: 16 days')
    parser.add_argument('--data_dir', default='./data', help='Path of directory that\'ll be used to store the data. Temporary directories will be created in this directory while processing.')

    args = parser.parse_args()

    get_forecast(args.start_date, args.basin_grid, args.data_dir, os.path.join(args.data_dir, 'forecast'), args.lead_time)


if __name__ == '__main__':
    main()