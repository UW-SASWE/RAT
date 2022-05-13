import subprocess
import yaml
from datetime import datetime, timedelta
from tqdm import tqdm
import os
import shutil
import tempfile
import xarray as xr
from logging import getLogger
import pandas as pd

from utils.logging import LOG_NAME, NOTIFICATION
from utils.utils import create_directory
import configparser

log = getLogger(LOG_NAME)

def run_command(cmd,shell_bool=False):
    """Safely runs a command, and returns the returncode silently in case of no error. Otherwise,
    raises an Exception
    """
    res = subprocess.run(cmd, check=True, capture_output=True,shell=shell_bool)
    
    if res.returncode != 0:
        log.error(f"Error with return code {res.returncode}")
        raise Exception
    return res.returncode

def determine_precip_version(date):
    """Determines which version of IMERG to download. Most preferred is IMERG Late, followed by
    IMERG Early. IMERG-Final has some issues. Currently only running using IMERG Early and Late
    """
    version = None
    # if date < (datetime.today() - timedelta(days=4*30)):
    #     version = "IMERG-FINAL"
    # elif date < (datetime.today() - timedelta(days=10)):
    #     version = "IMERG-LATE"
    # else:
    #     version = "IMERG-EARLY"
    if date < (datetime.today() - timedelta(days=2)):
        version = "IMERG-LATE"
    else:
        version = "IMERG-EARLY"
    return version

def download_precip(date, version, outputpath, secrets):
    """
    Parameters:
        date: datetime object that defines the date for which data is required
        version: which version of data to download - IMERG-LATE or IMERG-EARLY
        outputpath: path where the data should be downloaded
    =======
    TODO: Add ability to select either CHIRPS or IMERG data
    """
    if version == "IMERG-FINAL":
        link = f"ftp://arthurhou.pps.eosdis.nasa.gov/gpmdata/{date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')}/gis/3B-DAY-GIS.MS.MRG.3IMERG.{date.strftime('%Y%m%d')}-S000000-E235959.0000.V06A.tif"
    elif version == "IMERG-LATE":
        link = f"https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/{date.strftime('%Y')}/{date.strftime('%m')}/3B-HHR-L.MS.MRG.3IMERG.{date.strftime('%Y%m%d')}-S233000-E235959.1410.V06B.1day.tif"
    else:
        link = f"https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/early/{date.strftime('%Y')}/{date.strftime('%m')}/3B-HHR-E.MS.MRG.3IMERG.{date.strftime('%Y%m%d')}-S233000-E235959.1410.V06B.1day.tif"

    # Define the command (different for FINAL, same for EARLY and LATE)
    if version == "IMERG-FINAL":
        cmd = [
            "curl",
            '-o',
            outputpath,
            '--ssl-reqd',
            '-u',
            f'{secrets["imerg"]["username"]}:{secrets["imerg"]["pwd"]}',
            link
        ]
    else:
        cmd = [
            "wget",
            "-O",
            outputpath,
            "--user",
            f'{secrets["imerg"]["username"]}',
            '--password',
            f'{secrets["imerg"]["pwd"]}',
            link,
            '--no-proxy'
        ]
    log.debug("Downloading precipitation file: %s (%s)", date.strftime('%Y-%m-%d'), version)
    return run_command(cmd)

def download_tmax(year, outputpath):
    """
    Parameters:
        year: year for which data is to be downloaded, as a string
        outputpath: path where the data has to be saved
    """
    cmd = [
        'wget', 
        '-O', 
        f'{outputpath}', 
        f'ftp://ftp.cdc.noaa.gov/Datasets/cpc_global_temp/tmax.{year}.nc'
    ]
    log.debug("Downloading tmax: %s", year)
    return run_command(cmd)

def download_tmin(year, outputpath):
    """
    Parameters:
        year: year for which data is to be downloaded, as a string
        outputpath: path where the data has to be saved
    """
    cmd = [
        'wget', 
        '-O', 
        f'{outputpath}', 
        f'ftp://ftp.cdc.noaa.gov/Datasets/cpc_global_temp/tmin.{year}.nc'
    ]
    log.debug("Downloading tmin: %s", year)
    return run_command(cmd)

def download_uwnd(year, outputpath):
    """
    Parameters:
        year: year for which data is to be downloaded, as a string
        outputpath: path where the data has to be saved
    """
    cmd = [
        'wget', 
        '-O', 
        f'{outputpath}', 
        f'ftp://ftp2.psl.noaa.gov/Datasets/ncep.reanalysis/surface_gauss/uwnd.10m.gauss.{year}.nc'
    ]
    log.debug("Downloading uwnd: %s", year)
    return run_command(cmd)

def download_vwnd(year, outputpath):
    """
    Parameters:
        year: year for which data is to be downloaded, as a string
        outputpath: path where the data has to be saved
    """
    cmd = [
        'wget', 
        '-O', 
        f'{outputpath}', 
        f'ftp://ftp2.psl.noaa.gov/Datasets/ncep.reanalysis/surface_gauss/vwnd.10m.gauss.{year}.nc']
    log.debug("Downloading vwnd: %s", year)
    return run_command(cmd)

def download_data(begin, end, datadir, secrets):
    """Downloads the data between dates defined by begin and end

    Parameters:
        begin: Data will start downloading from this date, including this date
        end: Data will be downloaded until this date, including this date
        datedir: Base directory for downloading data
    """

    # Obtain list of dates to be downloaded
    # required_dates = [begin+timedelta(days=n) for n in range((end-begin).days)]
    required_dates = pd.date_range(begin, end)
    required_years = list(set([d.strftime("%Y") for d in required_dates]))

    # Download Precipitation
    log.debug("Downloading Precipitation")
    # with tqdm(required_dates) as pbar:
    for date in required_dates:
        # determine what kind of data is required
        data_version = determine_precip_version(date)
        outputpath = os.path.join(datadir, "precipitation", f"{date.strftime('%Y-%m-%d')}_IMERG.tif")
        # pbar.set_description(f"{date.strftime('%Y-%m-%d')} ({data_version})")
        download_precip(date, data_version, outputpath, secrets)
        # pbar.update(1)
    
    # Download other forcing data
    log.debug("Downloading TMax, TMin, UWnd, and VWnd")
    # with tqdm(required_years, total=len(required_years)*4) as pbar:
    for year in required_years:
        # pbar.set_description(f"{year} (TMax)")
        download_tmax(year, os.path.join(datadir, "tmax", year+'.nc'))
        # pbar.update(1)

        # pbar.set_description(f"{year} (TMin)")
        download_tmin(year, os.path.join(datadir, "tmin", year+'.nc'))
        # pbar.update(1)

        # pbar.set_description(f"{year} (UWnd)")
        download_uwnd(year, os.path.join(datadir, "uwnd", year+'.nc'))
        # pbar.update(1)

        # pbar.set_description(f"{year} (VWnd)")
        download_vwnd(year, os.path.join(datadir, "vwnd", year+'.nc'))
        # pbar.update(1)

def process_precip(basin_bounds,srcpath, dstpath, temp_datadir=None):
    """For any IMERG Precipitation file located at `srcpath` is clipped, scaled and converted to
    ASCII grid file and saved at `dstpath`. All of this is done in a temporarily created directory
    which can be controlled by the `datadir` path
    """
    if temp_datadir is not None and not os.path.isdir(temp_datadir):
        raise Exception(f"ERROR: {temp_datadir} directory doesn't exist")
    
    log.debug("Processing Precipitation file: %s", srcpath)

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

        # Scale down (EARLY)
        scaled_temp_file = os.path.join(tempdir, 'scaled.tif')
        cmd = [
            "gdal_calc.py", 
            "-A", 
            clipped_temp_file, 
            f"--calc=A*0.1", 
            f"--outfile={scaled_temp_file}", 
            "--NoDataValue=-9999", 
            "--format=GTiff"
        ]
        run_command(cmd)

        # Change format, and save as processed file
        aai_temp_file = os.path.join(tempdir, 'processed.tif')
        cmd = [
            'gdal_translate',
            '-of', 
            'aaigrid', 
            scaled_temp_file, 
            aai_temp_file
        ]
        run_command(cmd)

        # Move to destination
        shutil.move(aai_temp_file, dstpath)

def process_nc(basin_bounds,date, srcpath, dstpath, temp_datadir=None):
    """For TMax, TMin, UWnd and VWnd, the processing steps are same, and can be performed using
    this function.

    Parameters:
        date: Datetime object of the date of data
        srcpath: path of the nc file
        dstpath: path where the final ascii file will be saved
        temp_datadir: directory where the temporary data will be stored
    """
    if temp_datadir is not None and not os.path.isdir(temp_datadir):
        raise Exception(f"ERROR: {temp_datadir} directory doesn't exist")
    
    log.debug("Processing NC file: %s for date %s", srcpath, date.strftime('%Y-%m-%d'))

    with tempfile.TemporaryDirectory(dir=temp_datadir) as tempdir:
        # Convert from NC to Tif
        band = date.strftime("%-j")   # required band number is defined by `day of year`
        converted_tif_temp_file = os.path.join(tempdir, "converted.tif")

        # NC to tiff format
        cmd = ["gdal_translate", "-of", "Gtiff", "-b", band, srcpath, converted_tif_temp_file]
        run_command(cmd)

        #warping it so that lon represents -180 to 180 rather than 0 to 360
        warped_temp_file = os.path.join(tempdir, "warped.tif")
        cmd = ["gdalwarp", 
        "-s_srs",
        "'+proj=latlong +datum=WGS84 +pm=180dW'",
        "-dstnodata",
        "-9999.0",
        "-te",
        "-180",
        "-90",
        "180",
        "90",
        "-of",
        "GTiff",
        "-overwrite", 
        converted_tif_temp_file, 
        warped_temp_file]
        # runs and return non-zero code, so don't use run_command
        cmd = " ".join(cmd)
        subprocess.run(cmd,shell=True,stdout=subprocess.DEVNULL)
        
        
        # Change resolution
        scaled_temp_file = os.path.join(tempdir, "scaled.tif")
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
            warped_temp_file, 
            scaled_temp_file]

            
        run_command(cmd)

        # Convert GeoTiff to AAI
        aai_temp_file = os.path.join(tempdir, "final_aai.tif")
        cmd = ["gdal_translate", "-of", "aaigrid", scaled_temp_file, aai_temp_file]
        run_command(cmd)

        # Move file to destination
        shutil.move(aai_temp_file, dstpath)


def process_data(basin_bounds,raw_datadir, processed_datadir, begin, end, temp_datadir):
    if not os.path.isdir(temp_datadir):
        os.makedirs(temp_datadir)

    #### Process precipitation ####
    log.debug("Processing Precipitation")
    raw_datadir_precip = os.path.join(raw_datadir, "precipitation")
    processed_datadir_precip = os.path.join(processed_datadir, "precipitation")


    # with tqdm(os.listdir(raw_datadir_precip)) as pbar:
    ds = pd.date_range(begin, end)
    for srcname in os.listdir(raw_datadir_precip):
        if datetime.strptime(srcname.split(os.sep)[-1].split("_")[0], "%Y-%m-%d") in ds:
            srcpath = os.path.join(raw_datadir_precip, srcname)
            dstpath = os.path.join(processed_datadir_precip, srcname.replace("tif", "asc"))

            # pbar.set_description(f"Precipitation: {srcname.split('_')[0]}")
            process_precip(basin_bounds,srcpath, dstpath, temp_datadir)
            # pbar.update(1)

    #### Process NC files ####
    # required_dates = [begin+timedelta(days=n) for n in range((end-begin).days)]
    required_dates = pd.date_range(begin, end)
    #### Process TMAX ####
    log.debug("Processing TMAX")
    raw_datadir_tmax = os.path.join(raw_datadir, "tmax")
    processed_datadir_tmax = os.path.join(processed_datadir, "tmax")

    # with tqdm(required_dates) as pbar:
    for date in required_dates:
        srcpath = os.path.join(raw_datadir_tmax, date.strftime('%Y')+'.nc')
        dstpath = os.path.join(processed_datadir_tmax, f"{date.strftime('%Y-%m-%d')}_TMAX.asc")

        # pbar.set_description(f"TMAX: {date.strftime('%Y-%m-%d')}")
        process_nc(basin_bounds,date, srcpath, dstpath, temp_datadir)
        # pbar.update(1)
    
    #### Process TMin ####
    log.debug("Processing TMIN")
    raw_datadir_tmin = os.path.join(raw_datadir, "tmin")
    processed_datadir_tmin = os.path.join(processed_datadir, "tmin")

    # with tqdm(required_dates) as pbar:
    for date in required_dates:
        srcpath = os.path.join(raw_datadir_tmin, date.strftime('%Y')+'.nc')
        dstpath = os.path.join(processed_datadir_tmin, f"{date.strftime('%Y-%m-%d')}_TMIN.asc")

        # pbar.set_description(f"TMIN: {date.strftime('%Y-%m-%d')}")
        process_nc(basin_bounds,date, srcpath, dstpath, temp_datadir)
        # pbar.update(1)

    #### Process UWND ####
    log.debug("Processing UWND")
    raw_datadir_uwnd = os.path.join(raw_datadir, "uwnd")
    daily_datadir_uwnd = os.path.join(raw_datadir, "uwnd_daily")
    processed_datadir_uwnd = os.path.join(processed_datadir, "uwnd")

    uwnd_files = [os.path.join(raw_datadir_uwnd, f) for f in os.listdir(raw_datadir_uwnd)]

    for uwnd_f in uwnd_files:
        xr.open_dataset(uwnd_f).resample(time='1D').mean().to_netcdf(os.path.join(daily_datadir_uwnd, uwnd_f.split(os.sep)[-1]))
        # xr.open_dataset(vwnd_f).resample(time='1D').mean().to_netcdf(os.path.join(vwnd_outdir, vwnd_f.split(os.sep)[-1]))

    # with tqdm(required_dates) as pbar:
    for date in required_dates:
        srcpath = os.path.join(daily_datadir_uwnd, date.strftime('%Y')+'.nc')
        dstpath = os.path.join(processed_datadir_uwnd, f"{date.strftime('%Y-%m-%d')}_UWND.asc")

        # pbar.set_description(f"UWND: {date.strftime('%Y-%m-%d')}")
        process_nc(basin_bounds,date, srcpath, dstpath, temp_datadir)
        # pbar.update(1)

    #### Process VWND ####
    log.debug("Processing VWND")
    raw_datadir_vwnd = os.path.join(raw_datadir, "vwnd")
    daily_datadir_vwnd = os.path.join(raw_datadir, "vwnd_daily")
    processed_datadir_vwnd = os.path.join(processed_datadir, "vwnd")

    vwnd_files = [os.path.join(raw_datadir_vwnd, f) for f in os.listdir(raw_datadir_vwnd)]

    for vwnd_f in vwnd_files:
        xr.open_dataset(vwnd_f).resample(time='1D').mean().to_netcdf(os.path.join(daily_datadir_vwnd, vwnd_f.split(os.sep)[-1]))

    # with tqdm(required_dates) as pbar:
    for date in required_dates:
        srcpath = os.path.join(daily_datadir_vwnd, date.strftime('%Y')+'.nc')
        dstpath = os.path.join(processed_datadir_vwnd, f"{date.strftime('%Y-%m-%d')}_VWND.asc")

        # pbar.set_description(f"VWND: {date.strftime('%Y-%m-%d')}")
        process_nc(basin_bounds,date, srcpath, dstpath, temp_datadir)
        # pbar.update(1)


def get_newdata(basin_name,basin_bounds,data_dir, startdate, enddate, secrets_file, download=True, process=True):
    datadir = os.path.join(data_dir,'basins',basin_name,'')
    raw_datadir = os.path.join(data_dir, "raw",'')
    processed_datadir = os.path.join(datadir, "processed",'')
    temp_datadir = os.path.join(datadir, "temp",'')
    
    ####Creating the required directories####

    dir_paths=[raw_datadir,processed_datadir]
    #Creating data directory if does not exist
    create_directory(datadir)

    #Creating temp directory if does not exist
    create_directory(temp_datadir)
    data_vars_folder_names=['precipitation','tmax','tmin','uwnd','vwnd']
    raw_data_vars_folder_names=['uwnd_daily','vwnd_daily']
    
    for dir_path in dir_paths:
        
        # Making directories with each name in data_vars_folder_names in dir_paths
        for data_var_name in data_vars_folder_names:
            temp_dir_path_var=os.path.join(dir_path,data_var_name,'')
            create_directory(temp_dir_path_var)

        # Making directories with each name in raw_data_vars_folder_names in raw_datadir
        if(dir_path==raw_datadir):
            for data_var_name in raw_data_vars_folder_names:
                temp_dir_path_var=os.path.join(dir_path,data_var_name,'')
                create_directory(temp_dir_path_var)
    
    secrets = configparser.ConfigParser()
    secrets.read(secrets_file)  # assuming there's a secret ini file with user/pwd

    enddate = enddate

    startdate_str = startdate.strftime("%Y-%m-%d")
    enddate_str = enddate.strftime("%Y-%m-%d")

    log.log(NOTIFICATION, "Started Downloading and Processing data from %s -> %s", startdate_str, enddate_str)
    log.debug("Raw data directory: %s", raw_datadir)
    log.debug("Processed data directory: %s", processed_datadir)

    #### DATA DOWNLOADING ####
    if download:
        download_data(startdate, enddate, raw_datadir, secrets)

    #### DATA PROCESSING ####
    if process:
        process_data(basin_bounds,raw_datadir, processed_datadir, startdate, enddate, temp_datadir)