import geopandas as gpd
import numpy as np
import pandas as pd
import netCDF4 
from rat.utils.utils import round_up


def get_j3_tracks(reservoir, reservoir_column_dict, tracks_df, custom_reservoir_range_dict):
    """Returns a list of Jason-3 ground tracks and the min-max latitudes of intersection

    Args:
        reservoir (geopandas dataframe): Geodataframe object of reservoir having only one row
        tracks_df (geopandas dataframe): Geodataframe object of altimeter tracks; Should contains 'track' column 
            containing the track numbers, and a geometry column containing linestrings of the path
    
    Returns:
        (dict) if overlapping tracks are found. 
            {'tracks': (list) of tracks that overlap with reservoir
             'lat_range': (list) of (tuples) of minimum and maximum latitudes for each track}
        (None) if no overlapping tracks are found
    """
    gdf = tracks_df
    
    res = reservoir
    
    res_name = str(res[reservoir_column_dict['unique_identifier']])
    res_geom = res['geometry'] #.unary_union
    tracks = gdf[gdf.intersects(res_geom)]['track'].unique()

    minmax_lats = []
    if(custom_reservoir_range_dict):
        if res_name in custom_reservoir_range_dict.keys():
            minmax_lats.append(custom_reservoir_range_dict[res_name])
        else:
            for track in tracks:
                track_geom = gdf[gdf['track']==track] #.geometry
                intersect = gpd.clip(track_geom, res_geom)
                minx, miny, maxx, maxy = intersect.geometry.unary_union.bounds

                minmax_lats.append((round_up(miny,6), round_up(maxy,6)))
    else:
        for track in tracks:
            track_geom = gdf[gdf['track']==track] #.geometry
            intersect = gpd.clip(track_geom, res_geom)
            minx, miny, maxx, maxy = intersect.geometry.unary_union.bounds

            minmax_lats.append((round_up(miny,6), round_up(maxy,6)))
    
    if len(tracks) == 0:
        return_data = None
    else:
        return_data = {'tracks': tracks, 'lat_range' : minmax_lats}
    
    return return_data


import netCDF4
import zipfile
import os
import os, sys
import os.path
import numpy as np
import pandas as pd
from netCDF4 import Dataset
from datetime import datetime
from ftplib import FTP
import glob
import time
from numpy import empty
from numpy import loadtxt
import math
import statistics
from scipy import io
from scipy import stats
from scipy import interpolate
from scipy.cluster.vq import kmeans2
from sklearn.cluster import KMeans

import configparser
from datetime import timedelta, datetime


def _get_suffix():
    return datetime.today().strftime("%Y%m%d_%H_%M_%S")


def _convert_partial_year(number):
    is_leap = lambda y: True if y % 400 == 0 else False if y % 100 == 0 else True if y % 4 == 0 else False

    year = int(number)
    d = timedelta(days=(number - year)*(365 + is_leap(year)))
    day_one = datetime(year,1,1)
    date = d + day_one
    return date


def download_data(username, password, savedir, passnum, startcycle, endcycle, series):
    """Downloads Jason data

    Args:
        username (str): Username of the AVISO account that'll be used to download data
        password (str): Password of the AVISO account that'll be used to download data
        savedir (str): Directory where to save data
        passnum (int): Which pass-number to download? - analogous to location/space
        startcycle (int): Start of the cycle - analogous to start time of required data
        endcycle (int): End of cycle - analogous to end time of required data
        series (int): Which Jason series of data to download? (1, 2, 3)
    """
    username = username
    PW = password
    if series == 3:
        # Jason3
        if len(str(passnum)) == 1:
            passnum2 = "00" + str(passnum)
        elif len(str(passnum)) == 2:
            passnum2 = "0" + str(passnum)
        else:
            passnum2 = str(passnum)

        count = 0
        for cycle in range(startcycle, endcycle + 1):
            count = count + 1
            if len(str(cycle)) == 1:
                cycle2 = "00" + str(cycle)
            elif len(str(cycle)) == 2:
                cycle2 = "0" + str(cycle)
            else:
                cycle2 = str(cycle)

            savepath = savedir + "/j%s_%s/gdr_f/cycle_%s/" % (
                str(series),
                str(passnum2),
                str(cycle2),
            )
            # try:
            os.makedirs(os.path.dirname(savepath), exist_ok=True)  # will not skip if empty directory exists, coz sometimes empty directory will be created and we don't want to skip it
            # except FileExistsError:
            #     print(f"Existing data found {os.path.dirname(savepath).split(os.sep)[-1]}; Skipping")
            #     continue
            os.chdir(savepath)

            ftp = FTP("ftp-access.aviso.altimetry.fr")
            ftp.login(user=str(username), passwd=str(PW))
            ftp.cwd(
                "geophysical-data-record/jason-%s/gdr_f/cycle_%s"
                % (str(series), str(cycle2))
            )

            if cycle < 22:
                fnbase = "JA%s_GPN_2PfP%s_%s_" % (
                    str(series),
                    str(cycle2),
                    str(passnum2),
                )
            else:
                fnbase = "JA%s_GPN_2PfP%s_%s_" % (
                    str(series),
                    str(cycle2),
                    str(passnum2),
                )
            if cycle < 167:
                filesineed = [filename for filename in ftp.nlst() if fnbase in filename]
                for filename in filesineed:
                    local_filename = os.path.join(os.getcwd(), filename)
                    if not (os.path.exists(local_filename)):       # Will not download if the file already exists
                        with open(local_filename, "wb") as f:
                            ftp.retrbinary("RETR %s" % filename, f.write)
                        print(f"Downloaded file for cycle {cycle} - {filename}")
                    else:
                        print(f"Existing file found for cycle {cycle} - {filename}")
            else:
                fnbase2 = "JA%s_GPN_2PfP%s_%s_" % (
                    str(series),
                    str(cycle2),
                    str(passnum2),
                )
                filesineed2 = [
                    filename2 for filename2 in ftp.nlst() if fnbase2 in filename2
                ]
                for filename2 in filesineed2:
                    local_filename2 = os.path.join(os.getcwd(), filename2)
                    if not (os.path.exists(local_filename2)):            # Will not download if the file already exists
                        with open(local_filename2, "wb") as f2:
                            ftp.retrbinary("RETR %s" % filename2, f2.write)
                        print(f"Downloaded file for cycle {cycle} - {filename2}")
                    else:
                        print(f"Existing file found for cycle {cycle} - {filename2}")
            
            print("Finished downloading cycle_%s" % (str(cycle2)))
    else:
        if len(str(passnum)) == 1:
            passnum3 = "00" + str(passnum)
        elif len(str(passnum)) == 2:
            passnum3 = "0" + str(passnum)
        else:
            passnum3 = str(passnum)

        count = 0
        for cycle in range(startcycle, endcycle + 1):
            count = count + 1
            if len(str(cycle)) == 1:
                cycle3 = "00" + str(cycle)
            elif len(str(cycle)) == 2:
                cycle3 = "0" + str(cycle)
            else:
                cycle3 = str(cycle)

            ftp = FTP("ftp-access.aviso.altimetry.fr")
            ftp.login(user=str(username), passwd=str(PW))
            ftp.cwd(
                "geophysical-data-record/jason-%s/gdr_d/cycle_%s"
                % (str(series), str(cycle3))
            )
            fnbase3 = "JA%s_GPN_2PdP%s_%s_" % (str(series), str(cycle3), str(passnum3))
            savepath2 = savedir + "/j%s_%s/gdr_d/cycle_%s/" % (
                str(series),
                str(passnum3),
                str(cycle3),
            )
            os.makedirs(os.path.dirname(savepath2))
            os.chdir(savepath2)
            filesineed3 = [
                filename3 for filename3 in ftp.nlst() if fnbase3 in filename3
            ]
            for filename3 in filesineed3:
                local_filename3 = os.path.join(os.getcwd(), filename3)
                if not (os.path.exists(local_filename3)):            # Will not download if the file already exists
                    with open(local_filename3, "wb") as f3:
                        ftp.retrbinary("RETR %s" % filename3, f3.write)
                    print(f"Downloaded file for cycle {cycle} - {filename3}")
                else:
                    print(f"Existing file found for cycle {cycle} - {filename3}")
            
            print("Finished downloading cycle_%s" % (str(cycle3)))
    print("Data Download complete")


def extract_data(datadir, savedir, minlat, maxlat, passnum, series, startcycle, endcycle, suffix="auto"):
    """Extract and process Jason data

    Args:
        datadir (str):  Directory of raw data
        savedir (str):  Directory where to save processed data
        minlat (float): minimum latitude of ROI
        maxlat (float): maximum latitude of ROI
        passnum (int):  pass number of data
        series (int):   Which Jason series is the data from?
        startcycle (int): Start of the cycle - analogous to start time of required data
        endcycle (int): End of cycle - analogous to end time of required data
        suffix (str, optional): Suffix of directory where data will be saved. If left at "auto", 
                        the time of run will be used. Defaults to "auto".
    """
    directory = datadir
    lat_range = [minlat, maxlat]
    directory2 = "/%s/" % (directory)

    if suffix == "auto":
        suffix = _get_suffix()

    wgs84_to_tp = 0.7

    if len(str(passnum)) == 1:
        passnum22 = "00" + str(passnum)
    elif len(str(passnum)) == 2:
        passnum22 = "0" + str(passnum)
    else:
        passnum22 = str(passnum)

    filename = "j%s_gdr_p%s_%s_%s_%s_info" % (
        str(series),
        str(passnum22),
        str(suffix),
        str(minlat),
        str(maxlat),
    )
    savepath11 = savedir + "/j%s_%s/extract_%s/" % (
        str(series),
        str(passnum22),
        str(suffix),
    )
    os.makedirs(os.path.dirname(savepath11),exist_ok=True)
    completename = os.path.join(savepath11, filename + ".txt")

    # Extract complete data if file not exists otherwise append to the existing file
    if not os.path.exists(completename):
        extract_condition = True
        text_file = open(completename, "w")
        print(f"Creating a new extract file - {filename}")
    else:
        extract_condition = False
        text_file = open(completename, "r+")
        file_lines = text_file.readlines()
        text_file.seek(0,2)
        print(f"Appending to extract file - {filename}")

    ##Scale Factors & FillValues & Read Data Record
    if series == 3:
        pattern = os.path.join(datadir, "gdr_f", "cycle_*", "*.nc")
        allpath = glob.glob(pattern)
        for fn in range(len(allpath)):
            dir_cycle_no = int(os.path.basename(os.path.dirname(allpath[fn]))[-3:])
            # Extract data only if raw file downloaded in current run or if the file does not exist
            
            if(extract_condition or (dir_cycle_no>= startcycle and dir_cycle_no<= endcycle)):
                print(f"Extracting data from raw netcdf file for cycle: {dir_cycle_no}")
                allpath1 = str(allpath[fn])
                allpath11 = os.path.basename(allpath1)
                data = netCDF4.Dataset(allpath1)
                cycno2 = allpath11[12:15]
                ###-- 1Hz record --
                iono_corr_gim_ku_sc = 0.0001
                iono_corr_gim_ku_FV = 32767 * iono_corr_gim_ku_sc
                solid_earth_tide_sc = 0.0001
                solid_earth_tide_FV = 32767 * solid_earth_tide_sc
                pole_tide_sc = 0.0001
                pole_tide_FV = 32767 * pole_tide_sc
                ### -- 20Hz record --
                alt_20hz_sc = 0.0001
                alt_20hz_add = 1300000
                alt_20hz_FV = 2147483647 * alt_20hz_sc + alt_20hz_add
                lat_20hz_sc = 0.000001
                lat_20hz_FV = 2147483647 * lat_20hz_sc
                lon_20hz_sc = 0.000001
                lon_20hz_FV = 2147483647 * lon_20hz_sc
                model_dry_tropo_corr_sc = 0.0001
                model_dry_tropo_corr_FV = 32767 * model_dry_tropo_corr_sc
                model_wet_tropo_corr_sc = 0.0001
                model_wet_tropo_corr_FV = 32767 * model_wet_tropo_corr_sc
                ice_range_20hz_ku_sc = 0.0001
                ice_range_20hz_ku_add = 1300000
                ice_range_20hz_ku_FV = (
                    2147483647 * ice_range_20hz_ku_sc + ice_range_20hz_ku_add
                )
                ice_sig0_20hz_ku_sc = 0.01
                ice_sig0_20hz_ku_FV = 32767 * ice_sig0_20hz_ku_sc
                # percent = 100 * (fn + 1) / (len(allpath) + 1)
                # print("%1.3f percent complete" % (percent))
                ###-- 1Hz record --
                alt_state_band_status_flag = (
                    data.groups["data_01"]
                    .groups["ku"]
                    .variables["alt_state_band_status_flag"][:]
                )
                iono_corr_gim_ku = (
                    data.groups["data_01"].groups["ku"].variables["iono_cor_gim"][:]
                )  # *iono_corr_gim_ku_sc
                solid_earth_tide = data.groups["data_01"].variables["solid_earth_tide"][
                    :
                ]  # *solid_earth_tide_sc
                pole_tide = data.groups["data_01"].variables["pole_tide"][
                    :
                ]  # *pole_tide_sc

                ### -- 20Hz record --
                indx_20hzIn01hz = data.groups["data_20"].variables["index_1hz_measurement"][
                    :
                ]
                alt_20hz = data.groups["data_20"].variables["altitude"][
                    :
                ]  # *alt_20hz_sc+alt_20hz_add
                lat_20hz = data.groups["data_20"].variables["latitude"][:]  # *lat_20hz_sc
                lon_20hz = data.groups["data_20"].variables["longitude"][:]  # *lon_20hz_sc
                time_20hz = data.groups["data_20"].variables["time"][:]
                model_dry_tropo_corr = data.groups["data_20"].variables[
                    "model_dry_tropo_cor_measurement_altitude"
                ][
                    :
                ]  # *model_dry_tropo_corr_sc
                model_wet_tropo_corr = data.groups["data_20"].variables[
                    "model_wet_tropo_cor_measurement_altitude"
                ][
                    :
                ]  # *model_wet_tropo_corr_sc
                ice_range_20hz_ku = (
                    data.groups["data_20"].groups["ku"].variables["range_ocog"][:]
                )  # *ice_range_20hz_ku_sc+ice_range_20hz_ku_add
                ice_sig0_20hz_ku = (
                    data.groups["data_20"].groups["ku"].variables["sig0_ocog"][:]
                )  # *ice_sig0_20hz_ku_sc
                ice_qual_flag_20hz_ku = (
                    data.groups["data_20"].groups["ku"].variables["ocog_qual"][:]
                )

                for p in range(len(alt_20hz)):
                    if lat_20hz[p] < lat_range[0] or lat_20hz[p] > lat_range[1]:
                        continue
                    if model_dry_tropo_corr[p] == model_dry_tropo_corr_FV:
                        dry_count = 1
                    else:
                        dry_count = 0

                    if model_wet_tropo_corr[p] == model_wet_tropo_corr_FV:
                        wet_count = 1
                    else:
                        wet_count = 0

                    if iono_corr_gim_ku[indx_20hzIn01hz[p]] == iono_corr_gim_ku_FV:
                        iono_count = 1
                    else:
                        iono_count = 0

                    if solid_earth_tide[indx_20hzIn01hz[p]] == solid_earth_tide_FV:
                        sTide_count = 1
                    else:
                        sTide_count = 0

                    if pole_tide[indx_20hzIn01hz[p]] == pole_tide_FV:
                        pTide_count = 1
                    else:
                        pTide_count = 0

                    if alt_state_band_status_flag[indx_20hzIn01hz[p]] != 0:
                        kFlag_count = 1
                    else:
                        kFlag_count = 0

                    if lat_20hz[p] == lat_20hz_FV:
                        lat_count = 1
                    else:
                        lat_count = 0

                    if ice_qual_flag_20hz_ku[p] != 0:
                        ice_count = 1
                    else:
                        ice_count = 0

                    media_corr = (
                        model_dry_tropo_corr[p]
                        + model_wet_tropo_corr[p]
                        + iono_corr_gim_ku[indx_20hzIn01hz[p]]
                        + solid_earth_tide[indx_20hzIn01hz[p]]
                        + pole_tide[indx_20hzIn01hz[p]]
                    )
                    mjd_20hz = time_20hz[p] * 1 / 86400 + 51544
                    icehgt_20hz = (
                        alt_20hz[p] - (media_corr + ice_range_20hz_ku[p]) + wgs84_to_tp
                    )

                    Flags = (
                        dry_count
                        + wet_count
                        + iono_count
                        + sTide_count
                        + pTide_count
                        + kFlag_count
                        + lat_count
                        + ice_count
                    )
                    write_line = "%4s %4s %4s %4s %3s %4s %4s %4s %4s %4s %20.6f %20.6f %20.6f %20.6f %20.6f %10.3f\n" % (
                            dry_count,
                            wet_count,
                            iono_count,
                            sTide_count,
                            pTide_count,
                            kFlag_count,
                            lat_count,
                            ice_count,
                            Flags,
                            cycno2,
                            mjd_20hz,
                            lon_20hz[p],
                            lat_20hz[p],
                            icehgt_20hz,
                            ice_sig0_20hz_ku[p],
                            ice_qual_flag_20hz_ku[p],
                        )
                    if not extract_condition:
                        if write_line not in file_lines:
                            text_file.write(write_line)
                        else:
                            print("Nothing new to append")
                    else:
                        text_file.write(write_line)

    else:
        allpath = glob.glob(savedir + directory2 + "gdr_d/" + "cycle_*" + "\\*.nc")
        for fn in range(len(allpath)):
            dir_cycle_no = int(os.path.basename(os.path.dirname(allpath[fn]))[-3:])
             # Extract data only if raw file downloaded in current run or if the file does not exist
            if((dir_cycle_no <= endcycle and dir_cycle_no >= startcycle) or (not os.path.exists(completename))):
                print(f"Extracting data from raw file for cycle: {dir_cycle_no}")
                allpath1 = str(allpath[fn])
                allpath11 = os.path.basename(allpath1)
                cycno2 = allpath11[12:15]
                data = netCDF4.Dataset(allpath1)
                ### -- 20Hz record --
                lat_20hz_sc = 0.000001
                lat_20hz_FV = 2147483647 * lat_20hz_sc
                lat_20hz = data.variables["lat_20hz"][:]
                lon_20hz_sc = 0.000001
                lon_20hz_FV = 2147483647 * lon_20hz_sc
                lon_20hz = data.variables["lon_20hz"][:]
                time_20hz = data.variables["time_20hz"][:]
                time_20hz_FV = 1.8446744073709552e19
                ice_range_20hz_ku_sc = 0.0001
                ice_range_20hz_ku_add = 1300000
                ice_range_20hz_ku_FV = (
                    2147483647 * ice_range_20hz_ku_sc + ice_range_20hz_ku_add
                )
                ice_range_20hz_ku = data.variables["ice_range_20hz_ku"][
                    :
                ]  # *ice_range_20hz_ku_sc+ice_range_20hz_ku_add
                ice_qual_flag_20hz_ku = data.variables["ice_qual_flag_20hz_ku"][:]
                ice_qual_flag_20hz_ku_FV = 127
                ice_sig0_20hz_ku_sc = 0.01
                ice_sig0_20hz_ku_FV = 32767 * ice_sig0_20hz_ku_sc
                ice_sig0_20hz_ku = data.variables["ice_sig0_20hz_ku"][:]
                alt_20hz_sc = 0.0001
                alt_20hz_add = 1300000
                alt_20hz_FV = 2147483647 * alt_20hz_sc + alt_20hz_add
                alt_20hz = data.variables["alt_20hz"][:]  # *alt_20hz_sc+alt_20hz_add
                ### -- 1Hz record --
                alt_state_flag_ku_band_status_FV = 127
                alt_state_flag_ku_band_status = data.variables[
                    "alt_state_flag_ku_band_status"
                ][:]
                lat_sc = 0.000001
                lat = data.variables["lat"][:]  # *lat_sc
                lon_sc = 0.000001
                lon = data.variables["lon"][:]  # *lon_sc
                model_dry_tropo_corr_sc = 0.0001
                model_dry_tropo_corr_FV = 32767
                model_dry_tropo_corr = data.variables["model_dry_tropo_corr"][
                    :
                ]  # *model_dry_tropo_corr_sc
                model_wet_tropo_corr_sc = 0.0001
                model_wet_tropo_corr_FV = 32767
                model_wet_tropo_corr = data.variables["model_wet_tropo_corr"][
                    :
                ]  # *model_wet_tropo_corr_sc
                iono_corr_gim_ku_sc = 0.0001
                iono_corr_gim_ku_FV = 32767
                iono_corr_gim_ku = data.variables["iono_corr_gim_ku"][
                    :
                ]  # *iono_corr_gim_ku_sc
                solid_earth_tide_sc = 0.0001
                solid_earth_tide_FV = 32767
                solid_earth_tide = data.variables["solid_earth_tide"][
                    :
                ]  # *solid_earth_tide_sc
                pole_tide_sc = 0.0001
                pole_tide_FV = 32767
                pole_tide = data.variables["pole_tide"][:] * pole_tide_sc
                percent = 100 * (fn + 1) / (len(allpath) + 1)
                print("%1.3f percent complete" % (percent))
                for p in range(len(lat)):
                    if str(lat[p]) < lat_range[0] or str(lat[p]) > lat_range[1]:
                        continue
                    if model_dry_tropo_corr[p] == model_dry_tropo_corr_FV:
                        continue
                        dry_count = 1
                    else:
                        dry_count = 0
                    if model_wet_tropo_corr[p] == model_wet_tropo_corr_FV:
                        wet_count = 1
                    else:
                        wet_count = 0

                    if iono_corr_gim_ku[p] == iono_corr_gim_ku_FV:
                        iono_count = 1
                    else:
                        iono_count = 0

                    if solid_earth_tide[p] == solid_earth_tide_FV:
                        sTide_count = 1
                    else:
                        sTide_count = 0

                    if pole_tide[p] == pole_tide_FV:
                        pTide_count = 1
                    else:
                        pTide_count = 0

                    if alt_state_flag_ku_band_status[p] != 0:
                        kFlag_count = 1
                    else:
                        kFlag_count = 0

                    media_corr = (
                        model_dry_tropo_corr[p]
                        + model_wet_tropo_corr[p]
                        + iono_corr_gim_ku[p]
                        + solid_earth_tide[p]
                        + pole_tide[p]
                    )
                    for q in range(len(lat_20hz[0, :])):
                        if lat_20hz[p, q] == lat_20hz_FV:
                            lat_count = 1
                        else:
                            lat_count = 0
                        if ice_qual_flag_20hz_ku[p, q] != 0:
                            ice_count = 1
                        else:
                            ice_count = 0

                        mjd_20hz = time_20hz[p, q] * 1 / 86400 + 51544
                        icehgt_20hz = alt_20hz[p, q] - (
                            media_corr + ice_range_20hz_ku[p, q]
                        )
                        Flags = (
                            dry_count
                            + wet_count
                            + iono_count
                            + sTide_count
                            + pTide_count
                            + kFlag_count
                            + lat_count
                            + ice_count
                        )
                        if Flags == 0:
                            write_line = "%4s %4s %4s %4s %3s %4s %4s %4s %4s %4s %20.6f %20.6f %20.6f %20.6f %20.6f %10.3f\n" % (
                                    dry_count,
                                    wet_count,
                                    iono_count,
                                    sTide_count,
                                    pTide_count,
                                    kFlag_count,
                                    lat_count,
                                    ice_count,
                                    Flags,
                                    cycno2,
                                    mjd_20hz,
                                    lon_20hz[p, q],
                                    lat_20hz[p, q],
                                    icehgt_20hz,
                                    ice_sig0_20hz_ku[p, q],
                                    ice_qual_flag_20hz_ku[p, q],
                                )
                            if not extract_condition:
                                if write_line not in file_lines:
                                    text_file.write(write_line)
                                else:
                                    print("Nothing new to append")
                            else:
                                text_file.write(write_line)
    print("NetCDF Extraction Complete")
    return savepath11


def generate_timeseries(extracteddir, savepath, minlat, maxlat, geoiddata):
    import warnings
    warnings.filterwarnings("ignore")
    lat_range=[minlat, maxlat]
    # directory2="/%s/" %(directory)
    # directory4='%s/' %(directory3)

    #load geoid
    lonbp=io.loadmat(geoiddata)['lonbp']
    latbp=io.loadmat(geoiddata)['latbp']
    grid=io.loadmat(geoiddata)['grid']

    #Import Data from multiple tracks for a reservoir to get one time-series
    extracted_file_pattern = os.path.join(extracteddir,'j*', 'extract_*', '*.txt')
    extracted_files = glob.glob(extracted_file_pattern)

    alldata = [] ## Initialising in case the code doesn't enter loop, alldata will be defined
    for i in range(len(extracted_files)):
        filepath = str(extracted_files[i])
        alldata = np.loadtxt(filepath)
        pathname = extracted_files[i]
        base=os.path.basename(str(extracted_files))

    # If there is no data, return 
    if len(alldata) == 0:
        print(f"No data found to generate time-series - {os.path.basename(savepath)}")
        return

    latitude = alldata[:, 12]
    ind = []
    for j in range(len(alldata)):
        if latitude[j] > minlat and latitude[j] < maxlat:
            ind.append(j)

    alldata_info = alldata[ind,9:15]
    cycno2 = alldata_info[:,0]
    time = alldata_info[:,1]
    lon = alldata_info[:,2]
    lat = alldata_info[:,3]
    hgt = alldata_info[:,4]
    sig = alldata_info[:,5]
    hgt = hgt[~np.isnan(hgt)]

    # IQ Range for hgt
    IQR = stats.iqr(hgt,interpolation = 'midpoint')
    Lower_Lim_All = np.quantile(hgt,0.25)-(1.5*IQR)
    Upper_Lim_All = np.quantile(hgt,0.75)+(1.5*IQR)
    indx_limit_All = []
    for i in range(len(hgt)):
        if hgt[i]>Lower_Lim_All and hgt[i]<Upper_Lim_All and ~any(np.isnan(alldata[i,:])):
            indx_limit_All.append(i)
    Data_Seg_IQR=alldata_info[indx_limit_All,:]

    # Uncertainty function
    def uncertainty(h):
        if len(h)==0:
            rmse=0
        else:
            e=abs(h-np.mean(h))
            sigma2=(sum(e**2)/(len(h)-1))
            rmse=math.sqrt((sigma2/len(h)))
        return rmse

    # Altimetry Outlier
    count2=0
    cycle2=np.unique(Data_Seg_IQR[:,0])
    timeseries_report=np.zeros([len(cycle2),9])
    for k in range(len(cycle2)): 
        count2=count2+1
        cycN=cycle2[k]
        indy=np.where(Data_Seg_IQR[:,0]==cycle2[k])
        cyc_Pass=Data_Seg_IQR[indy,:][0]
        cyc_Pass=cyc_Pass[~np.isnan(cyc_Pass).any(axis=1)]
        ini_sampl=len(cyc_Pass)
        out_mean=np.mean(cyc_Pass[:,4])
        out_stdev=np.std(cyc_Pass[:,4])
        out_err=cyc_Pass[:,4]-out_mean
        Range=max(cyc_Pass[:,4])-min(cyc_Pass[:,4])
        cyc=cyc_Pass[:,4]

        while Range > 5:
                kmeans = KMeans(n_clusters=2,random_state=0).fit(cyc.reshape(-1,1))
                indx=kmeans.labels_
                cyc_Pass=pd.DataFrame(cyc_Pass)
                cyc_Pass=np.array(cyc_Pass.loc[indx==stats.mode(indx).mode])
                Range=max(cyc_Pass[:,4])-min(cyc_Pass[:,4])
                break
        out_mean=np.mean(cyc_Pass[:,4])
        out_stdev=np.std(cyc_Pass[:,4])
        out_err=cyc_Pass[:,4]-out_mean
        stdhgt_cyc=uncertainty(cyc_Pass[:,4])
        while out_stdev > 0.3:
                out_err=cyc_Pass[:,4]-out_mean
                locate=np.where((abs(out_err)==max(abs(out_err))))
                cyc_Pass=np.delete(cyc_Pass,locate[0],axis=0)
                out_stdev=np.std(cyc_Pass[:,4])
                continue
                break
        final_sampl=len(cyc_Pass)
        avgmjd=np.mean(cyc_Pass[:,1])
        Julyr=(avgmjd+2108-50000)/365.25 +1990
        Hgt_WGS =np.mean(cyc_Pass[:,4])
        if((len(cyc_Pass[:,4])!=0)):
            timeseries_report[count2-1,:]=[cycN, Julyr, statistics.mean(cyc_Pass[:,2]), statistics.mean(cyc_Pass[:,3]), Hgt_WGS,  uncertainty(cyc_Pass[:,4]), np.std(cyc_Pass[:,4]), statistics.mean(cyc_Pass[:,5]), final_sampl*100/ini_sampl]


    # IQ range for timeseries_report
    IQR2=stats.iqr(timeseries_report[:,4],interpolation = 'midpoint')
    Lower_Lim=np.quantile(timeseries_report[:,4],0.25)-(1.5*IQR2)
    Upper_Lim=np.quantile(timeseries_report[:,4],0.75)+(1.5*IQR2)

    indx_limit=[]
    for i2 in range(len(timeseries_report)):
        if timeseries_report[i2,4]>Lower_Lim and timeseries_report[i2,4]<Upper_Lim and ~any(np.isnan(timeseries_report[i2,:])):
            indx_limit.append(i2)
    FinalSeries=timeseries_report[indx_limit,:]
    ip=interpolate.interp2d(lonbp,latbp,grid,kind='linear')
    N=ip(FinalSeries[:,2],FinalSeries[:,3])
    Hgt_Datum=FinalSeries[:,4]-N[0]-0.7
    FinalSeries[:,4]=Hgt_Datum

    BB=FinalSeries[:,[0,4,5,1]]
    
    # np.savetxt(savepath, BB)
    df = pd.DataFrame(data=BB, columns=['cycle_num', 'H [m w.r.t. EGM2008 Geoid]', 'Uncertainty [m]', 'year_frac'])
    df = df[df['cycle_num'] != 0]
    df['date'] = df['year_frac'].apply(_convert_partial_year)
    # df['date'] = df['year_frac'].apply(lambda x: netCDF4.num2year(x, only_use_python_datetimes=True))
    # Save (Writing new file)
    df.to_csv(savepath, index=False)
    print(f"Altimetry timeseries file saved successfully - {os.path.basename(savepath)}")


def get_latest_cycle(username, password, lastcycle_no, series=3):
    lastcycle = int(lastcycle_no)
    nextcycle = lastcycle + 1

    ftp = FTP("ftp-access.aviso.altimetry.fr")
    ftp.login(user=str(username), passwd=str(password))
    try:
        ftp.cwd(f"geophysical-data-record/jason-{series}/gdr_f/cycle_{nextcycle}")

        latest_cycle = nextcycle
    except:
        latest_cycle = lastcycle
    
    return latest_cycle