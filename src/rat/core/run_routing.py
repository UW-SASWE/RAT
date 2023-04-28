from http.server import executable
import pandas as pd
import dask.dataframe as dd
import rasterio as rio
import os
import datetime
from pathlib import Path

from logging import getLogger
from rat.utils.logging import LOG_NAME, NOTIFICATION
from rat.utils.run_command import run_command

log = getLogger(LOG_NAME)

class RoutingRunner():
    # TODO Clean up UH files
    def __init__(self, project_dir, result_dir, inflow_dir, model_path, param_path, fdr_path, station_path_latlon, station_xy) -> None:
        self.project_dir = project_dir
        self.result_dir = result_dir
        self.inflow_dir = inflow_dir
        self.station_path_xy = station_xy
        self.model_path = model_path
        self.param_path = param_path

        log.log(NOTIFICATION, "---------- Started Routing at %s ---------- \n", datetime.datetime.now())
        log.debug("Result Directory: %s", self.result_dir)
        log.debug("Inflow Directory: %s", self.inflow_dir)
        log.debug("Model Path: %s", self.model_path)
        log.debug("Parameter File: %s", self.param_path)
        log.debug("Flow Direction file: %s", fdr_path)
        log.debug("Station file (Lat-Lon): %s", station_path_latlon)
        log.debug("Station file (X-Y): %s", self.station_path_xy)

        self.fdr = self._read_fdr(fdr_path)
        if os.path.exists(station_path_latlon):
            self.stations = self._read_stations(station_path_latlon)
            self.station_file_creation = True
        else:
            self.station_file_creation = False

    def _read_fdr(self, fdr_path) -> rio.DatasetReader:
        log.debug("Reading Flow Direction file from: %s", fdr_path)
        return rio.open(fdr_path)
    
    def _get_xy(self, lat, lon) -> tuple:
        row, col = self.fdr.index(lon, lat)
        row = self.fdr.height - row
        col = col + 1  # I know, this looks ridiculous, but this is the way it works. Don't change
        log.debug("Converted (Lat: %s, Lon: %s ) to (X: %s, Y: %s)", lat, lon, col, row)
        return row, col
    
    def _read_stations(self, station_path) -> pd.DataFrame:
        log.debug("Intiializing station DF from: %s", station_path)

        stations = pd.read_csv(station_path)
        log.debug(f"{stations}")
        stations['x'] = None
        stations['y'] = None
        stations['filler'] = -9999
        return stations

    def create_station_file(self):
        if(self.station_file_creation):
            log.debug("Creating station file (X-Y) at: %s", self.station_path_xy)

            self.stations[['y', 'x']] = self.stations.apply(
                lambda row: pd.Series(
                        self._get_xy(row['lat'], row['lon']), 
                        index=['x', 'y']), 
                        axis=1
                    )
            self.stations[['run', 'name', 'x', 'y', 'filler']].to_csv(
                self.station_path_xy, 
                sep='\t', 
                header=False, 
                index=False, 
                line_terminator='\nNONE\n'
            )

    def run_routing(self):
        # TODO parse the output of routing model to to remove logs, keep a track of files stations
        #   and their generated output files
        log.log(NOTIFICATION, "Running Routing model")

        # args = self.model_path, self.param_path]
        args = f'cd {self.project_dir} && {self.model_path} {self.param_path}'
        log.debug("Running: %s", " ".join(args))
        # ret_code = run_command(['cd',self.project_dir])
        ret_code = run_command(args,shell=True, executable="/bin/bash")

        # clean up
        log.debug("Cleaning up routing files")
        uh_s_fs = [os.path.join(self.project_dir, f) for f in os.listdir(self.project_dir) if f .endswith("uh_s")]
        for f in uh_s_fs:
            log.debug("Deleting %s", f)
            os.remove(f)

def read_rat_out(fn, model=None):
    fn = Path(fn)

    if fn.suffix == '.day':
        df = pd.read_csv(fn, sep='\s+', header=None, names=['year', 'month', 'day', 'streamflow'])
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
        df.drop(['year', 'month', 'day'], axis=1, inplace=True)
        df['streamflow'] = df['streamflow'] * 0.028316847 # convert cfs to cms
    elif fn.suffix == '.csv':
        df = pd.read_csv(fn, parse_dates=['date'])

    if model:
        df['model'] = model
        df = df.set_index(['model', 'date'])
    else:
        df = df.set_index('date')

    return df

def generate_inflow(src_dir, dst_dir):
    src_dir = Path(src_dir)
    dst_dir = Path(dst_dir)
    # TODO Temp implementation. Later change it so that it operates on a disctionary of stations and generated outputs
    log.log(NOTIFICATION, "Starting inflow generation")
    log.debug(f"Looking at directory: {src_dir}")
    files = [src_dir / f for f in src_dir.glob('*.day')]
    
    if not dst_dir.exists():
        log.error("Directory does not exist: %s", dst_dir)

    
    for f in files:
        outpath = (dst_dir / f.name).with_suffix('.csv')
        log.debug("Converting %s, writing to %s", f, outpath)
        ## Appending data if files exists otherwise creating new
        if outpath.exists():
            existing_data = dd.read_csv(outpath, parse_dates=['date'])
            new_data = read_rat_out(f)
            # Concat the two dataframes into a new dataframe holding all the data (memory intensive):
            complement = dd.concat([existing_data, new_data], ignore_index=True)
            # Remove all duplicates:
            complement.drop_duplicates(subset=['date'],inplace=True, keep='first')
            complement.sort_values(by='date', inplace=True)
            complement.reset_index().to_csv(outpath, index=False)
        else:
            read_rat_out(f).reset_index().to_csv(outpath, index=False)