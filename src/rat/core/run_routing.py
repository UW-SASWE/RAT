from http.server import executable
import pandas as pd
import rasterio as rio
import os
import datetime

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

    def generate_inflow(self):
        # TODO Temp implementation. Later change it so that it operates on a disctionary of stations and generated outputs
        log.log(NOTIFICATION, "Starting inflow generation")
        log.debug(f"Looking at directory: {self.result_dir}")
        files = [os.path.join(self.result_dir, f) for f in os.listdir(self.result_dir) if f.endswith('.day')]
        
        if not os.path.isdir(self.inflow_dir):
            log.error("Directory does not exist: %s", self.inflow_dir)

        
        for f in files:
            outpath= os.path.join(self.inflow_dir, f.split(os.sep)[-1]).replace('.day', '.csv')
            log.debug("Converting %s, writing to %s", f, outpath)
            ## Appending data if files exists otherwise creating new
            if os.path.isfile(outpath):
                existing_data = pd.read_csv(outpath, parse_dates=['date'])
                new_data = self._convert_streamflow(f)
                # Concat the two dataframes into a new dataframe holding all the data (memory intensive):
                complement = pd.concat([existing_data, new_data], ignore_index=True)
                # Remove all duplicates:
                complement.drop_duplicates(subset=['date'],inplace=True, keep='first')
                complement.sort_values(by='date', inplace=True)
                complement.to_csv(outpath, index=False)
            else:
                self._convert_streamflow(f).to_csv(outpath, index=False)


    def _convert_streamflow(self, df_path) -> pd.DataFrame:
        
        df = pd.read_csv(
            df_path, 
            sep=r"\s+", 
            names=['year', 'month', 'day', 'streamflow'],
            parse_dates=[['year', 'month', 'day']]
        ).rename({"year_month_day": "date"}, axis=1)
        
        df['streamflow'] = df['streamflow'] * 0.028316847 # Imperial unit to SI units

        log.log(NOTIFICATION, "Converting streamflow: %s - %s", df_path, df.tail())

        return df