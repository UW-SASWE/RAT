from http.server import executable
import pandas as pd
import rasterio as rio
import os
import datetime
from pathlib import Path
import dask
from tempfile import TemporaryDirectory
import subprocess

from logging import getLogger
from rat.utils.route_param_reader import RouteParameterFile
from rat.utils.logging import LOG_NAME, LOG_LEVEL, NOTIFICATION
from rat.utils.run_command import run_command

log = getLogger(LOG_NAME)
log.setLevel(LOG_LEVEL)

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
        if isinstance(station_path_latlon, pd.DataFrame):
            self.stations = self._read_stations(station_path_latlon)
            self.station_file_creation = True
        elif isinstance(station_path_latlon, str):
            if os.path.exists(station_path_latlon):
                self.stations = self._read_stations(pd.read_csv(station_path_latlon))
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
    
    def _read_stations(self, stations) -> pd.DataFrame:
        log.debug(f"{stations}")
        stations['x'] = None
        stations['y'] = None
        stations['filler'] = -9999
        return stations

    def create_station_file(self):

        if(self.station_file_creation):
            log.debug("Creating station file (X-Y) at: %s", self.station_path_xy)

            f = lambda row: pd.Series(self._get_xy(row['lat'], row['lon']), index=['x', 'y'])

            self.stations[['y', 'x']] = self.stations.apply(f, axis=1)
            self.stations[['run', 'name', 'x', 'y', 'filler']].to_csv(
                self.station_path_xy,
                sep='\t', 
                header=False, 
                index=False, 
                lineterminator='\nNONE\n'
            )

    def run_routing(self, cd=None):
        # TODO parse the output of routing model to to remove logs, keep a track of files stations
        #   and their generated output files
        log.log(NOTIFICATION, "Running Routing model")

        # args = self.model_path, self.param_path]
        if cd: # if change directory is passed, change directory to that directory
            # remove any .uh_s files in the directory
            _ = [f.unlink() for f in Path(cd).glob("*.uh_s")]
            args = f'{self.model_path} {Path(self.param_path).relative_to(cd)}'
            log.debug("Running: %s", " ".join(args))
            log.debug("Changing directory to %s", cd)
            ret_code = subprocess.run(args.split(), capture_output=True, cwd=cd)
        else: # else change directory to project directory
            args = f'{self.model_path} {self.param_path}'
            log.debug("Running: %s", " ".join(args))
            ret_code = subprocess.run(args.split(), capture_output=True, shell=True, cwd=self.project_dir, executable="/bin/bash")

        # clean up
        log.debug("Cleaning up routing files")
        uh_s_fs = [os.path.join(self.project_dir, f) for f in os.listdir(self.project_dir) if f .endswith("uh_s")]
        for f in uh_s_fs:
            log.debug("Deleting %s", f)
            os.remove(f)
        return ret_code

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

def generate_inflow(name, src, dst_dir):
    """Convert routing ouputs as rat output format

    Args:
        srcs (Path): List of VIC routing output files (.day)
        dst_dir (Path): Directory to write the output files
    """
    dst_dir = Path(dst_dir)

    log.log(NOTIFICATION, "Starting inflow generation")
    
    if not dst_dir.exists():
        log.error("Directory does not exist: %s", dst_dir)

    outpath = dst_dir / f"{name}.csv"
    log.debug("Converting %s, writing to %s", src, outpath)
    ## Appending data if files exists otherwise creating new
    if outpath.exists():
        existing_data = pd.read_csv(outpath, parse_dates=['date'])
        new_data = read_rat_out(src).reset_index()
        # Concat the two dataframes into a new dataframe holding all the data (memory intensive):
        complement = pd.concat([existing_data, new_data], ignore_index=True)
        # Remove all duplicates:
        complement.drop_duplicates(subset=['date'], inplace=True, keep='first')
        complement.sort_values(by='date', inplace=True)
        complement.to_csv(outpath, index=False)
    else:
        read_rat_out(src).reset_index().to_csv(outpath, index=False)

@dask.delayed(pure=True)
def run_for_station(station_name, config, start, end, basin_flow_direction_file, rout_input_path_prefix, inflow_dir, station_path_latlon, clean=False):
    if isinstance(station_path_latlon, pd.Series):
        station_path_latlon = station_path_latlon.to_frame().T
    log.debug("Running routing for station: %s", station_name)

    route_dir = Path(config['GLOBAL']['data_dir']) / f'{config["BASIN"]["region_name"]}' / 'basins' / f'{config["BASIN"]["basin_name"]}' / 'ro'
    # create workspace directory
    route_workspace_dir = route_dir / 'wkspc' / f'{station_name}' 
    route_workspace_dir.mkdir(parents=True, exist_ok=True)

    # creating symlinks
    log.debug(f"Creating symlinks at {route_workspace_dir}")
    
    # input files
    input_files_src = Path(rout_input_path_prefix).parent
    input_files_dst = route_workspace_dir / 'in'
    if input_files_dst.is_symlink():
        log.warn("Symlink already exists at %s, deleting it", input_files_dst)
        input_files_dst.unlink()
    input_files_dst.symlink_to(input_files_src, target_is_directory=True)
    input_files_glob = input_files_dst / Path(rout_input_path_prefix).stem
    
    # output files
    output_files_dst = route_workspace_dir / 'ou'
    output_files_dst.mkdir(parents=True, exist_ok=True)

    # flow direction file
    flow_direction_file_src = Path(basin_flow_direction_file)
    flow_direction_file_dst = route_workspace_dir / 'fl.asc'
    if flow_direction_file_dst.is_symlink():
        log.warn("Symlink already exists at %s, deleting it", flow_direction_file_dst)
        flow_direction_file_dst.unlink()
    flow_direction_file_dst.symlink_to(flow_direction_file_src)

    # uh file
    uh_file_src = Path(config['GLOBAL']['project_dir']) / 'params' / 'routing' / 'uh.txt'
    assert uh_file_src.exists()
    uh_file_dst = route_workspace_dir / 'uh.txt'
    if uh_file_dst.is_symlink():
        log.warn("Symlink already exists at %s, deleting it", uh_file_dst)
        uh_file_dst.unlink()
    uh_file_dst.symlink_to(uh_file_src)

    # parameter file path, not a symlink, parameter file will be saved here
    param_file_path = route_workspace_dir / 'route_param.txt'
    # station file path, not a symlink, station file will be saved here
    station_file_path = route_workspace_dir / 'sta_xy.txt'

    with RouteParameterFile(
        config = config,
        basin_name = config['BASIN']['basin_name'],
        start = start,
        end = end,
        route_param_path=str(param_file_path),
        basin_flow_direction_file = str(flow_direction_file_dst.relative_to(route_workspace_dir)),
        rout_input_path_prefix = str(input_files_glob.relative_to(route_workspace_dir)),
        output_dst = str(output_files_dst.relative_to(route_workspace_dir)) + '/',
        station_path = str(station_file_path.relative_to(route_workspace_dir)),
        uh = str(uh_file_dst.relative_to(route_workspace_dir)),
        clean=False
    ) as r:
        log.debug("Routing Parameter file: %s", r.route_param_path)
        route = RoutingRunner(    
            project_dir = config['GLOBAL']['project_dir'], 
            result_dir = str(output_files_dst), 
            inflow_dir = str(input_files_dst), 
            model_path = config['ROUTING']['route_model'],
            param_path = r.route_param_path, 
            fdr_path = flow_direction_file_src, 
            station_path_latlon = station_path_latlon,
            station_xy = station_file_path
        )
        route.create_station_file()
        ret_code = route.run_routing(cd=route_workspace_dir)
        basin_station_xy_path = route.station_path_xy

        output_path = Path(r.params['output_dir']) / f"{station_name}.day"
    return output_path, basin_station_xy_path, ret_code

def run_routing(config, start, end, basin_flow_direction_file, rout_input_path_prefix, inflow_dir, station_path_latlon, clean=False):
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    
    stations = pd.read_csv(station_path_latlon)
    
    futures = []
    for i, station in stations.iterrows():
        future = run_for_station(station['name'], config, start, end, basin_flow_direction_file, rout_input_path_prefix, inflow_dir, station, clean)
        futures.append(future)
    routing_results = dask.compute(*futures)

    output_paths = [r[0] for r in routing_results]
    station_xy_path = routing_results[0][1]
    ret_codes = [r[2] for r in routing_results]

    return output_paths, station_xy_path, ret_codes