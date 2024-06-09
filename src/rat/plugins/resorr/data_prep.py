import geopandas as gpd
from pathlib import Path
import pandas as pd
import xarray as xr
import rioxarray as rxr
import numpy as np
import networkx as nx
import geonetworkx as gnx


def generate_network(
        flow_dir_fn, 
        stations_fn, 
        save_dir=None, 
        dist_proj=None, 
        elevation_fn=None
    ) -> (gpd.GeoDataFrame, gpd.GeoDataFrame): 
    """generate reservoir network using flow direction file and reservoir locations.

    Args:
        flow_dir_fn (str or Path): path to flow direction file
        stations_fn (str or Path): path to reservoir locations file
        save_dir (str or Path): path to save directory
        dist_proj (str, optional): projection to use for optionally calculating distance between reservoirs. Defaults to None.
        elevtion_fn (str or Path, optional): path to elevation file used for optionally adding elevation data to each reservoir. Defaults to None.

    Returns:
        (gpd.GeoDataFrame, gpd.GeoDataFrame): tuple of (edges, nodes) GeoDataFrames
    """

    # check if files exist
    flow_dir_fn = Path(flow_dir_fn)
    assert flow_dir_fn.exists()

    stations_fn = Path(stations_fn)
    assert stations_fn.exists()

    if save_dir:
        save_dir = Path(save_dir)
        if not save_dir.exists():
            print(f"passed save_dir does not exist, creating {save_dir}")
            save_dir.mkdir(parents=True)
    
    if elevation_fn:
        elevation_fn = Path(elevation_fn)
        assert elevation_fn.exists()

    fdr = rxr.open_rasterio(flow_dir_fn, masked=True)
    band = fdr.sel(band=1)

    band_vicfmt = band

    reservoirs = gpd.read_file(stations_fn)
    reservoirs['geometry'] = gpd.points_from_xy(reservoirs['lon'], reservoirs['lat'])
    reservoirs.set_crs('epsg:4326', inplace=True)

    reservoir_location_raster = xr.full_like(band_vicfmt, np.nan)
    for resid, row in reservoirs.iterrows():
        reslat = float(row.lat)
        reslon = float(row.lon)

        rast_lat = reservoir_location_raster.indexes['y'].get_indexer([reslat], method="nearest")[0]
        rast_lon = reservoir_location_raster.indexes['x'].get_indexer([reslon], method="nearest")[0]

        reservoir_location_raster[rast_lat, rast_lon] = resid

    # convert all points to nodes. Use index value to identify
    G = gnx.GeoDiGraph()
    G.add_nodes_from(reservoirs.index)

    operations = {
        1: [-1, 0],  # N
        2: [-1, 1],  # NE
        3: [0, 1],   # E
        4: [1, 1],   # SE
        5: [1, 0],   # S
        6: [1, -1],  # SW
        7: [0, -1],  # W
        8: [-1, -1], # NW
    }

    for node in G.nodes:
        resdata = reservoirs[reservoirs.index==node]
        
        x = float(resdata['lon'].values[0])
        y = float(resdata['lat'].values[0])

        idxx = band_vicfmt.indexes['x'].get_indexer([x], method="nearest")[0]
        idxy = band_vicfmt.indexes['y'].get_indexer([y], method="nearest")[0]
        
        # travel downstream until another node, np.nan or out-of-bounds is found, or if travelling in a loop

        visited = [(idxx, idxy)]
        current_pix = band_vicfmt.isel(x=idxx, y=idxy)

        attrs_n = {
            node: {
                'x': reservoirs['geometry'][node].x,
                'y': reservoirs['geometry'][node].y,
                'name': reservoirs['name'][node]
            }
        }
        nx.set_node_attributes(G, attrs_n)

        if not np.isnan(current_pix):
            END = False
            while not END:
                op = operations[int(current_pix)]
                new_idxy, new_idxx = np.array((idxy, idxx)) + np.array(op)
                idxy, idxx = new_idxy, new_idxx
                
                if (new_idxx, new_idxy) in visited:
                    # In a loop, exit
                    END=True
                    break
                else:
                    visited.append((new_idxx, new_idxy))

                current_pix = band_vicfmt.isel(x=new_idxx, y=new_idxy)
                if np.isnan(current_pix):
                    # NaN value found, exit loop
                    END=True
                    break

                try:
                    any_reservoir = reservoir_location_raster.isel(x=new_idxx, y=new_idxy)
                    if not np.isnan(any_reservoir):
                        # another reservoir found
                        G.add_edge(node, int(any_reservoir))
                        if dist_proj:
                            attrs_e = {
                                (node, int(any_reservoir)): {
                                    'length': reservoirs.to_crs(dist_proj)['geometry'][node].distance(reservoirs.to_crs(dist_proj)['geometry'][int(any_reservoir)])
                                }
                            }
                            nx.set_edge_attributes(G, attrs_e)
                        END = True
                        break
                except IndexError:
                    # Reached end
                    END=True

    G_gdf = gpd.GeoDataFrame(gnx.graph_edges_to_gdf(G))
    G_gdf_pts = gpd.GeoDataFrame(gnx.graph_nodes_to_gdf(G))

    # add elevation data to nodes
    if elevation_fn:
        elev = rxr.open_rasterio(elevation_fn, chunks='auto')

        G_gdf_pts['elevation'] = G_gdf_pts[['x', 'y']].apply(lambda row: float(elev.sel(x=row.x, y=row.y, method='nearest')), axis=1)
        G_gdf_pts.head()

    if save_dir:
        pts_save_fn = Path(save_dir) / 'rivreg_network_pts.shp'
        edges_save_fn = Path(save_dir) / 'rivreg_network.shp'
        
        G_gdf_pts.to_file(pts_save_fn)
        G_gdf.to_file(edges_save_fn)

    return G

# aggregate
def aggregate(ds, frequency='weekly'):
    if frequency == 'daily':
        resampled = ds
        resampled['dt'] = ds['time'].resample(time='1D').count()
    elif frequency == 'weekly':
        resampled = ds.resample(time='1W').mean()
        resampled['dt'] = ds['time'].resample(time='1W').count()
    elif frequency == 'monthly':
        resampled = ds.resample(time='1M').mean()
        resampled['dt'] = ds['time'].resample(time='1M').count()
    elif frequency == 'annual':
        resampled = ds.resample(time='1Y').mean()
        resampled['dt'] = ds['time'].resample(time='1Y').count()
    else:
        raise ValueError(f'frequency {frequency} not supported')
    
    return resampled

def calculate_volumes(
        ds, 
        fluxes=['unregulated_inflow', 'storage_change']
    ):
    """Calculate volume values using flow rates and ∆t

    Args:
        ds (xr.Dataset): Dataset containing flow rates in m3/day
        fluxes (list, optional): List of fluxes to calculate volumes for. Defaults to ['unregulated_inflow', 'storage_change'].
    """
    for flux in fluxes:
        ds[flux] = ds[flux] * ds['dt']
        ds[flux].attrs['units'] = 'm3'
        ds[flux].attrs['long_name'] = f'Volume of {flux}'
        ds[flux].attrs['description'] = f'Volume of {flux} in m3'

    return ds

def _rat_read_inflow(unregulated_inflow_fn, resorr_node_id, rat_output_level='final_outputs') -> xr.Dataset:
    """returns inflow files generated by RAT under pristine conditions as unregulated inflow.

    Args:
        unregulated_inflow_fn (str): path of the unregulated inflow file
        rat_output_level (str, optional): whether to read from `final_outputs` or `rat_outputs`, which are slightly different in units and file formatting. Defaults to 'final_outputs'.

    Returns:
        xr.Dataset: unregulated inflow
    """
    if rat_output_level == 'final_outputs':
        unregulated_inflow = pd.read_csv(unregulated_inflow_fn, parse_dates=['date']).rename({
            'date': 'time',
            'inflow (m3/d)': 'unregulated_inflow'
            }, axis='columns')
    elif rat_output_level == 'rat_outputs':
        unregulated_inflow = pd.read_csv(unregulated_inflow_fn, parse_dates=['date']).rename({
            'date': 'time',
            'streamflow': 'unregulated_inflow'
            }, axis='columns')
        unregulated_inflow['unregulated_inflow'] = unregulated_inflow['unregulated_inflow'] * (24*60*60) # m3/s -> m3/day

    unregulated_inflow['node'] = resorr_node_id
    unregulated_inflow.set_index(['time', 'node'], inplace=True)
    unregulated_inflow = unregulated_inflow.to_xarray()

    return unregulated_inflow

def _rat_read_storage_change(storage_change_fn, resorr_node_id):
    storage_change = pd.read_csv(storage_change_fn, parse_dates=['date']).rename({
        'date': 'time',
        'dS (m3)': 'storage_change'
    }, axis='columns')[['time', 'storage_change']]

    # convert storage_change to daily - https://stackoverflow.com/a/73724900
    storage_change = storage_change.set_index('time')
    storage_change = storage_change.resample('1D').apply(lambda x: np.nan if x.empty else x)
    groups = storage_change['storage_change'].notna()[::-1].cumsum()
    storage_change['storage_change'] = storage_change['storage_change'].fillna(0).groupby(groups).transform('mean')
    storage_change['node'] = resorr_node_id
    storage_change = storage_change.reset_index().set_index(['time', 'node'])
    storage_change = storage_change.to_xarray()

    return storage_change

def generate_forcings_from_rat(
        reservoir_network, 
        inflow_dir, 
        storage_change_dir, 
        save_dir, 
        aggregate_freq='daily',
        rat_output_level='final_outputs'
    ):
    """

    Args:
        reservoir_network (gnx.GeoDiGraph or str or pathlib.Path): reservoir network as GeoDiGraph or path to directory containing files generated by generate_network
        inflow_dir (str or Path): path to directory containing inflow files
        storage_change_dir (str or Path): path to directory containing storage change files
        save_dir (str or Path): directory to save regulation data to
    """
    # check if files exist
    inflow_dir = Path(inflow_dir)
    assert inflow_dir.exists()

    storage_change_dir = Path(storage_change_dir)
    assert storage_change_dir.exists()

    save_dir = Path(save_dir)
    if not save_dir.exists():
        print(f"passed save_dir does not exist, creating {save_dir}")
        save_dir.mkdir(parents=True)

    if isinstance(reservoir_network, gnx.GeoDiGraph):
        G = reservoir_network
    elif isinstance(reservoir_network, str) or isinstance(reservoir_network, Path):
        reservoir_network = gpd.read_file(Path(reservoir_network) / 'rivreg_network.shp')
        reservoir_network_pts = gpd.read_file(Path(reservoir_network) / 'rivreg_network_pts.shp')
        print(reservoir_network, reservoir_network_pts)
        G = gnx.read_geofiles(reservoir_network, reservoir_network_pts, directed=True)
    else:
        raise TypeError(f"reservoir_network must be of type gnx.GeoDiGraph or str or pathlib.Path. type of passed reservoir_network - {type(reservoir_network)}")

    datasets_to_join = []
    for node_id in G:
        node = G.nodes[node_id]
        name = node['name']

        # unregulated inflow
        unregulated_inflow_fn = inflow_dir / f"{name}.csv"

        if not unregulated_inflow_fn.exists():
            print(f"Missing {unregulated_inflow_fn}")
            continue
        unregulated_inflow = _rat_read_inflow(
            unregulated_inflow_fn, node_id, rat_output_level=rat_output_level)
        datasets_to_join.append(unregulated_inflow)

        # storage change
        storage_change_fn = storage_change_dir / f"{name}.csv"
        if storage_change_fn.exists():
            storage_change = _rat_read_storage_change(storage_change_fn, node_id)
            datasets_to_join.append(storage_change)

    rat_data = xr.merge(datasets_to_join)

    aggregated_volumes = calculate_volumes(aggregate(rat_data, frequency=aggregate_freq))

    forcings = xr.Dataset(
        data_vars={
            'theoretical_natural_runoff': aggregated_volumes['unregulated_inflow'],
            'storage_change': aggregated_volumes['storage_change'],
            'dt': aggregated_volumes['dt']
        }
    )

    # save regulation data
    forcings.to_netcdf(save_dir / 'resorr_forcings.nc')

    return forcings
