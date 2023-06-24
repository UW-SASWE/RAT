import numpy as np
import os
import xarray as xr
import rioxarray as rxr
import rasterio
from shapely.geometry import mapping
import geopandas as gpd
import pandas as pd

from rat.utils.utils import round_pixels,round_up
from rat.utils.run_command import run_command


def create_basingridfile(basin_bounds,basin_geometry,basingridfile_path,xres,yres):    
    xmin,ymin,xmax,ymax=basin_bounds
    nx=round_pixels((xmax-xmin)/xres)
    ny=round_pixels((ymax-ymin)/yres)
    Z=np.ones((ny,nx))
    trf=rasterio.transform.from_origin(xmin, ymax, xres, yres)
    with rasterio.open(
        basingridfile_path[:-4]+'_boundbox.tif',
        'w',
        driver='GTiff',
        height=Z.shape[0],
        width=Z.shape[1],
        count=1,
        dtype=Z.dtype,
        crs='+init=epsg:4326',
        transform=trf
    ) as dst:
        dst.write(Z, 1)
    basin_grid = rxr.open_rasterio(basingridfile_path[:-4]+'_boundbox.tif')
    basin_grid=basin_grid.rio.clip(basin_geometry.apply(mapping),drop=False)
    basin_grid.rio.to_raster(basingridfile_path)

def create_basin_domain_nc_file(elevation_tif_filepath,basingridfile_path,output_path):
    ele=rxr.open_rasterio(elevation_tif_filepath)
    basin_grid=rxr.open_rasterio(basingridfile_path)
    #Interpolating and cropping elevation xarray
    basin_ele=ele.interp(x=basin_grid['x'].values,y=basin_grid['y'].values)
    #Changing vars and their names in xarrays
    basin_ele=basin_ele.drop_vars(['band','spatial_ref']).rename({'x':'lon','y':'lat'})[0]
    basin_grid=basin_grid.drop_vars(['band','spatial_ref']).rename({'x':'lon','y':'lat'})[0]
    #Creating a dataset from two xarrays
    basin_domain = basin_grid.to_dataset(name = 'mask')
    basin_domain['elev'] = basin_ele
    basin_domain['lat']=np.array(basin_grid.lat.data).round(5)
    basin_domain['lon']=np.array(basin_grid.lon.data).round(5)
    #Saving dataset as netcdf
    basin_domain.to_netcdf(output_path)

def create_vic_domain_param_file(global_vic_param_file,global_vic_domain_file,basingridfile_path,output_dir_path):
    #Reading global files
    gl_param = xr.open_dataset(global_vic_param_file)
    gl_domain = xr.open_dataset(global_vic_domain_file)

    #Reading basing grid file
    basin_grid=rxr.open_rasterio(basingridfile_path)
    basin_grid=basin_grid.drop_vars(['band','spatial_ref']).rename({'y':'lat','x':'lon'})[0]
    basin_grid=basin_grid.fillna(0)

    #Inserting run_cell as mask of basin_grid in vic_param.nc
    basin_vic_param=gl_param.interp(lon=np.array([round_up(x,5) for x in basin_grid.lon.data ]),lat=np.array([round_up(x,5) for x in basin_grid.lat.data ]),method='nearest')
    basin_vic_param['run_cell']=(('lat','lon'),basin_grid.data.astype('int32'))
    #Saving vic_param.nc
    basin_vic_param.to_netcdf(os.path.join(output_dir_path,'vic_soil_param.nc'))

    #Inserting run_cell as mask of basin_grid in vic_param.nc
    basin_vic_domain=gl_domain.interp(lon=np.array([round_up(x,5) for x in basin_grid.lon.data ]),lat=np.array([round_up(x,5) for x in basin_grid.lat.data ]),method='nearest')
    basin_vic_domain['mask']=(('lat','lon'),basin_grid.data.astype('int32'))
    #Saving vic_domain.nc
    basin_vic_domain.to_netcdf(os.path.join(output_dir_path,'vic_domain.nc'))

def create_basin_grid_flow_asc(global_flow_grid_dir_tif, basingridfile_path, savepath, flow_direction_replace_dict = None):
    global_flow_grid_dir=rxr.open_rasterio(global_flow_grid_dir_tif)
    basin_grid=rxr.open_rasterio(basingridfile_path)
    basin_flow_grid_dir = global_flow_grid_dir.interp(x=np.array([round_up(i,5) for i in basin_grid.x.data ]),
                                                                    y=np.array([round_up(i,5) for i in basin_grid.y.data ]),method='nearest')
    if (flow_direction_replace_dict):
        for i in flow_direction_replace_dict:
            basin_flow_grid_dir = basin_flow_grid_dir.where(basin_flow_grid_dir!=i, flow_direction_replace_dict[i])

    basin_flow_grid_dir = basin_flow_grid_dir.rio.write_nodata(0)
    basin_flow_grid_dir = basin_flow_grid_dir.where(basin_grid.data==1,0)
    basin_flow_grid_dir.rio.to_raster(savepath+'.tif', dtype='int16')

    # Change format, and save as asc file
    cmd = [
        'gdal_translate',
        '-of', 
        'aaigrid', 
        savepath+'.tif', 
        savepath+'.asc'
    ]
    cmd_out_code = run_command(cmd)

def create_basin_station_latlon_csv(region_name, basin_name, global_station_file, basin_gpd_df, column_dict, savepath, geojson_file=True):
    basins_station=gpd.read_file(global_station_file)
    basin_data_crs_changed = basin_gpd_df.to_crs(basins_station.crs)
    basins_station_spatialjoin = gpd.sjoin(basins_station, basin_data_crs_changed, "inner")[[
                        column_dict['id_column'],
                        column_dict['name_column'],
                        column_dict['lon_column'],
                        column_dict['lat_column'],
                        'geometry']]
    if(geojson_file):
        ## Creates a geojson file of stations with columns 'DAM_NAME', 'regionname', 'basinname' and 'filename', used by frontend.
        basins_station_spatialjoin['regionname'] = str(region_name)
        basins_station_spatialjoin['basinname'] = str(basin_name)
        basins_station_spatialjoin['filename'] = basins_station_spatialjoin[column_dict['id_column']].astype(str)+'_'+ \
                                        basins_station_spatialjoin[column_dict['name_column']].str.replace(' ','_')
        basins_station_spatialjoin.rename(columns={column_dict['name_column']: "DAM_NAME"})
        geojson_save_path = os.path.join(os.path.dirname(savepath),basin_name+'_station.geojson')
        basins_station_spatialjoin.to_file(geojson_save_path, driver= "GeoJSON")

    basins_station_lat_lon = basins_station_spatialjoin[[
                            column_dict['id_column'],
                            column_dict['name_column'],
                            column_dict['lon_column'],
                            column_dict['lat_column']]]
    basins_station_lat_lon['name'] = basins_station_lat_lon[column_dict['id_column']].astype(str
                                                                )+'_'+basins_station_lat_lon[column_dict['name_column']].astype(str).str.replace(' ','_')
    basins_station_lat_lon['run'] = 1
    basins_station_lat_lon['lon'] = basins_station_lat_lon[column_dict['lon_column']]
    basins_station_lat_lon['lat'] = basins_station_lat_lon[column_dict['lat_column']]
    basins_station_lat_lon[['run','name','lon','lat']].to_csv(savepath,index=False)

def create_basin_station_geojson(region_name, basin_name, station_csv_file, savepath):
    '''
    Creates a geojson shape file for the stations(reservoirs) for a particular basin with the following columns:
        DAM_NAME: the name of the station
        basinname: the name of the basin in which the station is located
        regionname: the name of the region in which the basin of that station is located
        filename: the name which will be used to save a station's output data like inflow, outflow etc.
    '''
    # Reading the station csv file
    basin_station_df =  pd.read_csv(station_csv_file)
    # Converting pandas df to geopandas dataframe
    basin_station_gdf= gpd.GeoDataFrame(
        basin_station_df, geometry=gpd.points_from_xy(basin_station_df.lon, basin_station_df.lat))
    # Removing unwanted columns
    basin_station_gdf = basin_station_gdf.drop(columns='run')
    # Adding the desired columns
    basin_station_gdf['regionname'] = str(region_name)
    basin_station_gdf['basinname'] = str(basin_name)
    basin_station_gdf['filename'] = basin_station_gdf['name']
    basin_station_gdf['DAM_NAME'] = basin_station_gdf['name'].str.replace('_',' ')
    # Setting crs of the geopandas dataframe
    basin_station_gdf = basin_station_gdf.set_crs('epsg:4326')
    # Saving the geopandas datframe as geojson file
    geojson_save_path = os.path.join(os.path.dirname(savepath),basin_name+'_station.geojson')
    basin_station_gdf.to_file(geojson_save_path, driver= "GeoJSON")

def create_basin_reservoir_shpfile(reservoir_shpfile,reservoir_shpfile_column_dict,basin_gpd_df,routing_station_global_data,savepath):
    reservoirs = gpd.read_file(reservoir_shpfile)
    basin_data_crs_changed = basin_gpd_df.to_crs(reservoirs.crs)
    #stations_df = pd.read_csv(station_xy_file,sep='\t',header=None,names=['run','name','x','y','area']).dropna().reset_index(drop=True)
    reservoirs_gdf_column_dict = reservoir_shpfile_column_dict

    if routing_station_global_data:
        reservoirs_spatialjoin = gpd.sjoin(reservoirs, basin_data_crs_changed, "inner")[[
                        reservoirs_gdf_column_dict['id_column'],
                        reservoirs_gdf_column_dict['dam_name_column'],
                        reservoirs_gdf_column_dict['area_column'],
                        'geometry']]
        reservoirs_spatialjoin['uniq_id'] = reservoirs_spatialjoin[reservoirs_gdf_column_dict['id_column']].astype(str)+'_'+ \
                            reservoirs_spatialjoin[reservoirs_gdf_column_dict['dam_name_column']].astype(str).str.replace(' ','_')
    else:
        reservoirs_spatialjoin = gpd.sjoin(reservoirs, basin_data_crs_changed, "inner")[[
                        reservoirs_gdf_column_dict['dam_name_column'],
                        reservoirs_gdf_column_dict['area_column'],
                        'geometry']]
    
    if(reservoirs_spatialjoin.empty):
        raise Exception('Reservoir names in reservoir shapefile are not matching with the station names in the station file used for routing.')
    reservoirs_spatialjoin.to_file(savepath, index=False)

