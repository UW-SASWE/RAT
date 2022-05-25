import numpy as np
import os
import xarray as xr
import rioxarray as rxr
import rasterio
from shapely.geometry import mapping

from utils.utils import round_pixels,round_up


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
    basin_domain['lat']=np.array([round_up(x,5) for x in basin_grid.lat.data ])
    basin_domain['lon']=np.array([round_up(x,5) for x in basin_grid.lon.data ])
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
    basin_vic_param=gl_param.interp(lon=np.array([round_up(x,5) for x in basin_grid.lon.data ]),lat=np.array([round_up(x,5) for x in basin_grid.lat.data ]))
    basin_vic_param['run_cell']=(('lat','lon'),basin_grid.data)
    #Saving vic_param.nc
    basin_vic_param.to_netcdf(os.path.join(output_dir_path,'vic_soil_param.nc'))

    #Inserting run_cell as mask of basin_grid in vic_param.nc
    basin_vic_domain=gl_domain.interp(lon=np.array([round_up(x,5) for x in basin_grid.lon.data ]),lat=np.array([round_up(x,5) for x in basin_grid.lat.data ]))
    basin_vic_domain['mask']=(('lat','lon'),basin_grid.data)
    #Saving vic_domain.nc
    basin_vic_domain.to_netcdf(os.path.join(output_dir_path,'vic_domain.nc'))