import ee
import os
import geopandas as gpd
import pandas as pd
import numpy as np
from itertools import zip_longest,chain
from rat.ee_utils.ee_utils import poly2feature

BUFFER_DIST = 500
DEM = ee.Image('USGS/SRTMGL1_003')

def grouper(iterable, n, *, incomplete='fill', fillvalue=None):
    "Collect data into non-overlapping fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, fillvalue='x') --> ABC DEF Gxx
    # grouper('ABCDEFG', 3, incomplete='strict') --> ABC DEF ValueError
    # grouper('ABCDEFG', 3, incomplete='ignore') --> ABC DEF
    args = [iter(iterable)] * n
    if incomplete == 'fill':
        return zip_longest(*args, fillvalue=fillvalue)
    if incomplete == 'strict':
        return zip(*args, strict=True)
    if incomplete == 'ignore':
        return zip(*args)
    else:
        raise ValueError('Expected fill, strict, or ignore')

def _aec(n,elev_dem,roi):
  ii = ee.Image.constant(n).reproject(elev_dem.projection())
  DEM141 = elev_dem.lte(ii)

  DEM141Count = DEM141.reduceRegion(
    geometry= roi,
    scale= 30,
    reducer= ee.Reducer.sum()
  )
  area=ee.Number(DEM141Count.get('elevation')).multiply(30*30).divide(1e6)
  return area

def aec_file_creator(reservoir_shpfile, shpfile_column_dict, aec_dir_path):
  # Obtaining list of csv files in aec_dir_path
  aec_filenames = []
  for f in os.listdir(aec_dir_path):
    if f.endswith(".csv"):
        aec_filenames.append(f[:-4])
  
  reservoirs_polygon = gpd.read_file(reservoir_shpfile)
  for reservoir_no,reservoir in reservoirs_polygon.iterrows():
      # Reading reservoir information
      reservoir_name = str(reservoir[shpfile_column_dict['unique_identifier']])
      if reservoir_name in aec_filenames:
        print(f"Skipping {reservoir_name} as its AEC file already exists")
      else:
        print(f"Creating AEC file for {reservoir_name}")
        reservoir_polygon = reservoir.geometry
        aoi = poly2feature(reservoir_polygon,BUFFER_DIST).geometry()
        min_elev = DEM.reduceRegion( reducer = ee.Reducer.min(),
                    geometry = aoi,
                    scale = 30,
                    maxPixels = 1e10
                    ).get('elevation')
        max_elev = DEM.reduceRegion( reducer = ee.Reducer.max(),
                    geometry = aoi,
                    scale = 30,
                    maxPixels = 1e10
                    ).get('elevation')

        elevs = ee.List.sequence(min_elev, max_elev, 1)
        elevs_list = elevs.getInfo()
        grouped_elevs_list = grouper(elevs_list,5)

        areas_list = []
        for subset_elevs_tuples in grouped_elevs_list:
          ## Removing None objects from grouped tuples and converting them to list and then ee.List and then calculating  area for that 
          subset_elevs_list = list(filter(lambda x: x != None, subset_elevs_tuples))
          subset_elevs = ee.List(subset_elevs_list)
          subset_areas = subset_elevs.map(lambda elevation_i: _aec(elevation_i, DEM, aoi))
          subset_areas_list = subset_areas.getInfo()
          ## Appendning areas to the main list
          areas_list.append(subset_areas_list)
        
        ## Converting list of lists to list
        areas_list = list(chain(*areas_list))
        areas_list = np.round(areas_list,3)
        ## Preparing dataframe to write down to csv 
        aec_df = pd.DataFrame(data = {'Elevation' :elevs_list, 'CumArea':areas_list})
        aec_df.to_csv(os.path.join(aec_dir_path,reservoir_name+'.csv'),index=False)
        print(f"AEC file created succesfully for {reservoir_name}")
  print("AEC file exists for all reservoirs in this basin")
  return 1

