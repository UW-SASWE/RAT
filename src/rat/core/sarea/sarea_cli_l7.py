import ee
from datetime import datetime, timedelta, timezone
import pandas as pd
import time
import os
from random import randint
from itertools import zip_longest

from rat.ee_utils.ee_utils import poly2feature
from rat.utils.logging import LOG_NAME, NOTIFICATION
from rat.utils.utils import days_between
from logging import getLogger

# Defining global constants
l7= ee.ImageCollection("LANDSAT/LE07/C02/T1_L2")
gswd = ee.Image("JRC/GSW1_4/GlobalSurfaceWater")

NDWI_THRESHOLD = 0.3
SMALL_SCALE = 30
MEDIUM_SCALE = 120
LARGE_SCALE = 500
BUFFER_DIST = 500
CLOUD_COVER_LIMIT = 90
TEMPORAL_RESOLUTION = 16
RESULTS_PER_ITER = 5
QUALITY_PIXEL_BAND_NAME = 'QA_PIXEL'
BLUE_BAND_NAME = 'SR_B1'
GREEN_BAND_NAME = 'SR_B2'
RED_BAND_NAME = 'SR_B3'
NIR_BAND_NAME = 'SR_B4'
SWIR1_BAND_NAME = 'SR_B5'
SWIR2_BAND_NAME = 'SR_B7'

def preprocess(im):
    clipped = im   # clipping adds processing overhead, setting clipped = im
    
    # clipped = ee.Image(ee.Algorithms.If('BQA' in clipped.bandNames(), preprocess_image_mask(im), clipped.updateMask(ee.Image.constant(1))))
    # Mask appropriate QA bits
    QA = im.select([QUALITY_PIXEL_BAND_NAME])
    
    cloudShadowBitMask = 1 << 3
    cloudsBitMask = 1 << 4
    
    cloud = (QA.bitwiseAnd(cloudsBitMask).neq(0)).Or(QA.bitwiseAnd(cloudShadowBitMask).neq(0)).rename("cloud")
    clipped = clipped.updateMask(cloud.eq(0).select("cloud"))
    
    # SR scaling
    clipped = clipped.select('SR_B.').multiply(0.0000275).add(-0.2)
    clipped = clipped.addBands(cloud)
    
    clipped = clipped.set('system:time_start', im.get('system:time_start'))
    clipped = clipped.set('system:time_end', im.get('system:time_end'))
    # clipped = clipped.set('cloud_area', cloud_area)
    
    return clipped


##########################################################/
## Processing individual images - water classification   ##
##########################################################/

def identify_water_cluster(im):
    im = ee.Image(im)
    mbwi = im.select('MBWI')

    max_cluster_value = ee.Number(im.select('cluster').reduceRegion(
        reducer = ee.Reducer.max(),
        geometry = aoi,
        scale = LARGE_SCALE,
        maxPixels =  1e10
    ).get('cluster'))

    clusters = ee.List.sequence(0, max_cluster_value)

    def calc_avg_mbwi(cluster_val):
        cluster_val = ee.Number(cluster_val)
        avg_mbwi = mbwi.updateMask(im.select('cluster').eq(ee.Image(cluster_val))).reduceRegion(
            reducer = ee.Reducer.mean(),
            scale = MEDIUM_SCALE,
            geometry = aoi,
            maxPixels = 1e10
        ).get('MBWI')
        return avg_mbwi

    avg_mbwis = ee.Array(clusters.map(calc_avg_mbwi))

    max_mbwi_index = avg_mbwis.argmax().get(0)

    water_cluster = clusters.get(max_mbwi_index)

    return water_cluster


def cordeiro(im):
    band_subset = ee.List(['NDWI', 'MNDWI', SWIR2_BAND_NAME])   # using NDWI, MNDWI and B7 (SWIR2)
    sampled_pts = im.select(band_subset).sample(
        region = aoi,
        scale = SMALL_SCALE,
        numPixels = 5e3-1  ## limit of 5k points
    )
    
    ## Agglomerative Clustering isn't available, using Cascade K-Means Clustering based on
    ##  calinski harabasz's work
    ## https:##developers.google.com/earth-engine/apidocs/ee-clusterer-wekacascadekmeans
    clusterer = ee.Clusterer.wekaCascadeKMeans(
        minClusters = 2,
        maxClusters = 7,
        init = True
    ).train(sampled_pts)
    
    # Classify clusters
    classified = im.select(band_subset).cluster(clusterer)
    im = im.addBands(classified)
    
    # Select cluster with highest average MBWI and say it as water map cordeiro
    water_cluster = identify_water_cluster(im)
    water_map = classified.select('cluster').eq(ee.Image.constant(water_cluster)).select(['cluster'], ['water_map_cordeiro'])
    im = im.addBands(water_map)

    return im


def process_image(im):
    # Landsat 7 process Scan Line Corrector (SLC) 

    ndwi = im.normalizedDifference([NIR_BAND_NAME, SWIR1_BAND_NAME]).rename('NDWI')
    im = im.addBands(ndwi)
    mndwi = im.normalizedDifference([GREEN_BAND_NAME, SWIR1_BAND_NAME]).rename('MNDWI')
    im = im.addBands(mndwi)
    mbwi = im.expression("MBWI = 3*green-red-nir-swir1-swir2", {
        'green': im.select(GREEN_BAND_NAME),
        'red': im.select(RED_BAND_NAME),
        'nir': im.select(NIR_BAND_NAME),
        'swir1': im.select(SWIR1_BAND_NAME),
        'swir2': im.select(SWIR2_BAND_NAME)
    })
    im = im.addBands(mbwi)
    
    #cloud_area = AOI area - area od pixels in cloud band where there is no data because we masked it due to presence of clouds
    cloud_area = aoi.area().subtract(im.select('cloud').Not().multiply(ee.Image.pixelArea()).reduceRegion(
        reducer = ee.Reducer.sum(),
        geometry = aoi,
        scale = SMALL_SCALE,
        maxPixels = 1e10
    ).get('cloud'))
    cloud_percent = cloud_area.multiply(100).divide(aoi.area())
    
    cordeiro_will_run_when = cloud_percent.lt(CLOUD_COVER_LIMIT)

    # Clustering based classification of water pixels for cloud pixels when cloud cover is less than CLOUD_COVER_LIMIT
    im = im.addBands(ee.Image(ee.Algorithms.If(cordeiro_will_run_when, cordeiro(im), ee.Image.constant(-1e6))))  # run cordeiro only if cloud percent is < 90%

    # Calculate water area in water_area_cordeiro map
    water_area_cordeiro = ee.Number(ee.Algorithms.If(cordeiro_will_run_when,
        ee.Number(im.select('water_map_cordeiro').eq(1).multiply(ee.Image.pixelArea()).reduceRegion(
            reducer = ee.Reducer.sum(), 
            geometry = aoi, 
            scale = SMALL_SCALE, 
            maxPixels = 1e10
        ).get('water_map_cordeiro')),
        ee.Number(-1e6)
    ))
    # Calculate non-water area in water_area_cordeiro map.
    non_water_area_cordeiro = ee.Number(ee.Algorithms.If(cordeiro_will_run_when,
        ee.Number(im.select('water_map_cordeiro').neq(1).multiply(ee.Image.pixelArea()).reduceRegion(
            reducer = ee.Reducer.sum(), 
            geometry = aoi, 
            scale = SMALL_SCALE, 
            maxPixels = 1e10
        ).get('water_map_cordeiro')),
        ee.Number(-1e6)
    ))

    # NDWI based water map: Classify water wherever NDWI is greater than NDWI_THRESHOLD and add water_map_NDWI band.
    im = im.addBands(ndwi.gte(NDWI_THRESHOLD).select(['NDWI'], ['water_map_NDWI']))
    # Calculate water area in water_map_NDWI.
    water_area_NDWI = ee.Number(im.select('water_map_NDWI').eq(1).multiply(ee.Image.pixelArea()).reduceRegion(
        reducer = ee.Reducer.sum(), 
        geometry = aoi, 
        scale = SMALL_SCALE, 
        maxPixels = 1e10
    ).get('water_map_NDWI'))
    # Calculate non-water area in water_map_NDWI.
    non_water_area_NDWI = ee.Number(im.select('water_map_NDWI').neq(1).multiply(ee.Image.pixelArea()).reduceRegion(
        reducer = ee.Reducer.sum(), 
        geometry = aoi, 
        scale = SMALL_SCALE, 
        maxPixels = 1e10
    ).get('water_map_NDWI'))
    # Calculate red band sum for water area in water_map_NDWI.
    water_red_sum = ee.Number(im.select('water_map_NDWI').eq(1).multiply(im.select(RED_BAND_NAME)).reduceRegion(
        reducer = ee.Reducer.sum(), 
        geometry = aoi, 
        scale = SMALL_SCALE, 
        maxPixels = 1e10
    ).get('water_map_NDWI'))
    # Calculate green band sum for water area in water_map_NDWI.
    water_green_sum = ee.Number(im.select('water_map_NDWI').eq(1).multiply(im.select(GREEN_BAND_NAME)).reduceRegion(
        reducer = ee.Reducer.sum(), 
        geometry = aoi, 
        scale = SMALL_SCALE, 
        maxPixels = 1e10
    ).get('water_map_NDWI'))
    # Calculate nir band sum for water area in water_map_NDWI.
    water_nir_sum = ee.Number(im.select('water_map_NDWI').eq(1).multiply(im.select(NIR_BAND_NAME)).reduceRegion(
        reducer = ee.Reducer.sum(), 
        geometry = aoi, 
        scale = SMALL_SCALE, 
        maxPixels = 1e10
    ).get('water_map_NDWI'))
    
    # Set attributes to retrive later
    im = im.set('cloud_area', cloud_area.multiply(1e-6))
    im = im.set('cloud_percent', cloud_percent)
    im = im.set('water_area_cordeiro', water_area_cordeiro.multiply(1e-6))
    im = im.set('non_water_area_cordeiro', non_water_area_cordeiro.multiply(1e-6))
    im = im.set('water_area_NDWI', water_area_NDWI.multiply(1e-6))
    im = im.set('non_water_area_NDWI', non_water_area_NDWI.multiply(1e-6))
    im = im.set('water_red_sum', water_red_sum)
    im = im.set('water_green_sum', water_green_sum)
    im = im.set('water_nir_sum', water_nir_sum)
    
    return im


def postprocess_wrapper(im, bandName, raw_area):
    im = ee.Image(im)
    bandName = ee.String(bandName)
    date = im.get('to_date')

    def postprocess():
        gswd_masked = gswd.updateMask(im.select(bandName).eq(1))
        
        hist = ee.List(gswd_masked.reduceRegion(
            reducer = ee.Reducer.autoHistogram(minBucketWidth = 1),
            geometry = aoi,
            scale = SMALL_SCALE,
            maxPixels = 1e10
        ).get('occurrence'))
        
        counts = ee.Array(hist).transpose().toList()
        
        omega = ee.Number(0.17)
        count_thresh = ee.Number(counts.map(lambda lis: ee.List(lis).reduce(ee.Reducer.mean())).get(1)).multiply(omega)
        
        count_thresh_index = ee.Array(counts.get(1)).gt(count_thresh).toList().indexOf(1)
        occurrence_thresh = ee.Number(ee.List(counts.get(0)).get(count_thresh_index))

        water_map = im.select([bandName], ['water_map'])
        gswd_improvement = gswd.clip(aoi).gte(occurrence_thresh).updateMask(water_map.mask().Not()).select(["occurrence"], ["water_map"])
        
        improved = ee.ImageCollection([water_map, gswd_improvement]).mosaic().select(['water_map'], ['water_map_zhao_gao'])
        
        corrected_area = ee.Number(improved.select('water_map_zhao_gao').multiply(ee.Image.pixelArea()).reduceRegion(
            reducer = ee.Reducer.sum(), 
            geometry = aoi, 
            scale = SMALL_SCALE, 
            maxPixels = 1e10
        ).get('water_map_zhao_gao'))
        
        improved = improved.set("corrected_area", corrected_area.multiply(1e-6))
        return improved

    def dont_post_process():
        improved = ee.Image.constant(-1)
        improved = improved.set("corrected_area", -1)
        return improved

    condition = ee.Number(im.get('cloud_percent')).lt(CLOUD_COVER_LIMIT).And(ee.Number(raw_area).gt(0))
    improved = ee.Image(ee.Algorithms.If(condition, postprocess(), dont_post_process()))
    
    improved = improved.set("to_date", date)
    
    return improved


############################################################/
## Code from here takes care of the time-series generation   ##
############################################################/
def calc_ndwi(im):
    return im.addBands(im.normalizedDifference([NIR_BAND_NAME, SWIR1_BAND_NAME]).rename('NDWI'))

def slc_failure_correction(im):
    filled_image = im.select('SR_B.')
        
    # Apply focal mean with the radius of 2 

    focal_mean_image = filled_image.focal_mean(2, 'square', 'pixels', 2)
    
    # Create a mask for NaN areas in the filled image
    nan_mask = filled_image.mask().Not()
    
    # Only update the NaN areas with focal mean result
    filled_image = filled_image.blend(focal_mean_image.updateMask(nan_mask))

    # Preserve the time metadata
    filled_image = filled_image.set('system:time_start', im.get('system:time_start'))
    filled_image = filled_image.set('system:time_end', im.get('system:time_end'))

    #Add the Quality pixel band
    filled_image = filled_image.addBands(im.select(QUALITY_PIXEL_BAND_NAME))
    
    return filled_image


def process_date(date):
    # Given date, calculate end date by adding TEMPORAL_RESOLUTION - 1 days
    date = ee.Date(date)
    from_date = date
    to_date = date.advance(TEMPORAL_RESOLUTION - 1, 'day')
    
    # from_date_client_obj = from_date.format('YYYY-MM-dd')
    # from_date_client_obj = pd.to_datetime(from_date_client_obj)

    # Only do SLC failure correction if date is greater than 31st may 2003.
    # if(from_date_client_obj>=pd.to_datetime('2003-05-31')):
        # Filter the image collection for these dates and AOI and do SLC failure correction for landsat-7 and run preprocess function (cloud calculations, scaling, adding & setting start and end) on them
    l7_subset = ee.ImageCollection(ee.Algorithms.If(ee.Date('2003-05-31').millis().lt(to_date.millis()),
                                 l7.filterDate(from_date, to_date).filterBounds(aoi).map(slc_failure_correction).map(preprocess),
                                 l7.filterDate(from_date, to_date).filterBounds(aoi).map(preprocess)))
    # else:
    #     # Filter the image collection for these dates and AOI and run preprocess function (cloud calculations, scaling, adding & setting start and end) on them
    #     l7_subset = l7.filterDate(from_date, to_date).filterBounds(aoi).map(preprocess)

    # Get mosaic of images if there is atleast one image for this time duration with NDWI as the quality factor (keep High NDWI)
    im = ee.Image(ee.Algorithms.If(l7_subset.size().neq(0), l7_subset.map(calc_ndwi).qualityMosaic('NDWI'), ee.Image.constant(0)))
    # Process NDWI Image if there is atleast one image for this time duration
    im = ee.Image(ee.Algorithms.If(l7_subset.size().neq(0), process_image(im), ee.Image.constant(0)))

    # Set attributes of from and to date along with number of images during the time duration
    im = im.set('from_date', from_date.format("YYYY-MM-dd"))
    im = im.set('to_date', to_date.format("YYYY-MM-dd"))
    im = im.set('l7_images', l7_subset.size())
    
    # im = ee.Algorithms.If(im.bandNames().size().eq(1), ee.Number(0), im)
    
    return ee.Image(im)


def generate_timeseries(dates):
    raw_ts = dates.map(process_date)
    # raw_ts = raw_ts.removeAll([0])
    imcoll = ee.ImageCollection.fromImages(raw_ts)

    return imcoll

def get_first_obs(start_date, end_date):
    first_im = l7.filterBounds(aoi).filterDate(start_date, end_date).first()
    str_fmt = 'YYYY-MM-dd'
    return ee.Date.parse(str_fmt, ee.Date(first_im.get('system:time_start')).format(str_fmt))

def run_process_long(res_name, res_polygon, start, end, datadir):
    fo = start #fo: first observation
    enddate = end

    # Extracting reservoir geometry 
    global aoi
    aoi = poly2feature(res_polygon,BUFFER_DIST).geometry()

    ## Checking the number of images in the interval as Landsat 7 might have missing data for a lot of places for longer durations.
    number_of_images = l7.filterBounds(aoi).filterDate(start, end).size().getInfo()
    
    if(number_of_images):
        # getting first observation in the filtered collection
        print('Checking first observation date in the given time interval.')
        fo = get_first_obs(start, end).format('YYYY-MM-dd').getInfo() 
        first_obs = datetime.strptime(fo, '%Y-%m-%d')
        print(f"First Observation: {first_obs}")

        scratchdir = os.path.join(datadir, "_scratch")

        # If data already exists, only get new data starting from the last one
        savepath = os.path.join(datadir, f"{res_name}.csv")
        
        # If an existing file exists, 
        if os.path.isfile(savepath):
            # Read the existing file
            temp_df = pd.read_csv(savepath, parse_dates=['mosaic_enddate']).set_index('mosaic_enddate')

            # Get the last date in the existing file and adjust the first observation to before last date (last date might not be for this satellite. Its TMS-OS data's ;ast date.)
            last_date = temp_df.index[-1].to_pydatetime()
            fo = (last_date - timedelta(days=TEMPORAL_RESOLUTION*2)).strftime("%Y-%m-%d")
            # Create an array with filepath
            to_combine = [savepath]
            print(f"Existing file found - Last observation ({TEMPORAL_RESOLUTION*2} day lag): {last_date}")

            # If {TEMPORAL_RESOLUTION} days have not passed since last observation, skip the processing
            days_passed = (datetime.strptime(end, "%Y-%m-%d") - last_date).days
            print(f"No. of days passed since: {days_passed}")
            if days_passed < TEMPORAL_RESOLUTION:
                print(f"No new observation expected. Quitting early")
                return None
        # If no file exists already, create an empty array 
        else:
            to_combine = []
        
        # Extracting data in scratch directory
        savedir = os.path.join(scratchdir, f"{res_name}_l7_cordeiro_zhao_gao_{fo}_{enddate}")
        if not os.path.isdir(savedir):
            os.makedirs(savedir)
        
        print(f"Extracting SA for the period {fo} -> {enddate}")

        # Creating list of dates from fo to enddate with frequency of TEMPORAL_RESOLUTION
        dates = pd.date_range(fo, enddate, freq=f'{TEMPORAL_RESOLUTION}D')
        # Grouping dates into smaller arrays to process in GEE
        grouped_dates = grouper(dates, RESULTS_PER_ITER)
        
        # For each smaller array of dates
        for subset_dates in grouped_dates:
            try:
                print(subset_dates)
                # Convert dates list to earth engine object
                dates = ee.List([ee.Date(d) for d in subset_dates if d is not None])
                # Generate Timeseries of one image corresponding to each date with water area in its attributes
                res = generate_timeseries(dates).filterMetadata('l7_images', 'greater_than', 0)
                # Extracting uncorrected water area and other information from attributes 
                uncorrected_columns_to_extract = ['from_date', 'to_date', 'water_area_cordeiro', 'non_water_area_cordeiro', 'cloud_area', 'l7_images',
                                                  'water_red_sum', 'water_green_sum', 'water_nir_sum']
                uncorrected_final_data_ee = res.reduceColumns(ee.Reducer.toList(len(uncorrected_columns_to_extract)), uncorrected_columns_to_extract).get('list')
                uncorrected_final_data = uncorrected_final_data_ee.getInfo()
                print("Uncorrected", uncorrected_final_data)
                # Extracting corrected area after corrrecting for cloud covered pixels using zhao gao correction.
                res_corrected_cordeiro = res.map(lambda im: postprocess_wrapper(im, 'water_map_cordeiro', im.get('water_area_cordeiro')))
                corrected_columns_to_extract = ['to_date', 'corrected_area']
                corrected_final_data_cordeiro_ee = res_corrected_cordeiro \
                                                    .filterMetadata('corrected_area', 'not_equals', None) \
                                                    .reduceColumns(
                                                        ee.Reducer.toList(
                                                            len(corrected_columns_to_extract)), 
                                                            corrected_columns_to_extract
                                                            ).get('list')
                corrected_final_data_cordeiro = corrected_final_data_cordeiro_ee.getInfo()
                print("Corrected - Cordeiro", corrected_final_data_cordeiro)
                # If no data point for this duration, then skip
                if len(uncorrected_final_data) == 0:
                    continue
                # Create pandas dataframes with the extracted information and merge them
                uncorrected_df = pd.DataFrame(uncorrected_final_data, columns=uncorrected_columns_to_extract)
                corrected_cordeiro_df = pd.DataFrame(corrected_final_data_cordeiro, columns=corrected_columns_to_extract).rename({'corrected_area': 'corrected_area_cordeiro'}, axis=1)
                df = pd.merge(uncorrected_df, corrected_cordeiro_df, 'left', 'to_date')

                df['from_date'] = pd.to_datetime(df['from_date'], format="%Y-%m-%d")
                df['to_date'] = pd.to_datetime(df['to_date'], format="%Y-%m-%d")
                df['mosaic_enddate'] = df['to_date'] - pd.Timedelta(1, unit='day')
                df = df.set_index('mosaic_enddate')
                print(df.head(2))
                # Save the dataframe on the disk
                fname = os.path.join(savedir, f"{df.index[0].strftime('%Y%m%d')}_{df.index[-1].strftime('%Y%m%d')}_{res_name}.csv")
                df.to_csv(fname)
                # Create a randonm sleep time
                s_time = randint(20, 30)
                print(f"Sleeping for {s_time} seconds")
                time.sleep(randint(20, 30))

                if (datetime.strptime(enddate, "%Y-%m-%d")-df.index[-1]).days < TEMPORAL_RESOLUTION:
                    print(f"Quitting: Reached enddate {enddate}")
                    break
                elif df.index[-1].strftime('%Y-%m-%d') == fo:
                    print(f"Reached last available observation - {fo}")
                    break
            except Exception as e:
                print(e)
                # log.error(e)
                continue
        
        # Combine the files into one database
        to_combine.extend([os.path.join(savedir, f) for f in os.listdir(savedir) if f.endswith(".csv")])
        if len(to_combine):
            files = [pd.read_csv(f, parse_dates=["mosaic_enddate"]).set_index("mosaic_enddate") for f in to_combine]

            data = pd.concat(files).drop_duplicates().sort_values("mosaic_enddate")
            data.to_csv(savepath)

            return savepath
        else:
            print("Observed data could not be processed to get surface area.")
            return None
    else:
        print(f"No observation observed between {start} and {end}. Quitting!")
        return None

# User-facing wrapper function
def sarea_l7(res_name,res_polygon, start, end, datadir):
    return run_process_long(res_name,res_polygon, start, end, datadir)