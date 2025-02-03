import ee
from datetime import date, datetime, timedelta, timezone
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
log = getLogger(f"{LOG_NAME}.{__name__}")

l5 = ee.ImageCollection("LANDSAT/LT05/C02/T1_L2")
gswd = ee.Image("JRC/GSW1_4/GlobalSurfaceWater")

NDWI_THRESHOLD = 0.3
SMALL_SCALE = 30
MEDIUM_SCALE = 120
LARGE_SCALE = 500
BUFFER_DIST = 500
CLOUD_COVER_LIMIT = 90
TEMPORAL_RESOLUTION = 16
RESULTS_PER_ITER = 5
MIN_RESULTS_PER_ITER = 1
QUALITY_PIXEL_BAND_NAME = 'QA_PIXEL'
BLUE_BAND_NAME = 'SR_B1'
GREEN_BAND_NAME = 'SR_B2'
RED_BAND_NAME = 'SR_B3'
NIR_BAND_NAME = 'SR_B4'
SWIR1_BAND_NAME = 'SR_B5'
SWIR2_BAND_NAME = 'SR_B7'
MISSION_END_DATE = date(2012,5,6) # last date of landsat 5 data on GEE

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

def identify_water_cluster(im, max_cluster_value):
    im = ee.Image(im)
    mbwi = im.select('MBWI')
    
    clusters = ee.List.sequence(0, max_cluster_value)

    def calc_avg_mbwi(cluster_val):
        cluster_val = ee.Number(cluster_val)
        avg_mbwi = mbwi.updateMask(im.select('cluster').eq(ee.Image(cluster_val))).reduceRegion(
            reducer = ee.Reducer.mean(),
            scale = MEDIUM_SCALE,
            geometry = aoi,
            maxPixels = 1e10
        ).get('MBWI')
        avg_mbwi_not_null = ee.Number(ee.Algorithms.If(ee.Algorithms.IsEqual(ee.Number(avg_mbwi)),
                                                   ee.Number(-99), 
                                                   ee.Number(avg_mbwi)))
        return avg_mbwi_not_null
    # print(calc_avg_mbwi(clusters.get(0)).getInfo())
    avg_mbwis = ee.Array(clusters.map(calc_avg_mbwi))

    max_mbwi_index = avg_mbwis.argmax().get(0)

    water_cluster = clusters.get(max_mbwi_index)

    return water_cluster


def cordeiro(im):
    ## Agglomerative Clustering isn't available, using Cascade K-Means Clustering based on
    ##  calinski harabasz's work
    ## https:##developers.google.com/earth-engine/apidocs/ee-clusterer-wekacascadekmeans
    band_subset = ee.List(['NDWI', 'MNDWI', SWIR2_BAND_NAME])   # using NDWI, MNDWI and B7 (SWIR2)
    sampled_pts = im.select(band_subset).sample(
        region = aoi,
        scale = SMALL_SCALE,
        numPixels = 5e3-1  ## limit of 5k points
    )
    
    no_sampled_pts = sampled_pts.size()
    
    def if_enough_sample_pts(im):
        clusterer = ee.Clusterer.wekaCascadeKMeans(
            minClusters = 2,
            maxClusters = 7,
            init = True
        ).train(sampled_pts)
        
        classified = im.select(band_subset).cluster(clusterer)
        im = im.addBands(classified)
        max_cluster_value = ee.Number(im.select('cluster').reduceRegion(
            reducer = ee.Reducer.max(),
            geometry = aoi,
            scale = LARGE_SCALE,
            maxPixels =  1e10
        ).get('cluster'))
        return ee.Dictionary({'max_cluster_value': max_cluster_value, 'classified': classified})
    
    def if_not_enough_sample_pts():
        return ee.Dictionary({'max_cluster_value': ee.Number(0), 'classified': ee.Image(0).rename('cluster')})
    
    # If clustering is possible do clustering
    def if_clustering_possible(max_cluster_value,classified,im):
        im = im.addBands(classified)
        
        water_cluster = identify_water_cluster(im, max_cluster_value)
        water_map = classified.select('cluster').eq(ee.Image.constant(water_cluster)).select(['cluster'], ['water_map_cordeiro'])
        return water_map
    
    # If no clustering is possible, use NDWI water map
    def if_clustering_not_possible(im):
        water_map = im.select(['water_map_NDWI'],['water_map_cordeiro'])
        return water_map
    
    
    after_training_dict = ee.Dictionary(ee.Algorithms.If(ee.Number(no_sampled_pts),
                                        if_enough_sample_pts(im),
                                        if_not_enough_sample_pts()))
    
    max_cluster_value = ee.Number(ee.Algorithms.If(ee.Algorithms.IsEqual(
                                    ee.Number(after_training_dict.get('max_cluster_value'))),
                                                   ee.Number(0), 
                                                   ee.Number(after_training_dict.get('max_cluster_value'))))
    
    classified = ee.Image(after_training_dict.get('classified'))
    water_map = ee.Image(ee.Algorithms.If(ee.Algorithms.IsEqual(max_cluster_value,ee.Number(0)),
                                          if_clustering_not_possible(im),
                                          if_clustering_possible(max_cluster_value,
                                                                 classified,
                                                                 im)
                                          ))
    im = im.addBands(water_map)

    return im


def process_image(im):
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
    
    #cloud_area = AOI area - area od pixels in cloud band where there is no data because we masked it coz of clouds
    cloud_area = aoi.area().subtract(im.select('cloud').Not().multiply(ee.Image.pixelArea()).reduceRegion(
        reducer = ee.Reducer.sum(),
        geometry = aoi,
        scale = SMALL_SCALE,
        maxPixels = 1e10
    ).get('cloud'))
    cloud_percent = cloud_area.multiply(100).divide(aoi.area())
    
    cordeiro_will_run_when = cloud_percent.lt(CLOUD_COVER_LIMIT)
    # NDWI based water map: Classify water wherever NDWI is greater than NDWI_THRESHOLD and add water_map_NDWI band.
    im = im.addBands(ndwi.gte(NDWI_THRESHOLD).select(['NDWI'], ['water_map_NDWI']))
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
    # Calculate red band/green band mean for water area in water_map_NDWI.
    water_red_green_mean = ee.Number(im.select('water_map_NDWI').eq(1).multiply(im.select(RED_BAND_NAME)).divide(im.select(
                    GREEN_BAND_NAME)).reduceRegion(
        reducer = ee.Reducer.mean(), 
        geometry = aoi, 
        scale = SMALL_SCALE, 
        maxPixels = 1e10
    ).get('water_map_NDWI'))
    # Calculate nir band/red band mean for water area in water_map_NDWI.
    water_nir_red_mean = ee.Number(im.select('water_map_NDWI').eq(1).multiply(im.select(NIR_BAND_NAME)).divide(im.select(
                    RED_BAND_NAME)).reduceRegion(
        reducer = ee.Reducer.mean(), 
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
    im = im.set('water_red_green_mean', water_red_green_mean)
    im = im.set('water_nir_red_mean', water_nir_red_mean)
    
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
        
        def if_hist_not_null(im,hist):
    
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
            
            improved = improved.set("corrected_area", corrected_area.multiply(1e-6));
            improved = improved.set("POSTPROCESSING_SUCCESSFUL", 1);
        
            return improved
        
        def if_hist_null(im):
            # Preserve the uncorrected area in the corrected area column & POSTPROCESSING=1
            # because otherwise it will be substituted with nan.
            uncorrected_area = ee.Number(im.get('water_area_clustering'))
            improved = im.set('corrected_area', uncorrected_area)
            improved = improved.set('POSTPROCESSING_SUCCESSFUL', 1)
            return improved
        
        improved = ee.Image(ee.Algorithms.If(
            hist, if_hist_not_null(im,hist), if_hist_null(im)
        ))
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

def process_date(date):
    # Given date, calculate end date by adding TEMPORAL_RESOLUTION - 1 days
    date = ee.Date(date)
    from_date = date
    to_date = date.advance(TEMPORAL_RESOLUTION - 1, 'day')
    # Filter the image collection for these dates and AOI and run preprocess function (cloud calculations, scaling, adding & setting start and end) on them
    l5_subset = l5.filterDate(from_date, to_date).filterBounds(aoi).map(preprocess)

    # Get mosaic of images if there is atleast one image for this time duration with NDWI as the quality factor (keep High NDWI)
    im = ee.Image(ee.Algorithms.If(l5_subset.size().neq(0), l5_subset.map(calc_ndwi).qualityMosaic('NDWI'), ee.Image.constant(0)))
    # Process NDWI Image if there is atleast one image for this time duration
    im = ee.Image(ee.Algorithms.If(l5_subset.size().neq(0), process_image(im), ee.Image.constant(0)))

    # Set attributes of from and to date along with number of images during the time duration
    im = im.set('from_date', from_date.format("YYYY-MM-dd"))
    im = im.set('to_date', to_date.format("YYYY-MM-dd"))
    im = im.set('l5_images', l5_subset.size())
    
    # im = ee.Algorithms.If(im.bandNames().size().eq(1), ee.Number(0), im)
    
    return ee.Image(im)


def generate_timeseries(dates):
    raw_ts = dates.map(process_date)
    # raw_ts = raw_ts.removeAll([0])
    imcoll = ee.ImageCollection.fromImages(raw_ts)

    return imcoll

def get_first_obs(start_date, end_date):
    # Filter collection, sort by date, and get the first image's timestamp
    first_im = (
        l5.filterBounds(aoi)
        .filterDate(start_date, end_date)
        .limit(1, 'system:time_start')  # Explicitly limit by earliest date
        .reduceColumns(ee.Reducer.first(), ['system:time_start'])
    )
    
    # Convert timestamp to formatted date
    first_date = ee.Date(first_im.get('first')).format('YYYY-MM-dd')
    
    return first_date

def run_process_long(res_name, res_polygon, start, end, datadir, results_per_iter=RESULTS_PER_ITER):
    fo = start #fo: first observation
    enddate = end

    # Extracting reservoir geometry 
    global aoi
    aoi = poly2feature(res_polygon,BUFFER_DIST).geometry()

    ## Checking the number of images in the interval as Landsat 5 might have missing data for a lot of places for longer durations.
    number_of_images = l5.filterBounds(aoi).filterDate(start, end).size().getInfo()
    
    if(number_of_images):
        # getting first observation in the filtered collection
        print('Checking first observation date in the given time interval.')
        fo = get_first_obs(start, end).getInfo() 
        first_obs = datetime.strptime(fo, '%Y-%m-%d')
        print(f"First Observation: {first_obs}")

        scratchdir = os.path.join(datadir, "_scratch")

        # If data already exists, only get new data starting from the last one
        savepath = os.path.join(datadir, f"{res_name}.csv")
        
        # If an existing file exists, 
        if os.path.isfile(savepath):
            # Read the existing file
            temp_df = pd.read_csv(savepath, parse_dates=['mosaic_enddate']).set_index('mosaic_enddate')

            # Get the last date in the existing file and adjust the first observation to before last date (last date might not be for this satellite. Its TMS-OS data's last date.)
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
        savedir = os.path.join(scratchdir, f"{res_name}_l5_cordeiro_zhao_gao_{fo}_{enddate}")
        if not os.path.isdir(savedir):
            os.makedirs(savedir)
        
        print(f"Extracting SA for the period {fo} -> {enddate}")

        # Creating list of dates from fo to enddate with frequency of TEMPORAL_RESOLUTION
        dates = pd.date_range(fo, enddate, freq=f'{TEMPORAL_RESOLUTION}D')
        # Grouping dates into smaller arrays to process in GEE
        grouped_dates = grouper(dates, results_per_iter)
        
        # Until results per iteration is less than min results per iteration
        while results_per_iter >= MIN_RESULTS_PER_ITER:
            # try to run for each subset of dates
            try:
                # For each smaller array of dates
                for subset_dates in grouped_dates:
                    try:
                        print(subset_dates)
                        # Check if the start of subset_dates is after the end date of the mission. If so quit.
                        if subset_dates[0].date() > MISSION_END_DATE:
                            print(f"Reached mission end date. No further data available from Landsat-5 satellite mission in GEE - {MISSION_END_DATE}")
                            break
                        # Convert dates list to earth engine object
                        dates = ee.List([ee.Date(d) for d in subset_dates if d is not None])
                        # Generate Timeseries of one image corresponding to each date with water area in its attributes
                        res = generate_timeseries(dates).filterMetadata('l5_images', 'greater_than', 0)
                        # Extracting uncorrected water area and other information from attributes 
                        uncorrected_columns_to_extract = ['from_date', 'to_date', 'water_area_cordeiro', 'non_water_area_cordeiro', 'cloud_area', 'l5_images',
                                                        'water_red_sum', 'water_green_sum', 'water_nir_sum','water_red_green_mean','water_nir_red_mean']
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
                        log.error(e)
                        # Adjust results_per_iter only if error includes "Too many concurrent aggregations"
                        if "Too many concurrent aggregations" in str(e):
                            results_per_iter -= 1
                            print(f"Reducing Results per iteration to {results_per_iter} due to error.")
                            if results_per_iter < MIN_RESULTS_PER_ITER:
                                print("Minimum Results per iteration reached. Continuing to next group of dates.")
                                results_per_iter = MIN_RESULTS_PER_ITER
                                continue
                            else:
                                raise Exception(f'Reducing Results per iteration to {results_per_iter}.')
                        else:
                            continue
            # This exception will be only raised if the error is "Too many concurrent aggregations".
            # and Results per iteration will be reduced but still be greater than or equal to minimum results per iteration.
            # We will continue while loop and for loop within while loop from the left over grouped dates.
            except Exception as e:
                dates = pd.date_range(subset_dates[0], enddate, freq=f'{TEMPORAL_RESOLUTION}D')
                grouped_dates = grouper(dates, results_per_iter)
                continue
            # In case no exception is raised and the complete for loop ran succesfully, break the while loop 
            # because we need to run the for loop only once.
            else:
                break
        
        # Combine the files into one database
        to_combine.extend([os.path.join(savedir, f) for f in os.listdir(savedir) if f.endswith(".csv")])
        if len(to_combine):
            files = [pd.read_csv(f, parse_dates=["mosaic_enddate"]).set_index("mosaic_enddate") for f in to_combine]

            data = pd.concat(files).drop_duplicates().sort_values("mosaic_enddate")
            data.to_csv(savepath)

            return savepath
        else:
            print(f"Observed data between {start} and {end} could not be processed to get surface area. It may be due to cloud cover or other issues, Quitting!")
            return None
    else:
        print(f"No observation observed between {start} and {end}. Quitting!")
        return None

# User-facing wrapper function
def sarea_l5(res_name,res_polygon, start, end, datadir):
    return run_process_long(res_name,res_polygon, start, end, datadir)



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
    
