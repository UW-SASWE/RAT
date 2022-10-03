# import geemap as gee
from asyncio.log import logger
import ee
from datetime import datetime, timedelta, timezone
import pandas as pd
import time
import os
from random import randint
import argparse
from itertools import zip_longest
import pprint
import numpy as np

from utils.logging import LOG_NAME, NOTIFICATION
from logging import getLogger

log = getLogger(f"{LOG_NAME}.{__name__}")


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

ee.Initialize()

# L8 = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
# gswd = ee.Image("JRC/GSW1_3/GlobalSurfaceWater").select("occurrence")
# Threshold = 0.0

# NEW STUFF
s2 = ee.ImageCollection("COPERNICUS/S2_SR")
gswd = ee.Image("JRC/GSW1_3/GlobalSurfaceWater")
reservoir_geom_prefix = "users/saswegee/pritam/RAT-Mekong/"
reservoir = ee.FeatureCollection("users/pdas47/RAT/Sirindhorn")
rgb_vis_params = {"bands":["B4","B3","B2"],"min":0,"max":0.4}

NDWI_THRESHOLD = 0.3;
SMALL_SCALE = 20;
MEDIUM_SCALE = 120;
LARGE_SCALE = 500;
BUFFER_DIST = 500
CLOUD_COVER_LIMIT = 90
start_date = ee.Date('2019-01-01')
end_date = ee.Date('2019-02-01')
TEMPORAL_RESOLUTION = 5
RESULTS_PER_ITER = 5

# AOI = reservoir.geometry().simplify(100).buffer(500);


# s2_subset = s2.filterBounds(AOI).filterDate(start_date, end_date)

##############################################
##       Defining necessary functions       ##
##############################################


##########################
## Dealing with Cloud   ##
##########################

def scl_cloud_mask(im):
    cloudmask = im.expression("cloud = (SCL==3)|(SCL==8)|(SCL==9)|(SCL==10)", {'SCL': im.select('SCL')})
    im = im.addBands(cloudmask)
    return im.updateMask(cloudmask.select('cloud').Not())


##########################################################/
## Processing individual images - water classification   ##
##########################################################/
def preprocess(im):
    ## Apply scaling factor
    ## clipped = im.clip(AOI)
    im = scl_cloud_mask(im)
    cloud = im.select('cloud')
    im = im.addBands(im.select(['B.', 'B..']).multiply(0.0001), None, True)
    im = im.addBands(cloud)

    return im

def identify_water_cluster(im):
    im = ee.Image(im)
    mbwi = im.select('MBWI')

    max_cluster_value = ee.Number(im.select('cluster').reduceRegion(
        reducer = ee.Reducer.max(),
        geometry = AOI,
        scale = LARGE_SCALE,
        maxPixels =  1e10
    ).get('cluster'))

    clusters = ee.List.sequence(0, max_cluster_value)

    def calc_avg_mbwi(cluster_val):
        cluster_val = ee.Number(cluster_val)
        avg_mbwi = mbwi.updateMask(im.select('cluster').eq(ee.Image(cluster_val))).reduceRegion(
            reducer = ee.Reducer.mean(),
            scale = MEDIUM_SCALE,
            geometry = AOI,
            maxPixels = 1e10
        ).get('MBWI')
        return avg_mbwi

    avg_mbwis = ee.Array(clusters.map(calc_avg_mbwi))

    max_mbwi_index = avg_mbwis.argmax().get(0)

    water_cluster = clusters.get(max_mbwi_index)

    return water_cluster

def clustering(im):
    band_subset = ee.List(['NDWI', 'B12'])
    sampled_pts = im.select(band_subset).sample(
        region = AOI,
        scale = SMALL_SCALE,
        numPixels = 4999  ## limit of 5k points, staying at 4k
    )
    
    ## Agglomerative Clustering isn't available, using Cascade K-Means Clustering based on
    ##  calinski harabasz's work
    ## https:##developers.google.com/earth-engine/apidocs/ee-clusterer-wekacascadekmeans
    clusterer = ee.Clusterer.wekaCascadeKMeans(
        minClusters = 2,
        maxClusters = 7,
        init = True
    ).train(sampled_pts)
    
    classified = im.select(band_subset).cluster(clusterer)
    im = im.addBands(classified)
    
    water_cluster = identify_water_cluster(im)
    water_map = classified.select('cluster').eq(ee.Image.constant(water_cluster)).select(['cluster'], ['water_map_clustering'])
    im = im.addBands(water_map)

    return im


def process_image(im):
    # Process Image
    ndwi = im.normalizedDifference(['B3', 'B8']).rename('NDWI');
    im = im.addBands(ndwi);
    mndwi = im.normalizedDifference(['B3', 'B12']).rename('MNDWI');
    im = im.addBands(mndwi);
    mbwi = im.expression("MBWI = 3*B3-B4-B8-B11-B12", {
        'B3': im.select('B3'),
        'B4': im.select('B4'),
        'B8': im.select('B8'),
        'B11': im.select('B11'),
        'B12': im.select('B12')
    })
    im = im.addBands(mbwi);
    cloud_area = AOI.area().subtract(im.select('cloud').Not().multiply(ee.Image.pixelArea()).reduceRegion(
        reducer = ee.Reducer.sum(),
        geometry = AOI,
        scale = SMALL_SCALE,
        maxPixels = 1e10
    ).get('cloud'))
    cloud_percent = cloud_area.multiply(100).divide(AOI.area())
    
    CLOUD_LIMIT_SATISFIED = cloud_percent.lt(CLOUD_COVER_LIMIT)

    # Clustering based
    im = im.addBands(
        ee.Image(
            ee.Algorithms.If(
                CLOUD_LIMIT_SATISFIED, 
                clustering(im), 
                ee.Image.constant(-1e6)
            )
        )
    )  # run clustering only if cloud percent is < 90%

    water_area_clustering = ee.Number(
        ee.Algorithms.If(
            CLOUD_LIMIT_SATISFIED,
            ee.Number(im.select('water_map_clustering').eq(1).multiply(ee.Image.pixelArea()).reduceRegion(
                reducer = ee.Reducer.sum(), 
                geometry = AOI, 
                scale = SMALL_SCALE, 
                maxPixels = 1e10
            ).get('water_map_clustering')),
            ee.Number(-1e6)
        )
    )
    non_water_area_clustering = ee.Number(
        ee.Algorithms.If(
            CLOUD_LIMIT_SATISFIED,
            ee.Number(im.select('water_map_clustering').neq(1).multiply(ee.Image.pixelArea()).reduceRegion(
                reducer = ee.Reducer.sum(), 
                geometry = AOI, 
                scale = SMALL_SCALE, 
                maxPixels = 1e10
            ).get('water_map_clustering')),
            ee.Number(-1e6)
        )
    )

    im = im.set('cloud_area', cloud_area.multiply(1e-6))
    im = im.set('cloud_percent', cloud_percent)
    im = im.set('water_area_clustering', water_area_clustering.multiply(1e-6))
    im = im.set('non_water_area_clustering', non_water_area_clustering.multiply(1e-6))
    im = im.set('PROCESSING_SUCCESSFUL', CLOUD_LIMIT_SATISFIED)
    
    return im

def postprocess(im, bandName='water_map_clustering'):
    gswd_masked = gswd.updateMask(im.select(bandName).eq(1))
    
    hist = ee.List(gswd_masked.reduceRegion(
        reducer = ee.Reducer.autoHistogram(minBucketWidth = 1),
        geometry = AOI,
        scale = SMALL_SCALE,
        maxPixels = 1e10
    ).get('occurrence'))
    
    counts = ee.Array(hist).transpose().toList()
    
    omega = ee.Number(0.17)
    count_thresh = ee.Number(counts.map(lambda lis: ee.List(lis).reduce(ee.Reducer.mean())).get(1)).multiply(omega)
    
    count_thresh_index = ee.Array(counts.get(1)).gt(count_thresh).toList().indexOf(1)
    occurrence_thresh = ee.Number(ee.List(counts.get(0)).get(count_thresh_index))

    water_map = im.select([bandName], ['water_map'])
    gswd_improvement = gswd.clip(AOI).gte(occurrence_thresh).updateMask(water_map.mask().Not()).select(["occurrence"], ["water_map"])
    
    improved = ee.ImageCollection([water_map, gswd_improvement]).mosaic().select(['water_map'], ['water_map_zhao_gao'])
    
    corrected_area = ee.Number(improved.select('water_map_zhao_gao').multiply(ee.Image.pixelArea()).reduceRegion(
        reducer = ee.Reducer.sum(), 
        geometry = AOI, 
        scale = SMALL_SCALE, 
        maxPixels = 1e10
    ).get('water_map_zhao_gao'))
    
    improved = improved.set("corrected_area", corrected_area.multiply(1e-6));
    improved = improved.set("POSTPROCESSING_SUCCESSFUL", 1);
    
    return improved

def postprocess_wrapper(im, bandName='water_map_clustering'):
    
    def do_not_postprocess():
        im = ee.Image.constant(-1)
        im = im.set('corrected_area', -1)
        im = im.set('POSTPROCESSING_SUCCESSFUL', 0)

        return im

    improved = ee.Algorithms.If(
        im.get('PROCESSING_SUCCESSFUL'),
        postprocess(im, bandName),
        do_not_postprocess()
    )

    return improved


############################################################/
## Code from here takes care of the time-series generation   ##
############################################################/
def calc_ndwi(im):
    return im.addBands(im.normalizedDifference(['B3', 'B8']).rename('NDWI'))

def process_date(date):
    date = ee.Date(date)
    to_date = date.advance(1, 'day')
    from_date = date.advance(-(TEMPORAL_RESOLUTION-1), 'day')
    s2_subset = s2.filterDate(from_date, to_date).filterBounds(AOI).map(preprocess)

    ENOUGH_IMAGES = s2_subset.size().neq(0)
    im = ee.Image(
        ee.Algorithms.If(
            ENOUGH_IMAGES, 
            s2_subset.map(calc_ndwi).qualityMosaic('NDWI'), 
            ee.Image.constant(-1)
        )
    )
    
    def not_enough_images():
        im = ee.Image.constant(-1)
        im = im.set('PROCESSING_SUCCESSFUL', 0)

        return im

    im = ee.Image(
        ee.Algorithms.If(
            ENOUGH_IMAGES, 
            process_image(im), 
            not_enough_images()
        )
    )

    im = im.set('from_date', from_date.format("YYYY-MM-dd"))
    im = im.set('to_date', to_date.format("YYYY-MM-dd"))
    im = im.set('system:time_start', date.format("YYYY-MM-dd"))
    im = im.set('s2_images', s2_subset.size())
    im = im.set('WAS_PROCESSED', ENOUGH_IMAGES)

    return ee.Image(im)


def generate_timeseries(dates):
    raw_ts = dates.map(process_date)
    # raw_ts = raw_ts.removeAll([0]);
    imcoll = ee.ImageCollection.fromImages(raw_ts)

    return imcoll

def get_first_obs(start_date, end_date):
    first_im = s2.filterBounds(AOI).filterDate(start_date, end_date).first()
    str_fmt = 'YYYY-MM-dd'
    return ee.Date.parse(str_fmt, ee.Date(first_im.get('system:time_start')).format(str_fmt))

def run_process_long(res_name, start, end, datadir):
    fo = start
    enddate = end

    reservoir = ee.FeatureCollection(reservoir_geom_prefix + res_name)
    global AOI
    AOI = reservoir.geometry().buffer(BUFFER_DIST)
    
    fo = get_first_obs(start, end).format('YYYY-MM-dd').getInfo()
    first_obs = datetime.strptime(fo, '%Y-%m-%d')

    scratchdir = os.path.join(datadir, "_scratch")

    # flag = True
    num_runs = 0

    # If data already exists, only get new data starting from the last one
    savepath = os.path.join(datadir, f"{res_name}.csv")
    
    if os.path.isfile(savepath):
        temp_df = pd.read_csv(savepath, parse_dates=['date']).set_index('date')

        last_date = temp_df.index[-1].to_pydatetime()
        fo = (last_date - timedelta(days=TEMPORAL_RESOLUTION*2)).strftime("%Y-%m-%d")
        to_combine = [savepath]
        print(f"Existing file found - Last observation ({TEMPORAL_RESOLUTION*2} day lag): {last_date}")

        # If 16 days have not passed since last observation, skip the processing
        days_passed = (datetime.strptime(end, "%Y-%m-%d") - last_date).days
        print(f"No. of days passed since: {days_passed}")
        if days_passed < TEMPORAL_RESOLUTION:
            print(f"No new observation expected. Quitting early")
            return None
    else:
        to_combine = []
    
    savedir = os.path.join(scratchdir, f"{res_name}_s2_cordeiro_zhao_gao_{fo}_{enddate}")
    if not os.path.isdir(savedir):
        os.makedirs(savedir)
    
    print(f"Extracting SA for the period {fo} -> {enddate}")

    dates = pd.date_range(fo, enddate, freq=f'{TEMPORAL_RESOLUTION}D')
    grouped_dates = grouper(dates, RESULTS_PER_ITER)

    for subset_dates in grouped_dates:
        try:
            print(subset_dates)
            dates = ee.List([ee.Date(d) for d in subset_dates if d is not None])
            
            ts_imcoll = generate_timeseries(dates)
            postprocessed_ts_imcoll = ts_imcoll.map(postprocess_wrapper)

            # Download the data locally
            ts_imcoll_L = ts_imcoll.getInfo()
            postprocessed_ts_imcoll_L = postprocessed_ts_imcoll.getInfo()

            # Parse the data to create dataframe
            PROCESSING_STATUSES = []
            POSTPROCESSING_STATUSES = []
            cloud_areas = []
            cloud_percents = []
            from_dates = []
            to_dates = []
            obs_dates = []
            non_water_areas = []
            water_areas = []
            water_areas_zhaogao = []
            for f, f_postprocessed in zip(ts_imcoll_L['features'], postprocessed_ts_imcoll_L['features']):
                PROCESSING_STATUS = f['properties']['PROCESSING_SUCCESSFUL']
                PROCESSING_STATUSES.append(PROCESSING_STATUS)
                POSTPROCESSING_STATUS = f_postprocessed['properties']['POSTPROCESSING_SUCCESSFUL']
                POSTPROCESSING_STATUSES.append(POSTPROCESSING_STATUS)
                obs_dates.append(pd.to_datetime(f['properties']['system:time_start']))
                from_dates.append(pd.to_datetime(f['properties']['from_date']))
                to_dates.append(pd.to_datetime(f['properties']['to_date']))
                if PROCESSING_STATUS:
                    water_areas.append(f['properties']['water_area_clustering'])
                    non_water_areas.append(f['properties']['non_water_area_clustering'])
                    cloud_areas.append(f['properties']['cloud_area'])
                    cloud_percents.append(f['properties']['cloud_percent'])
                else:
                    water_areas.append(np.nan)
                    non_water_areas.append(np.nan)
                    cloud_areas.append(np.nan)
                    cloud_percents.append(np.nan)
                if POSTPROCESSING_STATUS:
                    water_areas_zhaogao.append(f_postprocessed['properties']['corrected_area'])
                else:
                    water_areas_zhaogao.append(np.nan)
            
            df = pd.DataFrame({
                'date': obs_dates,
                'PROCESSING_STATUS': PROCESSING_STATUSES,
                'POSTPROCESSING_STATUS': POSTPROCESSING_STATUSES,
                'from_date': from_dates,
                'to_date': to_dates,
                'cloud_area': cloud_areas,
                'cloud_percent': cloud_percents,
                'water_area_uncorrected': water_areas,
                'non_water_area': non_water_areas,
                'water_area_corrected': water_areas_zhaogao
            }).set_index('date')

            fname = os.path.join(savedir, f"{df.index[0].strftime('%Y%m%d')}_{df.index[-1].strftime('%Y%m%d')}_{res_name}.csv")
            df.to_csv(fname)
            print(df.tail())

            s_time = randint(5, 10)
            print(f"Sleeping for {s_time} seconds")
            time.sleep(s_time)

            if (datetime.strptime(enddate, "%Y-%m-%d")-df.index[-1]).days < TEMPORAL_RESOLUTION:
                print(f"Quitting: Reached enddate {enddate}")
                break
            elif df.index[-1].strftime('%Y-%m-%d') == fo:
                print(f"Reached last available observation - {fo}")
                break
            elif num_runs > 1000:
                print("Quitting: Reached 1000 iterations")
                break
        except Exception as e:
            log.error(e)
            continue

    # Combine the files into one database
    to_combine.extend([os.path.join(savedir, f) for f in os.listdir(savedir) if f.endswith(".csv")])

    files = [pd.read_csv(f, parse_dates=["date"]).set_index("date") for f in to_combine]
    data = pd.concat(files).drop_duplicates().sort_values("date")

    data.to_csv(savepath)

    return savepath

# User-facing wrapper function
def sarea_s2(reservoir, start, end, datadir):
    return run_process_long(reservoir, start, end, datadir)


def main():
    # Setup argument parser
    parser = argparse.ArgumentParser(description="Generate Reservoir Surface area time series")

    parser.add_argument("reservoir")
    parser.add_argument("start_date")
    parser.add_argument("end_date")

    args = parser.parse_args()

    reservoir = args.reservoir
    start = args.start_date
    end = args.end_date

    run_process_long(reservoir, start, end, "data/s2")



if __name__ == '__main__':
    main()