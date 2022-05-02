# import geemap as gee
import ee
from datetime import datetime, timedelta, timezone
import pandas as pd
import time
import os
from random import randint
import argparse
from itertools import zip_longest
import pprint

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
RESULTS_PER_ITER = 2

# aoi = reservoir.geometry().simplify(100).buffer(500);


# s2_subset = s2.filterBounds(aoi).filterDate(start_date, end_date)

##############################################
##       Defining necessary functions       ##
##############################################


##########################
## Dealing with Cloud   ##
##########################

def scl_cloud_mask(im):
    cloudmask = im.expression("cloud = (SCL==3)|(SCL==8)|(SCL==9)|(SCL==10)", {'SCL': im.select('SCL')})
    # cloud_area = cloudmask.reduceRegion(
    #     reducer = ee.Reducer.sum(),
    #     geometry = aoi,
    #     scale = SMALL_SCALE,
    #     maxPixels = 1e10
    # ).get('')
    im = im.addBands(cloudmask)
    return im.updateMask(cloudmask.select('cloud').Not())


##########################################################/
## Processing individual images - water classification   ##
##########################################################/
def preprocess(im):
    ## Apply scaling factor
    ## clipped = im.clip(aoi)
    clipped = im
    clipped = scl_cloud_mask(clipped)
    cloud = clipped.select('cloud')
    clipped = clipped.addBands(clipped.select(['B.', 'B..']).multiply(0.0001), None, True)

    return clipped


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
    band_subset = ee.List(['NDWI', 'B12'])
    sampled_pts = im.select(band_subset).sample(
        region = aoi,
        scale = SMALL_SCALE,
        numPixels = 4e3  ## limit of 5k points, staying at 4k
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
    water_map = classified.select('cluster').eq(ee.Image.constant(water_cluster)).select(['cluster'], ['water_map_cordeiro'])
    im = im.addBands(water_map)

    return im


def process_image(im):
    ## FOR SENTINEL-2
    # im = im.select(['B3', 'B12', 'B2', 'B4', 'B8', 'B11', 'cloud'])
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
    
    cloud_area = aoi.area().subtract(im.select('cloud').Not().multiply(ee.Image.pixelArea()).reduceRegion(
        reducer = ee.Reducer.sum(),
        geometry = aoi,
        scale = SMALL_SCALE,
        maxPixels = 1e10
    ).get('cloud'))
    cloud_percent = cloud_area.multiply(100).divide(aoi.area())
    
    cordeiro_will_run_when = cloud_percent.lt(CLOUD_COVER_LIMIT)

    # Clusting based
    im = im.addBands(ee.Image(ee.Algorithms.If(cordeiro_will_run_when, cordeiro(im), ee.Image.constant(-1e6))))  # run cordeiro only if cloud percent is < 90%

    water_area_cordeiro = ee.Number(ee.Algorithms.If(cordeiro_will_run_when,
        ee.Number(im.select('water_map_cordeiro').eq(1).multiply(ee.Image.pixelArea()).reduceRegion(
            reducer = ee.Reducer.sum(), 
            geometry = aoi, 
            scale = SMALL_SCALE, 
            maxPixels = 1e10
        ).get('water_map_cordeiro')),
        ee.Number(-1e6)
    ))
    non_water_area_cordeiro = ee.Number(ee.Algorithms.If(cordeiro_will_run_when,
        ee.Number(im.select('water_map_cordeiro').neq(1).multiply(ee.Image.pixelArea()).reduceRegion(
            reducer = ee.Reducer.sum(), 
            geometry = aoi, 
            scale = SMALL_SCALE, 
            maxPixels = 1e10
        ).get('water_map_cordeiro')),
        ee.Number(-1e6)
    ))

    # NDWI based
    im = im.addBands(ndwi.gte(NDWI_THRESHOLD).select(['NDWI'], ['water_map_NDWI']))
    water_area_NDWI = ee.Number(im.select('water_map_NDWI').eq(1).multiply(ee.Image.pixelArea()).reduceRegion(
        reducer = ee.Reducer.sum(), 
        geometry = aoi, 
        scale = SMALL_SCALE, 
        maxPixels = 1e10
    ).get('water_map_NDWI'))
    non_water_area_NDWI = ee.Number(im.select('water_map_NDWI').neq(1).multiply(ee.Image.pixelArea()).reduceRegion(
        reducer = ee.Reducer.sum(), 
        geometry = aoi, 
        scale = SMALL_SCALE, 
        maxPixels = 1e10
    ).get('water_map_NDWI'))
    
    im = im.set('cloud_area', cloud_area.multiply(1e-6))
    im = im.set('cloud_percent', cloud_percent)
    im = im.set('water_area_cordeiro', water_area_cordeiro.multiply(1e-6))
    im = im.set('non_water_area_cordeiro', non_water_area_cordeiro.multiply(1e-6))
    im = im.set('water_area_NDWI', water_area_NDWI.multiply(1e-6))
    im = im.set('non_water_area_NDWI', non_water_area_NDWI.multiply(1e-6))
    
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
        
        improved = improved.set("corrected_area", corrected_area.multiply(1e-6));
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
    return im.addBands(im.normalizedDifference(['B3', 'B8']).rename('NDWI'))

def process_date(date):
    date = ee.Date(date)
    from_date = date
    to_date = date.advance(TEMPORAL_RESOLUTION - 1, 'day')
    s2_subset = s2.filterDate(from_date, to_date).filterBounds(aoi).map(preprocess)

    # im = ee.Image(ee.Algorithms.If(s2_subset.size().neq(0), s2_subset.map(process_image).qualityMosaic('NDWI'), ee.Image.constant(0)))
    im = ee.Image(ee.Algorithms.If(s2_subset.size().neq(0), s2_subset.map(calc_ndwi).qualityMosaic('NDWI'), ee.Image.constant(0)))
    
    im = ee.Image(ee.Algorithms.If(s2_subset.size().neq(0), process_image(im), ee.Image.constant(0)))

    ## im = ee.Algorithms.If(im.bandNames process_image(im))
    im = im.set('from_date', from_date.format("YYYY-MM-dd"))
    im = im.set('to_date', to_date.format("YYYY-MM-dd"))
    im = im.set('s2_images', s2_subset.size())
    
    # im = ee.Algorithms.If(im.bandNames().size().eq(1), ee.Number(0), im)
    
    return ee.Image(im)


def generate_timeseries(dates):
    raw_ts = dates.map(process_date)
    # raw_ts = raw_ts.removeAll([0]);
    imcoll = ee.ImageCollection.fromImages(raw_ts)

    return imcoll

def get_first_obs(start_date, end_date):
    first_im = s2.filterBounds(aoi).filterDate(start_date, end_date).first()
    str_fmt = 'YYYY-MM-dd'
    return ee.Date.parse(str_fmt, ee.Date(first_im.get('system:time_start')).format(str_fmt))

def run_process_long(res_name, start, end, datadir):
    fo = start
    enddate = end

    reservoir = ee.FeatureCollection("users/pdas47/RAT/" + res_name)
    global aoi
    aoi = reservoir.geometry().buffer(BUFFER_DIST)
    
    fo = get_first_obs(start, end).format('YYYY-MM-dd').getInfo()
    first_obs = datetime.strptime(fo, '%Y-%m-%d')

    scratchdir = os.path.join(datadir, "_scratch")

    # flag = True
    num_runs = 0

    # If data already exists, only get new data starting from the last one
    savepath = os.path.join(datadir, f"{res_name}.csv")
    
    if os.path.isfile(savepath):
        temp_df = pd.read_csv(savepath, parse_dates=['mosaic_enddate']).set_index('mosaic_enddate')

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

    # # redo the calculations part and see where it is complaining about too many aggregations
    # subset_dates = next(grouped_dates)
    # dates = ee.List([ee.Date(d) for d in subset_dates if d is not None])

    # print(subset_dates)
    # res = generate_timeseries(dates).filterMetadata('s2_images', 'greater_than', 0)
    # pprint.pprint(res.aggregate_array('s2_images').getInfo())

    # uncorrected_columns_to_extract = ['from_date', 'to_date', 'water_area_cordeiro', 'non_water_area_cordeiro', 'water_area_NDWI', 'non_water_area_NDWI', 'cloud_area', 's2_images']
    # uncorrected_final_data_ee = res.reduceColumns(ee.Reducer.toList(len(uncorrected_columns_to_extract)), uncorrected_columns_to_extract).get('list')
    # uncorrected_final_data = uncorrected_final_data_ee.getInfo()
    


    for subset_dates in grouped_dates:
        print(subset_dates)
        dates = ee.List([ee.Date(d) for d in subset_dates if d is not None])
        
        res = generate_timeseries(dates).filterMetadata('s2_images', 'greater_than', 0)
        # pprint.pprint(res.getInfo())

        uncorrected_columns_to_extract = ['from_date', 'to_date', 'water_area_cordeiro', 'non_water_area_cordeiro', 'water_area_NDWI', 'non_water_area_NDWI', 'cloud_area', 's2_images']
        uncorrected_final_data_ee = res.reduceColumns(ee.Reducer.toList(len(uncorrected_columns_to_extract)), uncorrected_columns_to_extract).get('list')
        uncorrected_final_data = uncorrected_final_data_ee.getInfo()
        print("Uncorrected", uncorrected_final_data)

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

        res_corrected_NDWI = res.map(lambda im: postprocess_wrapper(im, 'water_map_NDWI', im.get('water_area_NDWI')))
        corrected_final_data_NDWI_ee = res_corrected_NDWI \
                                            .filterMetadata('corrected_area', 'not_equals', None) \
                                            .reduceColumns(
                                                ee.Reducer.toList(
                                                    len(corrected_columns_to_extract)), 
                                                    corrected_columns_to_extract
                                                    ).get('list')
        
        corrected_final_data_NDWI = corrected_final_data_NDWI_ee.getInfo()
        
        print(uncorrected_final_data, corrected_final_data_cordeiro, corrected_final_data_NDWI)

        uncorrected_df = pd.DataFrame(uncorrected_final_data, columns=uncorrected_columns_to_extract)
        corrected_cordeiro_df = pd.DataFrame(corrected_final_data_cordeiro, columns=corrected_columns_to_extract).rename({'corrected_area': 'corrected_area_cordeiro'}, axis=1)
        corrected_NDWI_df = pd.DataFrame(corrected_final_data_NDWI, columns=corrected_columns_to_extract).rename({'corrected_area': 'corrected_area_NDWI'}, axis=1)
        corrected_df = pd.merge(corrected_cordeiro_df, corrected_NDWI_df, 'left', 'to_date')
        df = pd.merge(uncorrected_df, corrected_df, 'left', 'to_date')

        df['from_date'] = pd.to_datetime(df['from_date'], format="%Y-%m-%d")
        df['to_date'] = pd.to_datetime(df['to_date'], format="%Y-%m-%d")
        df['mosaic_enddate'] = df['to_date'] - pd.Timedelta(1, unit='day')
        df = df.set_index('mosaic_enddate')
        print(df.head(2))

        fname = os.path.join(savedir, f"{df.index[0].strftime('%Y%m%d')}_{df.index[-1].strftime('%Y%m%d')}_{res_name}.csv")
        df.to_csv(fname)

        s_time = randint(20, 30)
        print(f"Sleeping for {s_time} seconds")
        time.sleep(randint(20, 30))

        if (datetime.strptime(enddate, "%Y-%m-%d")-df.index[-1]).days < TEMPORAL_RESOLUTION:
            print(f"Quitting: Reached enddate {enddate}")
            break
        elif df.index[-1].strftime('%Y-%m-%d') == fo:
            print(f"Reached last available observation - {fo}")
            break
        elif num_runs > 1000:
            print("Quitting: Reached 1000 iterations")
            break

    # Combine the files into one database
    to_combine.extend([os.path.join(savedir, f) for f in os.listdir(savedir) if f.endswith(".csv")])

    files = [pd.read_csv(f, parse_dates=["mosaic_enddate"]).set_index("mosaic_enddate") for f in to_combine]
    data = pd.concat(files).drop_duplicates().sort_values("mosaic_enddate")

    data.to_csv(savepath)

    return savepath

# User-facing wrapper function
def sarea_s2(reservoir, start, end, datadir):
    run_process_long(reservoir, start, end, datadir)


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

    # print(f"{reservoir = }, {start = }, {end = }")
    run_process_long(reservoir, start, end, "data/s2")



if __name__ == '__main__':
    main()