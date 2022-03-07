# import geemap as gee
import ee
from datetime import datetime, timedelta
import pandas as pd
import time
import os
from random import randint
import argparse

ee.Initialize()

L8 = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
gswd = ee.Image("JRC/GSW1_3/GlobalSurfaceWater").select("occurrence")
Threshold = 0.0


def preprocess_image(im):
    # Clip
    clipped = im.clip(aoi);

    # Mask appropriate QA bits
    QA = im.select(['QA_PIXEL']);

    cloudShadowBitMask = 1 << 3
    cloudsBitMask = 1 << 4
    clipped = clipped.updateMask(QA.bitwiseAnd(cloudsBitMask).eq(0));
    clipped = clipped.updateMask(QA.bitwiseAnd(cloudShadowBitMask).eq(0));

    # SR scaling
    clipped = clipped.select('SR_B.').multiply(0.0000275).add(-0.2);

    return clipped;

def process_image(im):
    im = ee.Image(im)
    mndwi = im.normalizedDifference(['SR_B3', 'SR_B6']).rename('MNDWI')
    water_map_uncorrected = mndwi.gt(Threshold).rename('water_map_uncorrected')

    mndwiarea_uncorrected = mndwi.gt(Threshold).multiply(ee.Image.pixelArea()).divide(1e6).reduceRegion(reducer = ee.Reducer.sum(), geometry = aoi, scale = 30, maxPixels = 1e10).get('MNDWI')

    mndwiarea_nonwater = mndwi.lte(Threshold).multiply(ee.Image.pixelArea()).divide(1e6).reduceRegion(reducer = ee.Reducer.sum(), geometry = aoi, scale = 30, maxPixels = 1e10).get('MNDWI')

    mndwiarea_cloud = ee.Number(aoi.area().divide(1e6)).subtract(ee.Number(mndwiarea_nonwater).add(ee.Number(mndwiarea_uncorrected)))

    im = im.addBands(mndwi);
    im = im.addBands(water_map_uncorrected);

    im = im.set('mndwi_sarea', mndwiarea_uncorrected);
    im = im.set('mndwi_nonwater_area', mndwiarea_nonwater);
    im = im.set('masked_area', mndwiarea_cloud);

    return im

def addQualityBands(image):
    return preprocess_image(image).addBands(image.metadata('system:time_start'))

def process_date(date):
    date = ee.Date(date)
    from_date = date.advance(-15, 'day')
    to_date = date.advance(1, 'day')
    l8_subset = L8.filterDate(from_date, to_date).filterBounds(aoi).map(preprocess_image)
    im = l8_subset.map(process_image).qualityMosaic('MNDWI')

    # There is a special case (2020-10-02) when (probably) due to heavy
    # clouds, all of the aoi is masked. In that case special treatment 
    # has to be done to ignore that image. Following code does that - 
    im = ee.Image(ee.Algorithms.If(im.reduceRegion(reducer=ee.Reducer.anyNonZero(), scale = 30, geometry = aoi, maxPixels = 1e10).contains('SR_B3'),  process_image(im), ee.Image.constant(0)))

    im = im.set('from_date', from_date.format("YYYY-MM-dd"))
    im = im.set('to_date', to_date.format("YYYY-MM-dd"))
    im = im.set('l8_images', l8_subset.size())

    im = ee.Algorithms.If(im.bandNames().size().eq(1), ee.Number(0), im)

    return im

def generate_timeseries(dates):
    raw_ts = dates.map(process_date)
    raw_ts = raw_ts.removeAll([0]);
    raw_ts = raw_ts.map(ee.Image)
    imcoll = ee.ImageCollection.fromImages(raw_ts)

    return imcoll

def postprocess(im):
    hist = ee.List(gswd.reduceRegion(reducer=ee.Reducer.autoHistogram(minBucketWidth=1), geometry=aoi, scale=30, maxPixels=1e10).get('occurrence'))
    
    counts = ee.Array(hist).transpose().toList()

    omega = ee.Number(0.17)
    count_thresh = ee.Number(counts.map(lambda lis: ee.List(lis).reduce(ee.Reducer.mean())).get(1)).multiply(omega)

    count_thresh_index = ee.Array(counts.get(1)).gt(count_thresh).toList().indexOf(1)
    occurrence_thresh = ee.Number(ee.List(counts.get(0)).get(count_thresh_index))

    water_map = im.select(["MNDWI"], ["water_map"]).gt(Threshold)
    gswd_improvement = gswd.clip(aoi).gte(occurrence_thresh).updateMask(water_map.mask().Not()).select(["occurrence"], ["water_map"])

    improved = ee.ImageCollection([water_map, gswd_improvement]).mosaic()

    mndwiarea_corrected = improved.multiply(ee.Image.pixelArea()).divide(1e6).reduceRegion(reducer = ee.Reducer.sum(), geometry = aoi, scale = 30, maxPixels = 1e10).get('water_map');

    improved = improved.set("corrected_area", mndwiarea_corrected);
    improved = improved.set("from_date", im.get("from_date"))
    improved = improved.set("to_date", im.get("to_date"))
    improved = improved.set("l8_images", im.get("l8_images"))
    improved = improved.set('uncorrected_area', im.get('mndwi_sarea'));
    improved = improved.set('uncorrected_nonwater_area', im.get('mndwi_nonwater_area'));
    improved = improved.set('uncorrected_masked_area', im.get('masked_area'));

    return improved

def run_process_long(res_name, start, end):
    fo = start
    enddate = end

    reservoir = ee.FeatureCollection("users/pdas47/RAT/" + res_name)
    first_obs = datetime.strptime(L8.filterBounds(reservoir).first().get("DATE_ACQUIRED").getInfo(), "%Y-%m-%d")
    
    fo = first_obs.strftime("%Y-%m-%d")

    global aoi
    aoi = reservoir.geometry().buffer(1000)
    

    datadir = "/houston2/pritam/rat_mekong_v3/backend/data/sarea"
    scratchdir = os.path.join(datadir, "_scratch")

    flag = True
    num_runs = 0

    # If data already exists, only get new data starting from the last one
    savepath = os.path.join(datadir, f"{res_name}.csv")
    
    if os.path.isfile(savepath):
        temp_df = pd.read_csv(savepath, parse_dates=['mosaic_enddate']).set_index('mosaic_enddate')

        last_date = temp_df.index[-1].to_pydatetime()
        fo = (last_date - timedelta(days=32)).strftime("%Y-%m-%d")
        to_combine = [savepath]
        print(f"Existing file found - Last observation (32 day lag): {last_date}")

        # If 16 days have not passed since last observation, skip the processing
        days_passed = (datetime.strptime(end, "%Y-%m-%d") - last_date).days
        print(f"No. of days passed since: {days_passed}")
        if days_passed < 16:
            print(f"No new observation expected. Quitting early")
            return None
    else:
        to_combine = []
    
    savedir = os.path.join(scratchdir, f"{res_name}_l8_{fo}_{enddate}")
    if not os.path.isdir(savedir):
        os.makedirs(savedir)
    per_iter = 6

    print(f"Extracting SA for the period {fo} -> {enddate}")
    while flag:
        try:
            first_obs = ee.Date(fo)
            fo_copy = fo
            print(f"Procesing from: {fo}")
            # dates = ee.List.sequence(0, per_iter*16+1, 16).map(lambda num: first_obs.advance(num, 'day')).filter(ee.Filter.lte((datetime.strptime(enddate, "%Y-%m-%d")+timedelta(days=1)).strftime("%Y-%m-%d")))
            dates = ee.List.sequence(0, per_iter*16+1, 16).map(lambda num: first_obs.advance(num, 'day')).map(lambda item: item if ee.Date(item).millis().lte(ee.Date(enddate).advance(1, 'days').millis()) else None, dropNulls=True)
            res = generate_timeseries(dates)
            res_corrected = res.map(postprocess)
            
            corrected_final_data_ee = res_corrected.reduceColumns(ee.Reducer.toList(7), ['from_date', 'to_date', 'corrected_area', 'uncorrected_area', 'uncorrected_nonwater_area', 'uncorrected_masked_area', 'l8_images']).get('list')
            corrected_final_data = corrected_final_data_ee.getInfo()
            print(corrected_final_data)

            df_corrected = pd.DataFrame(corrected_final_data, columns=['from_date', 'to_date_exclusive', 'corrected_area', 'uncorrected_area', 'uncorrected_nonwater_area', 'uncorrected_masked_area', 'l8_images'])
            df_corrected['from_date'] = pd.to_datetime(df_corrected['from_date'], format="%Y-%m-%d")
            df_corrected['to_date_exclusive'] = pd.to_datetime(df_corrected['to_date_exclusive'], format="%Y-%m-%d")
            df_corrected['mosaic_enddate'] = df_corrected['to_date_exclusive'] - pd.Timedelta(1, unit='day')
            df_corrected = df_corrected.set_index('mosaic_enddate')

            fname = os.path.join(savedir, f"{df_corrected.index[0].strftime('%Y%m%d')}_{df_corrected.index[-1].strftime('%Y%m%d')}_{res_name}.csv")
            df_corrected.to_csv(fname)

            fo = df_corrected.index[-1].strftime('%Y-%m-%d')
        except Exception as e:
            print(e)
            raise e

        # finally:
        num_runs += 1
        
        s_time = randint(20, 30)
        print(f"Sleeping for {s_time} seconds")
        time.sleep(randint(20, 30))

        if (datetime.strptime(enddate, "%Y-%m-%d")-df_corrected.index[-1]).days < 16:
            print(f"Quitting: Reached enddate {enddate}")
            flag = False
        elif fo == fo_copy:
            print(f"Reached last available observation - {fo}")
            flag = False
        elif num_runs > 1000:
            print("Quitting: Reached 1000 iterations")
            flag = False

    # Combine the files into one database
    to_combine.extend([os.path.join(savedir, f) for f in os.listdir(savedir) if f.endswith(".csv")])

    files = [pd.read_csv(f, parse_dates=["mosaic_enddate"]).set_index("mosaic_enddate") for f in to_combine]
    data = pd.concat(files).drop_duplicates().sort_values("mosaic_enddate")

    data.to_csv(savepath)

    return savepath

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
    run_process_long(reservoir, start, end)



if __name__ == '__main__':
    main()