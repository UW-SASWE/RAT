from core.sarea.sarea_cli_s2 import TEMPORAL_RESOLUTION
import ee
import numpy as np
import pandas as pd
import argparse
import os
from datetime import datetime, timedelta

ee.Initialize()


reservoir = "Lam_Pao"

s1 = ee.ImageCollection("COPERNICUS/S1_GRD")
# ROI = ee.FeatureCollection(f"users/pdas47/RAT/{reservoir}")

start_date = '2018-07-01'
end_date = '2022-01-01'
# definitions
ANGLE_THRESHOLD_1 = ee.Number(45.4);
ANGLE_THRESHOLD_2 = ee.Number(31.66)
REVISIT_TIME = ee.Number(12)
BUFFER_DIST = 500


# functions
def getfirstobs(imcoll):
    first_im = ee.Image(imcoll.toList(1000).get(0))
    first_date = ee.Date(first_im.get('system:time_start'))
#     # get the cycle numbers
#     cycle_nums = imcoll.filterMetadata('orbitProperties_pass', 'equals', 'ASCENDING') \
#         .toList(100000) \
#         .map(lambda item: ee.Image(item).get("cycleNumber")).distinct()
    return first_date

def focal_median(img):
    fm = img.focal_max(30, 'circle', 'meters')
    fm = fm.rename("Smooth")
    return img.addBands(fm)

# Define masking function for removing erroneous pixels
def mask_by_angle(img):
    angle = img.select('angle')
    vv = img.select('VV')
    mask1 = angle.lt(ANGLE_THRESHOLD_1)
    mask2 = angle.gt(ANGLE_THRESHOLD_2)
    vv = vv.updateMask(mask1)
    return vv.updateMask(mask2)

# Calculating the water pixels
def calcWaterPix(img):
    water_pixels = ee.Algorithms.If(
        img.bandNames().contains('Class'),
        img.reduceRegion(reducer = ee.Reducer.sum(), geometry = ROI, scale = 10, maxPixels = 10e9).get('Class'),
        None)
    return img.set("water_pixels", water_pixels)

def detectWaterSAR(d, ref_image):
    d = ee.Date(d)
    s1_subset = s1.filterDate(d, d.advance(REVISIT_TIME, 'days')) \
        .filterBounds(ROI) \
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
        .filter(ee.Filter.eq('instrumentMode', 'IW'))

    vv = s1_subset.map(mask_by_angle)
    vv = vv.map(focal_median);
    vv_median = vv.select("Smooth").median();
    
    def in_case_we_have_obs():
        clas = vv_median.lt(-13);
        mask = vv_median.gt(-32);
        clas  = clas.mask(mask);
        sardate=ee.Date(s1_subset.first().get('system:time_end'))
        return clas.addBands(vv_median).rename(['Class','Median']).set("system:time_start", ee.Date(d).millis());
    in_case_we_dont_have_obs = lambda: ref_image.multiply(0).add(-1)
    res = ee.Algorithms.If(
        vv_median.bandNames().length().eq(0),
        in_case_we_dont_have_obs(),
        in_case_we_have_obs()
    )
    
    return res

# client side code
def ee_get_data(ee_Date_Start, ee_Date_End):
    ee_Date_Start, ee_Date_End = ee.Date(ee_Date_Start), ee.Date(ee_Date_End)
    S1 = s1.filterDate(ee_Date_Start, ee_Date_End) \
            .filterBounds(ROI) \
            .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
            .filter(ee.Filter.eq('instrumentMode', 'IW'))
    
    ref_image = S1.first()
    first_date = getfirstobs(S1)

    n_days = ee_Date_End.difference(ee_Date_Start, 'day').round()
    dates = ee.List.sequence(0, n_days, REVISIT_TIME)
    dates = dates.map(lambda n: first_date.advance(n, 'day'))

    classified_water_sar = ee.ImageCollection(dates.map(lambda d: detectWaterSAR(d, ref_image)))
    classified_water_sar = classified_water_sar.map(calcWaterPix)

    wc = ee.Array(classified_water_sar.aggregate_array('water_pixels')).multiply(0.0001).getInfo() # area in sq. km
    d = classified_water_sar.aggregate_array('system:time_start').getInfo() # convert from miliseconds to seconds from epoch
    
    df = pd.DataFrame({
        'time': d,  # https://stackoverflow.com/a/15056365
        'sarea': wc
    })
    df['time'] = df['time'].apply(lambda t: np.datetime64(t, 'ms'))
    
    return df

def retrieve_sar(start_date, end_date, res='ys'):
    date_ranges = list(pd.date_range(start_date, end_date, freq=res).strftime("%Y-%m-%d").tolist()) + [end_date]
    print(date_ranges)
    dfs = []
    # for begin, end in zip(date_ranges[:-1], date_ranges.shift(1)[:-1]):
    for begin, end in zip(date_ranges[:-1], date_ranges[1:]):
        # begin_str, end_str = begin.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
        print(f"Processing: {begin} - {end} ")
        dfs.append(ee_get_data(begin, end))
        print(dfs[-1].head())
        print(f"Processed: {begin} - {end} ")

    return pd.concat(dfs)

# data = retrieve_sar(start_date, end_date, res='6MS')

# data.to_csv(f"../data/sar/{reservoir}_12d_sar.csv")

def sarea_s1(reservoir, start_date, end_date, datadir):
    global ROI 
    reservoir_ee = ee.FeatureCollection(f"users/pdas47/RAT/{reservoir}")
    ROI = reservoir_ee.geometry().buffer(BUFFER_DIST)
    TEMPORAL_RESOLUTION = 12

    savepath = os.path.join(datadir, f"{reservoir}_12d_sar.csv")
    if os.path.isfile(savepath):
        existing_df = pd.read_csv(savepath, parse_dates=['time']).set_index('time')

        last_date = existing_df.index[-1].to_pydatetime()
        start_date = (last_date - timedelta(days=TEMPORAL_RESOLUTION*2)).strftime("%Y-%m-%d")
        to_combine = [existing_df.reset_index()]
        print(f"Existing file found - Last observation ({TEMPORAL_RESOLUTION*2} day lag): {last_date}")

        # If <TEMPORAL RESOLUTION> days have not passed since last observation, skip the processing
        days_passed = (datetime.strptime(end_date, "%Y-%m-%d") - last_date).days
        print(f"No. of days passed since: {days_passed}")
        if days_passed < TEMPORAL_RESOLUTION:
            print(f"No new observation expected. Quitting early")
            return savepath
    else:
        to_combine = []

    results = retrieve_sar(start_date, end_date, res='6MS')
    to_combine.append(results)
    data = pd.concat(to_combine).drop_duplicates().sort_values("time")
    
    if not os.path.isdir(datadir):
        os.makedirs(datadir)
    data.to_csv(savepath, index=False)

    return savepath


def main():
    # Setup argument parser
    parser = argparse.ArgumentParser(description="Generate Reservoir Surface area time series - Sentinel-1")

    parser.add_argument("reservoir")
    parser.add_argument("start_date")
    parser.add_argument("end_date")

    args = parser.parse_args()

    reservoir = args.reservoir
    start = args.start_date
    end = args.end_date
    
    print(f"{reservoir = }, {start = }, {end = }")
    
    global ROI 
    reservoir_ee = ee.FeatureCollection(f"users/pdas47/RAT/{reservoir}")
    ROI = reservoir_ee.geometry().buffer(BUFFER_DIST)

    results = retrieve_sar(start, end, res='6MS')
    savepath = f"data/sar/{reservoir}_12d_sar.csv"
    results.to_csv(savepath, index=False)


if __name__ == '__main__':
    main()