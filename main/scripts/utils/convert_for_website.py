import os
import datetime
import pandas as pd
import geopandas as gpd
import numpy as np
from utils.utils import create_directory

def convert_sarea(sarea_dir, website_v_dir):

    sarea_paths = [os.path.join(sarea_dir, f) for f in os.listdir(sarea_dir) if f.endswith(".csv")]
    sarea_web_dir = create_directory(os.path.join(website_v_dir,'sarea' ),True)
    for sarea_path in sarea_paths:
        res_name = os.path.splitext(os.path.split(sarea_path)[-1])[0]

        
        savepath = os.path.join(sarea_web_dir, f"{res_name}.txt")

        df = pd.read_csv(sarea_path)
        df = df[['date', 'area']]
        df['area'] = np.round(df['area'], 3)

        print(f"Converting [Surface Area]: {res_name}")
        df.to_csv(savepath, index=False)


def convert_inflow(inflow_dir, reservoir_shpfile, reservoir_shpfile_column_dict,  website_v_dir):

    reservoirs = gpd.read_file(reservoir_shpfile)
    reservoirs['Inflow_filename'] = reservoirs[reservoir_shpfile_column_dict['unique_identifier']].astype(str).str[:5]

    inflow_paths = [os.path.join(inflow_dir, f) for f in os.listdir(inflow_dir) if f.endswith(".csv")]
    inflow_web_dir = create_directory(os.path.join(website_v_dir,'inflow' ),True)

    for inflow_path in inflow_paths:
        res_name = os.path.splitext(os.path.split(inflow_path)[-1])[0]

        if res_name in reservoirs['Inflow_filename'].tolist():
            savename = reservoirs[reservoirs['Inflow_filename'] == res_name][reservoir_shpfile_column_dict['unique_identifier']]
            savepath = os.path.join(inflow_web_dir ,f"{savename}.txt")

            df = pd.read_csv(inflow_path)
            print(f"Converting [Inflow]: {res_name}")
            df.to_csv(savepath, index=False)
            print(df.tail())
        else:
            print(f"Currently not displayed in website: {res_name}")

def convert_dels_outflow(dels_dir, outflow_dir, website_v_dir):

    # Delta S
    dels_paths = [os.path.join(dels_dir, f) for f in os.listdir(dels_dir) if f.endswith(".csv")]
    dels_web_dir = create_directory(os.path.join(website_v_dir,'dels' ),True)

    for dels_path in dels_paths:
        res_name = os.path.splitext(os.path.split(dels_path)[-1])[0]
        savename = res_name

        savepath = os.path.join(dels_web_dir , f"{savename}.txt")

        df = pd.read_csv(dels_path)
        df = df[['date', 'dS']]
        df['dS'] = np.round(df['dS'], 2)

        print(f"Converting [∆S]: {res_name}, {savepath}")
        df.to_csv(savepath, index=False, header=False)
    
    # Outflow
    outflow_paths = [os.path.join(outflow_dir, f) for f in os.listdir(outflow_dir) if f.endswith(".csv")]
    outflow_web_dir = create_directory(os.path.join(website_v_dir,'outflow' ),True)

    for outflow_path in outflow_paths:
        res_name = os.path.splitext(os.path.split(outflow_path)[-1])[0]

        savename = res_name

        savepath = os.path.join(outflow_web_dir, f"{savename}.txt")

        df = pd.read_csv(outflow_path)
        df = df[['date', 'outflow_rate']].rename({
            'date': 'Date',
            'outflow_rate': 'Streamflow'
        }, axis=1)
        df['Streamflow'] = np.round(df['Streamflow'], 2)
        df.loc[df['Streamflow']<0, 'Streamflow'] = 0

        print(f"Converting [Outflow]: {res_name}, {savepath}")
        df.to_csv(savepath, index=False)

def convert_altimeter(altimeter_ts_dir, website_v_dir):

    # Delta S
    if os.path.exists(altimeter_ts_dir):
        altimeter_tses = [os.path.join(altimeter_ts_dir, f) for f in os.listdir(altimeter_ts_dir) if f.endswith(".csv")]
        altimeter_web_dir = create_directory(os.path.join(website_v_dir,'altimeter' ),True)

        for altimeter_ts_path in altimeter_tses:
            res_name = os.path.splitext(os.path.split(altimeter_ts_path)[-1])[0]

            savepath = os.path.join(altimeter_web_dir , f"{savename}.txt")

            df = pd.read_csv(altimeter_ts_path, parse_dates=['date'])
            df = df[['date', 'H [m w.r.t. EGM2008 Geoid]']]
            df['date'] = df['date'].dt.strftime("%Y-%m-%d")
            df['H [m w.r.t. EGM2008 Geoid]'] = np.round(df['H [m w.r.t. EGM2008 Geoid]'], 2)

            print(f"Converting [Heights]: {res_name}")
            df.to_csv(savepath, index=False, header=False)

def main():
    pass

if __name__ == '__main__':
    main()