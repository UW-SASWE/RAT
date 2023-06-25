import os
import pandas as pd
import geopandas as gpd
import numpy as np
from pathlib import Path
from rat.utils.utils import create_directory

def convert_sarea(sarea_dir, website_v_dir):
    # Surface Area
    sarea_paths = [os.path.join(sarea_dir, f) for f in os.listdir(sarea_dir) if f.endswith(".csv")]
    sarea_web_dir = create_directory(os.path.join(website_v_dir,'sarea_tmsos' ),True)
    for sarea_path in sarea_paths:
        res_name = os.path.splitext(os.path.split(sarea_path)[-1])[0]

        
        savepath = os.path.join(sarea_web_dir, f"{res_name}.csv")

        df = pd.read_csv(sarea_path, parse_dates=['date'])
        df = df[['date', 'area']]
        df['area'] = np.round(df['area'], 3)
        df.rename({'area':'area (km2)'}, axis=1, inplace=True)
        print(f"Converting [Surface Area]: {res_name}")
        df.to_csv(savepath, index=False)


def convert_inflow(inflow_dir, reservoir_shpfile, reservoir_shpfile_column_dict,  final_out_dir):
    # Inflow
    reservoirs = gpd.read_file(reservoir_shpfile)
    reservoirs['Inflow_filename'] = reservoirs[reservoir_shpfile_column_dict['unique_identifier']].astype(str)

    inflow_paths = list(Path(inflow_dir).glob('*.csv'))
    final_out_inflow_dir = Path(final_out_dir) / 'inflow'
    final_out_inflow_dir.mkdir(exist_ok=True)

    for inflow_path in inflow_paths:
        res_name = os.path.splitext(os.path.split(inflow_path)[-1])[0]

        if res_name in reservoirs['Inflow_filename'].tolist():
            savepath = final_out_inflow_dir / inflow_path.name

            df = pd.read_csv(inflow_path, parse_dates=['date'])
            df['inflow (m3/d)'] = df['streamflow'] * (24*60*60)        # indicate units, convert from m3/s to m3/d
            df = df[['date', 'inflow (m3/d)']]

            print(f"Converting [Inflow]: {res_name}")
            df.to_csv(savepath, index=False)
            print(df.tail())
        else:
            print(f"Currently not displayed in website: {res_name}")

def convert_dels(dels_dir, website_v_dir):
    # Delta S
    dels_paths = [os.path.join(dels_dir, f) for f in os.listdir(dels_dir) if f.endswith(".csv")]
    dels_web_dir = create_directory(os.path.join(website_v_dir,'dels' ),True)

    for dels_path in dels_paths:
        res_name = os.path.splitext(os.path.split(dels_path)[-1])[0]
        savename = res_name

        savepath = os.path.join(dels_web_dir , f"{savename}.csv")

        df = pd.read_csv(dels_path, parse_dates=['date'])[['date', 'dS', 'days_passed']]
        df['dS (m3)'] = df['dS'] * 1e9                                     # indicate units, convert from BCM to m3
        df = df[['date', 'dS (m3)']]

        print(f"Converting [∆S]: {res_name}, {savepath}")
        df.to_csv(savepath, index=False)

def convert_evaporation(evap_dir, website_v_dir):
    # Evaporation
    evap_paths = [os.path.join(evap_dir, f) for f in os.listdir(evap_dir) if f.endswith(".csv")]
    evap_web_dir = create_directory(os.path.join(website_v_dir,'evaporation' ),True)

    for evap_path in evap_paths:
        res_name = os.path.splitext(os.path.split(evap_path)[-1])[0]
        savename = res_name

        savepath = os.path.join(evap_web_dir , f"{savename}.csv")

        df = pd.read_csv(evap_path)
        df = df[['time', 'OUT_EVAP']]
        df.rename({'time':'date', 'OUT_EVAP':'evaporation (mm)'}, axis=1, inplace=True)

        print(f"Converting [Evaporation]: {res_name}, {savepath}")
        df.to_csv(savepath, index=False)
    
def convert_outflow(outflow_dir, website_v_dir):
    # Outflow
    outflow_paths = [os.path.join(outflow_dir, f) for f in os.listdir(outflow_dir) if f.endswith(".csv")]
    outflow_web_dir = create_directory(os.path.join(website_v_dir,'outflow' ),True)

    for outflow_path in outflow_paths:
        res_name = os.path.splitext(os.path.split(outflow_path)[-1])[0]

        savename = res_name

        savepath = os.path.join(outflow_web_dir, f"{savename}.csv")

        df = pd.read_csv(outflow_path, parse_dates=['date'])[['date', 'outflow_rate']]
        df.loc[df['outflow_rate']<0, 'outflow_rate'] = 0
        df['outflow (m3/d)'] = df['outflow_rate'] * (24*60*60)        # indicate units, convert from m3/s to m3/d
        df = df[['date', 'outflow (m3/d)']]

        print(f"Converting [Outflow]: {res_name}, {savepath}")
        df.to_csv(savepath, index=False)

def convert_altimeter(altimeter_ts_dir, website_v_dir):
    # Altimeter
    if os.path.exists(altimeter_ts_dir):
        altimeter_tses = [os.path.join(altimeter_ts_dir, f) for f in os.listdir(altimeter_ts_dir) if f.endswith(".csv")]
        altimeter_web_dir = create_directory(os.path.join(website_v_dir,'altimeter' ),True)

        for altimeter_ts_path in altimeter_tses:
            res_name = os.path.splitext(os.path.split(altimeter_ts_path)[-1])[0]
            savename = res_name
            savepath = os.path.join(altimeter_web_dir , f"{savename}.csv")

            df = pd.read_csv(altimeter_ts_path, parse_dates=['date'])
            df = df[['date', 'H [m w.r.t. EGM2008 Geoid]']]
            df['date'] = df['date'].dt.strftime("%Y-%m-%d")
            df['H [m w.r.t. EGM2008 Geoid]'] = np.round(df['H [m w.r.t. EGM2008 Geoid]'], 2)
            df.rename({'H [m w.r.t. EGM2008 Geoid]':'height (m)'}, axis=1, inplace=True)

            print(f"Converting [Heights]: {res_name}")
            df.to_csv(savepath, index=False)

def copy_aec_files(src_dir, dst_dir):
    src_dir = Path(src_dir)
    dst_dir = Path(dst_dir)

    for src_path in src_dir.glob('*.csv'):
        aec = pd.read_csv(src_path)
        aec.rename({
            'Elevation': 'elevation',
            'CumArea': 'area'
        }, axis=1, inplace=True)
        aec.to_csv(dst_dir / src_path.name, index=False)


def convert_v2_frontend(basin_data_dir, res_name, inflow_src, sarea_src, dels_src, outflow_src):
    """Converts the files according to the newer version of the frontend (v2).

    Args:
        basin_data_dir (str): path of basin data directory
        res_name (str): name of reservoir
        inflow_src (str): source .csv file containing inflow in cumecs
        sarea_src (str): source .csv file containing surface area estiamted by TMS-OS
        dels_src (str): source .csv file containing delta-S estimates
        outflow_src (str): source .csv file containing outflow estimates
    """
    # inflow
    inflow = pd.read_csv(inflow_src, parse_dates=['date'])
    inflow['inflow (m3/d)'] = inflow['streamflow'] * (24*60*60)        # indicate units, convert from m3/s to m3/d
    inflow_dst_dir = create_directory(os.path.join(basin_data_dir, "v2_website_version",'inflow'),True)
    inflow_dst = os.path.join(inflow_dst_dir, f"{res_name}.csv")
    inflow = inflow[['date', 'inflow (m3/d)']]
    inflow.to_csv(inflow_dst, index=False)
    
    # sarea
    sarea = pd.read_csv(sarea_src, parse_dates=['date'])[['date', 'area']]
    sarea['area (km2)'] = sarea['area']                                     # indicate units
    sarea_dst_dir = create_directory(os.path.join(basin_data_dir, "v2_website_version",'sarea_tmsos'),True)
    sarea_dst = os.path.join(sarea_dst_dir, f"{res_name}.csv")
    sarea = sarea[['date', 'area (km2)']]
    sarea.to_csv(sarea_dst, index=False)
    
    # dels
    dels = pd.read_csv(dels_src, parse_dates=['date'])[['date', 'dS', 'days_passed']]
    dels['dS (m3)'] = dels['dS'] * 1e9                                     # indicate units, convert from BCM to m3
    dels_dst_dir = create_directory(os.path.join(basin_data_dir, "v2_website_version",'dels'),True)
    dels_dst = os.path.join(dels_dst_dir, f"{res_name}.csv")
    dels = dels[['date', 'dS (m3)']]
    dels.to_csv(dels_dst, index=False)
    
    # outflow
    outflow =  pd.read_csv(outflow_src, parse_dates=['date'])[['date', 'outflow_rate']]
    outflow.loc[outflow['outflow_rate']<0, 'outflow_rate'] = 0
    outflow['outflow (m3/d)'] = outflow['outflow_rate'] * (24*60*60)        # indicate units, convert from m3/s to m3/d
    outflow_dst_dir = create_directory(os.path.join(basin_data_dir, "v2_website_version",'outflow'),True)
    outflow_dst = os.path.join(outflow_dst_dir, f"{res_name}.csv")
    outflow = outflow[['date', 'outflow (m3/d)']]
    outflow.to_csv(outflow_dst, index=False)

def main():
    pass

if __name__ == '__main__':
    main()