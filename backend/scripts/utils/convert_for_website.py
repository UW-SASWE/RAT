import os
import datetime
import pandas as pd
import numpy as np


def convert_sarea(project_dir):
    sarea_dir = os.path.join(project_dir, "backend/data/sarea_tmsos")
    website_v_dir = os.path.join(project_dir, "backend/data/sarea_tmsos/website_version")

    sarea_paths = [os.path.join(sarea_dir, f) for f in os.listdir(sarea_dir) if f.endswith(".csv")]

    for sarea_path in sarea_paths:
        res_name = os.path.splitext(os.path.split(sarea_path)[-1])[0]

        savepath = os.path.join(website_v_dir, f"{res_name}.txt")

        df = pd.read_csv(sarea_path)
        df = df[['date', 'area']]
        df['area'] = np.round(df['area'], 3)

        print(f"Converting [Surface Area]: {res_name}")
        df.to_csv(savepath, index=False)


def convert_inflow(project_dir):
    inflow_dir = os.path.join(project_dir, "backend/data/inflow")
    website_v_dir = os.path.join(inflow_dir, "website_version")

    names_to_ids = {
        "Nam_N":"Nam_Ngum_1",
        "Ubol_":"Ubol_Ratana",
        "Lam_P":"Lam_Pao",
        "Sirid":"Sirindhorn",
        "Nam_T":"Nam_Theun_2",
        "Xe_Ka":"Xe_Kaman_1",
        "Sesan":"Sesan_4",
        "Lower":"Lower_Sesan_2",
        "Batta":"Battambang_1",
        "Phumi":"Phumi_Svay_Chrum",
        "Sesan":"Sesan_4",
        "Sre_P":"Sre_Pok_4",
        "NamTo": "Nam_Ton",
        "Yali ":"Yali",
        "5117 ":"5117",
        "5138 ":"5138",
        "5143 ":"5143",
        "5147 ":"5147",
        "5148 ":"5148",
        "5151 ":"5151",
        "5152 ":"5152",
        "5155 ":"5155",
        "5156 ":"5156",
        "5160 ":"5160",
        "5162 ":"5162",
        "5795 ":"5795",
        "5797 ":"5797",
        "7000 ":"7000",
        "7001 ":"7001",
        "7002 ":"7002",
        "7004 ":"7004",
        "7037 ":"7037",
        "7087 ":"7087",
        "7158 ":"7158",
        "7159 ":"7159",
        "7164 ":"7164",
        "7181 ":"7181",
        "7201 ":"7201",
        "7232 ":"7232",
        "7284 ":"7284",
        "7303 ":"7303"
    }

    inflow_paths = [os.path.join(inflow_dir, f) for f in os.listdir(inflow_dir) if f.endswith(".csv")]

    for inflow_path in inflow_paths:
        res_name = os.path.splitext(os.path.split(inflow_path)[-1])[0]

        if res_name in names_to_ids.keys():
            savename = names_to_ids[res_name]
            savepath = os.path.join(website_v_dir, f"{savename}.txt")

            df = pd.read_csv(inflow_path)
            print(f"Converting [Inflow]: {res_name}")
            df.to_csv(savepath, index=False)
            print(df.tail())
        else:
            print(f"Currently not displayed in website: {res_name}")

def convert_dels_outflow(project_dir):
    mapping = {
        # 'Battambang_1': '99999',
        'Lam_Pao': '5150',
        'Lower_Sesan_2': '7303',
        'Nam_Ngum_1': '5136', 
        # 'Phumi_Svay_Chrum': '99999',
        'Sesan_4': '7203',
        'Sirindhorn': '5796',
        # 'Sre_Pok_4': '99999',
        'Ubol_Ratana': '5149',
        'Nam_Theun_2': '6999',
        'Xe_Kaman_1': '7003'
    }

    # Delta S
    dels_dir = os.path.join(project_dir, "backend/data/dels")
    dels_paths = [os.path.join(dels_dir, f) for f in os.listdir(dels_dir) if f.endswith(".csv")]

    for dels_path in dels_paths:
        res_name = os.path.splitext(os.path.split(dels_path)[-1])[0]

        if res_name in mapping:
            savename = mapping[res_name]
        else:
            savename = res_name

        savepath = os.path.join(dels_dir, f"website_version/{savename}.txt")

        df = pd.read_csv(dels_path)
        df = df[['date', 'dS']]
        df['dS'] = np.round(df['dS'], 2)

        print(f"Converting [∆S]: {res_name}, {savepath}")
        df.to_csv(savepath, index=False, header=False)
    
    # Outflow
    outflow_dir = os.path.join(project_dir, "backend/data/outflow")
    outflow_paths = [os.path.join(outflow_dir, f) for f in os.listdir(outflow_dir) if f.endswith(".csv")]

    for outflow_path in outflow_paths:
        res_name = os.path.splitext(os.path.split(outflow_path)[-1])[0]

        if res_name in mapping:
            savename = mapping[res_name]
        else:
            savename = res_name

        savepath = os.path.join(outflow_dir, f"website_version/{savename}.txt")

        df = pd.read_csv(outflow_path)
        df = df[['date', 'outflow_rate']].rename({
            'date': 'Date',
            'outflow_rate': 'Streamflow'
        }, axis=1)
        df['Streamflow'] = np.round(df['Streamflow'], 2)
        df.loc[df['Streamflow']<0, 'Streamflow'] = 0

        print(f"Converting [Outflow]: {res_name}, {savepath}")
        df.to_csv(savepath, index=False)

def convert_altimeter(project_dir):
    mapping = {
        # 'Battambang_1': '99999',
        'Lam_Pao': '5150',
        # 'Lower_Sesan_2': '7303',
        # 'Nam_Ngum_1': '5136', 
        # 'Phumi_Svay_Chrum': '99999',
        # 'Sesan_4': '7203',
        'Siridhorn': '5796',
        # 'Sre_Pok_4': '99999',
        # 'Ubol_Ratana': '5149',
        # 'Nam_Theun_2': '6999',
        'Xe_Kaman_1': '7003'
    }

    # Delta S
    altimeter_ts_dir = os.path.join(project_dir, "backend/data/altimetry_timeseries")
    altimeter_tses = [os.path.join(altimeter_ts_dir, f) for f in os.listdir(altimeter_ts_dir) if f.endswith(".csv")]

    for altimeter_ts_path in altimeter_tses:
        res_name = os.path.splitext(os.path.split(altimeter_ts_path)[-1])[0]

        if not res_name.isnumeric():
            grandid = mapping[res_name]
        else:
            grandid = res_name

        savepath = os.path.join(altimeter_ts_dir, f"website_version/{grandid}.txt")

        df = pd.read_csv(altimeter_ts_path, parse_dates=['date'])
        df = df[['date', 'H [m w.r.t. EGM2008 Geoid]']]
        df['date'] = df['date'].dt.strftime("%Y-%m-%d")
        df['H [m w.r.t. EGM2008 Geoid]'] = np.round(df['H [m w.r.t. EGM2008 Geoid]'], 2)

        print(f"Converting [Heights]: {res_name}")
        df.to_csv(savepath, index=False, header=False)

def convert_v2_frontend(project_dir, res_name, inflow_src, sarea_src, dels_src, outflow_src):
    """Converts the files according to the newer version of the frontend (v2).

    Args:
        project_dir (str): path of project directory
        res_name (str): name of reservoir
        inflow_src (str): source .csv file containing inflow in cumecs
        sarea_src (str): source .csv file containing surface area estiamted by TMS-OS
        dels_src (str): source .csv file containing delta-S estimates
        outflow_src (str): source .csv file containing outflow estimates
    """
    # inflow
    inflow = pd.read_csv(inflow_src, parse_dates=['date'])
    inflow['inflow (m3/d)'] = inflow['streamflow'] * (24*60*60)        # indicate units, convert from m3/s to m3/d
    inflow_dst_dir = os.path.join(project_dir, "backend/data/inflow/v2_website_version")
    if not os.path.isdir(inflow_dst_dir):
        os.makedirs(inflow_dst_dir)
    inflow_dst = os.path.join(inflow_dst_dir, f"{res_name}.csv")
    inflow = inflow[['date', 'inflow (m3/d)']]
    inflow.to_csv(inflow_dst, index=False)
    
    # sarea
    sarea = pd.read_csv(sarea_src, parse_dates=['date'])[['date', 'area']]
    sarea['area (km2)'] = sarea['area']                                     # indicate units
    sarea_dst_dir = os.path.join(project_dir, "backend/data/sarea_tmsos/v2_website_version")
    if not os.path.isdir(sarea_dst_dir):
        os.makedirs(sarea_dst_dir)
    sarea_dst = os.path.join(sarea_dst_dir, f"{res_name}.csv")
    sarea = sarea[['date', 'area (km2)']]
    sarea.to_csv(sarea_dst, index=False)
    
    # dels
    dels = pd.read_csv(dels_src, parse_dates=['date'])[['date', 'dS', 'days_passed']]
    dels['dS (m3)'] = dels['dS'] * 1e9                                     # indicate units, convert from BCM to m3
    dels_dst_dir = os.path.join(project_dir, "backend/data/dels/v2_website_version")
    if not os.path.isdir(dels_dst_dir):
        os.makedirs(dels_dst_dir)
    dels_dst = os.path.join(dels_dst_dir, f"{res_name}.csv")
    dels = dels[['date', 'dS (m3)']]
    dels.to_csv(dels_dst, index=False)
    
    # outflow
    outflow =  pd.read_csv(outflow_src, parse_dates=['date'])[['date', 'outflow_rate']]
    outflow.loc[outflow['outflow_rate']<0, 'outflow_rate'] = 0
    outflow['outflow (m3/d)'] = outflow['outflow_rate'] * (24*60*60)        # indicate units, convert from m3/s to m3/d
    outflow_dst_dir = os.path.join(project_dir, "backend/data/outflow/v2_website_version")
    if not os.path.isdir(outflow_dst_dir):
        os.makedirs(outflow_dst_dir)
    outflow_dst = os.path.join(outflow_dst_dir, f"{res_name}.csv")
    outflow = outflow[['date', 'outflow (m3/d)']]
    outflow.to_csv(outflow_dst, index=False)


def main():
    pass

if __name__ == '__main__':
    main()