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

def main():
    pass

if __name__ == '__main__':
    main()