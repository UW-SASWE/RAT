import os
import datetime
import subprocess

from logging import getLogger
from utils.logging import LOG_NAME, NOTIFICATION

from core.sarea.sarea_cli_s2 import sarea_s2
from core.sarea.sarea_cli_l8 import sarea_l8
from core.sarea.sarea_cli_sar import sarea_s1
from core.sarea.TMS import TMS

log = getLogger(f"{LOG_NAME}.{__name__}")

grand_areas = {                   # Areas in km2, from GRAND if available, or calcualted
    "Nam_Ngum_1": 436.9299,
    "Ubol_Ratana": 313.38,
    "Lam_Pao": 202.51,
    "Sirindhorn": 235.58,
    "Nam_Theun_2": 414.34,
    "Xe_Kaman_1": 101.43,
    "Lower_Sesan_2": 53.08,  # GRAND ID 7203
    "Sesan_4": 53.08,
    "Sre_Pok_4": 3.7,
    # "7303": 332.96,
    "5117": 36.91,
    "5138": 9.23,
    "5143": 38.43999,
    "5147": 73.22,
    "5148": 15.79,
    "5151": 6.96,
    "5152": 1.84,
    "5155": 4.78,
    "5156": 26.89,
    "5160": 9.69,
    "5162": 11.57,
    "5795": 86.9,
    "5797": 31,
    "7000": 10.4,
    "7001": 93.77,
    "7002": 4.42,
    "7004": 4.12,
    "7037": 20.46,
    # "7087": 9.64,
    "7158": 31.76,
    "7159": 6.82,
    "7164": 12.85,
    "7181": 24.91,
    "7201": 43.95,
    "7232": 246.16,
    "7284": 154.34,
    "7303": 332.96,
    "Phumi_Svay_Chrum": 0.8,
    "Battambang_1": 15,
    "Kaptai": 600,
    "Yali": 50
}

def run_sarea(start_date, end_date, datadir):
    reservoirs =[
        "Battambang_1", "Lam_Pao", "Lower_Sesan_2", "Nam_Ngum_1", 
        "Phumi_Svay_Chrum", "Sesan_4", "Sirindhorn", "Sre_Pok_4", "Ubol_Ratana", "Xe_Kaman_1", 
        "Yali", "5117", "5138", "5143", "5147", "5148", "5151", "5152", "5155", "5156", "5160",
        "5162", "5795", "5797", "7000", "7001", "7002", "7004", "7037", "7087", "7158", "7159",
        "7164", "7181", "7201", "7232", "7284", "7303", "Nam_Theun_2", "Phumi_Svay_Chrum"#, "Nam_Ton"
    ]
    
    for reservoir in reservoirs:
        if reservoir in grand_areas.keys():
            run_sarea_for_res(reservoir, start_date, end_date, datadir)


def run_sarea_for_res(reservoir, start_date, end_date, datadir):
    # Obtain surface areas
    # Sentinel-2
    log.debug(f"Reservoir: {reservoir}; Downloading Sentinel-2 data from {start_date} to {end_date}")
    sarea_s2(reservoir, start_date, end_date, os.path.join(datadir, 's2'))
    s2_dfpath = os.path.join(datadir, 's2', reservoir+'.csv')

    # Landsat-8
    log.debug(f"Reservoir: {reservoir}; Downloading Landsat-8 data from {start_date} to {end_date}")
    sarea_l8(reservoir, start_date, end_date, os.path.join(datadir, 'l8'))
    l8_dfpath = os.path.join(datadir, 'l8', reservoir+'.csv')

    # Sentinel-1
    log.debug(f"Reservoir: {reservoir}; Downloading Sentinel-1 data from {start_date} to {end_date}")
    s1_dfpath = sarea_s1(reservoir, start_date, end_date, os.path.join(datadir, 'sar'))

    tmsos = TMS(reservoir, grand_areas[reservoir])
    result = tmsos.tms_os(l8_dfpath, s2_dfpath, s1_dfpath)

    tmsos_savepath = os.path.join(datadir, reservoir+'.csv')
    log.debug(f"Saving surface area of {reservoir} at {tmsos_savepath}")
    result.reset_index().rename({'index': 'date', 'filled_area': 'area'}, axis=1).to_csv(tmsos_savepath, index=False)

def main():
    run_sarea("2021-01-01", "2022-05-02", datadir='backend/data/sarea_tmsos')


if __name__ == '__main__':
    main()