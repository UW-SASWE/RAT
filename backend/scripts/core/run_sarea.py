import os
import datetime
import subprocess

from logging import getLogger
from utils.logging import LOG_NAME, NOTIFICATION

from core.sarea.sarea_cli_s2 import sarea_s2
from core.sarea.sarea_cli_l8 import sarea_l8
# from core.sarea.sarea_cli_sar import sarea_sar

log = getLogger(f"{LOG_NAME}.{__name__}")

def run_sarea(start_date, end_date, datadir):
    reservoirs = [
        "Battambang_1", 
        # "Lam_Pao", "Lower_Sesan_2", 
        # "Nam_Ngum_1", 
        # "Phumi_Svay_Chrum", "Sesan_4", 
        # "Sirindhorn", 
        # "Sre_Pok_4", "Ubol_Ratana", "Xe_Kaman_1","Yali", "5117", "5138", "5143",
        # "5147", "5148", "5151", "5152", "5155", "5156", "5160", "5162", "5795", "5797", "7000",
        # "7001", "7002", "7004", "7037", "7087", "7158", "7159", "7164", "7181", "7201", "7232",
        # "7284", "7303", "Nam_Theun_2", "Phumi_Svay_Chrum"#, "Nam_Ton"
    ]
    
    # Obtain surface areas
    # Sentinel-2
    for reservoir in reservoirs:
        log.debug(f"Reservoir: {reservoir}; Downloading data from {start_date} to {end_date}")
        sarea_s2(reservoir, start_date, end_date, datadir)


def main():
    run_sarea("2022-03-01", "2022-05-02", datadir='backend/data/sarea_tmsos/s2')


if __name__ == '__main__':
    main()