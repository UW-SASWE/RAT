import subprocess
import os
import pandas as pd
import shutil

from logging import getLogger
from utils.logging import LOG_NAME, NOTIFICATION
from utils.utils import run_command

log = getLogger(LOG_NAME)

# Temporarily use this script to run the old model
def run_old_model():
    log.debug(f"Running RAT-V1.0")
    run_command(r"bash /houston2/pritam/mekong_rat/automate.sh > /houston2/pritam/mekong_rat/cronlogs/`date +\%Y\%m\%d`-from_ratv3.log", shell=True)

def copy_generate_inflow():
    log.debug(f"Copying RAT-V3 inflows over")
    paths = {
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/Nam_N.csv":"/houston2/pritam/mekong_rat/backend/Inflow/5136.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/Ubol_.csv":"/houston2/pritam/mekong_rat/backend/Inflow/5149.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/Lam_P.csv":"/houston2/pritam/mekong_rat/backend/Inflow/5150.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/Sirid.csv":"/houston2/pritam/mekong_rat/backend/Inflow/5796.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/Nam_T.csv":"/houston2/pritam/mekong_rat/backend/Inflow/6999.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/Xe_Ka.csv":"/houston2/pritam/mekong_rat/backend/Inflow/7003.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/Sesan.csv":"/houston2/pritam/mekong_rat/backend/Inflow/7203.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/Lower.csv":"/houston2/pritam/mekong_rat/backend/Inflow/7303.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5117 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5117.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5138 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5138.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5143 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5143.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5147 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5147.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5148 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5148.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5151 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5151.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5152 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5152.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5155 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5155.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5156 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5156.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5160 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5160.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5162 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5162.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5795 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5795.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/5797 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/5797.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7000 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7000.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7001 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7001.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7002 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7002.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7004 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7004.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7037 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7037.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7087 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7087.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7158 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7158.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7159 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7159.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7164 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7164.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7181 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7181.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7201 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7201.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7232 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7232.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7284 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7284.txt",
        "/houston2/pritam/rat_mekong_v3/backend/data/inflow/7303 .csv":"/houston2/pritam/mekong_rat/backend/Inflow/7303.txt"
    }

    for p in paths:
        log.debug(f"Copying {p} to {paths[p]}")
        shutil.copy2(p, paths[p])

def run_postprocess():
    log.debug(f"Running old Postprocessing")
    run_command("bash postprocess.sh", cwd='/houston2/pritam/mekong_rat/backend/Postprocessing', shell=True)

def publish():
    log.debug(f"Pushing results - Inflow")
    run_command("sshpass -v -p 'SECRET' scp -o stricthostkeychecking=no /houston2/pritam/mekong_rat/backend/Inflow/* SECRET:path/at/server/data/inflow/", shell=True)













