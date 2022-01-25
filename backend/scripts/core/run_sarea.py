import os
import datetime
import subprocess

from logging import getLogger
from utils.logging import LOG_NAME, NOTIFICATION
from utils.utils import run_command

log = getLogger(f"{LOG_NAME}.{__name__}")

def run_sarea(end_date):
    reservoirs = [
        "Battambang_1", "Lam_Pao", "Lower_Sesan_2", "Nam_Ngum_1", "Phumi_Svay_Chrum", "Sesan_4", 
        "Sirindhorn", "Sre_Pok_4", "Ubol_Ratana", "Xe_Kaman_1","Yali", "5117", "5138", "5143",
        "5147", "5148", "5151", "5152", "5155", "5156", "5160", "5162", "5795", "5797", "7000",
        "7001", "7002", "7004", "7037", "7087", "7158", "7159", "7164", "7181", "7201", "7232",
        "7284", "7303"
    ]
    start_date = "2013-01-01"
    # end_date = datetime.datetime.today().strftime("%Y-%m-%d")
    script_path = "/houston2/pritam/rat_mekong_v3/backend/scripts/core/sarea_cli.py"

    for reservoir in reservoirs:
        arg = f"python {script_path} {reservoir} {start_date} {end_date}".split()

        log.debug(f"Reservoir: {reservoir}; Running command: {arg}")

        print(subprocess.run(arg, stdout=subprocess.PIPE), flush=True)


def main():
    run_sarea()


if __name__ == '__main__':
    main()