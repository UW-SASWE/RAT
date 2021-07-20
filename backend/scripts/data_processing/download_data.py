import subprocess
from scripts.utils import run_command

def get_precip_link(date, version):
    if version == "IMERG-LATE":
        link = f"https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/{date.strftime('%Y')}/{date.strftime('%m')}/3B-HHR-L.MS.MRG.3IMERG.{date.strftime('%Y%m%d')}-S233000-E235959.1410.V06B.1day.tif"
    elif version == "IMERG-EARLY":
        link = f"https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/early/{date.strftime('%Y')}/{date.strftime('%m')}/3B-HHR-E.MS.MRG.3IMERG.{date.strftime('%Y%m%d')}-S233000-E235959.1410.V06B.1day.tif"
    # else version:
    #     link = f"ftp://arthurhou.pps.eosdis.nasa.gov/gpmdata/{date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')}/gis/3B-DAY-GIS.MS.MRG.3IMERG.{date.strftime('%Y%m%d')}-S000000-E235959.0000.V06A.tif"
    
    return link


def download_precip(date, outputpath, version=None):
    """
    Parameters:
        date: datetime object of the day for which precipitation is required
        outputpath: path where file will be saved
        version: Optionally specify version as IMERG-LATE or IMERG-EARLY. If not specified, 
            function tries to download Late if available followed by Early
    """
    
    if version is None:
        # Using default
        try:
            cmd_late = [
                "wget",
                "-O",
                outputpath,
                "--user",
                'SECRET',
                '--password',
                'SECRET',
                get_precip_link(date, "IMERG-LATE"),
                '--no-proxy'
            ]
            res = run_command(cmd_late)
        except:
            try:
                cmd_early = [
                    "wget",
                    "-O",
                    outputpath,
                    "--user",
                    'SECRET',
                    '--password',
                    'SECRET',
                    get_precip_link(date, "IMERG-EARLY"),
                    '--no-proxy'
                ]
                res = run_command(cmd_early)
            except Exception as e:
                raise e
    else:
        cmd = [
            "wget",
            "-O",
            outputpath,
            "--user",
            'SECRET',
            '--password',
            'SECRET',
            get_precip_link(date, version),
            '--no-proxy'
        ]
        res = run_command(cmd)

    return res