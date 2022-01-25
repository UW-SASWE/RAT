import configparser
import os

from data_processing import altimetry as alt


def altimeter_routine(reservoirpath, j3tracks, user, password, metafile, project_dir, geoidpath):
    resname = reservoirpath.split(os.sep)[-1].split('.')[0]

    j3_pass = alt.get_j3_tracks(reservoirpath, j3tracks)
    print(j3_pass)

    if j3_pass is None:
        return None

    # savedir = os.path.join(project_dir, 'backend/data/altimetry', resname, 'raw')
    savedir = os.path.join(project_dir, 'backend/data/altimetry', 'raw')
    if not os.path.isdir(savedir):
        os.makedirs(savedir)
    # extracteddir = os.path.join(project_dir, 'backend/data/altimetry', resname, 'extracted')
    extracteddir = os.path.join(project_dir, 'backend/data/altimetry', 'extracted', resname)
    if not os.path.isdir(extracteddir):
        os.makedirs(extracteddir)
    
    resultsdir = os.path.join(project_dir, 'backend/data/altimetry_timeseries')
    savepath = os.path.join(resultsdir, f'{resname}.csv')

    tracks = j3_pass['tracks']
    lat_ranges = j3_pass['lat_range']

    for track, lat_range in zip(tracks, lat_ranges):
        latest_cycle = alt.get_latest_cycle(user, password, metafile)
        print(f"Latest cycle: {latest_cycle}")

        alt.download_data(user, password, savedir, track, 1, latest_cycle, 3)
        extractedf = alt.extract_data(savedir + f'/j3_{track:03}', extracteddir, lat_range[0], lat_range[1], track, 3)

        alt.generate_timeseries(extractedf, savepath, lat_range[0], lat_range[1], geoidpath)

def run_altimetry(project_dir):
    reservoirs_dir = os.path.join(project_dir, 'backend/data/ancillary/reservoirs')

    to_process = ['7003', 'Xe_Kaman_1', 'Siridhorn', '5796']

    reservoirs = [os.path.join(reservoirs_dir, f) for f in os.listdir(reservoirs_dir) if f.endswith(".shp") and f.split('.')[0] in to_process]

    secrets = configparser.ConfigParser()
    secrets.read(os.path.join(project_dir, 'backend/params/secrets.ini'))
    username = secrets["aviso"]["username"]
    pwd = secrets["aviso"]["pwd"]

    metafile = os.path.join(project_dir, 'backend/params/j3_meta.txt')
    
    j3_tracks = os.path.join(project_dir, 'backend/data/ancillary/j3_tracks.geojson')

    geoidpath = os.path.join(project_dir, 'backend/data/ancillary/geoidegm2008grid.mat')

    for reservoir in reservoirs:
        print(f"Processing {reservoir}")
        # if reservoir.split('/')[-1].startswith("Siri"):
        altimeter_routine(reservoir, j3_tracks, username, pwd, metafile, project_dir, geoidpath)


if __name__ == '__main__':
    run_altimetry("/houston2/pritam/rat_mekong_v3")