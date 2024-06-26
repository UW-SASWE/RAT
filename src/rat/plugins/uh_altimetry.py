import io
import numpy as np
import os.path
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
import pandas as pd
from pathlib import Path
from tqdm.autonotebook import trange, tqdm
import shutil

# If modifying these scopes, delete the file token.json.
# to generate token.json: 
SCOPES = ["https://www.googleapis.com/auth/drive"]
GEOID_DIFF = { # EGM08-EGM96
    'Lam_Pao': 0.24, # m; -27.4462 - (-27.6864),
    'Lamtakhong': 0.154, # m; -29.0343 - (-29.1883)
    'Nam_Leuk': 0.6086, # m; -31.2049 - (-31.8135)
    'Nam_Ngum_2': 0.6086, # m; -31.2049 - (-31.8135)
    'Se_San_IV': -0.0825, # m; -6.4698 - (-6.3873)
    'Sirindhorn': 0.7595, # m; -18.5737 - (-19.3332)
}
UH_NAMING_CONVENTION = {
    'Lam_Pao': 'Lam_Paonew',
    'Lamtakhong': 'Lamtakhong',
    'Nam_Leuk': 'Nam_Leuk',
    'Nam_Ngum_2': 'Nam_Ngum2',
    'Sirindhorn': 'Noi',
    'Se_San_IV': 'Se_San_IV'
}

## utility to replace RAT-generated AEC files with extrapolated aec files if available
def replace_aec_file(
        extrapolated_dir, 
        rat_output_aec_dir
    ):
    extrapolated_dir = Path(extrapolated_dir)
    rat_output_aec_dir = Path(rat_output_aec_dir)

    extrapolated_aecs = extrapolated_dir.glob('*.csv')
    
    for aec_file in extrapolated_aecs:
        rat_output_fp = rat_output_aec_dir / f"{aec_file.name}"
        if rat_output_fp.exists():
            # make backup
            shutil.copy(rat_output_fp, rat_output_fp.with_suffix('.bak'))
        print(f"Copying {aec_file} to {rat_output_fp}")
        shutil.copy(aec_file, rat_output_fp)


def listfolders(service, filid, des):
    results = service.files().list(
        pageSize=1000, q="\'" + filid + "\'" + " in parents",
        fields="nextPageToken, files(id, name, mimeType)").execute()
    # logging.debug(folder)
    folder = results.get('files', [])
    for item in folder:
        if str(item['mimeType']) == str('application/vnd.google-apps.folder'):
            if not os.path.isdir(des+"/"+item['name']):
                os.mkdir(path=des+"/"+item['name'])
            # print(item['name'])
            listfolders(service, item['id'], des+"/"+item['name'])  # LOOP un-till the files are found
        else:
            downloadfiles(service, item['id'], item['name'], des)
            # print(item['name'])
    return folder


def downloadfiles(service, dowid, name,dfilespath):
  try:
    request = service.files().get_media(fileId=dowid)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        # print("Download %d%%." % int(status.progress() * 100))
    with io.open(dfilespath + "/" + name, 'wb') as f:
        fh.seek(0)
        f.write(fh.read())
  except Exception as e:
    print(e)

# Download sentinel-6 altimetry data from UH
def get_uh_altimetry(
        save_dir = Path('/cheetah2/pdas47/rat3_mekong/data/SE-Asia/basins/mekong/altimetry/s6'),
        service_acc_fn = Path("/cheetah2/pdas47/rat3_mekong/secrets/rat3-mekong-2931415a163b.json"),
        overwrite=False
    ):
    """
    Downloads newly available altimetry data from google drive
    """
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    service_acc_fn = Path(service_acc_fn)

    creds = None
    # The service account file is needed for authentication. The service account email should also
    # need to have access to the google drive folder, which can be given by sharing the folder with
    # the email.
    if service_acc_fn.exists():
        creds = service_account.Credentials.from_service_account_file(
            filename=service_acc_fn, scopes=SCOPES
        )

    try:
        service = build("drive", "v3", credentials=creds)

        # Call the drive v3 API
        folderId = service.files().list(
            q = "mimeType = 'application/vnd.google-apps.folder' and name = 'RAT-Mekong-Altimetry'", 
            pageSize=10, fields="nextPageToken, files(id, name)"
        ).execute()
        folderIdResult = folderId.get('files', [])
        id = folderIdResult[0].get('id')
        # Now, using the folder ID gotten above, we get all the files from
        # that particular folder
        results = service.files().list(
            q = "'" + id + "' in parents", pageSize=1e3, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])
        df = pd.DataFrame(columns = ['id', 'name'], data=items)
        df['time'] = pd.to_datetime(df['name'], format="%Y-%m-%d")
        df = df.set_index('time')
        
        for f in range(0, len(items)):
            fId = items[f].get('id')
            fName = items[f].get('name')
            print(f"{fId}: {fName}")

        # check for existing data
        existing_files = list(save_dir.glob("*"))
        if len(existing_files) == 0: 
            required_data_df = df
        else:
            existing_df = pd.DataFrame(
                data={
                    "id": [np.nan for f in existing_files],
                    "time": [pd.to_datetime(f.name) for f in existing_files]
                }
            ).set_index('time')
            print(f'Existing data from {existing_df.index[0]:%Y-%m-%d} to {existing_df.index[-1]:%Y-%m-%d}')
            print(f'New data from {df.index[0]:%Y-%m-%d} to {df.index[-1]:%Y-%m-%d}')


            required_data = df.index.difference(existing_df.index)
            required_data_df = df.loc[required_data]
        
        print(f"Downloading {len(required_data_df)} files")
        
        if len(required_data_df) == 0:
            print(f"No new data to download, exiting...")
            return

        # pbar1 = tqdm(total=len(required_data_df.index))
        for index, row in required_data_df.iterrows():
            # pbar1.set_description_str(f'Downloading {row["name"]}')
            print(f'Downloading {row["name"]}')
            Folder_id = row['id']  # Enter The Downloadable folder ID From Shared Link
            date = pd.to_datetime(row['name'], format="%Y-%m-%d")

            results = service.files().list(
                q = "'" + Folder_id + "' in parents", 
                pageSize=1e3, 
                fields="nextPageToken, files(id, name, mimeType)"
            ).execute()

            items = results.get('files', [])
            if not items:
                print('No files found.')
            else:
                print(f'Found {len(items)} items')
                pbar2 = tqdm(total=len(items))
                for item in items:
                    pbar2.set_description(f'-- Downloading {item["name"]}')
                    if item['mimeType'] == 'application/vnd.google-apps.folder':
                        des = save_dir / f"{date:%Y-%m-%d}" / item['name']
                        des.mkdir(parents=True, exist_ok=True)
                        listfolders(service, item['id'], str(des))
                    else:
                        des = save_dir / f"{date:%Y-%m-%d}" / item['name']
                        downloadfiles(service, item['id'], item['name'], str(des))
                    pbar2.update(1)
            # pbar1.update(1)

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")

# calculate ∆S
def calc_dels_s6(altim_fp, aec_fp, geoid_diff=0, savefp=None):
    """Calculate ∆S from Sentinel-6 observations.

    Args:
        altim_fp (str): Filepath to Sentinel-6 altimetry data.
        aec_fp (str): Filepath to AEC data.
        geoid_diff (float, optional): Difference between EGM2008 and EGM96 geoids at the location of reservoir. Defaults to 0.
    """
    altim_fp = Path(altim_fp)
    aec_fp = Path(aec_fp)

    altim_df = pd.read_csv(
        altim_fp, delimiter='\s+',
        names=['cycle', 'H [m w.r.t. EGM2008 Geoid]', 'uncertainty', 'date', 'lon', 'lat']
    )
    altim_df['date'] = pd.to_datetime(altim_df['date'], format='%Y%m%d')
    aec_df = pd.read_csv(aec_fp)
    altim_df['H [m w.r.t. EGM96 Geoid]'] = altim_df['H [m w.r.t. EGM2008 Geoid]'] - geoid_diff

    H = altim_df['H [m w.r.t. EGM96 Geoid]'].values
    A = np.interp(H, aec_df['elevation'], aec_df['area'])

    dels = np.full_like(H, np.nan)

    for t in range(1, len(H)):
        dels[t] = (H[t] - H[t-1])*(A[t] + A[t-1])/2  # mil. m^3

    altim_df['area (km2)'] = A
    altim_df['dS'] = dels
    if 'date' in altim_df.columns:
        altim_df = altim_df.set_index('date')

    if savefp is not None:
        # save dels from altimetry
        altim_df.to_csv(savefp)

    return altim_df.reset_index()

def calc_outflow(inflowpath, dels, epath, area, savepath):
    if os.path.isfile(inflowpath):
        inflow = pd.read_csv(inflowpath, parse_dates=["date"])[['streamflow', 'date']]
        inflow = inflow.dropna()

    else:
        raise Exception('Inflow file does not exist. Outflow cannot be calculated.')
    if os.path.isfile(epath):
        E = pd.read_csv(epath, parse_dates=['time'])
    else:
        raise Exception('Evaporation file does not exist. Outflow cannot be calculated.')

    if isinstance(dels, str):
        if os.path.isfile(dels):
            df = pd.read_csv(dels, parse_dates=['date'])
        else:
            raise Exception('Storage Change file does not exist. Outflow cannot be calculated.')
    else:
        df = dels
    
    inflow = inflow[inflow['date']>=df['date'].iloc[0]]
    inflow = inflow[inflow['date']<=df['date'].iloc[-1]]
    inflow = inflow.set_index('date')

    inflow['streamflow'] = inflow['streamflow'] * (60*60*24)

    E = E[E['time']>=df['date'].iloc[0]]
    E = E[E['time']<=df['date'].iloc[-1]]
    E = E.set_index('time')

    E['OUT_EVAP'] = E['OUT_EVAP'] * (0.001 * area * 1000*1000)  # convert mm to m3. E in mm, area in km2

    last_date = df['date'][:-1]
    df = df.iloc[1:,:]
    
    df['last_date'] = last_date.values
    
    df['inflow_vol'] = df.apply(lambda row: inflow.loc[row['last_date']:row['date'], 'streamflow'].sum(), axis=1)
    df['evap_vol'] = df.apply(lambda row: E.loc[(E.index > row['last_date'])&(E.index <= row['date']), 'OUT_EVAP'].sum(), axis=1)
    df['outflow_vol'] = df['inflow_vol'] - (df['dS']*1e6) - df['evap_vol']
    df['days_passed'] = (df['date'] - df['last_date']).dt.days
    df['outflow_rate'] = df['outflow_vol']/(df['days_passed']*24*60*60)   # cumecs
    df['outflow_rate'] = df['outflow_rate'].clip(lower=0)

    if savepath is not None:
        df.to_csv(savepath, index=False)
    
    return df


def uh_altimetry_routine(
        reservoirs, raw_s6_dir,
        rat_aec_dir, dels_savedir, outflow_savedir, inflow_savedir, evap_savedir
    ):
    outflows = {}
    for reservoir in reservoirs:
        print(f"UH altimetry: Processing {reservoir}")
        uh_name = UH_NAMING_CONVENTION[reservoir]

        # get data of latest date
        latest_date = sorted([pd.to_datetime(f.name) for f in raw_s6_dir.glob("*")])[-1]
        altim_fp = list(Path(raw_s6_dir / f'{latest_date:%Y-%m-%d}/timeseries_output_{uh_name}/').glob("*iqr.txt"))[0]
        aec_fp = rat_aec_dir / f'{reservoir}.csv'

        dels_savedir = Path(dels_savedir)
        dels_savefp = dels_savedir / f"{reservoir}.csv"
        # calcualte ∆S
        dels = calc_dels_s6(altim_fp, aec_fp, geoid_diff=GEOID_DIFF[reservoir], savefp=dels_savefp)

        outflow_savedir = Path(outflow_savedir)
        outflow_savefp = outflow_savedir / f"{reservoir}.csv"

        # calculate outflow
        outflow = calc_outflow(
            inflowpath = Path(inflow_savedir) / f'{reservoir}.csv',
            dels = dels,
            epath = Path(evap_savedir) / f'{reservoir}.csv',
            area = dels['area (km2)'], savepath = outflow_savefp
        )
        outflows[reservoir] = outflow

    return outflow


def uh_altimetry_convert_dels(dels_dir, website_v_dir):
    # Delta S
    dels_paths = [os.path.join(dels_dir, f) for f in os.listdir(dels_dir) if f.endswith(".csv")]
    dels_web_dir = Path(website_v_dir) / 'altimetry_dels'
    dels_web_dir.mkdir(parents=True, exist_ok=True)

    for dels_path in dels_paths:
        res_name = os.path.splitext(os.path.split(dels_path)[-1])[0]
        savename = res_name

        savepath = os.path.join(dels_web_dir , f"{savename}.csv")
        df = pd.read_csv(dels_path, parse_dates=['date'])[['date', 'dS']]
        df['days_passed'] = (df['date'] - df['date'].shift(1)).dt.days
        df['dS (m3)'] = df['dS'] * 1e6                                     # indicate units, convert from MCM to m3
        df = df[['date', 'dS (m3)']]

        print(f"Converting [∆S]: {res_name}, {savepath}")
        df.to_csv(savepath, index=False)

def uh_altimetry_convert_outflow(outflow_dir, website_v_dir):
    # Outflow
    outflow_paths = [os.path.join(outflow_dir, f) for f in os.listdir(outflow_dir) if f.endswith(".csv")]
    outflow_web_dir = Path(website_v_dir) /'altimetry_outflow'
    outflow_web_dir.mkdir(exist_ok=True)

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


if __name__ == "__main__":
    date = pd.to_datetime('2024-06-03')
    extrapolated_aec_dir = Path(f'/cheetah2/pdas47/rat3_mekong/data/extrapolated-aec')
    raw_s6_dir = Path('/cheetah2/pdas47/rat3_mekong/data/SE-Asia/basins/mekong/altimetry/s6')
    secret_file_fn = Path("/cheetah2/pdas47/rat3_mekong/secrets/rat3-mekong-2931415a163b.json")

    rat_aec_dir = Path('/cheetah2/pdas47/rat3_mekong/data/SE-Asia/basins/mekong/rat_outputs/aec')
    inflow_dir = Path('/cheetah2/pdas47/rat3_mekong/data/SE-Asia/basins/mekong/rat_outputs/inflow')
    evap_dir = Path('/cheetah2/pdas47/rat3_mekong/data/SE-Asia/basins/mekong/rat_outputs/Evaporation')
    outflow_dir = Path('/cheetah2/pdas47/rat3_mekong/data/SE-Asia/basins/mekong/rat_outputs/rat_outflow')
    dels_savedir = Path('/cheetah2/pdas47/rat3_mekong/data/SE-Asia/basins/mekong/rat_outputs/altimetry_dels')
    outflow_savedir = Path('/cheetah2/pdas47/rat3_mekong/data/SE-Asia/basins/mekong/rat_outputs/rat_altimetry_outflow')

    reservoirs = ['Lam_Pao', 'Lamtakhong', 'Nam_Leuk', 'Nam_Ngum_2', 'Se_San_IV', 'Sirindhorn']

    replace_aec_file(
        extrapolated_aec_dir, 
        rat_aec_dir
    )
    get_uh_altimetry(
        save_dir=raw_s6_dir, 
        secret_file_fn=secret_file_fn
    )
    
    outflow = uh_altimetry_routine(
        reservoirs, raw_s6_dir,
        rat_aec_dir, dels_savedir, outflow_savedir, inflow_dir, evap_dir
    )