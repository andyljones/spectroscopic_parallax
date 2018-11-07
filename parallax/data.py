import requests
from io import BytesIO
import tempfile
import scipy as sp
import astropy
import astropy.table
import os
from .aws import s3
import logging
import time
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import pickle

log = logging.getLogger(__name__)

PATH = 'alj.data/parallax/apogee_gaia.fits'

APRED_VERS = 'r8'
ASPCAP_VER = 'l31c'
RESULTS_VER = 'l31c.2'

def drop_multidim_cols(table):
    return table[[k for k, (d, _) in table.dtype.fields.items() if d.shape == ()]]

def stringify(df):
    # None of the bytestrings in these tables actually look like they should be bytestrings.
    df = df.copy()

    bytecols = df.columns[df.dtypes == object]
    for c in bytecols:
        df[c] = df[c].str.decode('ascii')

    return df

def fetch_apogee():
    """APOGEE DR14 info: https://www.sdss.org/dr14/irspec/spectro_data/"""

    url = f'https://data.sdss.org/sas/dr14/apogee/spectro/redux/{APRED_VERS}/stars/{ASPCAP_VER}/{RESULTS_VER}/allStar-{RESULTS_VER}.fits'
    r = requests.get(url)
    table = astropy.table.Table.read(BytesIO(r.content), hdu=1)
    table = drop_multidim_cols(table)
    
    return (table
                    .to_pandas()
                    .rename(columns=str.lower)
                    .pipe(stringify))

def fetch_gaia(tmass_ids):
    from astroquery.gaia import Gaia

    # Can't prefix columns with the table name, so have to settle on using 'panda_start' and 
    # 'wise_start'
    query = """
        select
            mine.tmass_id as tmass_id,
            '' as gaia_start,
            gaia.*, 
            '' as wise_start,
            allwise.*
        from gaiadr2.gaia_source as gaia

            inner join gaiadr2.tmass_best_neighbour as tmass_xmatch
                on gaia.source_id = tmass_xmatch.source_id
            inner join tap_upload.mine as mine 
                on tmass_xmatch.original_ext_source_id = mine.tmass_id

            inner join gaiadr2.allwise_best_neighbour as allwise_xmatch
                on gaia.source_id = allwise_xmatch.source_id
            inner join gaiadr1.allwise_original_valid as allwise
                on allwise_xmatch.original_ext_source_id = allwise.designation"""

    with tempfile.NamedTemporaryFile(suffix='.xml') as tmp:
        os.remove(tmp.name) # astropy will complain if the file already exists
        (astropy.table.Table(tmass_ids[:, None].astype(bytes), names=['tmass_id'])
            .write(tmp.name, format='votable'))

        log.info(f'Launching job for {len(tmass_ids)} 2MASS IDs')
        job = Gaia.launch_job_async(query, upload_resource=tmp.name, upload_table_name='mine')
        log.info(f'Job ID is {job.get_jobid()}')

    while True:
        time.sleep(5)
        log.info(f'Job is {job.get_phase()}')
        if job.get_phase() == 'COMPLETED':
            table = job.get_results()
            break

    df = table.to_pandas().pipe(stringify)

    gaia_start = list(df.columns).index('gaia_start')
    wise_start = list(df.columns).index('wise_start')
    indices = sp.arange(len(df.columns))
    masks = {'tmass': df.columns == 'tmass_id',
             'gaia': (gaia_start < indices) & (indices < wise_start),
             'wise': (wise_start < indices)}
    df = pd.concat({k: df.loc[:, m] for k, m in masks.items()}, 1)

    # Some fields have a _2 suffixed because they're replicated in GAIA and WISE
    df = df.rename(columns=lambda c: c.split('_2')[0])

    return df

def fetch_catalogue():
    apogee = fetch_apogee()

    apogee['tmass_id'] = (apogee['apogee_id']
                                .str.strip()
                                .str[2:])
                                
    apogee = apogee[apogee.tmass_id.apply(len) == 16]

    gaia = fetch_gaia(apogee['tmass_id'].unique())

    apogee = pd.concat({'apogee': apogee}, 1)
    catalogue = pd.merge(apogee, gaia, left_on=(('apogee', 'tmass_id'),), right_on=(('tmass', 'tmass_id'),))
    return catalogue

def load_catalogue():
    path = s3.Path(PATH)
    if not path.exists():
        log.info('No apogee-gaia cache available, creating it from scratch')
        catalogue = fetch_catalogue()
        path.write_bytes(pickle.dumps(catalogue))
        time.sleep(1) # Going straight to reading can time out sometimes

    return pickle.loads(path.read_bytes())

def fetch_spectrum(telescope, location_id, file):
    """Data model: https://data.sdss.org/datamodel/files/APOGEE_REDUX/APRED_VERS/APSTAR_VERS/TELESCOPE/LOCATION_ID/apStar.html#hdu1"""

    url = f'https://data.sdss.org/sas/dr14/apogee/spectro/redux/{APRED_VERS}/stars/{telescope.strip()}/{location_id}/{file.strip()}'
    r = requests.get(url)
    r.raise_for_status()
    hdus = astropy.io.fits.open(BytesIO(r.content))

    flux, errors = hdus[1].data[0], hdus[2].data[0]

    header = hdus[1].header
    wavelengths = 10**(header['CRVAL1'] + header['CDELT1']*sp.arange(header['NAXIS1']))
    wavelengths = sp.around(wavelengths, 2)
    #TODO: Return astropy Tables
    return pd.DataFrame({
            'flux': hdus[1].data[0].astype(float), 
            'error': hdus[2].data[0].astype(float),
            'mask': hdus[3].data[0].astype(int)
        }, index=wavelengths)

def downsample(spectra):
    spectra = spectra.copy()
    for field in ['flux', 'error']:
        if field in spectra:
            spectra[field] = spectra[field].astype(sp.float32)
    for field in ['mask']:
        if field in spectra:
            spectra[field] = spectra[field].astype(sp.int32)
    return spectra

def load_spectrum_group(telescope, location_id, files):
    path = s3.Path(f'alj.data/parallax/spectra/{telescope}/{location_id}')
    if not path.exists():
        spectra = {}
        for file in files:
            try:
                spectra[file] = fetch_spectrum(telescope, location_id, file)
            except:
                log.exception(f'Failed on {file}')

        if spectra:
            spectra = pd.concat(spectra, 1).swaplevel(0, 1, 1).sort_index(axis=1)
        else:
            spectra = pd.DataFrame(columns=pd.MultiIndex.from_arrays([[], []]))

        path.write_bytes(pickle.dumps(spectra))
        return spectra
    
    #TODO: These are needed to get down to a reasonable combined file size. Would be better if it
    # was done in `fetch_spectrum` though! Not changing it now because I dont have 3hr to spare 
    # re-caching everything.
    spectra = downsample(pd.read_pickle(BytesIO(path.read_bytes())))
    return spectra

def load_spectra(parent):
    log.warn('If the cuts change, the spectra will not be updated')
    #TODO: Handle updated file lists. Need to make note of missing files
    #TODO: Move away from pickling - will break when pandas changes
    #TODO: Restore parallelism. Kept hitting broken process pool errors, despite everything working fine in serial?
    path = s3.Path(f'alj.data/parallax/spectra/parent')
    if not path.exists():
        keys = parent[['TELESCOPE', 'LOCATION_ID']]
        groups = parent['FILE'].group_by(keys).groups
        spectra = []
        for (telescope, location_id), files in parent.groupby(['TELESCOPE', 'LOCATION_ID']).FILE:
            telescope = telescope.decode()
            spectra.append(load_spectrum_group(telescope, location_id, list(files)))
        spectra = pd.concat(spectra, 1)        
        path.write_bytes(pickle.dumps(spectra))
        return spectra
    
    spectra = pd.read_pickle(BytesIO(path.read_bytes()))
    return spectra