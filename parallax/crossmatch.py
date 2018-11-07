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

def fetch_apogee():
    """APOGEE DR14 info: https://www.sdss.org/dr14/irspec/spectro_data/"""

    url = f'https://data.sdss.org/sas/dr14/apogee/spectro/redux/{APRED_VERS}/stars/{ASPCAP_VER}/{RESULTS_VER}/allStar-{RESULTS_VER}.fits'
    r = requests.get(url)
    return astropy.table.Table.read(BytesIO(r.content), hdu=1)

def fetch_gaia(tmass_ids):
    from astroquery.gaia import Gaia

    query = """
        select
            mine.tmass_id as tmass_id,
            '' as sep,
            gaia.*, 
            '' as sep,
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
        (astropy.table.Table(tmass_ids[:, None], names=['tmass_id'])
            .write(tmp.name, format='votable'))

        log.info(f'Launching job for {len(tmass_ids)} 2MASS IDs')
        job = Gaia.launch_job_async(query, upload_resource=tmp.name, upload_table_name='mine')

    while True:
        time.sleep(5)
        log.info(f'Job is {job.get_phase()}')
        if job.get_phase() == 'COMPLETED':
            return job.get_results()

def fetch_catalogue():
    apogee = fetch_apogee()

    apogee['tmass_id'] = (pd.Series(apogee['APOGEE_ID'])
                                .str.strip()
                                .str[2:]
                                .astype(bytes).values)
    apogee = apogee[sp.vectorize(len)(apogee['tmass_id']) == 16]

    gaia = fetch_gaia(sp.unique(apogee['tmass_id']))

    catalogue = astropy.table.join(gaia, apogee, 'tmass_id')

    return catalogue

def stringify(catalogue):
    objects = [k for k, v in catalogue.dtype.fields.items() if v[0] == 'O']
    for o in objects:
        catalogue[o] = catalogue[o].astype(str)

def load_catalogue():
    path = s3.Path(PATH)
    if not path.exists():
        log.info('No apogee-gaia cache available, creating it from scratch')
        catalogue = fetch_catalogue()

        stringify(catalogue)
        bytesio = BytesIO()
        catalogue.write(bytesio, format='fits')
        bytesio.seek(0)
        bytestring = bytesio.read()
        path.write_bytes(bytestring)
        time.sleep(1) # Going straight to reading can time out sometimes

    return astropy.table.Table.read(BytesIO(path.read_bytes()))

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
        for (telescope, location_id), files in tqdm(zip(groups.keys, groups), total=len(groups)):
            telescope = telescope.decode()
            spectra.append(load_spectrum_group(telescope, location_id, files))
        spectra = pd.concat(spectra, 1)        
        path.write_bytes(pickle.dumps(spectra))
        return spectra
    
    spectra = pd.read_pickle(BytesIO(path.read_bytes()))
    return spectra