from io import BytesIO
import tempfile
import scipy as sp
import astropy
import os
from astroquery.gaia import Gaia
from .aws import s3
import logging
import time
import pandas as pd

os.environ['GAIA_TOOLS_DATA'] = '/home/ec2-user/code/data/gaia_tools'
import gaia_tools
import gaia_tools.load
import gaia_tools.xmatch

log = logging.getLogger(__name__)

PATH = 'alj.data/parallax/apogee_gaia.fits'

def _load_gaia(apogee):
    """This was all crap; didn't realise there were official matchings
    GAIA-WISE match comes from the official GAIA release: https://gea.esac.esa.int/archive/
    GAIA-APOGEE match is derived from GAIA-2MASS match, as 2MASS was used to guide APOGEE target selection.
    So: load APOGEE data, cross-match to GAIA on the 2MASS ID (REDUCTION_ID column, with the 2M stripped), cross-match to WISE on the WISE ID
    """

    tmass_ids = (pd.Series(apogee.APOGEE_ID)
        .apply(bytes.decode)
        .str.strip()
        .loc[lambda s: s.apply(len) == 18]
        .str[2:]
        .unique())
    
    query = """
        select top 100 
            gaia.*, 
            allwise.original_ext_source_id as allwise_id, 
            tmass.original_ext_source_id as tmass_id
        from gaiadr2.gaia_source as gaia
            inner join gaiadr2.allwise_best_neighbour as allwise 
                on gaia.source_id = allwise.source_id
            inner join gaiadr2.tmass_best_neighbour as tmass
                on gaia.source_id = tmass.source_id
            inner join tap_upload.mine as mine 
                on mine.tmass_id = tmass.original_ext_source_id"""

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

def _load():
    apogee = gaia_tools.load.apogee()
    gaia = _load_gaia(apogee)


def load():
    path = s3.Path(PATH)
    if not path.exists():
        log.info('No apogee-gaia cache available, creating it from scratch')
        path.write_bytes(_load())
    return astropy.table.Table.read(BytesIO(path.read_bytes()))