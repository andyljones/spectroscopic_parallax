from io import BytesIO
import tempfile
import scipy as sp
import astropy
import os
from astroquery.gaia import Gaia
from . import s3
import logging

os.environ['GAIA_TOOLS_DATA'] = '/home/ec2-user/code/data/gaia_tools'
import gaia_tools
import gaia_tools.load

log = logging.getLogger(__name__)

PATH = 'alj.data/parallax/apogee_gaia.fits'

def expand_gaia(gaia_subtable):
    # Write another temporary file with the XML output of the cross-match
    query = sp.array([gaia_subtable['source_id'], gaia_subtable['RA'], gaia_subtable['DEC']]).T
    query = astropy.table.Table(query, names=('source_id','RA','DEC'), dtype=('int64','float64','float64'))
    xmlfilename = tempfile.mktemp('.xml',dir=os.getcwd())
    query.write(xmlfilename, format='votable')

    job = Gaia.launch_job_async(
        """select g.*, m.RA as mRA, m.DEC as mDEC
from gaiadr2.gaia_source as g 
inner join tap_upload.my_table as m on m.source_id = g.source_id""",
                                upload_resource=xmlfilename,
                                upload_table_name="my_table")
    gaia_table = job.get_results()
    gaia_table.rename_column('mra','RA')
    gaia_table.rename_column('mdec','DEC')
    return gaia_table

def stringify(table):
    objects = [k for k, v in table.dtype.fields.items() if v[0] == 'O']
    for o in objects:
        table[o] = table[o].astype(str)

def _load():
    log.info('Constructing APOGEE-GAIA crossmatch')
    apogee_cat = gaia_tools.load.apogee()
    gaia_subtable, _ = gaia_tools.xmatch.cds(apogee_cat, xcat='vizier:I/345/gaia2')

    log.info('Expanding GAIA matches to the full columnset')
    gaia_table = expand_gaia(gaia_subtable)

    indices = gaia_tools.xmatch.cds_matchback(apogee_cat, gaia_table)

    log.info('Saving crossmatch to S3')
    apogee_table = astropy.table.Table(apogee_cat[indices])
    table = astropy.table.hstack([gaia_table, apogee_table])
    stringify(table)

    content = BytesIO()
    table.write(content, format='fits')
    content.seek(0)
    content = content.read()
    return content

def load():
    path = s3.Path(PATH)
    if not path.exists():
        log.info('No apogee-gaia cache available, creating it from scratch')
        path.write_bytes(_load())
    return astropy.table.Table.read(BytesIO(path.read_bytes()))