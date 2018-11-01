from io import BytesIO
import tempfile
import scipy as sp
import astropy
import logging
import sys
import os
from astroquery.gaia import Gaia
from . import s3

os.environ['GAIA_TOOLS_DATA'] = '/home/ec2-user/code/data/gaia_tools'
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

import gaia_tools
import gaia_tools.load
import gaia_tools.xmatch

def startup():
    import aws
    instance = aws.request_spot('python', .25, script=aws.CONFIG, image='python-ec2')
    aws.await_boot(instance)
    aws.tunnel(instance)
    aws.rsync(instance)
    aws.remote_console()

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

def run():
    apogee_cat = gaia_tools.load.apogee()
    gaia_subtable, _ = gaia_tools.xmatch.cds(apogee_cat, xcat='vizier:I/345/gaia2')
    gaia_table = expand_gaia(gaia_subtable)

    indices = gaia_tools.xmatch.cds_matchback(apogee_cat, gaia_table)

    apogee_table = astropy.table.Table(apogee_cat[indices])
    table = astropy.table.hstack([gaia_table, apogee_table])
    stringify(table)

    bytestring = BytesIO()
    table.write(bytestring, format='fits')
    bytestring.seek(0)
    bytestring = bytestring.read()

    s3.Path('alj.data/parallax/apogee_gaia.fits').write_bytes(bytestring)
    pass