import requests
import pandas as pd
import astropy 
from tqdm import tqdm
from . import s3
from concurrent.futures import ProcessPoolExecutor, wait

# i \in [1, 48]
ROOT = "https://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part{:02d}.bz2"
S3 = "alj.wise/data/part{:02d}.bz2"

def cache(i=None):
    if i is None:
        with ProcessPoolExecutor() as pool:
            wait([pool.submit(cache, i) for i in range(1, 49)])

    path = s3.Path(S3.format(i))
    if path.exists():
        return

    with requests.get(ROOT.format(i), stream=True) as r, path.write_multipart() as write:
        length = int(r.headers['Content-Length'])//int(1e6)
        desc = f'WISE {i}/48'
        with tqdm(total=length, desc=desc) as progress:
            for chunk in r.iter_content(chunk_size=int(100e6)):
                if chunk:
                    write(chunk)
                    progress.update(len(chunk)//int(1e6))

# c = s3.client()
# result = c.select_object_content(
#     Bucket='alj.wise',
#     Key='data/part01.bz2',
#     ExpressionType='SQL',
#     Expression="""SELECT * FROM s3object AS s LIMIT 10 """,
#     InputSerialization={'CSV': {'FieldDelimiter': '|', 'FileHeaderInfo': 'NONE'},
#                         'CompressionType': 'BZIP2'},
#     OutputSerialization={'CSV': {}})
#
# for event in result['Payload']:
#     if 'Records' in event:
#         df = pd.read_csv(BytesIO(event['Records']['Payload']), header=None)

def get(wise_ids):
    """Plan: stream the download of the parts directly onto S3, and then multipart-upload it in batches to S3
        - Can probably do it in parallel
        - Can probably stick it in infrequent access
    """

    pass