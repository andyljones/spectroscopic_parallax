import requests
import pandas as pd
import astropy 

ROOT = "https://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part{:.02d}.bz2"

def get(wise_ids):
    """Plan: stream the download of the parts directly onto S3, and then multipart-upload it in batches to S3
        - Can probably do it in parallel
        - Can probably stick it in infrequent access
    """
    pass