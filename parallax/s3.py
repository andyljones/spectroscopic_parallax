import boto3
import botocore
import json
from io import BytesIO

__all__ = ('Path',)

def config(key):
    config = {**json.load(open('config.json')), **json.load(open('credentials.json'))}
    return config[key]

_s3 = None
def s3(): 
    global _s3
    if _s3 is None:
        _s3 = boto3.resource('s3', region_name=config('REGION'), 
                              aws_access_key_id=config('AWS_ID'), 
                              aws_secret_access_key=config('AWS_SECRET'))
    
    return _s3

class Path(object):

    def __init__(self, path):
        parts = path.split('/')
        bucket, key = parts[0], '/'.join(parts[1:])
        
        self._bucket = s3().Bucket(bucket)
        self._bucket.create()

        self._object = self._bucket.Object(key)
    
    def write_bytes(self, data):
        self._object.upload_fileobj(BytesIO(data))
    
    def read_bytes(self):
        data = BytesIO()
        self._object.download_fileobj(data)
        data.seek(0)
        return data.read()

    def exists(self):
        try:
            self._object.content_length
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise
        else:
            return True
