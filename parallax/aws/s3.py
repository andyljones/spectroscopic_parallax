import boto3
import botocore
import json
import pickle
from io import BytesIO
from contextlib import contextmanager
from . import config

__all__ = ('Path',)

_resource = None
def resource(): 
    global _resource
    if _resource is None:
        _resource = boto3.resource('s3', region_name=config('REGION'))
    
    return _resource

_client = None
def client(): 
    global _client
    if _client is None:
        _client = boto3.client('s3', region_name=config('REGION')) 
    
    return _client

class Path(object):
    """Represents S3 as a pathlib object. For example:

    `Path('alj.data/dirname/datafile.pkl').write_bytes(pickle.dumps(obj))`

    will store an object to the bucket 'alj.data' under the key 'dirname/datafile.pkl'. If the bucket
    doesn't exist, it'll be created. To read the data back,

    `obj = pickle.loads(Path('alj.data/dirname/datafile.pkl').read_bytes())`

    There is also a `write_multipart` function for streaming large files to S3. It works well with 
    requests' streaming capabilities.
    """

    def __init__(self, path):
        parts = path.split('/')
        bucket, key = parts[0], '/'.join(parts[1:])
        
        self._bucket = resource().Bucket(bucket)
        self._bucket.create()

        self._object = self._bucket.Object(key)
    
    def write_bytes(self, data):
        self._object.upload_fileobj(BytesIO(data))
    
    @contextmanager
    def write_multipart(self):
        """Needs to have less than 10000 parts"""
        uploader = self._object.initiate_multipart_upload()

        parts = {}
        try:
            def write(data):
                i = len(parts) + 1
                parts[i] = uploader.Part(i).upload(Body=data)
            
            yield write
            
            parts = [{'PartNumber': i , 'ETag': p['ETag']} for i, p in parts.items()]
            uploader.complete(MultipartUpload={'Parts': parts})
        except Exception as e:
            uploader.abort()
            raise IOError('Multipart upload failed') from e
    
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
