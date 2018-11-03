import boto3
import botocore
import json
from io import BytesIO
from contextlib import contextmanager

__all__ = ('Path',)

def config(key):
    config = {**json.load(open('config.json')), **json.load(open('credentials.json'))}
    return config[key]

_resource = None
def resource(): 
    global _resource
    if _resource is None:
        _resource = boto3.resource('s3', region_name=config('REGION'), 
                              aws_access_key_id=config('AWS_ID'), 
                              aws_secret_access_key=config('AWS_SECRET'))
    
    return _resource

_client = None
def client(): 
    global _client
    if _client is None:
        _client = boto3.client('s3', region_name=config('REGION'), 
                              aws_access_key_id=config('AWS_ID'), 
                              aws_secret_access_key=config('AWS_SECRET'))
    
    return _client

class Path(object):

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
