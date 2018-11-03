import boto3
from . import config
import logging
import time

log = logging.getLogger(__name__)

CLUSTER_ID = 'cluster'
USERNAME = 'awsuser'
PASSWORD = 'R3dundant' # Instance won't be publically accessible anyway

CONNECTION = 'Driver={{Amazon Redshift (x64)}}; Server={Address}; Database=dev; UID={USERNAME}; PWD={PASSWORD}; Port={Port}'

_client = None
def client(): 
    global _client
    if _client is None:
        _client = boto3.client('redshift', region_name=config('REGION')) 
    
    return _client

def exists():
    identifiers = {c['ClusterIdentifier'] for c in client().describe_clusters()['Clusters']} 
    return CLUSTER_ID in identifiers

def instance():
    return {c['ClusterIdentifier']: c for c in client().describe_clusters()['Clusters']}[CLUSTER_ID]

def await_boot():
    while True:
        status = instance()['ClusterStatus']
        log.info(f'Cluster status is {status}')
        if status != 'creating':
            return
        time.sleep(5)

def create_instance():
    assert not exists()
    response = client().create_cluster(
        ClusterIdentifier=CLUSTER_ID,
        ClusterType='single-node',
        NodeType='dc2.large',
        MasterUsername=USERNAME,
        MasterUserPassword=PASSWORD,
        VpcSecurityGroupIds=['sg-050672ba3bacda0fc'],
        AvailabilityZone=config('AVAILABILITY_ZONE'),
        PubliclyAccessible=False,
        IamRoles=['arn:aws:iam::766263079615:role/ec2-parallax'],
    )
    await_boot()

def connection():
    conn = CONNECTION.format(**instance()['Endpoint'], USERNAME=USERNAME, PASSWORD=PASSWORD)