import sys
import logging
from . import apogeegaia

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def startup():
    import aws
    instance = aws.request_spot('python', .25, script=aws.CONFIG, image='python-ec2')
    aws.await_boot(instance)
    aws.tunnel(instance)
    aws.rsync(instance)
    aws.remote_console()

def run():
    raw = apogeegaia.load()