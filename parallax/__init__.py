import sys
import logging
import scipy as sp
from . import crossmatch
import matplotlib.pyplot as plt 

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def startup():
    import aws
    instance = aws.request_spot('python', .25, script=aws.CONFIG, image='python-ec2')
    aws.await_boot(instance)
    aws.tunnel(instance)
    aws.rsync(instance)
    aws.remote_console()

def run():
    raw = crossmatch.load()

    cuts = {'upper_g': raw['LOGG'] <= 2.2,
            'nonnull_g': raw['LOGG'] > 0., # there are a few values less than zero that are not null
            'nonnull_k': raw['K'] > 0,
            'nonnull_bp_rp': sp.isfinite(raw['bp_rp']),
            'nonnull_w1mpro': sp.isfinite(raw['w1mpro']),
            'nonnull_w2mpro': sp.isfinite(raw['w2mpro']),
            'nonvariable': raw['phot_variable_flag'] != 'VARAIBLE'}