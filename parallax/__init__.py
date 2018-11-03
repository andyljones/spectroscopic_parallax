import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

import scipy as sp
import matplotlib.pyplot as plt 
from . import crossmatch
from .aws import ec2

def startup():
    instance = ec2.request_spot('python', .25, script=ec2.CONFIG, image='python-ec2')
    sess = ec2.session(instance)

def run():
    raw = crossmatch.load()

    cuts = {'upper_g': raw['LOGG'] <= 2.2,
            'nonnull_g': raw['LOGG'] > 0., # there are a few values less than zero that are not null
            'nonnull_k': raw['K'] > 0,
            'nonnull_bp_rp': sp.isfinite(raw['bp_rp']),
            'nonnull_w1mpro': sp.isfinite(raw['w1mpro']),
            'nonnull_w2mpro': sp.isfinite(raw['w2mpro']),
            'nonvariable': raw['phot_variable_flag'] != 'VARAIBLE'}