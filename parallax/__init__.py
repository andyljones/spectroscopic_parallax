import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

import pandas as pd
import scipy as sp
import matplotlib.pyplot as plt 
from .aws import ec2
from parallax import data

def startup():
    instance = ec2.request_spot('python', .25, script=ec2.CONFIG, image='python-ec2')
    sess = ec2.session(instance)

def parent_sample(catalogue):
    cuts = {'upper_g': catalogue.apogee.logg <= 2.2,
            'nonnull_g': catalogue.apogee.logg > 0., # there are a few values less than zero that are not null
            'nonnull_k': catalogue.apogee.k > 0,
            'nonnull_bp_rp': sp.isfinite(catalogue.gaia.bp_rp),
            'nonnull_w1mpro': sp.isfinite(catalogue.wise.w1mpro),
            'nonnull_w2mpro': sp.isfinite(catalogue.wise.w2mpro),
            'nonvariable': catalogue.gaia.phot_variable_flag != 'VARAIBLE',
            'nonduplicate': ~catalogue.tmass.tmass_id.duplicated()}
    cut = sp.all(sp.vstack(cuts.values()).data, 0)
    return catalogue[cut]

def run():
    parent = parent_sample(data.load_catalogue())
    spectra = data.load_spectra(parent)