import sys
import logging
import matplotlib.pyplot as plt

import pandas as pd
import scipy as sp
import matplotlib.pyplot as plt 
from .aws import ec2
from . import data, specnorm, tools

def startup():
    instance = ec2.request_spot('python', .25, script=ec2.CONFIG, image='python-ec2')
    sess = ec2.session(instance)

def parent_sample(catalog):
    cuts = {'upper_g': catalog.apogee.logg <= 2.2,
            'nonnull_g': catalog.apogee.logg > 0., # there are a few values less than zero that are not null
            'nonnull_k': catalog.apogee.k > 0,
            'nonnull_bp_rp': sp.isfinite(catalog.gaia.bp_rp),
            'nonnull_w1mpro': sp.isfinite(catalog.wise.w1mpro),
            'nonnull_w2mpro': sp.isfinite(catalog.wise.w2mpro),
            'nonvariable': catalog.gaia.phot_variable_flag != 'VARAIBLE',
            'nonduplicate': ~catalog.tmass.tmass_id.duplicated()}
    cut = sp.all(sp.vstack(cuts.values()).data, 0)
    return catalog[cut]

def run():
    catalog = parent_sample(data.load_catalog())
    spectra = data.load_spectra(catalog)
    normed = specnorm.normalize(spectra)