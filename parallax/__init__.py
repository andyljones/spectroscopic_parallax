import sys
import logging
import matplotlib.pyplot as plt

import pandas as pd
import scipy as sp
import matplotlib.pyplot as plt 
from .aws import ec2
from . import data, specnorm, tools
from logging import getLogger

log = getLogger(__name__)

def run_local():
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
            'nonduplicate': ~catalog.tmass.tmass_id.duplicated(),
            'photometry_jk': (catalog.apogee.j - catalog.apogee.k) < (.4 + .45*catalog.gaia.bp_rp),
            'photometry_hw': (catalog.apogee.h - catalog.wise.w2mpro) > -.05,
            'finite_jhk': catalog.apogee[['j', 'h', 'k']].gt(-100).all(1),
            'positive_jhk_err': catalog.apogee[['j_err', 'h_err', 'k_err']].gt(0).all(1),
            'finite_wise': catalog.wise[['w1mpro', 'w2mpro']].apply(sp.isfinite).all(1),
            'positive_wise_err': catalog.wise[['w1mpro_error', 'w2mpro_error']].gt(0).all(1)}
    cut = sp.all(sp.vstack(cuts.values()).data, 0)

    for k, c in cuts.items():
        log.info(f'{1 - c.mean():>3.0%} of the population is cut away by {k}')
    log.info(f'{cut.mean():>3.0%} stars remain')
    
    return catalog[cut]

def run_remote():
    catalog = parent_sample(data.load_catalog())
    spectra = data.load_spectra(catalog)
    normed = specnorm.normalize(spectra)