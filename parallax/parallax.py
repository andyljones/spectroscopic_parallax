import pandas as pd
import scipy as sp

GAIA_BANDS = ['g', 'bp', 'rp']
TMASS_BANDS = ['j', 'h', 'k']
WISE_BANDS = ['w1mpro', 'w2mpro']
PARALLAX_OFFSET = 0.0483 #TODO: How much of a difference does this make?

def design_matrix(catalog, normed):
    constant = sp.ones((len(catalog), 1))
    gaia = catalog.gaia[[f'phot_{b}_mean_mag' for b in GAIA_BANDS]].values
    tmass = catalog.apogee[TMASS_BANDS].values
    wise = catalog.wise[WISE_BANDS].values
    apogee = sp.log(sp.clip(normed.flux.values, .01, 1.2)) #TODO: How much of an impact does this clipping have?
    return sp.concatenate([constant, gaia, tmass, wise, apogee], 1)

def design_errors(catalog, normed):
    #TODO: Where's the magic 1.09 come from?
    #TODO: Where's the magic 0.05 come from?
    #TODO: Why are the errors unaffected by taking the log? Just assume ln is linear around 1?
    constant = sp.zeros((len(catalog), 1))
    gaia = 1.09*pd.concat([catalog.gaia[f'phot_{b}_mean_flux_error']/catalog.gaia[f'phot_{b}_mean_flux'] for b in GAIA_BANDS], 1).values
    tmass = catalog.apogee[[f'{b}_err' for b in TMASS_BANDS]].values
    wise = catalog.wise[[f'{b}_error' for b in WISE_BANDS]].values
    apogee = sp.clip(normed.error.values, 0, .05) / sp.clip(normed.flux.values, .01, 1.2)
    return sp.concatenate([constant, gaia, tmass, wise, apogee], 1)

def fit(catalog, normed):
    X = design_matrix(catalog, normed)
    Xe = design_errors(catalog, normed)
    
    y = catalog.gaia.parallax + PARALLAX_OFFSET
    ye = catalog.gaia.parallax_error