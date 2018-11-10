from tqdm import tqdm
import pandas as pd 
import scipy as sp
from numpy.polynomial.chebyshev import Chebyshev

#TODO: This is smaller than many errors encountered in practice
ERROR_LIM = 3.0
CHIPS = {
    'a': (15150, 15800), 
    'b': (15890, 16540), 
    'c': (16490, 16950)}

def normalize(spectra):
    #TODO: Add parallelization
    stars = spectra.index
    wavelengths = spectra.flux.columns.values.copy()
    flux = spectra.flux.values.copy()
    error = spectra.error.reindex(columns=wavelengths).values.copy()

    bad_flux = sp.isnan(flux) | sp.isinf(flux)
    bad_error = sp.isnan(error) | sp.isinf(error) | (error < 0)
    bad = bad_flux | bad_error

    flux[bad] = 1
    error[bad] = ERROR_LIM

    #TODO: pixlist is supposed to be used to zero many of these vars. Where's it come from?
    var = ERROR_LIM**2 + sp.zeros_like(error)
    inv_var = 1/(ERROR_LIM**2 + error**2)

    norm_flux = sp.full_like(flux, 1)
    norm_error = sp.full_like(error, ERROR_LIM)
    for star in tqdm(range(len(stars))):
        for _, (left, right) in CHIPS.items():
            mask = (left < wavelengths) & (wavelengths < right)
            #TODO: Why are we using Chebyshev polynomials rather than smoothing splines?
            #TODO: Why are we using three polynomials rather than one?
            #TODO: Is the denominator being zero/negative ever an issue?
            fit = Chebyshev.fit(x=wavelengths[mask], y=flux[star][mask], w=inv_var[star][mask], deg=2)

            norm_flux[star][mask] = flux[star][mask] / fit(wavelengths[mask])
            norm_error[star][mask] = error[star][mask]/ fit(wavelengths[mask])

    #TODO: Why is the unreliability threshold different from the limit value?
    unreliable = (norm_error > .3)
    norm_flux[unreliable] = 1
    norm_error[unreliable] = ERROR_LIM

    norm_flux = pd.DataFrame(norm_flux, stars, wavelengths)
    norm_error = pd.DataFrame(norm_error, stars, wavelengths)
    
    return pd.concat({'flux': norm_flux, 'error': norm_error}, 1)
