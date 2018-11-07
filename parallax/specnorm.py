from tqdm import tqdm
import pandas as pd 
import scipy as sp
from numpy.polynomial.chebyshev import Chebyshev

#TODO: This is smaller than many errors encountered in practice
# It's also totally inconsistent with the clipping value used below
ERROR_LIM = 3.0
CHIPS = {
    'a': (15150, 15800), 
    'b': (15890, 16540), 
    'c': (16490, 16950)}


def normalize(spectrum):
    if isinstance(spectrum, pd.DataFrame):
        rows = {k: normalize(r) for k, r in tqdm(spectrum.iterrows(), total=len(spectrum))}
        return pd.DataFrame.from_dict(rows, orient='index').reindex_like(spectrum)
    #TODO: This is missing the pixmask from the original code. What's the pixmask?
    #TODO: This is slow. Operate on views of the underlying data.

    df = spectrum.unstack(0)
    wavelengths = df.index.values
    flux, error = df.flux.values.copy(), df.error.values.copy()

    bad = df.isnull().any(1) | df.eq(sp.inf).any(1) | df.error.le(0)
    flux[bad] = 1
    error[bad] = ERROR_LIM

    var = ERROR_LIM**2 +  sp.zeros_like(error)
    inv_var = 1/(var + error**2)

    norm_flux = sp.full_like(flux, 0)
    norm_error = sp.full_like(error, ERROR_LIM)
    for _, (left, right) in CHIPS.items():
        mask = (left < wavelengths) & (wavelengths < right)
        fit = Chebyshev.fit(x=wavelengths[mask], y=flux[mask], w=inv_var[mask], deg=2)

        #TODO: Is the denominator being negative an issue?
        norm_flux[mask] = flux[mask]/fit(wavelengths[mask])
        norm_error[mask] = error[mask]/fit(wavelengths[mask])

    unreliable = (norm_error > ERROR_LIM)
    norm_flux[unreliable] = 1
    norm_error[unreliable] = ERROR_LIM

    return pd.DataFrame({'error': norm_error, 'flux': norm_flux, 'mask': df['mask']}, index=wavelengths).unstack()