import pandas as pd
import scipy as sp
import scipy.optimize
from . import tools

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
    X = sp.concatenate([constant, gaia, tmass, wise, apogee], 1)

    m = sp.full(X.shape[1], 0)
    m[-len(apogee):] = 1

    return X, m

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

def solve(X, y, w, m, lambd=30):

    def f(b):   
        yhat = sp.exp(X @ b)
        return .5*w @ (y - yhat)**2 + lambd*m @ sp.fabs(b)
    
    def grad(b):
        yhat = sp.exp(X @ b)
        return -X.T @ (yhat * w * (y - yhat)) + lambd*m*sp.sign(b)

    D =  X.shape[1]
    b0 = sp.full(D, 1e-3/D) #TODO: Should this be constant in l2 norm rather than l1?

    assert sp.optimize.check_grad(f, grad, b0) < 1e-3*f(b0), '`grad` does not appear to implement the gradient'

    #TODO: Evaluating the full hessian is infeasible, but is there any way we could generate the `hessp` arg?
    #TODO: Why use -B rather than BFGS? There are no constraints here
    result = sp.optimize.minimize(f, b0, 'L-BFGS-B', grad)
    assert result.success

    return result.x

def training_catalog(catalog):
    cuts = {
        'finite_parallax': catalog.gaia.parallax < sp.inf,
        'multiobservation': catalog.gaia.visibility_periods_used >= 8,
        'low_error': catalog.gaia.parallax_error < .1,
        # This thresholds the goodness-of-fit of the astrometric solution to the observations made, along the scan direction
        'coryn': catalog.gaia.astrometric_chi2_al/sp.sqrt(catalog.gaia.astrometric_n_good_obs_al - 5) <= 35}
    return tools.cut(catalog, cuts)

def fit(catalog, normed):
    training = training_catalog(catalog)
    good = (training.gaia.parallax_over_error > 20)

    X, m = design_matrix(training, normed.reindex(training.apogee.file.str.strip()))
    y = training.gaia.parallax.values + PARALLAX_OFFSET
    w = 1/training.gaia.parallax_error.values**2

    b = solve(X, y, w, m)

    # Xe = design_errors(training, normed)
    # ye = training.gaia.parallax_error
    