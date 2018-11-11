import pickle
import pandas as pd
import scipy as sp
import scipy.optimize
from . import tools, aws
import logging

log = logging.getLogger(__name__)

GAIA_BANDS = ['g', 'bp', 'rp']
TMASS_BANDS = ['j', 'h', 'k']
WISE_BANDS = ['w1mpro', 'w2mpro']
PARALLAX_OFFSET = 0.0483 #TODO: How much of a difference does this make?

def design_matrix(catalog, normed):
    constant = sp.ones((len(catalog), 1))
    gaia = catalog.gaia[[f'phot_{b}_mean_mag' for b in GAIA_BANDS]]
    tmass = catalog.apogee[TMASS_BANDS]
    wise = catalog.wise[WISE_BANDS]

    aligned = normed.reindex(catalog.apogee.file.str.strip())
    apogee = sp.log(sp.clip(aligned.flux.values, .01, 1.2)) #TODO: How much of an impact does this clipping have?

    X = sp.concatenate([constant, gaia.values, tmass.values, wise.values, apogee], 1)

    cols = [('constant', ['constant']), ('gaia', gaia.columns), ('tmass', tmass.columns), ('wise', wise.columns), ('apogee', normed.flux.columns)]
    cols = pd.MultiIndex.from_tuples([(d, c) for d, cs in cols for c in cs])

    m = sp.zeros(len(cols))
    m[cols.get_level_values(0) == 'apogee'] = 1

    return X, m, cols

def check_grad(f, grad, b0, eps=1e-6, k=10):
    """The easiest way to check the gradient is with sp.optimize.check_grad, but that checks the grad 
    in every.single.coordinate, which takes forever. Fast way to do it is to pick a bunch of random 
    vectors instead"""
    sp.random.seed(20181111)
    for _ in range(k):
        db = sp.random.normal(size=len(b0))
        db = eps*db/(db**2).sum()**.5

        df = f(b0 + db) - f(b0)
        dfhat = grad(b0) @ db
        assert abs(df - dfhat)/df < 1e-3, 'Change in `f` and gradient-implied change in `f` were substantially different'

def solve(X, y, w, m, b0=None, lambd=30, check=False):

    def f(b, *args):   
        yhat = sp.exp(X @ b)
        return .5*w @ (y - yhat)**2 + lambd*m @ sp.fabs(b)
    
    def grad(b, *args):
        yhat = sp.exp(X @ b)
        # Fun fact: if you do `X.T @ v` here instead of `v @ X`, it's x10 slower
        return -(yhat * w * (y - yhat)) @ X + lambd*m*sp.sign(b)
    
    i = 0
    def callback(b):
        nonlocal i
        i = i + 1
        log.info(f'Step {i}: loss is {f(b):.1f}')

    #TODO: My instinct is that this should this be constant in l2 norm rather than l1?
    b0 = sp.full(len(m), 1e-3/len(m)) if b0 is None else b0

    if check:
        check_grad(f, grad, b0)

    #TODO: Evaluating the full hessian is infeasible, but is there any way we could generate the `hessp` arg?
    #TODO: Why does the original use BFGS-B rather than BFGS? There are no constraints here
    #TODO: Oh lord this is slow. Can we parallelize it anyhow? 
    # I actually don't know any parallel quasi-Newton methods, worth reading up on.
    # Expect this to take ~100 odd iterations to converge on the 'good' stars.
    result = sp.optimize.minimize(f, b0, 
                    method='BFGS', 
                    jac=grad, 
                    callback=callback,
                    options={'disp': True, 'maxiter': 1000})
    assert result.success, 'Optimizer failed'

    bstar = result.x
    return bstar

def plot(b, cols):
    b = pd.Series(b, cols)
    b.apogee.plot()
    pass

def training_catalog(catalog):
    cuts = {
        'finite_parallax': catalog.gaia.parallax < sp.inf,
        'multiobservation': catalog.gaia.visibility_periods_used >= 8,
        'low_error': catalog.gaia.parallax_error < .1,
        # This thresholds the goodness-of-fit of the astrometric solution to the observations made, along the scan direction
        'coryn': catalog.gaia.astrometric_chi2_al/sp.sqrt(catalog.gaia.astrometric_n_good_obs_al - 5) <= 35}
    return tools.cut(catalog, cuts)

def save(b):
    path = aws.s3.Path('alj.data/params/b')
    path.write_bytes(pickle.dumps(b))
    pass

def fit(catalog, normed):
    training = training_catalog(catalog)
    good = (training.gaia.parallax_over_error > 20)

    X, m, cols = design_matrix(training, normed)
    y = training.gaia.parallax.values + PARALLAX_OFFSET
    w = 1/training.gaia.parallax_error.values**2

    #TODO: Replace this 'good' initialization with an explicit prior. Which is all it is really. 
    # Gonna need a strooooong prior to overcome the `exp` in the loss. L2 won't cut it.
    b = solve(X[good].copy(), y[good], w[good], m)
    b = solve(X, y, w, m, b)

    #TODO: Propogate the errors. Physicists pay attention to the second moment, weird.
    # Xe = design_errors(training, normed)
    # ye = training.gaia.parallax_error
    