import sys
import logging
import matplotlib.pyplot as plt
plt.rcParams['figure.figsize'] = (12, 8)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

from tqdm import tqdm
from contextlib import contextmanager
import multiprocessing
import types
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future, _base, as_completed

log = logging.getLogger(__name__)

class SerialExecutor(_base.Executor):
    """An executor that runs things on the main process/thread - meaning stack traces are interpretable
    and the debugger works!
    """
    
    def __init__(self, *args, **kwargs):
        pass
    
    def submit(self, f, *args, **kwargs):
        future = Future()
        future.set_result(f(*args, **kwargs))
        return future

@contextmanager
def VariableExecutor(N=None, processes=True):
    """An executor that can be easily switched between serial, thread and parallel execution.

    If N=0, a serial executor will be used.
    """
    
    N = N or multiprocessing.cpu_count()
    
    if N == 0:
        executor = SerialExecutor
    elif processes:
        executor = ProcessPoolExecutor
    else:
        executor = ThreadPoolExecutor
    
    log.debug('Launching a {} with {} processes'.format(executor.__name__, N))    
    with executor(N) as pool:
        yield pool
        
@contextmanager
def parallel(f, progress=True, **kwargs):
    """Sugar for using the VariableExecutor. Call as
    
    with parallel(f) as g:
        ys = g.wait({x: g(x) for x in xs})

    and f'll be called in parallel on each x, and the results collected in a dictionary
    """

    with VariableExecutor(**kwargs) as pool:

        def reraise(f, futures={}):
            e = f.exception()
            if e:
                log.warning('Exception raised on "{}"'.format(futures[f]), exc_info=e)
                raise e
            return f.result()

        submitted = set()
        def submit(*args, **kwargs):
            fut = pool.submit(f, *args, **kwargs)
            submitted.add(fut)
            fut.add_done_callback(submitted.discard) # Try to avoid memory leak
            return fut
        
        def wait(c):
            # Recurse on list-likes
            if type(c) in (list, tuple, types.GeneratorType):
                ctor = list if isinstance(c, types.GeneratorType) else type(c)
                results = wait(dict(enumerate(c)))
                return ctor(results[k] for k in sorted(results))

            # Now can be sure we've got a dict-like
            futures = {fut: k for k, fut in c.items()}
            
            results = {}
            for fut in tqdm(as_completed(futures), total=len(c), disable=not progress):
                results[futures[fut]] = reraise(fut, futures)
                
            return results
        
        def cancel():
            while True:
                remaining = list(submitted)
                for fut in remaining:
                    fut.cancel()
                    submitted.discard(fut)
                if not remaining:
                    break

        try:
            submit.wait = wait
            yield submit
        finally:
            cancel()

def cut(catalog, cuts):
    cut = sp.all(sp.vstack(cuts.values()).data, 0)

    for k, c in cuts.items():
        log.info(f'{1 - c.mean():>3.0%} of the population is cut away by {k}')
    log.info(f'{cut.mean():>3.0%} stars remain')
    
    return catalog[cut]