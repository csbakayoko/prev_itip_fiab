"""
Utilitaire de mesure de temps avec print [timing].

Aligné sur le style des prints [matching] / [late] : ▶ entrée, ↳ sortie + durée.

Usage :
    from modules._timing import timed

    with timed("load_cpt"):
        df = load_cpt_raw(spark, cfg)
"""

from contextlib import contextmanager
from time import perf_counter
from typing import Iterator


@contextmanager
def timed(label: str) -> Iterator[None]:
    """Imprime l'entrée d'un bloc et sa durée en sortie."""
    print(f"[timing] ▶ {label}")
    t0 = perf_counter()
    try:
        yield
    finally:
        elapsed = perf_counter() - t0
        print(f"[timing]   ↳ {label} : {elapsed:.2f}s")


def timed_fn(label: str = None):
    """Décorateur équivalent : `@timed_fn("ma_fonc")` ou `@timed_fn()` (utilise __name__)."""
    def decorator(fn):
        from functools import wraps

        name = label or fn.__name__

        @wraps(fn)
        def wrapper(*args, **kwargs):
            with timed(name):
                return fn(*args, **kwargs)
        return wrapper
    return decorator
