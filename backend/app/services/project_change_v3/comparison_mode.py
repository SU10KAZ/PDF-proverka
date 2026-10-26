"""Run-local algorithm selection, independent of model and reasoning depth."""
from contextlib import contextmanager
from contextvars import ContextVar

STANDARD = "standard"
SUBJECTS = "subjects"
MODES = (STANDARD, SUBJECTS)
_MODE = ContextVar("projectchange_comparison_mode", default=STANDARD)


def current():
    return _MODE.get()


@contextmanager
def selected(mode):
    if mode not in MODES:
        raise ValueError("comparison_mode must be standard or subjects")
    token = _MODE.set(mode)
    try:
        yield
    finally:
        _MODE.reset(token)


def provenance():
    if current() == STANDARD:
        return {"comparison_mode": STANDARD}
    from .subject_contracts import VERSION, CONTRACT_SHA256
    return {"comparison_mode": SUBJECTS, "subject_method_version": VERSION,
            "subject_contract_sha256": CONTRACT_SHA256}
