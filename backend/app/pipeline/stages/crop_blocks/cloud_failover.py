"""Job/source scoped crop failover. No network or rendering implementation here."""
from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import tempfile
import urllib.error
from uuid import uuid4


class SourceIdentityError(ValueError):
    pass


def availability_failure(exc: BaseException) -> bool:
    # HTTPError is also URLError/OSError: classify it first. Certificates and
    # malformed/corrupt payloads must never turn into a provider-wide outage.
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in {408, 502, 503, 504}
    if isinstance(exc, ssl.SSLError):
        return False
    if isinstance(exc, urllib.error.URLError):
        return isinstance(exc.reason, BaseException) and availability_failure(exc.reason)
    if isinstance(exc, (TimeoutError, ConnectionError, socket.gaierror)):
        return True
    return isinstance(exc, OSError) and exc.errno in {
        errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ENETDOWN,
        errno.ECONNREFUSED, errno.ECONNRESET, errno.ETIMEDOUT,
    }


def _stamp(path: Path) -> tuple:
    s = path.stat()
    return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns


def _sha(path: Path) -> str:
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


class LocalSource:
    """An explicitly resolved PDF/OCR pair; never searches for a substitute."""
    def __init__(self, pdf: Path, ocr: Path, *, expected_pdf: Path | None = None,
                 expected_sha256: str | None = None):
        self.pdf, self.ocr = pdf.resolve(), ocr.resolve()
        self.stamps = (_stamp(self.pdf), _stamp(self.ocr))
        pdf_sha = _sha(self.pdf)
        if expected_pdf is not None and _sha(expected_pdf) != pdf_sha:
            raise SourceIdentityError('Local PDF differs from the OCR-declared source')
        if expected_sha256 and expected_sha256.lower() != pdf_sha:
            raise SourceIdentityError('Local PDF hash differs from the declared SHA256')
        self.identity = hashlib.sha256(
            f'{self.pdf}\0{pdf_sha}\0{self.ocr}\0{_sha(self.ocr)}'.encode()
        ).hexdigest()
        self.verify()

    def verify(self):
        if self.stamps != (_stamp(self.pdf), _stamp(self.ocr)):
            raise SourceIdentityError('Local PDF/OCR changed during crop job')


class CropFailover:
    def __init__(self, job_id: str | None = None, state_root: Path | None = None):
        self.job_id = job_id or os.environ.get('AUDIT_CROP_JOB_ID') or uuid4().hex
        root = state_root or Path(tempfile.gettempdir()) / f'auditmanager-crop-{os.getuid()}'
        self.root = root / hashlib.sha256(self.job_id.encode()).hexdigest()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.metrics = dict.fromkeys((
            'cloud_crop_attempts', 'cloud_crop_failures',
            'cloud_crop_skipped_after_breaker', 'local_crop_attempts',
            'local_crop_successes', 'local_crop_failures',
        ), 0)

    def crop(self, *, source: LocalSource | None, provider: str,
             cloud, local):
        # Cross-process lock includes the first local fallback and OPEN write:
        # precrop and foreground (or 20 parallel workers) cannot probe together.
        key = hashlib.sha256(f'{provider}\0{source.identity if source else "no-local"}'.encode()).hexdigest()
        with (self.root / f'{key}.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            state_path = self.root / f'{key}.json'
            state = json.loads(state_path.read_text()) if state_path.exists() else {'open': False}
            if source:
                source.verify()
            skip = bool(cloud and source and state['open'])
            failure = None
            if cloud and not skip:
                self.metrics['cloud_crop_attempts'] += 1
                try:
                    return cloud(), 'cloud'
                except Exception as exc:
                    self.metrics['cloud_crop_failures'] += 1
                    # Preserve historical missing/expired URL fallback, but never
                    # classify it as provider unavailability. Integrity errors fail.
                    if not availability_failure(exc) and not (
                        isinstance(exc, urllib.error.HTTPError) and exc.code in {403, 404, 410}
                    ):
                        raise
                    failure = exc
            if skip:
                self.metrics['cloud_crop_skipped_after_breaker'] += 1
            if source is None:
                raise failure or FileNotFoundError('Exact local PDF unavailable for crop')
            source.verify()
            self.metrics['local_crop_attempts'] += 1
            try:
                result = local()
                source.verify()
            except Exception:
                self.metrics['local_crop_failures'] += 1
                raise
            self.metrics['local_crop_successes'] += 1
            if failure and availability_failure(failure) and not state['open']:
                state = {'open': True, 'job_id': self.job_id,
                         'source_identity': source.identity, 'provider': provider,
                         'reason': type(failure).__name__, 'fallback': 'LOCAL_PDF'}
                tmp = state_path.with_suffix(f'.{uuid4().hex}.tmp')
                tmp.write_text(json.dumps(state))
                os.replace(tmp, state_path)
                print('CLOUD_CROP_UNAVAILABLE ' + json.dumps({**state, 'circuit': 'OPEN'}), flush=True)
            return result, 'pdf_fallback'
