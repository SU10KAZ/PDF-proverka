"""Bound the complete crop download, including DNS, redirects, reads and retries.

A socket timeout alone only limits idle reads. A small disposable process lets
us stop even a server that keeps sending a few bytes before every idle timeout.
The child only downloads bytes; it never renders or writes crop artifacts.
"""
from __future__ import annotations

import json
from pathlib import Path
import ssl
import subprocess
import sys
import urllib.error
import urllib.request

_ERROR_PREFIX = 'CROP_DOWNLOAD_ERROR '


def download_with_deadline(req, timeout: float, *, budget_s: float,
                           fast_fail_availability: bool = False) -> bytes:
    payload = json.dumps({'url': req.full_url, 'headers': dict(req.headers),
                          'timeout': min(timeout, budget_s),
                          'fast_fail_availability': fast_fail_availability}).encode()
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'backend.app.pipeline.stages.crop_blocks.download_deadline'],
            input=payload, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=budget_s, cwd=str(Path(__file__).resolve().parents[5]),
        )
    except subprocess.TimeoutExpired as exc:
        # subprocess.run kills and reaps the downloader before returning.
        raise TimeoutError(f'Cloud crop download exceeded {budget_s:g}s total budget') from exc
    if result.returncode == 0:
        return result.stdout
    details = None
    for line in result.stderr.decode('utf-8', errors='replace').splitlines():
        if line.startswith(_ERROR_PREFIX):
            details = json.loads(line[len(_ERROR_PREFIX):])
    if details is None:
        raise RuntimeError(f'Crop downloader exited with code {result.returncode}')
    message = details['message']
    if details.get('http_status') is not None:
        raise urllib.error.HTTPError(req.full_url, details['http_status'], message, {}, None)
    if details.get('ssl_error'):
        raise ssl.SSLError(message)
    if details.get('availability'):
        raise TimeoutError(message)
    raise ValueError(message)


def _main() -> int:
    import contextlib
    from backend.app.pipeline.stages.crop_blocks.cloud_failover import availability_failure
    request = json.loads(sys.stdin.buffer.read())
    try:
        # Legacy imports can log configuration; reserve stdout for PDF bytes.
        with contextlib.redirect_stdout(sys.stderr):
            from backend.app.pipeline.stages.crop_blocks.blocks import _download_with_retry
            req = urllib.request.Request(request['url'], headers=request['headers'])
            data = _download_with_retry(
                req, request['timeout'],
                fast_fail_availability=request['fast_fail_availability'],
            )
        sys.stdout.buffer.write(data)
        return 0
    except Exception as exc:
        details = {'message': str(exc), 'availability': availability_failure(exc),
                   'http_status': exc.code if isinstance(exc, urllib.error.HTTPError) else None,
                   'ssl_error': isinstance(exc, ssl.SSLError)}
        print(_ERROR_PREFIX + json.dumps(details), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(_main())
