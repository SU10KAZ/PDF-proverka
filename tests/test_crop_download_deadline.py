"""Exercise the real disposable downloader against a local HTTP server only."""
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
import urllib.error
import urllib.request

import pytest

from backend.app.pipeline.stages.crop_blocks.download_deadline import download_with_deadline


@contextlib.contextmanager
def server(mode):
    disconnected = threading.Event()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_GET(self):
            if mode in ('404', '503'):
                self.send_error(int(mode))
                return
            if mode == 'redirect':
                time.sleep(0.25)
                hop = int(self.path.strip('/')) if self.path.strip('/').isdigit() else 0
                self.send_response(302)
                self.send_header('Location', f'/{hop + 1}')
                self.end_headers()
                return
            self.send_response(200)
            self.send_header('Content-Length', str(100000 if mode == 'trickle' else 9))
            self.end_headers()
            try:
                if mode == 'trickle':
                    while True:
                        self.wfile.write(b'x')
                        self.wfile.flush()
                        time.sleep(0.03)
                else:
                    self.wfile.write(b'%PDF-test')
            except (BrokenPipeError, ConnectionResetError):
                disconnected.set()
    http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    try:
        yield urllib.request.Request(f'http://127.0.0.1:{http.server_port}/pdf'), disconnected
    finally:
        http.shutdown()
        http.server_close()
        thread.join(timeout=2)


def test_fast_download_returns_cloud_bytes():
    with server('fast') as (req, _):
        assert download_with_deadline(req, 30, budget_s=5) == b'%PDF-test'


def test_trickling_body_cannot_reset_total_deadline():
    with server('trickle') as (req, disconnected):
        start = time.monotonic()
        with pytest.raises(TimeoutError, match='total budget'):
            download_with_deadline(req, 30, budget_s=1, fast_fail_availability=True)
        assert time.monotonic() - start < 2
        # The downloader is killed, not left in a background thread/request.
        assert disconnected.wait(1)


def test_http_error_preserves_fallback_status():
    with server('404') as (req, _):
        with pytest.raises(urllib.error.HTTPError) as exc:
            download_with_deadline(req, 30, budget_s=5)
        assert exc.value.code == 404


def test_redirects_share_the_total_deadline():
    with server('redirect') as (req, _):
        start = time.monotonic()
        with pytest.raises(TimeoutError, match='total budget'):
            download_with_deadline(req, 30, budget_s=1)
        assert time.monotonic() - start < 2


def test_retries_and_backoff_share_the_total_deadline():
    with server('503') as (req, _):
        start = time.monotonic()
        with pytest.raises(TimeoutError, match='total budget'):
            download_with_deadline(req, 30, budget_s=1)
        assert time.monotonic() - start < 2
