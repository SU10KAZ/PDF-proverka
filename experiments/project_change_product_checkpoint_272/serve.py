"""Local-only static preview. Serve an explicit release allowlist, never the corpus."""
import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        name=unquote(urlsplit(self.path).path).lstrip('/') or 'index.html'
        target=(Path(self.directory)/name).resolve()
        root=Path(self.directory).resolve()
        if not target.is_relative_to(root) or not target.is_file() or name not in self.server.allowed:
            self.send_error(404);return
        super().do_GET()
    def do_HEAD(self):
        name=unquote(urlsplit(self.path).path).lstrip('/') or 'index.html'
        if name not in self.server.allowed:self.send_error(404);return
        super().do_HEAD()
    def end_headers(self):
        self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        super().end_headers()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--directory',required=True);p.add_argument('--port',type=int,default=8774);a=p.parse_args()
    root=Path(a.directory).resolve()
    server=ThreadingHTTPServer(('127.0.0.1',a.port),partial(Handler,directory=str(root)))
    server.allowed={str(f.relative_to(root)) for f in root.rglob('*') if f.is_file()}
    # Pair B assets may be published after a successful live run: restart the server then.
    print(f'Preview http://127.0.0.1:{a.port}',flush=True);server.serve_forever()
