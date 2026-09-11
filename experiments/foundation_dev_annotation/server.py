"""Local-only annotation HTTP service. No production/EVAL imports or writes."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import argparse
import hmac
import json
import logging
import re
import secrets
import time
from urllib.parse import urlsplit

from .packet import NAMESPACE, Packet, REASONS
from .store import DEFAULT_STATE, EXPORT_NAME, SCHEMA_VERSION, Rejected, Store, encoded

ASSETS = Path(__file__).parent
LOG = logging.getLogger(__name__)


class AnnotationServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, packet, store, port=8768):
        self.packet, self.store = packet, store
        self.csrf = secrets.token_urlsafe(32)
        super().__init__(("127.0.0.1", port), Handler)


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(15)
        self.request_started = time.monotonic()
        self.save_receipt = None

    def log_message(self, *_):
        pass

    def log_request(self, code="-", size="-"):
        path = urlsplit(self.path).path
        if path == "/api/answers" or path == "/api/state" or path.startswith("/api/submissions/"):
            receipt = self.save_receipt or {}
            LOG.info("http method=%s path=%s status=%s elapsed_ms=%d case=%s submission=%s revision=%s",
                     self.command, path, code, (time.monotonic() - self.request_started) * 1000,
                     receipt.get("case_id", "-"), receipt.get("submission_id", "-"), receipt.get("revision", "-"))

    def guard(self, write=False):
        origin = f"http://127.0.0.1:{self.server.server_port}"
        if self.client_address[0] != "127.0.0.1" or self.headers.get("Host") != origin.removeprefix("http://"):
            raise Rejected("Доступ разрешён только с этого компьютера", 403)
        if self.headers.get("Origin") not in (None, origin) or self.headers.get("Sec-Fetch-Site") == "cross-site":
            raise Rejected("Откройте локальную страницу разметки", 403)
        if write and (not hmac.compare_digest(self.headers.get("X-Wave1-Token", ""), self.server.csrf)
                      or self.headers.get("Content-Type") != "application/json"):
            raise Rejected("Обновите локальную сессию", 403)

    def send_headers(self, status, content_type, size, extra=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(size))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()

    def respond(self, data, status=200, content_type="application/json; charset=utf-8", extra=None):
        if not isinstance(data, bytes):
            data = encoded(data)
        self.send_headers(status, content_type, len(data), extra)
        if self.command != "HEAD":
            self.wfile.write(data)
            if self.save_receipt:
                LOG.info("save_response_sent case=%s submission=%s revision=%s",
                         self.save_receipt["case_id"], self.save_receipt["submission_id"], self.save_receipt["revision"])

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        try:
            self.guard()
            path = urlsplit(self.path).path
            packet, store = self.server.packet, self.server.store
            if path in ("/", "/app.js", "/style.css"):
                filename, mime = {"/": ("index.html", "text/html; charset=utf-8"),
                                  "/app.js": ("app.js", "text/javascript; charset=utf-8"),
                                  "/style.css": ("style.css", "text/css; charset=utf-8")}[path]
                return self.respond((ASSETS / filename).read_bytes(), content_type=mime)
            if path == "/api/bootstrap":
                return self.respond({"namespace": NAMESPACE, "packet_sha256": packet.sha256, "schema_version": SCHEMA_VERSION,
                                     "annotator": store.annotator, "cases": packet.presentations, "reasons": REASONS,
                                     "csrf": self.server.csrf, **store.state()})
            if path == "/api/state":
                return self.respond({"csrf": self.server.csrf, **store.state()})
            if path == "/api/export":
                return self.respond(store.export(), extra={"Content-Disposition": f'attachment; filename="{EXPORT_NAME}"'})
            if match := re.fullmatch(r"/api/submissions/([0-9a-f-]{36})", path):
                return self.respond({"record": store.submission(match[1]), "csrf": self.server.csrf, **store.state()})
            if match := re.fullmatch(r"/pdf/(src_[0-9a-f]{64})", path):
                source = packet.sources.get(match[1])
                if source is None:
                    raise Rejected("Документ не входит в этот набор", 404)
                return self.serve_pdf(source["path"])
            if match := re.fullmatch(r"/page/(src_[0-9a-f]{64})/([0-9]{1,5})\.png", path):
                return self.respond(packet.page_image(match[1], int(match[2])), content_type="image/png")
            raise Rejected("Страница не найдена", 404)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Rejected as error:
            self.respond({"error": str(error)}, error.status)
        except KeyError:
            self.respond({"error": "Страница PDF не найдена"}, 404)
        except Exception:
            LOG.exception("Read failed")
            self.respond({"error": "Не удалось загрузить данные. Попробуйте проверить сохранение позже"}, 500)

    def serve_pdf(self, path):
        # Exact allowlist lookup has already happened; no request path reaches open().
        size = path.stat().st_size
        start, end, status = 0, size - 1, 200
        extra = {"Accept-Ranges": "bytes", "Content-Disposition": 'inline; filename="source.pdf"'}
        if requested := self.headers.get("Range"):
            match = re.fullmatch(r"bytes=(\d*)-(\d*)", requested)
            if not match or not any(match.groups()):
                return self.respond(b"", 416, extra={"Content-Range": f"bytes */{size}"})
            if match[1]:
                start = int(match[1])
                end = min(int(match[2]), size - 1) if match[2] else size - 1
            else:
                start = max(0, size - int(match[2]))
            if start > end or start >= size:
                return self.respond(b"", 416, extra={"Content-Range": f"bytes */{size}"})
            status = 206
            extra["Content-Range"] = f"bytes {start}-{end}/{size}"
        length = end - start + 1
        self.send_headers(status, "application/pdf", length, extra)
        if self.command != "HEAD":
            with path.open("rb") as stream:
                stream.seek(start)
                while length:
                    chunk = stream.read(min(length, 128 * 1024))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    length -= len(chunk)

    def do_POST(self):
        try:
            self.guard(write=True)
            if self.path != "/api/answers":
                raise Rejected("Страница не найдена", 404)
            if self.headers.get("Transfer-Encoding"):
                raise Rejected("Неверный формат запроса")
            try:
                size = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                raise Rejected("Неверный размер запроса") from None
            if not 0 < size <= 16000:
                raise Rejected("Неверный размер запроса", 413)
            try:
                request = json.loads(self.rfile.read(size))
            except (ValueError, UnicodeError):
                raise Rejected("Не удалось прочитать ответ") from None
            record = self.server.store.save(request)
            self.save_receipt = record
            self.respond({"record": record, **self.server.store.state()})
        except (BrokenPipeError, ConnectionResetError):
            LOG.warning("save_response_lost committed=%s submission=%s", bool(self.save_receipt),
                        (self.save_receipt or {}).get("submission_id", "-"))
        except Rejected as error:
            self.respond({"error": str(error)}, error.status)
        except Exception:
            LOG.exception("Save failed; client must reconcile its submission ID")
            self.respond({"error": "Ответ пока не подтверждён. Мы сохранили его как черновик. Не отправляйте повторно."}, 500)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    args = parser.parse_args()
    packet = Packet()
    store = Store(packet, args.state_dir)
    with AnnotationServer(packet, store) as server:
        print("DEV annotation ready: http://127.0.0.1:8768/", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
