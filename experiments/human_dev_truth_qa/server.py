"""Separate localhost QA service, with no route to original answers while blind."""
from http.server import ThreadingHTTPServer
from pathlib import Path
import argparse
import json
import logging
import re
import secrets
from urllib.parse import urlsplit

from experiments.foundation_dev_annotation.server import Handler as SourceHandler
from experiments.foundation_dev_annotation.store import Rejected
from .audit import audit
from .packet import QAPacket, STATE_DIR, NAMESPACE
from .store import Store, QA_NAME, FINAL_NAME

ASSETS = Path(__file__).parent


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, packet, store, port=8769):
        self.packet, self.store, self.csrf = packet, store, secrets.token_urlsafe(32)
        super().__init__(("127.0.0.1", port), Handler)


class Handler(SourceHandler):
    def do_GET(self):
        try:
            self.guard()
            path = urlsplit(self.path).path
            packet, store = self.server.packet, self.server.store
            assets = {"/": ("index.html", "text/html"), "/app.js": ("app.js", "text/javascript"), "/style.css": ("style.css", "text/css")}
            if path in assets:
                name, mime = assets[path]
                return self.respond((ASSETS / name).read_bytes(), content_type=mime + "; charset=utf-8")
            if path == "/api/bootstrap":
                return self.respond({"namespace": NAMESPACE, "packet_sha256": packet.sha256, "cases": packet.presentations,
                                     "csrf": self.server.csrf, **store.state()})
            if path == "/api/state":
                return self.respond({"csrf": self.server.csrf, **store.state()})
            if path in {"/api/export", "/api/final"}:
                final = path == "/api/final"
                return self.respond(store.export(final), extra={"Content-Disposition": f'attachment; filename="{FINAL_NAME if final else QA_NAME}"'})
            if path == "/api/comparison":
                state = store.state()
                if state["phase"] == "blind":
                    raise Rejected("Сначала завершите слепую перепроверку", 409)
                return self.respond({"stats": state["stats"], "disagreements": state["disagreements"]})
            if match := re.fullmatch(r"/pdf/(src_[0-9a-f]{64})", path):
                if match[1] not in packet.sources:
                    raise KeyError(match[1])
                return self.serve_pdf(packet.sources[match[1]]["path"])
            if match := re.fullmatch(r"/page/(src_[0-9a-f]{64})/(\d{1,5})\.png", path):
                return self.respond(packet.page_image(match[1], int(match[2])), content_type="image/png")
            if match := re.fullmatch(r"/text/(src_[0-9a-f]{64})/(\d{1,5})", path):
                sid, page = match[1], int(match[2])
                if sid not in packet.sources or not 1 <= page <= packet.sources[sid]["pages"]:
                    raise KeyError(sid)
                return self.respond({"lines": packet.page_text[sid].get(page, [])})
            raise Rejected("Страница не найдена", 404)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Rejected as error:
            self.respond({"error": str(error)}, error.status)
        except KeyError:
            self.respond({"error": "Страница PDF не найдена"}, 404)
        except Exception:
            logging.exception("QA read failed")
            self.respond({"error": "Не удалось загрузить данные"}, 500)

    def do_POST(self):
        try:
            self.guard(write=True)
            if self.path != "/api/answers":
                raise Rejected("Страница не найдена", 404)
            if self.headers.get("Transfer-Encoding"):
                raise Rejected("Неверный формат запроса")
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 16000:
                raise Rejected("Неверный размер запроса", 413)
            record = self.server.store.save(json.loads(self.rfile.read(size)))
            self.respond({"record": record, **self.server.store.state()})
        except (BrokenPipeError, ConnectionResetError):
            pass  # A retry with the same UUID returns the same committed event.
        except Rejected as error:
            self.respond({"error": str(error)}, error.status)
        except (ValueError, TypeError):
            self.respond({"error": "Не удалось проверить ответ или целостность исходных данных"}, 400)
        except Exception:
            logging.exception("QA save failed")
            self.respond({"error": "Сохранение не подтверждено. Повторите тот же запрос"}, 500)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=STATE_DIR)
    args = parser.parse_args()
    source, truth, report = audit()
    packet = QAPacket(source, truth)
    store = Store(packet, args.state_dir, report)
    with Server(packet, store) as server:
        print("Human QA ready: http://127.0.0.1:8769/", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
