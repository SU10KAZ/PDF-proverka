"""Durable append-only human revisions; one transaction per answer and freeze."""
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import getpass
import hashlib
import json
import os
import sqlite3
import tempfile
import uuid

from .packet import NAMESPACE, REASONS

SCHEMA_VERSION = "foundation-dev-human-wave1.v1"
DEFAULT_STATE = Path("/home/coder/auditmanager/corpus-audits/20260911_foundation_v3_simple_annotation") / NAMESPACE
EXPORT_NAME = "DEV_HUMAN_TRUTH_WAVE1.json"


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def atomic_write(path, data):
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix=".wave1-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class Rejected(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


class Store:
    def __init__(self, packet, directory=DEFAULT_STATE):
        self.packet, self.directory = packet, Path(directory).resolve()
        if self.directory.name != NAMESPACE:
            raise ValueError("State directory must use the isolated wave namespace")
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        config_path = self.directory / "local-session.json"
        if not config_path.exists():
            # Local OS identity is fixed here, never accepted from the browser.
            atomic_write(config_path, encoded({"namespace": NAMESPACE, "annotator": "local:" + getpass.getuser()}))
        config = json.loads(config_path.read_text())
        self.annotator = config["annotator"]
        if config["namespace"] != NAMESPACE or not isinstance(self.annotator, str) or not self.annotator.strip():
            raise ValueError("Invalid fixed local identity")
        self.path = self.directory / "wave1.sqlite3"
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS revisions (
                    case_id TEXT NOT NULL, revision INTEGER NOT NULL,
                    submission_id TEXT NOT NULL UNIQUE, request TEXT NOT NULL,
                    record TEXT NOT NULL, PRIMARY KEY (case_id, revision));
                CREATE TRIGGER IF NOT EXISTS revisions_no_update BEFORE UPDATE ON revisions
                    BEGIN SELECT RAISE(ABORT, 'append-only revisions'); END;
                CREATE TRIGGER IF NOT EXISTS revisions_no_delete BEFORE DELETE ON revisions
                    BEGIN SELECT RAISE(ABORT, 'append-only revisions'); END;
                CREATE TABLE IF NOT EXISTS freeze (singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload BLOB NOT NULL);
                CREATE TRIGGER IF NOT EXISTS freeze_no_update BEFORE UPDATE ON freeze
                    BEGIN SELECT RAISE(ABORT, 'immutable freeze'); END;
                CREATE TRIGGER IF NOT EXISTS freeze_no_delete BEFORE DELETE ON freeze
                    BEGIN SELECT RAISE(ABORT, 'immutable freeze'); END;
            """)
            expected = {"namespace": NAMESPACE, "packet_sha256": packet.sha256,
                        "schema_version": SCHEMA_VERSION, "annotator": self.annotator}
            for key, value in expected.items():
                db.execute("INSERT OR IGNORE INTO metadata VALUES (?, ?)", (key, value))
            if dict(db.execute("SELECT key, value FROM metadata")) != expected:
                raise ValueError("Registry identity, schema or packet mismatch")
        os.chmod(self.path, 0o600)
        self.publish_freeze()

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    def latest(self, db):
        return {case_id: json.loads(record) for case_id, record in db.execute("""
            SELECT r.case_id, r.record FROM revisions r JOIN
            (SELECT case_id, MAX(revision) AS revision FROM revisions GROUP BY case_id) latest
            USING(case_id, revision)""")}

    def progress(self, records):
        return {"answered": len(records), "total": len(self.packet.cases),
                "remaining": len(self.packet.cases) - len(records),
                "unsure": sum(r["human_answer"] == "UNSURE" for r in records.values()),
                "broken": sum(r["case_state"] == "BROKEN_CASE" for r in records.values())}

    def state(self):
        with self.connect() as db:
            db.execute("BEGIN")
            records = self.latest(db)
            frozen = bool(db.execute("SELECT 1 FROM freeze").fetchone())
        return {"records": records, "progress": self.progress(records), "frozen": frozen}

    def validate(self, request):
        required = {"case_id", "human_answer", "problem_reason", "note", "expected_revision", "submission_id", "namespace", "packet_sha256"}
        if not isinstance(request, dict) or set(request) != required:
            raise Rejected("Неверный формат ответа")
        if request["namespace"] != NAMESPACE or request["packet_sha256"] != self.packet.sha256:
            raise Rejected("Ответ относится к другому набору")
        if not isinstance(request["case_id"], str) or request["case_id"] not in self.packet.by_id:
            raise Rejected("Этот пример не входит в вопросы для человека")
        answer = request["human_answer"]
        if not isinstance(answer, str) or answer not in {"YES", "NO", "UNSURE", "BROKEN_CASE"}:
            raise Rejected("Выберите ответ")
        reason = request["problem_reason"]
        if answer == "BROKEN_CASE":
            if not isinstance(reason, str) or reason not in REASONS:
                raise Rejected("Укажите проблему с примером")
        elif reason is not None:
            raise Rejected("Причина проблемы не относится к смысловому ответу")
        if not isinstance(request["note"], str) or len(request["note"]) > 2000:
            raise Rejected("Комментарий слишком длинный")
        if type(request["expected_revision"]) is not int or request["expected_revision"] < 0:
            raise Rejected("Неверная версия ответа")
        try:
            if str(uuid.UUID(request["submission_id"])) != request["submission_id"]:
                raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise Rejected("Неверный идентификатор сохранения") from None

    def save(self, request):
        self.validate(request)
        canonical = encoded(request).decode()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute("SELECT request, record FROM revisions WHERE submission_id=?", (request["submission_id"],)).fetchone()
            if prior:
                if prior[0] != canonical:
                    raise Rejected("Этот запрос уже использован для другого ответа", 409)
                result = json.loads(prior[1])
            else:
                if db.execute("SELECT 1 FROM freeze").fetchone():
                    raise Rejected("Все ответы сохранены. Набор зафиксирован", 409)
                current = db.execute("SELECT MAX(revision) FROM revisions WHERE case_id=?", (request["case_id"],)).fetchone()[0] or 0
                if current != request["expected_revision"]:
                    raise Rejected("Ответ уже изменён в другом окне. Откройте сохранённый ответ перед исправлением", 409)
                answer = request["human_answer"]
                broken = answer == "BROKEN_CASE"
                result = {"case_id": request["case_id"], "human_answer": None if broken else answer,
                          "case_state": "BROKEN_CASE" if broken else "UNSURE" if answer == "UNSURE" else "ANSWERED",
                          "review_state": "NEEDS_REVIEW" if broken else None,
                          "problem_reason": request["problem_reason"], "note": request["note"],
                          "mapped_answer": self.packet.mapped_answer(request["case_id"], answer),
                          "annotator": self.annotator, "revision": current + 1, "submission_id": request["submission_id"],
                          "saved_at": datetime.now(timezone.utc).isoformat()}
                db.execute("INSERT INTO revisions VALUES (?, ?, ?, ?, ?)",
                           (request["case_id"], current + 1, request["submission_id"], canonical, encoded(result).decode()))
                records = self.latest(db)
                if len(records) == len(self.packet.cases):
                    db.execute("INSERT INTO freeze VALUES (1, ?)", (encoded(self.build_export(db, records, frozen=True)),))
        # A lost response, including a file publication failure, is safe to replay.
        self.publish_freeze()
        return result

    def submission(self, submission_id):
        with self.connect() as db:
            row = db.execute("SELECT record FROM revisions WHERE submission_id=?", (submission_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def build_export(self, db, records, frozen=False):
        cases, answers, owners, problems = [], {}, {}, {}
        for c in self.packet.cases:
            r = records.get(c["case_id"], {"case_id": c["case_id"], "human_answer": None, "case_state": "UNANSWERED",
                                         "review_state": None, "problem_reason": None, "note": "", "mapped_answer": None,
                                         "annotator": None, "revision": 0, "submission_id": None, "saved_at": None})
            cases.append({**r, "source_anchors": c["anchors"], "document_version": c["document_version"],
                          "kind": c["kind"], "packet_sha256": self.packet.sha256, "schema_version": SCHEMA_VERSION})
            (owners if c["kind"] == "OWNER" else answers)[c["case_id"]] = r["mapped_answer"]
            if r["case_state"] == "BROKEN_CASE":
                problems[c["case_id"]] = {"state": "BROKEN_CASE", "review_state": "NEEDS_REVIEW", "reason": r["problem_reason"]}
        return {"namespace": NAMESPACE, "packet_namespace": self.packet.packet["namespace"],
                "packet_sha256": self.packet.sha256, "schema_version": SCHEMA_VERSION, "annotator": self.annotator,
                "frozen": frozen, "frozen_at": datetime.now(timezone.utc).isoformat() if frozen else None,
                "progress": self.progress(records), "cases": cases, "answers": answers, "ownership_answers": owners,
                "problem_cases": problems,
                "automatic_controls": [{"case_id": c["case_id"], "source_anchors": c["anchors"],
                                         "control_expectation": c["control_expectation"]} for c in self.packet.controls],
                "revision_history": [json.loads(row[0]) for row in db.execute("SELECT record FROM revisions ORDER BY rowid")]}

    def export(self):
        with self.connect() as db:
            db.execute("BEGIN")
            frozen = db.execute("SELECT payload FROM freeze").fetchone()
            return frozen[0] if frozen else encoded(self.build_export(db, self.latest(db)))

    def publish_freeze(self):
        with self.connect() as db:
            frozen = db.execute("SELECT payload FROM freeze").fetchone()
        if frozen:
            path = self.directory / EXPORT_NAME
            if path.exists():
                if path.read_bytes() != frozen[0]:
                    raise ValueError("Frozen human export was modified")
            else:
                atomic_write(path, frozen[0])
            digest = hashlib.sha256(frozen[0]).hexdigest().encode() + b"\n"
            receipt = path.with_suffix(".json.sha256")
            if not receipt.exists():
                atomic_write(receipt, digest)
            elif receipt.read_bytes() != digest:
                raise ValueError("Frozen export receipt was modified")
