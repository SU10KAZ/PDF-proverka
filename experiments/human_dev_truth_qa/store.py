"""Append-only QA events; final freeze follows completed human decisions."""
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from threading import RLock
import getpass
import hashlib
import json
import os
import sqlite3
import uuid

from experiments.foundation_dev_annotation.packet import sha
from experiments.foundation_dev_annotation.store import atomic_write, encoded, Rejected
from .audit import ORIGINAL, ORIGINAL_SHA, now, require
from .packet import NAMESPACE

QA_NAME = "DEV_HUMAN_TRUTH_WAVE1_QA.json"
FINAL_NAME = "DEV_HUMAN_TRUTH_WAVE1_FINAL.json"
LABELS = {"YES", "NO", "UNSURE", "BROKEN"}


class Store:
    def __init__(self, packet, directory, report):
        self.packet, self.directory = packet, Path(directory).resolve()
        require(self.directory != ORIGINAL.parent.resolve(), "QA must use a separate directory")
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path, self.lock = self.directory / "qa.sqlite3", RLock()
        self.annotator = "local:" + getpass.getuser()
        self.identity = {"namespace": NAMESPACE, "original_sha256": ORIGINAL_SHA, "blind_packet_sha256": packet.sha256,
                         "selection_sha256": hashlib.sha256(encoded(packet.selection)).hexdigest(), "annotator": self.annotator}
        for name, payload in (("BLIND_QA_PACKET.json", packet.public_packet), ("QA_SELECTION_PRIVATE.json", packet.selection)):
            self.immutable_file(name, encoded(payload))
        if not (self.directory / "EXPORT_VERIFICATION.json").exists():
            self.immutable_file("EXPORT_VERIFICATION.json", encoded(report))
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS metadata (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, submission_id TEXT NOT NULL UNIQUE,
                    request TEXT NOT NULL, body TEXT NOT NULL, digest TEXT NOT NULL UNIQUE);
                CREATE TABLE IF NOT EXISTS freezes (name TEXT PRIMARY KEY, payload BLOB NOT NULL);
            """)
            for table in ("metadata", "events", "freezes"):
                for operation in ("UPDATE", "DELETE"):
                    db.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_no_{operation} BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'append-only QA'); END")
            db.execute("INSERT OR IGNORE INTO metadata VALUES (1, ?)", (encoded(self.identity).decode(),))
            require(json.loads(db.execute("SELECT body FROM metadata").fetchone()[0]) == self.identity, "QA registry identity mismatch")
            if not db.execute("SELECT 1 FROM events").fetchone():
                self.append(db, {"stage": "init", "submission_id": str(uuid.uuid4())}, self.identity)
            self.verify_chain(db)
        self.publish()

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    def immutable_file(self, name, raw):
        path = self.directory / name
        if path.exists():
            require(path.read_bytes() == raw, f"Immutable QA artifact changed: {name}")
        else:
            atomic_write(path, raw)
        receipt = hashlib.sha256(raw).hexdigest().encode() + b"\n"
        path = path.with_name(path.name + ".sha256")
        if path.exists():
            require(path.read_bytes() == receipt, "QA hash receipt changed")
        else:
            atomic_write(path, receipt)

    def verify_chain(self, db):
        previous = None
        for expected, (seq, submission, request, body, digest) in enumerate(db.execute("SELECT * FROM events ORDER BY seq"), 1):
            event = json.loads(body)
            require(seq == expected and event["seq"] == seq and event["previous_sha256"] == previous, "QA event chain broken")
            require(hashlib.sha256(body.encode()).hexdigest() == digest and event["submission_id"] == submission, "QA event hash mismatch")
            require(event["request_sha256"] == hashlib.sha256(request.encode()).hexdigest(), "QA request hash mismatch")
            previous = digest

    def append(self, db, request, payload):
        previous = db.execute("SELECT seq,digest FROM events ORDER BY seq DESC LIMIT 1").fetchone()
        canonical = encoded(request).decode()
        event = {"seq": previous[0] + 1 if previous else 1, "previous_sha256": previous[1] if previous else None,
                 "stage": request["stage"], "submission_id": request["submission_id"], "annotator": self.annotator,
                 "saved_at": now(), "request_sha256": hashlib.sha256(canonical.encode()).hexdigest(), **payload}
        body = encoded(event).decode()
        digest = hashlib.sha256(body.encode()).hexdigest()
        db.execute("INSERT INTO events VALUES (?,?,?,?,?)", (event["seq"], request["submission_id"], canonical, body, digest))
        return {**event, "event_sha256": digest}

    def events(self, db):
        return [{**json.loads(body), "event_sha256": digest} for body, digest in db.execute("SELECT body,digest FROM events ORDER BY seq")]

    def records(self, db, stage):
        return {e["case_id"]: e for e in self.events(db) if e["stage"] == stage}

    def comparisons(self, db):
        blind = self.records(db, "blind")
        return [{"case_id": qid, "original_case_id": cid, "original_answer": self.packet.original[cid]["human_answer"],
                 "blind_answer": blind[qid]["answer"],
                 "status": "CONSISTENT" if blind[qid]["answer"] == self.packet.original[cid]["human_answer"] else "NEEDS_HUMAN_REVIEW"}
                for qid, cid in self.packet.mapping.items() if qid in blind]

    def stats(self, db):
        comparison = self.comparisons(db)
        return {"selected": len(self.packet.mapping), "answered": len(comparison),
                "blind_matches": sum(c["status"] == "CONSISTENT" for c in comparison),
                "blind_disagreements": sum(c["status"] == "NEEDS_HUMAN_REVIEW" for c in comparison),
                "unsure": sum(c["blind_answer"] == "UNSURE" for c in comparison),
                "broken": sum(c["blind_answer"] == "BROKEN" for c in comparison)}

    def state(self):
        with self.connect() as db:
            db.execute("BEGIN")
            blind, review = self.records(db, "blind"), self.records(db, "review")
            complete = len(blind) == len(self.packet.mapping)
            final = bool(db.execute("SELECT 1 FROM freezes WHERE name=?", (FINAL_NAME,)).fetchone())
            # No original labels or match counts are disclosed before ALL answers.
            result = {"phase": "final" if final else "review" if complete else "blind",
                      "answered": len(blind), "total": len(self.packet.mapping), "records": blind,
                      "review_records": review}
            if complete:
                differences = [c for c in self.comparisons(db) if c["status"] != "CONSISTENT"]
                result.update(stats=self.stats(db), disagreements=differences)
            return result

    def qa_payload(self, db, frozen):
        blind = self.records(db, "blind")
        records = [{"case_id": qid, "original_case_id": cid, "source_anchors": self.packet.original[cid]["source_anchors"],
                    "document_version": self.packet.original[cid]["document_version"],
                    "answer": blind[qid]["answer"] if qid in blind else None,
                    "receipt": blind.get(qid)} for qid, cid in self.packet.mapping.items()]
        return {"schema": "human-dev-truth-qa.v1", **self.identity, "frozen": frozen,
                "frozen_at": max((e["saved_at"] for e in blind.values()), default=None) if frozen else None,
                "original_path": str(ORIGINAL), "cases": records,
                "provenance": [e for e in self.events(db) if e["stage"] in {"init", "blind"}]}

    def validate(self, request):
        required = {"stage", "case_id", "answer", "note", "expected_revision", "submission_id", "namespace", "packet_sha256"}
        if not isinstance(request, dict) or set(request) != required:
            raise Rejected("Неверный формат ответа")
        if request["namespace"] != NAMESPACE or request["packet_sha256"] != self.packet.sha256:
            raise Rejected("Ответ относится к другому набору")
        if request["stage"] not in {"blind", "review"}:
            raise Rejected("Неверный этап")
        if request["case_id"] not in self.packet.mapping or request["answer"] not in LABELS:
            raise Rejected("Выберите ответ для примера из этого набора")
        if type(request["expected_revision"]) is not int or request["expected_revision"] < 0 or not isinstance(request["note"], str) or len(request["note"]) > 2000:
            raise Rejected("Неверная ревизия или комментарий")
        try:
            if str(uuid.UUID(request["submission_id"])) != request["submission_id"]:
                raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise Rejected("Неверный идентификатор сохранения") from None

    def save(self, request):
        self.validate(request)
        require(sha(ORIGINAL) == ORIGINAL_SHA, "Original truth changed; QA stopped")
        with self.lock:
            with self.connect() as db:
                db.execute("BEGIN IMMEDIATE")
                prior = db.execute("SELECT request,body,digest FROM events WHERE submission_id=?", (request["submission_id"],)).fetchone()
                if prior:
                    if prior[0] != encoded(request).decode():
                        raise Rejected("Идентификатор уже использован для другого ответа", 409)
                    event = {**json.loads(prior[1]), "event_sha256": prior[2]}
                else:
                    if db.execute("SELECT 1 FROM freezes WHERE name=?", (FINAL_NAME,)).fetchone():
                        raise Rejected("Итоговый набор уже зафиксирован", 409)
                    blind = self.records(db, "blind")
                    complete = len(blind) == len(self.packet.mapping)
                    stage, case_id = request["stage"], request["case_id"]
                    if stage == "blind" and (complete or case_id in blind):
                        raise Rejected("Слепой ответ уже зафиксирован", 409)
                    if stage == "review" and not complete:
                        raise Rejected("Сначала завершите все слепые ответы", 409)
                    differences = {c["case_id"] for c in self.comparisons(db) if c["status"] != "CONSISTENT"}
                    if stage == "review" and case_id not in differences:
                        raise Rejected("Этот пример не требует решения по расхождению", 409)
                    current = self.records(db, stage).get(case_id, {}).get("revision", 0)
                    if request["expected_revision"] != current:
                        raise Rejected("Ответ изменён в другой вкладке. Обновите страницу", 409)
                    payload = {"case_id": case_id, "answer": request["answer"], "note": request["note"], "revision": current + 1}
                    if case_id is not None:
                        payload["original_case_id"] = self.packet.mapping[case_id]
                    if stage == "review":
                        qa_raw = db.execute("SELECT payload FROM freezes WHERE name=?", (QA_NAME,)).fetchone()[0]
                        payload["blind_qa_sha256"] = hashlib.sha256(qa_raw).hexdigest()
                    event = self.append(db, request, payload)
                    if stage == "blind" and len(self.records(db, "blind")) == len(self.packet.mapping):
                        db.execute("INSERT INTO freezes VALUES (?,?)", (QA_NAME, encoded(self.qa_payload(db, True))))
                    qa_row = db.execute("SELECT payload FROM freezes WHERE name=?", (QA_NAME,)).fetchone()
                    if qa_row:
                        differences = {c["case_id"] for c in self.comparisons(db) if c["status"] != "CONSISTENT"}
                        review = self.records(db, "review")
                        if all(review.get(cid, {}).get("answer") in {"YES", "NO"} for cid in differences):
                            freeze_request = {"stage": "freeze", "submission_id": str(uuid.uuid4()),
                                              "triggered_by_human_event_sha256": event["event_sha256"]}
                            freeze = self.append(db, freeze_request, {
                                "annotator": "system:human-dev-qa", "triggered_by_human_event_sha256": event["event_sha256"],
                                "blind_qa_sha256": hashlib.sha256(qa_row[0]).hexdigest(),
                                "reason": "ALL_DISAGREEMENTS_HUMAN_RESOLVED" if differences else "ALL_BLIND_ANSWERS_CONSISTENT"})
                            db.execute("INSERT INTO freezes VALUES (?,?)", (FINAL_NAME, encoded(self.final_payload(db, freeze))))
            self.publish()
        return event

    def final_payload(self, db, freeze_event):
        blind, review = self.records(db, "blind"), self.records(db, "review")
        reverse = {cid: qid for qid, cid in self.packet.mapping.items()}
        cases = []
        for original in self.packet.truth["cases"]:
            cid = original["case_id"]
            qid = reverse.get(cid)
            decision = review.get(qid)
            cases.append({"case_id": cid, "kind": original["kind"], "document_version": original["document_version"],
                          "source_anchors": deepcopy(original["source_anchors"]), "original_answer": original["human_answer"],
                          "blind_answer": blind[qid]["answer"] if qid else None,
                          "final_answer": decision["answer"] if decision else original["human_answer"],
                          "final_decision_sha256": decision["event_sha256"] if decision else None,
                          "basis": "HUMAN_RESOLUTION" if decision else "BLIND_CONSISTENT" if qid else "ORIGINAL_FROZEN_EXPORT"})
        return {"schema": "human-dev-truth-final.v1", **self.identity, "frozen": True, "frozen_at": freeze_event["saved_at"],
                "original_path": str(ORIGINAL), "blind_qa_path": str(self.directory / QA_NAME),
                "blind_qa_sha256": freeze_event["blind_qa_sha256"], "cases": cases,
                "answers": {c["case_id"]: c["final_answer"] for c in cases}, "stats": self.stats(db),
                "provenance": self.events(db), "freeze_event_sha256": freeze_event["event_sha256"],
                "human_decision_trigger_sha256": freeze_event["triggered_by_human_event_sha256"],
                "table_v3_development_gate": "HUMAN_QA_COMPLETE"}

    def export(self, final=False):
        with self.connect() as db:
            db.execute("BEGIN")
            row = db.execute("SELECT payload FROM freezes WHERE name=?", (FINAL_NAME if final else QA_NAME,)).fetchone()
            if final and not row:
                raise Rejected("Итоговый набор ещё не зафиксирован человеком", 409)
            return row[0] if row else encoded(self.qa_payload(db, False))

    def publish(self):
        with self.lock, self.connect() as db:
            db.execute("BEGIN")
            freezes = dict(db.execute("SELECT name,payload FROM freezes"))
            if QA_NAME in freezes:
                # A previous partial export is a recoverable view, never frozen truth.
                path = self.directory / QA_NAME
                if path.exists() and not json.loads(path.read_bytes())["frozen"]:
                    atomic_write(path, freezes[QA_NAME])
                self.immutable_file(QA_NAME, freezes[QA_NAME])
            else:
                atomic_write(self.directory / QA_NAME, encoded(self.qa_payload(db, False)))
            if FINAL_NAME in freezes:
                self.immutable_file(FINAL_NAME, freezes[FINAL_NAME])
            if QA_NAME in freezes:
                atomic_write(self.directory / "QA_COMPARISON.json", encoded({"stats": self.stats(db), "cases": self.comparisons(db),
                            "human_decisions": self.records(db, "review"), "final_frozen": FINAL_NAME in freezes}))
            raw = b"".join(encoded(e) for e in self.events(db))
            journal = self.directory / "QA_PROVENANCE.jsonl"
            existing = journal.read_bytes() if journal.exists() else b""
            require(raw.startswith(existing), "Published QA provenance is not an event prefix")
            if raw != existing:
                with journal.open("ab") as stream:
                    stream.write(raw[len(existing):])
                    stream.flush()
                    os.fsync(stream.fileno())
