"""Read-only verification of the frozen human export and every source receipt."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import re
import sqlite3
import unicodedata

from experiments.foundation_dev_annotation.packet import Packet, sha
from experiments.foundation_dev_annotation.store import DEFAULT_STATE, encoded

ORIGINAL = DEFAULT_STATE / "DEV_HUMAN_TRUTH_WAVE1.json"
ORIGINAL_SHA = "5f14cd327b3c6458d6b9d56a3cd5c055c5234c9dcce382cbcdc35f7239c2659e"


def now():
    return datetime.now(timezone.utc).isoformat()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def source_lines(text):
    """Reconstruct only frozen page/block/line coordinates, without classification."""
    pages = defaultdict(list)
    page = block = None
    started, within = False, 0
    for number, line in enumerate(text.splitlines(), 1):
        if match := re.match(r"^## Page\s+(\d+)\s*$", line):
            page, block = int(match[1]), None
            continue
        if match := re.match(r"^### BLOCK #([^ ]+) \[([^]]+)\]:\s*(\S+)\s*$", line):
            if page is not None:
                block, started, within = match[3], False, 0
            continue
        if block is None or line.startswith(("> **Stamp:**", "> **Created:**", "> **Crop:**")):
            continue
        if not started and (line.startswith(">") or not line.strip()):
            continue
        started = True
        within += 1
        if line.strip():
            pages[page].append({"page": page, "block_id": block, "markdown_line": number,
                                "within_block_line": within,
                                "line_sha256": hashlib.sha256(line.encode()).hexdigest()})
    return [line for page in sorted(pages) for line in pages[page]]


def page_hashes(text):
    chunks = re.split(r"^## Page (\d+)[ \t]*$", text, flags=re.M)
    result = {}
    for number, body in zip(chunks[1::2], chunks[2::2]):
        lines = [x for x in body.splitlines() if x.strip() and not x.startswith(("### BLOCK", "> **", "<!--", "---"))]
        normalized = unicodedata.normalize("NFC", "\n".join(lines)).casefold().replace("ё", "е")
        normalized = re.sub(r"[*_`#]", "", normalized)
        normalized = " ".join(re.sub(r"[^\w\d]+", " ", normalized).split())
        if normalized:
            result[int(number)] = hashlib.sha256(normalized.encode()).hexdigest()
    return result


def audit():
    require(sha(ORIGINAL) == ORIGINAL_SHA, "Original frozen export SHA-256 mismatch")
    truth = json.loads(ORIGINAL.read_bytes())
    packet = Packet()  # Read-only source presentation; never invokes Foundation.
    require(truth["frozen"] is True and bool(truth["frozen_at"]), "Export is not frozen")
    require(truth["packet_sha256"] == packet.sha256, "Export packet hash mismatch")
    require(truth["packet_namespace"] == packet.packet["namespace"], "Packet namespace mismatch")
    cases, history = truth["cases"], truth["revision_history"]
    require(len(cases) == len({c["case_id"] for c in cases}) == 104, "Duplicate/missing human cases")
    require({c["case_id"] for c in cases} == set(packet.by_id), "Human case set mismatch")
    require(Counter(c["kind"] for c in cases) == {"SECTION": 52, "TABLE": 44, "OWNER": 8}, "Kind counts mismatch")
    require(Counter(c["human_answer"] for c in cases) == {"YES": 77, "NO": 27}, "Answer counts mismatch")
    require(truth["progress"] == {"total": 104, "answered": 104, "remaining": 0, "unsure": 0, "broken": 0}, "Progress mismatch")
    require(len(history) == len({r["submission_id"] for r in history}), "Duplicate submission")
    revisions = defaultdict(list)
    for r in history:
        revisions[r["case_id"]].append(r)
    require(set(revisions) == set(packet.by_id), "History case set mismatch")
    for c in cases:
        source = packet.by_id[c["case_id"]]
        require(c["source_anchors"] == source["anchors"] and c["document_version"] == source["document_version"]
                and c["kind"] == source["kind"] and c["packet_sha256"] == packet.sha256, "Case provenance mismatch")
        rows = revisions[c["case_id"]]
        require([r["revision"] for r in rows] == list(range(1, c["revision"] + 1)), "Non-contiguous revisions")
        require(all(c[k] == v for k, v in rows[-1].items()), "Latest revision mismatch")
        require(all(r["annotator"] == truth["annotator"] for r in rows), "Revision author mismatch")
        require(c["saved_at"] <= truth["frozen_at"] and c["schema_version"] == truth["schema_version"], "Revision metadata mismatch")
        require(c["case_state"] == "ANSWERED" and c["problem_reason"] is None and c["review_state"] is None, "Answer state mismatch")
        require(c["mapped_answer"] == packet.mapped_answer(c["case_id"], c["human_answer"]), "Human mapping mismatch")
    for name, owner in (("answers", False), ("ownership_answers", True)):
        require(truth[name] == {c["case_id"]: c["mapped_answer"] for c in cases if (c["kind"] == "OWNER") == owner}, "Answer map mismatch")
    require(truth["problem_cases"] == {}, "Unexpected broken cases")
    require(truth["automatic_controls"] == [{"case_id": c["case_id"], "source_anchors": c["anchors"],
            "control_expectation": c["control_expectation"]} for c in packet.controls], "Control provenance mismatch")
    # immutable=1 cannot create WAL/SHM files. The original service has frozen this DB.
    db_path = ORIGINAL.parent / "wave1.sqlite3"
    require(not Path(str(db_path) + "-wal").exists() or Path(str(db_path) + "-wal").stat().st_size == 0,
            "Original DB has pending WAL; require a consistent read-only snapshot")
    with sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True) as db:
        require(db.execute("PRAGMA integrity_check").fetchone()[0] == "ok", "Original database integrity error")
        require(db.execute("SELECT payload FROM freeze").fetchone()[0] == ORIGINAL.read_bytes(), "DB freeze differs from export")
        require([json.loads(r[0]) for r in db.execute("SELECT record FROM revisions ORDER BY rowid")] == history, "DB history mismatch")
        requests = list(db.execute("SELECT case_id,revision,submission_id,request,record FROM revisions ORDER BY rowid"))
        for case_id, revision, submission_id, raw_request, raw_record in requests:
            request, record = json.loads(raw_request), json.loads(raw_record)
            require(request["case_id"] == case_id == record["case_id"] and request["submission_id"] == submission_id == record["submission_id"], "DB submission mismatch")
            require(request["expected_revision"] == revision - 1 and request["human_answer"] == record["human_answer"], "DB request revision mismatch")
    documents = json.loads((packet.root / "dev_documents.json").read_text())
    receipts, coordinates, hashes, block_indexes = {}, {}, {}, {}
    for doc in documents:
        artifacts = doc["artifacts"]
        checks = [*artifacts.values(), *doc["source_refs"]["original_sources"]]
        pdf = artifacts["pdf"]
        if doc["document_version"] in {c["document_version"] for c in packet.packet["cases"]}:
            checks.append({**pdf, "path": str(packet.root / "dev_namespace/pdf" / (pdf["sha256"] + ".pdf"))})
        for item in checks:
            path = Path(item["path"])
            if str(path) not in receipts:
                actual = sha(path)
                require(actual == item["sha256"] and path.stat().st_size == item["bytes"], f"Source hash/size mismatch: {path}")
                receipts[str(path)] = actual
        text = Path(artifacts["work_md"]["path"]).read_text()
        version = doc["document_version"]
        identity = {"namespace": packet.packet["namespace"], "source_hashes": [s["sha256"] for s in doc["source_refs"]["original_sources"]]}
        require(hashlib.sha256(encoded(identity)).hexdigest() == version, "Document version identity mismatch")
        coordinates[version], hashes[version] = source_lines(text), page_hashes(text)
        blocks = json.loads(Path(artifacts["blocks"]["path"]).read_text())["blocks"]
        block_indexes[version] = {(int(b["page_index"]) + 1, b["block_id"]) for b in blocks}
    anchors = 0
    for c in packet.packet["cases"]:
        version = c["document_version"]
        doc = next(d for d in documents if d["document_version"] == version)
        pdf_source = packet.sources["src_" + doc["artifacts"]["pdf"]["sha256"]]
        for a in c["anchors"]:
            line = coordinates[version][a["line_id"]]
            require(all(a[k] == v for k, v in line.items()), "Anchor coordinate mismatch")
            require((a["page"], a["block_id"]) in block_indexes[version], "Anchor block missing")
            require(1 <= a["page"] <= pdf_source["pages"], "Physical PDF anchor page unavailable")
            anchors += 1
        require([hashes[version][p] for p in c["page_pair"]] == c["page_content_hashes"], "Page content hash mismatch")
    require(sha(ORIGINAL) == ORIGINAL_SHA, "Original changed during audit")
    report = {"status": "PASS", "checked_at": now(), "original_path": str(ORIGINAL), "original_sha256": ORIGINAL_SHA,
              "packet_sha256": packet.sha256, "human_cases": 104, "answers": 104,
              "kind_counts": dict(Counter(c["kind"] for c in cases)),
              "answer_counts": dict(Counter(c["human_answer"] for c in cases)),
              "unique_submissions": len(history), "duplicate_submissions": 0,
              "revision_counts": dict(Counter(str(c["revision"]) for c in cases)),
              "revision_consistency": True, "database_freeze_matches_export": True,
              "documents_verified": len(documents), "anchors_verified": anchors,
              "page_hash_pairs_verified": len(packet.packet["cases"]), "source_files_verified": len(receipts),
              "source_receipts": receipts, "foundation_predictions_used": False}
    return packet, truth, report
