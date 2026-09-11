"""Synthetic answers in disposable registries only; frozen inputs are read-only."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import json
import sqlite3
import unittest
import uuid

from .packet import FROZEN_ROOT, NAMESPACE, PACKET_SHA256, REASONS, Packet, sha
from .server import AnnotationServer
from .store import EXPORT_NAME, Rejected, Store


def answer(packet, case=None, value="YES", revision=0, **changes):
    return {"case_id": (case or packet.cases[0])["case_id"], "human_answer": value,
            "problem_reason": None, "note": "synthetic test", "expected_revision": revision,
            "submission_id": str(uuid.uuid4()), "namespace": NAMESPACE, "packet_sha256": packet.sha256, **changes}


class AnnotationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.packet = Packet()

    def setUp(self):
        self.tmp = TemporaryDirectory(prefix="wave1-synthetic-")
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name) / NAMESPACE
        self.store = Store(self.packet, self.directory)

    def test_section_table_owner_yes_no_unsure_mapping(self):
        for kind in ("SECTION", "TABLE", "OWNER"):
            c = next(c for c in self.packet.cases if c["kind"] == kind)
            expected = ["OWNER_FOLLOWING_HEADING", "OWNER_PRECEDING_SECTION", "UNSURE"] if kind == "OWNER" else ["SAME", "NEW", "UNSURE"]
            for revision, (value, mapped) in enumerate(zip(("YES", "NO", "UNSURE"), expected)):
                with self.subTest(kind=kind, value=value):
                    record = self.store.save(answer(self.packet, c, value, revision))
                    self.assertEqual(record["human_answer"], value)
                    self.assertEqual(record["mapped_answer"], mapped)
                    self.assertEqual(record["revision"], revision + 1)
                    self.assertIsNone(record["review_state"])
        self.assertEqual(self.store.state()["progress"]["unsure"], 3)

    def test_broken_case_is_never_binary_truth_and_reasons_are_validated(self):
        for revision, reason in enumerate(REASONS):
            self.store.save(answer(self.packet, value="BROKEN_CASE", revision=revision, problem_reason=reason))
        data = json.loads(self.store.export())
        first = data["cases"][0]
        self.assertIsNone(first["human_answer"])
        self.assertIsNone(data["answers"][first["case_id"]])
        self.assertEqual(first["case_state"], "BROKEN_CASE")
        self.assertEqual(first["review_state"], "NEEDS_REVIEW")
        self.assertEqual(data["progress"]["broken"], 1)
        self.assertEqual(data["progress"]["unsure"], 0)
        for request in (answer(self.packet, value="BROKEN_CASE"), answer(self.packet, problem_reason="OTHER")):
            with self.assertRaises(Rejected):
                self.store.save(request)

    def test_restart_resume_append_only_history_and_stale_revision(self):
        request = answer(self.packet)
        self.store.save(request)
        reopened = Store(self.packet, self.directory)
        reopened.save(answer(self.packet, value="NO", revision=1))
        self.assertEqual(reopened.state()["progress"]["answered"], 1)
        self.assertEqual(len(json.loads(reopened.export())["revision_history"]), 2)
        self.assertEqual(reopened.submission(request["submission_id"])["revision"], 1)
        with self.assertRaises(Rejected):
            reopened.save(answer(self.packet, value="UNSURE"))
        for statement in ("UPDATE revisions SET revision=99", "DELETE FROM revisions"):
            with self.assertRaises(sqlite3.IntegrityError), reopened.connect() as db:
                db.execute(statement)

    def test_concurrent_duplicate_submit_and_id_body_conflict(self):
        request = answer(self.packet)
        with ThreadPoolExecutor(max_workers=8) as pool:
            records = list(pool.map(self.store.save, [request] * 8))
        self.assertTrue(all(r == records[0] for r in records))
        self.assertEqual(len(json.loads(self.store.export())["revision_history"]), 1)
        with self.assertRaises(Rejected):
            self.store.save({**request, "human_answer": "NO"})

    def test_concurrent_distinct_submissions_do_not_overwrite(self):
        def save(request):
            try:
                self.store.save(request)
                return "saved"
            except Rejected:
                return "conflict"
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(save, [answer(self.packet), answer(self.packet, value="NO")]))
        self.assertCountEqual(outcomes, ["saved", "conflict"])

    def test_author_namespace_packet_and_controls_cannot_be_injected(self):
        for changes in ({"annotator": "forged"}, {"author": "forged"}, {"namespace": "EVALUATION_TRUTH"},
                        {"packet_sha256": "wrong"}, {"case_id": self.packet.controls[0]["case_id"]},
                        {"human_answer": "SAME"}, {"expected_revision": True}):
            with self.subTest(changes=changes), self.assertRaises(Rejected):
                self.store.save(answer(self.packet, **changes))
        saved = self.store.save(answer(self.packet))
        self.assertEqual(saved["annotator"], self.store.annotator)
        config = self.directory / "local-session.json"
        config.write_text(json.dumps({"namespace": NAMESPACE, "annotator": "changed-author"}))
        with self.assertRaises(ValueError):
            Store(self.packet, self.directory)

    def test_atomic_last_answer_freeze_export_and_stop(self):
        for i, c in enumerate(self.packet.cases[:-1]):
            self.store.save(answer(self.packet, c, "UNSURE" if i == 0 else "YES"))
        final_request = answer(self.packet, self.packet.cases[-1], "BROKEN_CASE", problem_reason="MISSING_FRAGMENT")
        with patch.object(self.store, "build_export", side_effect=OSError("synthetic disk failure")):
            with self.assertRaises(OSError):
                self.store.save(final_request)
        self.assertEqual(self.store.state()["progress"]["answered"], 103)
        self.assertIsNone(self.store.submission(final_request["submission_id"]))
        # Commit succeeds, but publication/response fails: a restart repairs export.
        with patch.object(self.store, "publish_freeze", side_effect=OSError("synthetic publication failure")):
            with self.assertRaises(OSError):
                self.store.save(final_request)
        restarted = Store(self.packet, self.directory)
        frozen = restarted.export()
        payload = json.loads(frozen)
        self.assertTrue(payload["frozen"])
        self.assertEqual(len(payload["cases"]), 104)
        self.assertEqual(len(payload["automatic_controls"]), 22)
        self.assertEqual(payload["progress"], {"answered": 104, "total": 104, "remaining": 0, "unsure": 1, "broken": 1})
        self.assertEqual((self.directory / EXPORT_NAME).read_bytes(), frozen)
        self.assertEqual((self.directory / (EXPORT_NAME + ".sha256")).read_text().strip(), sha(self.directory / EXPORT_NAME))
        for c, record in zip(self.packet.cases, payload["cases"]):
            self.assertEqual(record["source_anchors"], c["anchors"])
            self.assertEqual(record["packet_sha256"], PACKET_SHA256)
        restarted.save(final_request)  # Lost ACK after freeze remains idempotent.
        with self.assertRaises(Rejected):
            restarted.save(answer(self.packet, value="NO", revision=1))
        self.assertEqual(restarted.export(), frozen)

    def test_empty_export_is_complete_unanswered_template_with_separate_controls(self):
        exported = json.loads(self.store.export())
        self.assertEqual(len(exported["cases"]), 104)
        self.assertEqual(len(exported["automatic_controls"]), 22)
        self.assertTrue(all(c["case_state"] == "UNANSWERED" for c in exported["cases"]))
        self.assertEqual(exported["revision_history"], [])

    def test_frozen_packet_source_presentation_and_eval_proof_unchanged(self):
        self.assertEqual(sha(FROZEN_ROOT / "reports/FIRST_WAVE_DEV_PACKET.json"), PACKET_SHA256)
        separation = json.loads((FROZEN_ROOT / "reports/DEV_EVAL_SEPARATION.json").read_text())
        self.assertTrue(separation["pass"])
        for name in ("version_overlap", "document_code_overlap", "source_hash_overlap", "page_hash_overlap"):
            self.assertEqual(separation[name], [])
        for receipt in separation["proof_inputs"]:
            self.assertEqual(sha(receipt["path"]), receipt["sha256"])
        for original, display in zip(self.packet.cases, self.packet.presentations):
            self.assertEqual(original["case_id"], display["case_id"])
            self.assertNotIn("prediction", display)
            for a, panel in zip(original["anchors"], display["panels"]):
                self.assertEqual(panel["page"], a["page"])
                self.assertTrue(panel["pdf_url"].endswith(f"#page={a['page']}"))
                self.assertEqual(sum(row["focus"] for row in panel["context"]), 1)
                self.assertFalse(any("### BLOCK" in row["text"] or "> **" in row["text"] for row in panel["full_context"]))

    def serve(self):
        server = AnnotationServer(self.packet, self.store, port=0)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def close():
            server.shutdown(); server.server_close(); thread.join()
        self.addCleanup(close)
        return server, f"http://127.0.0.1:{server.server_port}"

    def test_http_pdf_page_jump_ranges_allowlist_and_no_path_access(self):
        _, base = self.serve()
        presentation = json.load(urlopen(base + "/api/bootstrap"))["cases"][0]
        panel = presentation["panels"][0]
        pdf = panel["pdf_url"].split("#")[0]
        with urlopen(Request(base + pdf, headers={"Range": "bytes=0-15"})) as response:
            self.assertEqual(response.status, 206)
            self.assertTrue(response.read().startswith(b"%PDF-"))
            self.assertIn("bytes 0-15/", response.headers["Content-Range"])
        with urlopen(base + panel["image_url"]) as response:
            self.assertEqual(response.headers["Content-Type"], "image/png")
            self.assertTrue(response.read().startswith(b"\x89PNG"))
        for route in ("/pdf/../../etc/passwd", "/pdf/%2e%2e/etc/passwd", "/pdf/src_" + "0" * 64,
                      "/local-session.json", "/wave1.sqlite3", "/cases.json", "/api/history", panel["image_url"].rsplit("/", 1)[0] + "/0.png"):
            with self.subTest(route=route), self.assertRaises(HTTPError) as error:
                urlopen(base + route)
            self.assertEqual(error.exception.code, 404)

    def test_http_fixed_identity_cross_origin_host_csrf_and_lost_response(self):
        server, base = self.serve()
        request = answer(self.packet)
        raw = json.dumps(request).encode()
        good = {"Content-Type": "application/json", "X-Wave1-Token": server.csrf}
        for headers in ({"Content-Type": "application/json"}, {**good, "Origin": "http://evil.example"},
                        {**good, "Host": "evil.example"}, {**good, "Sec-Fetch-Site": "cross-site"}):
            with self.assertRaises(HTTPError) as error:
                urlopen(Request(base + "/api/answers", data=raw, headers=headers))
            self.assertEqual(error.exception.code, 403)
        urlopen(Request(base + "/api/answers", data=raw, headers=good)).close()  # Discard ACK.
        receipt = json.load(urlopen(base + "/api/submissions/" + request["submission_id"]))
        self.assertEqual(receipt["record"]["revision"], 1)
        replay = json.load(urlopen(Request(base + "/api/answers", data=raw, headers=good)))
        self.assertEqual(replay["progress"]["answered"], 1)
        self.assertEqual(len(json.loads(self.store.export())["revision_history"]), 1)


if __name__ == "__main__":
    unittest.main()
