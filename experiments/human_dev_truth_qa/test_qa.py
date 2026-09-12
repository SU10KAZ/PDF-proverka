"""All synthetic responses use temporary registries, never the human QA session."""
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import hashlib
import json
import sqlite3
import unittest
import uuid

from experiments.foundation_dev_annotation.packet import sha
from experiments.foundation_dev_annotation.store import Rejected, encoded
from .audit import audit, ORIGINAL, ORIGINAL_SHA
from .packet import QAPacket, NAMESPACE
from .server import Server
from .store import Store, QA_NAME, FINAL_NAME


def request(packet, qid=None, answer="YES", stage="blind", revision=0):
    return {"namespace": NAMESPACE, "packet_sha256": packet.sha256, "stage": stage,
            "case_id": qid, "answer": answer, "expected_revision": revision,
            "note": "SYNTHETIC TEST ONLY", "submission_id": str(uuid.uuid4())}


class QATests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source, cls.truth, cls.report = audit()
        cls.packet = QAPacket(cls.source, cls.truth)

    def setUp(self):
        self.tmp = TemporaryDirectory(prefix="human-qa-synthetic-")
        self.directory = Path(self.tmp.name)
        self.store = Store(self.packet, self.directory, self.report)

    def tearDown(self):
        self.assertEqual(sha(ORIGINAL), ORIGINAL_SHA)
        self.tmp.cleanup()

    def complete(self, overrides=None):
        for qid, cid in self.packet.mapping.items():
            answer = (overrides or {}).get(qid, self.packet.original[cid]["human_answer"])
            self.store.save(request(self.packet, qid, answer))

    def test_selection_source_audit_and_blind_payload(self):
        self.assertEqual(self.report["status"], "PASS")
        self.assertEqual(self.report["anchors_verified"], 252)
        self.assertEqual(len(self.packet.mapping), 10)
        coverage = {(c["kind"], c["original_answer"]) for c in self.packet.selection["cases"]}
        self.assertTrue({("SECTION","YES"),("SECTION","NO"),("TABLE","YES"),("TABLE","NO"),("OWNER","NO")} <= coverage)
        raw = encoded(self.packet.public_packet).decode()
        for forbidden in ('"human_answer"','"original_answer"','"stratum"','"diagnostics"','"mapped_answer"','"foundation_edge"','"expected_answer"','"prediction"','"SAME"','"NEW"','"REVIEW"'):
            self.assertNotIn(forbidden, raw)
        for c in self.packet.presentations:
            self.assertEqual(set(c), {"case_id", "question", "document", "panels"})
            self.assertTrue(all(any(r["focus"] for r in p["context"]) for p in c["panels"]))

    def test_replay_stale_edit_and_restart_keep_blind(self):
        qid = next(iter(self.packet.mapping))
        req = request(self.packet, qid)
        first = self.store.save(req)
        self.assertEqual(self.store.save(req), first)
        changed = {**req, "answer": "NO"}
        with self.assertRaises(Rejected): self.store.save(changed)
        with self.assertRaises(Rejected): self.store.save(request(self.packet,qid,"NO"))
        old_journal = (self.directory/"QA_PROVENANCE.jsonl").read_bytes()
        self.store = Store(self.packet, self.directory, self.report)
        state = self.store.state()
        self.assertEqual(state["answered"], 1)
        self.assertNotIn("stats",state)
        self.assertNotIn("disagreements",state)
        self.assertEqual((self.directory/"QA_PROVENANCE.jsonl").read_bytes(),old_journal)
        self.assertFalse(json.loads(self.store.export())["frozen"])
        with self.store.connect() as db:
            for statement in ("DELETE FROM events", "UPDATE events SET digest='x'", "DELETE FROM metadata"):
                with self.assertRaises(sqlite3.IntegrityError): db.execute(statement)

    def test_blind_completion_disagreements_resolution_and_final_freeze(self):
        ids = list(self.packet.mapping)
        original = self.packet.original[self.packet.mapping[ids[0]]]["human_answer"]
        opposite = "NO" if original == "YES" else "YES"
        with self.assertRaises(Rejected): self.store.save(request(self.packet, None, "CONFIRM_FREEZE", "finalize"))
        with self.assertRaises(Rejected): self.store.save(request(self.packet, ids[0], "YES", "review"))
        self.complete({ids[0]:opposite, ids[1]:"UNSURE", ids[2]:"BROKEN"})
        state=self.store.state()
        self.assertEqual(state["phase"],"review")
        self.assertEqual(state["stats"], {"selected":10,"answered":10,"blind_matches":7,"blind_disagreements":3,"unsure":1,"broken":1})
        self.assertEqual({c["case_id"] for c in state["disagreements"]},set(ids[:3]))
        self.assertFalse(state["can_finalize"])
        qa_raw = self.store.export()
        self.assertTrue(json.loads(qa_raw)["frozen"])
        self.assertEqual(qa_raw,(self.directory/QA_NAME).read_bytes())
        self.assertFalse((self.directory/FINAL_NAME).exists())
        with self.assertRaises(Rejected): self.store.save(request(self.packet,ids[3],"NO","review"))
        self.store.save(request(self.packet,ids[0],"UNSURE","review"))
        with self.assertRaises(Rejected): self.store.save(request(self.packet,None,"CONFIRM_FREEZE","finalize"))
        self.store.save(request(self.packet,ids[0],opposite,"review",revision=1))
        for qid in ids[1:3]: self.store.save(request(self.packet,qid,"YES","review"))
        self.assertTrue(self.store.state()["can_finalize"])
        self.assertFalse((self.directory/FINAL_NAME).exists())
        req=request(self.packet,None,"CONFIRM_FREEZE","finalize")
        receipt=self.store.save(req)
        self.assertEqual(self.store.save(req),receipt)
        final=json.loads(self.store.export(final=True))
        self.assertEqual(len(final["cases"]),104)
        self.assertEqual(final["blind_qa_sha256"],hashlib.sha256(qa_raw).hexdigest())
        self.assertEqual(final["answers"][self.packet.mapping[ids[0]]],opposite)
        self.assertEqual(self.store.export(),qa_raw)
        previous=None
        for event in final["provenance"]:
            digest=event["event_sha256"]; body={k:v for k,v in event.items() if k!="event_sha256"}
            self.assertEqual(hashlib.sha256(encoded(body)).hexdigest(),digest)
            self.assertEqual(event["previous_sha256"],previous); previous=digest
        with self.assertRaises(Rejected): self.store.save(request(self.packet,ids[0],"NO","review",revision=2))
        self.store=Store(self.packet,self.directory,self.report)
        self.assertEqual(self.store.state()["phase"],"final")

    def test_all_matching_still_requires_human_freeze_and_publication_recovers(self):
        self.complete()
        self.assertEqual(self.store.state()["disagreements"],[])
        self.assertTrue(self.store.state()["can_finalize"])
        self.assertFalse((self.directory/FINAL_NAME).exists())
        (self.directory/QA_NAME).unlink()
        self.store=Store(self.packet,self.directory,self.report)
        self.assertTrue((self.directory/QA_NAME).exists())
        with self.store.connect() as db:
            with self.assertRaises(sqlite3.IntegrityError): db.execute("DELETE FROM freezes")
        raw=(self.directory/QA_NAME).read_bytes()
        (self.directory/QA_NAME).write_bytes(raw+b" ")
        with self.assertRaises(ValueError): Store(self.packet,self.directory,self.report)

    def test_http_blinding_sources_and_guard(self):
        server=Server(self.packet,self.store,port=0)
        thread=Thread(target=server.serve_forever,daemon=True);thread.start()
        base=f"http://127.0.0.1:{server.server_port}"
        try:
            def get(path,headers=None):
                with urlopen(Request(base+path,headers=headers or {})) as response:return response.status,response.read()
            _,raw=get("/api/bootstrap");boot=json.loads(raw)
            self.assertNotIn("disagreements",boot)
            self.assertNotIn("stats",boot)
            for path in ("/api/comparison","/api/final","/QA_SELECTION_PRIVATE.json","/DEV_HUMAN_TRUTH_WAVE1.json","/../QA_SELECTION_PRIVATE.json"):
                with self.assertRaises(HTTPError):get(path)
            p=boot["cases"][0]["panels"][0]
            status,raw=get(p["pdf_url"].split("#")[0],{"Range":"bytes=0-9"})
            self.assertEqual(status,206);self.assertTrue(raw.startswith(b"%PDF"))
            self.assertTrue(get(p["image_url"])[1].startswith(b"\x89PNG"))
            self.assertIn("lines",json.loads(get(f'/text/{p["source_id"]}/{p["page"]}')[1]))
            other=next(sid for sid in self.source.sources if sid not in self.packet.sources)
            with self.assertRaises(HTTPError):get("/pdf/"+other)
            with self.assertRaises(HTTPError):get("/api/bootstrap",{"Origin":"https://example.invalid"})
            req=Request(base+"/api/answers",data=encoded(request(self.packet,boot["cases"][0]["case_id"])),headers={"Content-Type":"application/json"})
            with self.assertRaises(HTTPError):urlopen(req)
            self.assertEqual(self.store.state()["answered"],0)
        finally:
            server.shutdown();server.server_close();thread.join()


if __name__ == "__main__":
    unittest.main()
