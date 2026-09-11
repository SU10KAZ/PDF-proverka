import itertools
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from . import frozen_v1 as v1
from .boundary import BoundaryDecision
from .ledger import LineLedger
from .materialize import materialize_document
from .models import FirstRowKind, first_row_model
from .page_model import PageModel
from .scorer import AnchorResolver, freeze_predictions, score


class FoundationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def document(self, bodies, raw_extra=()):
        md = self.root / "document.md"
        md.write_text("\n".join(f"## Page {n}\n### BLOCK #1 [text]: b{n}\n{body}\n" for n, body in enumerate(bodies, 1)))
        raw = {"pages": [{"page_index": n, "width_px": 100, "height_px": 140} for n in range(len(bodies))],
               "blocks": [{"page_index": n, "block_type": "text", "block_id": f"b{n+1}", "coords_norm": [0, 0, 1, 1]}
                          for n in range(len(bodies))] + list(raw_extra)}
        blocks = self.root / "blocks.json"
        blocks.write_text(json.dumps(raw))
        return {"document_version": "synthetic-version", "document_code": "synthetic", "version_id": "v1", "source_refs": {},
                "artifacts": {k: {"path": str(p), "sha256": v1.file_sha(p)} for k, p in (("work_md", md), ("blocks", blocks))}}

    def test_nonempty_unique_and_raw_only_blocks(self):
        doc = self.document(["#### Intro\n\nBody\n  \nBody"], [{"page_index": 0, "block_id": "graphic", "block_type": "image"}])
        ledger, raw = LineLedger.read(doc)
        self.assertEqual(len(ledger.lines), 3)
        self.assertEqual(len(ledger.blocks), 2)
        ledger.claim(0, 0)
        with self.assertRaises(ValueError):
            ledger.claim(0, 1)
        result = materialize_document(doc)
        self.assertEqual(result["quality"]["duplicate_semantic_lines"], 0)
        self.assertEqual(result["quality"]["lost_source_blocks"], 0)
        self.assertGreater(result["quality"]["empty_lines"], 0)

    def test_v1_properties_computed_once_and_sheet_contract(self):
        doc = self.document(["#### Intro\nBody", "| A | B |\n| -- | -- |\n| a | b |", "ПРОЕКТНАЯ ДОКУМЕНТАЦИЯ\nТом 1", "План этажа\nContent"])
        path = Path(doc["artifacts"]["blocks"]["path"])
        raw = json.loads(path.read_text())
        raw["pages"][3].update(width_px=200, height_px=100)
        raw["blocks"].append({"page_index": 3, "block_id": "stamp", "block_type": "stamp", "coords_norm": [0, 0, 0.2, 0.2]})
        path.write_text(json.dumps(raw))
        ledger, raw = LineLedger.read(doc)
        from . import page_model
        with patch.object(page_model, "front_matter", wraps=page_model.front_matter) as called:
            model = PageModel(ledger, raw)
            self.assertEqual(called.call_count, len(ledger.pages))
        for n, p in ledger.pages.items():
            ev = v1.page_evidence(p, raw["pages"][n-1], raw["blocks"], ledger.pages)
            self.assertEqual(model.pages[n]["evidence"], ev)
            self.assertEqual(model.pages[n]["classification"], v1.classify_evidence(ev))
        baseline = v1.materialize_document(doc)["comparison_units"]
        sheets = model.sheets(ledger)
        self.assertEqual(sheets["units"], [u for u in baseline["units"] if u["unit_type"] == "SHEET"])
        self.assertEqual(sheets["page_routing"], [u for u in baseline["page_routing"] if u["unit_type"] == "SHEET"])

    def test_never_calls_nested_v1(self):
        doc = self.document(["#### Intro\nBody"])
        with patch.object(v1, "materialize_document", side_effect=AssertionError("Nested V1")):
            materialize_document(doc)

    def test_caption_heading_conflict_preserves_heading_and_review(self):
        doc = self.document(["#### Intro\nBody\n#### Таблица 1 Результаты\n| A | B |\n| -- | -- |\n| 1 | 2 |"])
        result = materialize_document(doc)
        self.assertEqual(result["quality"]["caption_heading_conflicts"], 1)
        d = next(d for d in result["decisions"]["decisions"] if d["conflict"])
        self.assertEqual((d["decision"], d["basis"]), ("REVIEW", "DEFAULT"))
        i = d["right_anchor"]
        self.assertIn(i, result["semantics"]["headings"])
        owner = result["ledger"]["columns"]["owner"][i]
        self.assertEqual(result["semantics"]["units"][owner]["kind"], "SECTION_FRAGMENT")
        self.assertEqual(result["semantics"]["units"][owner]["status"], "REVIEW")

    def test_caption_local_distance_and_single_point(self):
        for gap in range(4):
            doc = self.document(["#### Intro\nBody\nТаблица 1\n" + "line\n" * gap + "| A | B |\n| -- | -- |\n| 1 | 2 |"])
            result = materialize_document(doc)
            self.assertEqual(bool(result["semantics"]["captions"]), gap <= 2)
            if gap <= 2:
                i = next(iter(result["semantics"]["captions"]))
                owner = result["ledger"]["columns"]["owner"][i]
                self.assertEqual(result["semantics"]["units"][owner]["kind"], "TABLE_SEGMENT")

    def test_distant_lexical_heading_is_strong(self):
        doc = self.document(["#### Таблица результатов\nIntroduction\nAnother paragraph\nMore text\n| A | B |"])
        result = materialize_document(doc)
        self.assertFalse(result["semantics"]["captions"])
        self.assertEqual(result["decisions"]["decisions"][0]["decision"], "NEW")

    def test_furniture_transparent_and_page_change_not_new(self):
        doc = self.document(["#### Intro\nText here\n1", "2\ncontinues here\nlast paragraph"])
        result = materialize_document(doc)
        ds = result["decisions"]["decisions"]
        self.assertEqual(ds[-1]["decision"], "SAME")
        self.assertEqual(result["ledger"]["columns"]["kind"].count("FURNITURE"), 2)
        self.assertEqual(result["ledger"]["columns"]["owner"][1], result["ledger"]["columns"]["owner"][4])

    def test_heading_candidates_and_repeats_abstain(self):
        for heading in ("NEW HEADING", "**Unnumbered title**", "1.2 Plain title", "#### 1. Intro"):
            doc = self.document(["#### 1. Intro\nBody", heading + "\nContinuation"])
            result = materialize_document(doc)
            d = result["decisions"]["decisions"][-1]
            self.assertEqual(d["decision"], "REVIEW")
            self.assertNotEqual(result["ledger"]["columns"]["owner"][1], result["ledger"]["columns"]["owner"][2])

    def test_front_matter_breaks_open_scope(self):
        doc = self.document(["#### Intro\nBody", "Содержание\nТом 1", "continues?\nBody"])
        result = materialize_document(doc)
        self.assertEqual(result["decisions"]["decisions"][-1]["decision"], "REVIEW")
        self.assertIn("EXCLUDED", result["ledger"]["columns"]["owner"])

    def test_first_row_all_kinds_and_separator_independence(self):
        inputs = {FirstRowKind.NONE: ["| -- | -- |"], FirstRowKind.DATA_LIKE: ["| 10 | Something |"],
                  FirstRowKind.TEXTUAL: ["| Name | Unit |"], FirstRowKind.MULTI_ROW: ['| <th rowspan="2">Name | Unit |'],
                  FirstRowKind.UNKNOWN: ["| ? | ? |"]}
        for kind, rows in inputs.items():
            self.assertEqual(first_row_model(rows)["kind"], kind.value)
            for position in range(len(rows) + 1):
                modified = rows[:position] + ["| -- | -- |"] + rows[position:]
                model = first_row_model(modified)
                self.assertEqual(model["kind"], kind.value)
                self.assertFalse(model["semantic_header_proven"])

    def test_table_skeleton_keeps_every_segment_and_publishes_review(self):
        table = "| A | B |\n| -- | -- |\n| 1 | x |"
        result = materialize_document(self.document([table, table]))
        self.assertEqual(sum(u["kind"] == "TABLE_SEGMENT" for u in result["semantics"]["units"]), 2)
        d = result["decisions"]["decisions"][-1]
        self.assertEqual((d["kind"], d["decision"]), ("TABLE", "REVIEW"))
        self.assertIn("REPEATED_FIRST_ROW", d["evidence_codes"])

    def test_boundary_order_independent_conflict_and_no_evidence(self):
        expected = BoundaryDecision.collect("TABLE", 1, 2, join=["b", "a"], split=["y", "x"])
        for join, split in itertools.product(itertools.permutations(["a", "b", "a"]), itertools.permutations(["x", "y"])):
            self.assertEqual(expected, BoundaryDecision.collect("TABLE", 1, 2, join=join, split=split))
        self.assertTrue(expected.conflict)
        self.assertEqual(expected.decision, "REVIEW")
        self.assertEqual(BoundaryDecision.collect("TABLE", 1, 2).evidence_codes, ("NO_EVIDENCE",))
        self.assertEqual(BoundaryDecision.collect("TABLE", 1, 2, join=["a"]).decision, "SAME")
        self.assertEqual(BoundaryDecision.collect("TABLE", 1, 2, split=["a"]).decision, "NEW")

    def test_scorer_three_metrics_and_missing_truth(self):
        predictions = {"a": {"decision": "SAME", "basis": "PROVEN"}, "b": {"decision": "NEW", "basis": "PROVEN"},
                       "c": {"decision": "REVIEW", "basis": "DEFAULT"}, "d": {"decision": "REVIEW", "basis": "DEFAULT"}}
        metrics = score(predictions, {"a": "SAME", "b": "SAME", "c": "NEW", "d": "UNSURE"})
        self.assertEqual(metrics["accuracy_on_proven"], 0.5)
        self.assertEqual(metrics["coverage"], 2/3)
        self.assertEqual(metrics["legacy_review_incorrect"], 1/3)
        self.assertEqual(metrics["unsure"], 1)
        self.assertIsNone(score(predictions, {})["accuracy_on_proven"])
        self.assertEqual(score(predictions, {})["unanswered"], 4)

    def test_anchor_resolution_and_review_not_inferred_same(self):
        doc = self.document(["UNSUPPORTED TITLE\nBody"])
        result = materialize_document(doc)
        ledger, _ = LineLedger.read(doc)
        case = {"kind": "SECTION", "document_version": doc["document_version"], "anchors": [ledger.anchor(0, "LAST"), ledger.anchor(1, "FIRST")]}
        resolver = AnchorResolver(result)
        self.assertEqual(resolver.predict(case)["decision"], "REVIEW")
        case["anchors"][0]["line_sha256"] = "corrupt"
        self.assertEqual(resolver.predict(case)["evidence_codes"], ["MISSING_SOURCE_ANCHOR"])

    def test_prediction_freeze_and_namespace_guard(self):
        path = self.root / "predictions.json"
        freeze_predictions(path, {}, "SEMANTIC_FOUNDATION_V3_DEV", "sha")
        with self.assertRaises(FileExistsError):
            freeze_predictions(path, {}, "SEMANTIC_FOUNDATION_V3_DEV", "sha")
        with self.assertRaises(ValueError):
            freeze_predictions(self.root / "other.json", {}, "EVAL", "sha")

    def test_replay_byte_identical(self):
        doc = self.document(["#### Intro\nBody\nТаблица 1\n| A | B |\n| -- | -- |\n| 1 | x |"])
        self.assertEqual(v1.canonical_bytes(materialize_document(doc)), v1.canonical_bytes(materialize_document(doc)))


if __name__ == "__main__":
    unittest.main()
