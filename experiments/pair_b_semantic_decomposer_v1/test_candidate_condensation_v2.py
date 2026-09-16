import tempfile
import unittest
from pathlib import Path

from .candidate_condensation_v2 import (
    _eligible, condense, exact_duplicate_key, normalize_text, reference_audit,
)


def candidate(cid, kind="POTENTIAL_CHANGE", old=None, new=None, subject="Насос 1",
              claim_type="Расход", summary="Расход изменён", scope="Контур A",
              location="Помещение 1", confidence="LOW"):
    return {
        "candidate_id": cid, "local_candidate_id": "b/" + cid, "source_bundle": "b",
        "candidate_kind": kind, "claim_type": claim_type, "engineering_subject": subject,
        "possible_change_summary": summary, "old_evidence_refs": old or [],
        "new_evidence_refs": new or [], "supporting_evidence_refs": [],
        "required_modalities": [], "identity_confidence": confidence,
        "comparison_readiness": "READY", "subject_identity": "same",
        "scope": scope, "location": location, "system_or_subsystem": "ОВ",
        "reason": "fixture",
    }


def registry(old_route="TEXT", new_route="TEXT"):
    return {
        "o": {"side": "old", "page": 1, "route": old_route},
        "n": {"side": "new", "page": 2, "route": new_route},
        "wrong": {"side": "new", "page": 3, "route": "TEXT"},
    }


def payload(old_route="TEXT", new_route="TEXT"):
    def item(ref, side, page, route):
        value = {"evidence_id": ref, "side": side.upper(), "page": page,
                 "document_version": side + "-v", "quote": "payload", "route": route}
        if route == "GRAPHIC":
            value["raster"] = {"path": "raster.png", "sha256": "fixture"}
        return value
    return {"source_regions": {"old": [], "new": []}, "source_evidence": {
        "old": [item("o", "old", 1, old_route)], "new": [item("n", "new", 2, new_route)]}}


def run_fixture(items, reg=None, pay=None, pairs=None, base=None):
    return condense(items, {"b": {"coarse_heading": "Отопление"}},
                    {"b": reg or registry()}, {"b": pay or payload()}, pairs or [],
                    base=base or Path("/tmp"))


class CondensationTests(unittest.TestCase):
    def test_01_insufficient_goes_to_review_pool(self):
        result = run_fixture([candidate("1", "INSUFFICIENT_FOR_COMPARISON", ["o"], ["n"])])
        self.assertEqual(len(result["insufficient"]), 1)

    def test_02_one_sided_never_becomes_change(self):
        result = run_fixture([candidate("1", old=["o"])])
        self.assertFalse(result["ready"])
        self.assertEqual(result["rows"][0]["derived_disposition"], "ONE_SIDED_REVIEW")

    def test_03_invalid_ref_excluded_from_ready(self):
        result = run_fixture([candidate("1", ["missing"], ["n"])])
        self.assertFalse(result["ready"])
        self.assertEqual(len(result["invalid"]), 1)

    def test_04_potential_not_change_is_separate(self):
        result = run_fixture([candidate("1", "POTENTIAL_NOT_CHANGE", ["o"], ["n"])])
        self.assertEqual(len(result["not_change"]), 1)
        self.assertFalse(result["ready"])

    def test_05_exact_duplicate_collapses_deterministically(self):
        result = run_fixture([candidate("2", old=["o"], new=["n"]), candidate("1", old=["o"], new=["n"])])
        self.assertEqual(result["counts"]["exact_duplicates_removed"], 1)
        self.assertEqual(result["exact_groups"][0]["canonical_candidate_id"], "b/1")

    def test_06_differing_refs_prevent_exact_merge(self):
        reg = registry(); reg["n2"] = {"side": "new", "page": 4, "route": "TEXT"}
        pay = payload(); pay["source_evidence"]["new"].append({"evidence_id": "n2", "side": "new", "page": 4, "document_version": "n-v", "quote": "x"})
        result = run_fixture([candidate("1", old=["o"], new=["n"]), candidate("2", old=["o"], new=["n2"])], reg, pay)
        self.assertEqual(result["counts"]["exact_duplicates_removed"], 0)

    def test_07_differing_claim_type_prevents_exact_merge(self):
        result = run_fixture([candidate("1", old=["o"], new=["n"]), candidate("2", old=["o"], new=["n"], claim_type="Давление")])
        self.assertEqual(result["counts"]["exact_duplicates_removed"], 0)

    def test_08_same_subject_different_parameter_stays_separate(self):
        items = [candidate("1", old=["o"], new=["n"], claim_type="Расход"),
                 candidate("2", old=["o"], new=["n"], claim_type="Мощность")]
        self.assertEqual(len(run_fixture(items)["ready"]), 2)

    def test_09_text_old_graphic_new_allowed(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "raster.png").touch()
            result = run_fixture([candidate("1", old=["o"], new=["n"])], registry("TEXT", "GRAPHIC"), payload("TEXT", "GRAPHIC"), base=Path(tmp))
            self.assertEqual(len(result["ready"]), 1)

    def test_10_table_old_graphic_new_allowed(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "raster.png").touch()
            result = run_fixture([candidate("1", old=["o"], new=["n"])], registry("TABLE", "GRAPHIC"), payload("TABLE", "GRAPHIC"), base=Path(tmp))
            self.assertEqual(len(result["ready"]), 1)

    def test_11_cross_modal_flag_preserved(self):
        result = run_fixture([candidate("1", old=["o"], new=["n"])], registry("TEXT", "TABLE"), payload("TEXT", "TABLE"))
        self.assertTrue(result["ready"][0]["cross_modal"])

    def test_12_confidence_does_not_drop_candidate(self):
        result = run_fixture([candidate("1", old=["o"], new=["n"], confidence="LOW")])
        self.assertEqual(len(result["ready"]), 1)

    def test_13_lineage_preserved_for_every_candidate(self):
        items = [candidate(str(i), old=["o"], new=["n"], claim_type=str(i)) for i in range(4)]
        result = run_fixture(items)
        self.assertEqual({x["local_candidate_id"] for x in items}, {x["local_candidate_id"] for x in result["rows"]})

    def test_14_no_source_truth_files_accessed(self):
        source = Path(__file__).with_name("candidate_condensation_v2.py").read_text()
        self.assertNotIn("SOURCE_VERIFICATION.json", source)
        self.assertNotIn("PROVEN 10", source)

    def test_15_candidate_totals_reconcile(self):
        items = [candidate("1", old=["o"], new=["n"]), candidate("2", "POTENTIAL_NOT_CHANGE", ["o"], ["n"]), candidate("3", old=["o"])]
        result = run_fixture(items)
        self.assertEqual(result["count_reconciliation"], "PASS")
        self.assertEqual(sum(result["counts"]["primary_dispositions"].values()), 3)


if __name__ == "__main__":
    unittest.main()
