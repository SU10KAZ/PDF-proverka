import unittest
from .semantic_consolidator_v1 import MISSING_STATUS, audit_pass_a, audit_pass_b, invented_values, model_candidate


def candidate(cid="c1", readiness="PASS", location="L1", claim="flow", modality="TEXT"):
    return {"candidate_id": cid, "broad_context": "Ctx", "engineering_subject": "Fan",
        "subject_identity": "same", "scope": "S", "location": location, "system_or_subsystem": "V",
        "claim_type": claim, "possible_change_summary": claim + " changed", "old_refs": ["o" + cid],
        "new_refs": ["n" + cid], "modalities": [modality], "parameter_or_property": claim,
        "confidence": "HIGH", "comparison_readiness": readiness,
        "provenance": {"source_bundle": "b", "old_pages": [1], "new_pages": [2], "derived_disposition": "READY"}}


def local_group(gid, members, unresolved=False, system=False):
    return {"local_group_id": gid, "broad_context": "Ctx",
        "group_summary": " and ".join(x["possible_change_summary"] for x in members),
        "engineering_subject": "Fan", "scope": "S", "locations": sorted({x["location"] for x in members}),
        "event_type": "change", "atomic_candidate_ids": [x["candidate_id"] for x in members],
        "old_refs": sorted({v for x in members for v in x["old_refs"]}),
        "new_refs": sorted({v for x in members for v in x["new_refs"]}),
        "modalities": sorted({v for x in members for v in x["modalities"]}),
        "parameters": sorted({x["parameter_or_property"] for x in members}), "grouping_reason": "same event",
        "group_confidence": "HIGH", "comparison_readiness": "UNRESOLVED" if unresolved else "READY",
        "member_statuses": [{"candidate_id": x["candidate_id"], "status":
            MISSING_STATUS if x["comparison_readiness"] == "FAIL" else
            "STANDALONE_CHANGE" if len(members) == 1 else
            "UNRESOLVED_GROUPING" if unresolved else "MEMBER_OF_GROUP"} for x in members],
        "system_wide_change": system, "contradictions": ["conflict"] if unresolved else []}


def pass_a(groups, candidates):
    return ({"context_id": "ctx", "broad_context": "Ctx", "groups": groups},
            {"context_id": "ctx", "broad_context": "Ctx", "candidates": candidates})


def final_group(fid, groups):
    return {"consolidated_change_id": fid,
        "change_summary": " and ".join(g["group_summary"] for g in groups), "engineering_subject": "Fan", "scope": "S",
        "locations": sorted({v for g in groups for v in g["locations"]}), "event_type": "change",
        "member_local_group_ids": [g["local_group_id"] for g in groups],
        "atomic_candidate_ids": sorted({v for g in groups for v in g["atomic_candidate_ids"]}),
        "old_refs": sorted({v for g in groups for v in g["old_refs"]}),
        "new_refs": sorted({v for g in groups for v in g["new_refs"]}),
        "modalities": sorted({v for g in groups for v in g["modalities"]}),
        "parameters_changed": sorted({v for g in groups for v in g["parameters"]}),
        "comparison_readiness": "UNRESOLVED" if any(g["comparison_readiness"] == "UNRESOLVED" for g in groups) else "READY",
        "blocking_reasons": [], "merge_reason": "same event", "system_wide_change": any(g["system_wide_change"] for g in groups)}


class Tests(unittest.TestCase):
    def test_01_all_accounted(self):
        cs = [candidate("a"), candidate("b")]; a, p = pass_a([local_group("g", cs)], cs); self.assertFalse(audit_pass_a(a, p))
    def test_02_drop_detected(self):
        cs = [candidate("a"), candidate("b")]; a, p = pass_a([local_group("g", cs[:1])], cs); self.assertTrue(audit_pass_a(a, p))
    def test_03_invention_detected(self):
        cs = [candidate("a")]; a, p = pass_a([local_group("g", [candidate("x")])], cs); self.assertTrue(audit_pass_a(a, p))
    def test_04_same_subject_different_event_separate(self):
        cs = [candidate("a", claim="flow"), candidate("b", claim="location")]; a, p = pass_a([local_group("g1", [cs[0]]), local_group("g2", [cs[1]])], cs); self.assertFalse(audit_pass_a(a, p))
    def test_05_same_event_parameters_merge(self):
        cs = [candidate("a", claim="flow"), candidate("b", claim="pressure")]; a, p = pass_a([local_group("g", cs)], cs); self.assertFalse(audit_pass_a(a, p))
    def test_06_locations_separate(self):
        cs = [candidate("a", location="L1"), candidate("b", location="L2")]; a, p = pass_a([local_group("g1", [cs[0]]), local_group("g2", [cs[1]])], cs); self.assertFalse(audit_pass_a(a, p))
    def test_07_cross_modal_allowed(self):
        cs = [candidate("a", modality="TEXT"), candidate("b", modality="GRAPHIC")]; a, p = pass_a([local_group("g", cs)], cs); self.assertFalse(audit_pass_a(a, p))
    def test_08_one_to_many(self):
        cs = [candidate("a"), candidate("b")]; a, p = pass_a([local_group("g", cs)], cs); self.assertFalse(audit_pass_a(a, p))
    def test_09_many_to_one(self):
        cs = [candidate("x"), candidate("y")]; a, p = pass_a([local_group("g", cs)], cs); self.assertFalse(audit_pass_a(a, p))
    def test_10_missing_raster(self):
        cs = [candidate("a", readiness="FAIL")]; a, p = pass_a([local_group("g", cs)], cs); self.assertFalse(audit_pass_a(a, p))
    def test_11_conflict_unresolved(self):
        cs = [candidate("a"), candidate("b")]; a, p = pass_a([local_group("g", cs, unresolved=True)], cs); self.assertFalse(audit_pass_a(a, p))
    def test_12_singleton(self):
        cs = [candidate()]; a, p = pass_a([local_group("g", cs)], cs); self.assertFalse(audit_pass_a(a, p))
    def test_13_lineage_pass_b(self):
        gs = [local_group("g1", [candidate("a")]), local_group("g2", [candidate("b")])]; self.assertFalse(audit_pass_b({"groups": [final_group("f", gs)]}, gs))
    def test_14_no_truth_fields(self):
        row = {"local_candidate_id": "x", "broad_context": "c", "engineering_subject": "s", "subject_identity": "i", "scope": "s", "location": "l", "system_or_subsystem": "sys", "claim_type": "p", "possible_change_summary": "x", "old_evidence_refs": ["o"], "new_evidence_refs": ["n"], "old_modality": ["TEXT"], "new_modality": ["TABLE"], "identity_confidence": "HIGH", "comparison_readiness_audit": {"readiness": "PASS"}, "source_bundle": "b", "old_pages": [1], "new_pages": [2], "derived_disposition": "READY"}
        self.assertFalse(any("truth" in x or "expected" in x for x in model_candidate(row)))
    def test_15_no_new_values(self):
        self.assertFalse(invented_values("Расход 1200 м3/ч", "Было 1200 м3/ч")); self.assertTrue(invented_values("Расход 1300 м3/ч", "Было 1200 м3/ч"))


if __name__ == "__main__": unittest.main()
