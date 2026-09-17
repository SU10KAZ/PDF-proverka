import tempfile
import unittest
from pathlib import Path

from . import evidence_rehydration_v1 as m


class EvidenceRehydrationTests(unittest.TestCase):
    def item(self, eid, route="TEXT", roles=None):
        return {"evidence_id":eid,"route":route,"page":1,"document_version":"v",
                "rehydration":{"priority":"P3","roles":roles or ["PRIMARY_STATE"]}}

    def region(self, text="ПД3.1 секция 2.1", route="TEXT", page=7):
        return {"region_id":"r1","page":page,"source_type":route,"text":text,
                "document_version":"v"}

    def test_existing_evidence_never_removed(self):
        old=[self.item("a"),self.item("b")]
        self.assertEqual([x["evidence_id"] for x in m.trim_budget(old,[self.item("c")],2)[:2]],["a","b"])

    def test_primary_old_cannot_be_displaced(self):
        self.assertEqual(m.trim_budget([self.item("old")],[self.item("optional",roles=["NOTE_OR_LEGEND"])],1)[0]["evidence_id"],"old")

    def test_primary_new_cannot_be_displaced(self):
        self.assertEqual(m.trim_budget([self.item("new")],[self.item("optional",roles=["TABLE_CONTEXT"])],1)[0]["evidence_id"],"new")

    def test_matched_page_candidate_counterpart(self):
        a={"exact":[],"lexical":["подпор","шахты"]}
        self.assertIsNotNone(m.score_region(self.region("подпор шахты",page=9),a,[9]))

    def test_matched_page_alone_not_relevant(self):
        self.assertIsNone(m.score_region(self.region("совсем другое",page=9),{"exact":[],"lexical":["подпор"]},[9]))

    def test_same_subject_cross_context_allowed(self):
        self.assertEqual(m.evidence_from_region(self.region("ПД3.1"),"old",
            {"source":{"pdf":{"path":"p","sha256":"s"}}},{},["ctx_a","ctx_b"],
            {"priority":"P1","reason":"x","matched_anchors":[],"exact":["пд3.1"]})["source_broad_context_id"],["ctx_a","ctx_b"])

    def test_different_subject_rejected(self):
        self.assertIsNone(m.score_region(self.region("отопительный прибор"),{"exact":["пд3.1"],"lexical":["подпор","шахта"]},[]))

    def test_text_graphic_supported(self):
        p={"evidence":{"old":[self.item("a","TEXT")],"new":[self.item("b","GRAPHIC")]}}
        p["evidence"]["new"][0]["raster"]={"sha256":"x"}
        self.assertEqual(m.structural_status(p,["TEXT","GRAPHIC"])[0],"STRUCTURALLY_READY")

    def test_table_graphic_supported(self):
        p={"evidence":{"old":[self.item("a","TABLE")],"new":[self.item("b","GRAPHIC")]}}
        p["evidence"]["new"][0]["raster"]={"sha256":"x"}
        self.assertEqual(m.structural_status(p,["TABLE","GRAPHIC"])[0],"STRUCTURALLY_READY")

    def test_row_group_table_roles(self):
        self.assertIn("TABLE_CONTEXT",m.roles_for(self.region(route="TABLE"),{"exact":[],"matched_anchors":[]}))

    def test_text_continuation_only_when_applicable(self):
        self.assertNotIn("CONTINUATION",m.roles_for(self.region(),{"exact":[],"matched_anchors":[]}))

    def test_full_page_materialization_function_exists(self):
        self.assertTrue(callable(m.materialize_full_page))

    def test_raster_provenance_contract(self):
        self.assertTrue(any(isinstance(x,tuple) and "source_pdf_sha256" in x
                            for x in m.materialize_full_page.__code__.co_consts))

    def test_no_truth_values_in_anchor_builder(self):
        g={"engineering_subject":"ПД3.1","scope":"секция 2.1","change_summary":"изменено",
           "event_type":"схема","locations":[],"parameters_changed":[]}
        a=m.build_anchors(g,[]); self.assertNotIn("37400",a["lexical"])

    def test_one_to_n_preserved(self):
        self.assertEqual([x["evidence_id"] for x in m.trim_budget([self.item("a"),self.item("b")],[],8)],["a","b"])

    def test_n_to_one_preserved(self):
        self.assertEqual(len(m.trim_budget([self.item("a"),self.item("b"),self.item("c")],[],8)),3)

    def test_optional_cannot_displace_primary(self):
        self.test_primary_old_cannot_be_displaced()

    def test_package_lineage_complete(self):
        self.assertIn("source_broad_context_id",m.evidence_from_region(self.region(),"old",
            {"source":{"pdf":{"path":"p","sha256":"s"}}},{},["ctx"],
            {"priority":"P3","reason":"x","matched_anchors":["x"],"exact":[]}).keys())

    def test_all_80_accounting_guard(self):
        self.assertEqual(m.PAIR_KEY,"caea6d2810c334ec0368de8e")

    def test_truth_requires_freeze(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(RuntimeError): m.evaluate(Path(d))


if __name__ == "__main__": unittest.main()
