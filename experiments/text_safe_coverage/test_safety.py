"""Adversarial contracts, not mirrors of the implementation or DEV case IDs."""
import copy
import unittest

from experiments.text_comparison_v1.test_pipeline import PipelineTest
from experiments.text_comparison_v1.common import canonical
from experiments.text_comparison_v1.facts import extract
from .sections import materialize, predict
from .relations import relate
from .facts import compare


class SafetyTest(PipelineTest):
    def sections(self, text, version="old"):
        return materialize(self.doc([text], version))["sections"]

    def changes(self, old, new):
        return [c for r in relate(old, new) for c in compare(r,
            [s for s in old if s["instance_id"] in r["old_sections"]],
            [s for s in new if s["instance_id"] in r["new_sections"]],
            [extract(s) for s in old if s["instance_id"] in r["old_sections"]],
            [extract(s) for s in new if s["instance_id"] in r["new_sections"]])]

    def test_no_extraction_is_not_removal(self):
        a = self.sections("#### 1 Насосы\n\nДавление 240 Па.")
        b = self.sections("#### 1 Насосы\n\nДавление определяется отдельным расчётом.", "new")
        self.assertFalse(any(c["type"] == "FACT_REMOVED" for c in self.changes(a, b)))
        self.assertTrue(any(c["category"] == "REVIEW" for c in self.changes(a, b)))
        self.assertEqual(relate(a, [])[0]["kind"], "REVIEW")

    def test_same_words_different_assertion_is_not_numeric_proof(self):
        a = self.sections("#### 1 Насосы\n\nНасос А питает насос Б мощностью 15 кВт.")
        b = self.sections("#### 1 Насосы\n\nНасос Б питает насос А мощностью 25 кВт.", "new")
        self.assertFalse(any(c["category"] == "ENGINEERING_CHANGE" for c in self.changes(a, b)))

    def test_incomplete_scope_blocks_relation_and_change(self):
        a = self.sections("#### 1 Насосы\n\nДавление 240 Па.\n\nНЕРАЗРЕШЕННЫЙ ЗАГОЛОВОК\n\nДальнейший текст.")
        b = self.sections("#### 1 Насосы\n\nДавление 340 Па.", "new")
        self.assertTrue(all(r["status"] == "REVIEW" for r in relate(a, b)))
        self.assertFalse(any(c["category"] == "ENGINEERING_CHANGE" for c in self.changes(a, b)))

    def test_ambiguous_split_keeps_all_sections(self):
        a = self.sections("#### 1 Насосы\n\nПодача воды осуществляется отдельным насосом в независимую систему подачи воды корпуса здания.\n\nУдаление воды осуществляется другим насосом в отдельную систему удаления воды корпуса здания.")
        b = copy.deepcopy(a + a)
        b[1]["instance_id"] += "_another"
        r = relate(a, b)
        self.assertTrue(all(x["status"] == "REVIEW" for x in r))
        self.assertEqual(sum(len(x["new_sections"]) for x in r), 2)

    def test_numbered_paragraph_continues_across_page(self):
        prose = "1.1 Для подачи воды в помещения здания предусмотрен отдельный насос, который должен обеспечивать требуемую подачу воды во всех предусмотренных проектом режимах работы."
        d = self.doc(["#### 1 Насосы\n\n" + prose, "Продолжение расчётного описания.\n\n#### 2 Отопление\n\nОписание отопления."])
        r = materialize(d)
        self.assertEqual(len(r["sections"]), 2)
        self.assertEqual(r["sections"][0]["page_span"], [1, 2])
        self.assertEqual(r["sections"][0]["status"], "PROVEN")

    def test_relative_boundary_does_not_promote_unknown_owner(self):
        d = self.doc(["#### 1 Насосы\n\nСЛАБЫЙ ЗАГОЛОВОК\n\nПродолжение предложения", "#### 1 Насосы\n\nДальнейший текст."])
        r = materialize(d)
        anchors = [r["ownership"][i]["source_ref"] for i in (2, 3)]
        c = {"kind": "SECTION", "source_anchors": anchors}
        self.assertEqual(predict(c, r)["answer"], "YES")
        self.assertEqual(r["ownership"][2]["status"], "REVIEW")
        self.assertEqual(r["sections"][0]["status"], "REVIEW")

    def test_source_anchor_tamper_fails_closed(self):
        r = materialize(self.doc(["#### 1 Насосы\n\nОписание.\n\n#### 1 Насосы"]))
        anchors = [copy.deepcopy(r["ownership"][i]["source_ref"]) for i in (1, 2)]
        anchors[0]["line_sha256"] = "wrong"
        self.assertEqual(predict({"kind": "SECTION", "source_anchors": anchors}, r)["answer"], "REVIEW")

    def test_split_merge_requires_closed_ownership(self):
        p = "Подача воды осуществляется отдельным насосом в независимую систему подачи воды корпуса здания."
        q = "Удаление воды осуществляется другим насосом в отдельную систему удаления воды корпуса здания."
        a = self.sections("#### 1 Насосы\n\n" + p + "\n\n" + q)
        b = self.sections("#### 1.1 Насосы подачи\n\n" + p + "\n\n#### 1.2 Насосы удаления\n\n" + q, "new")
        self.assertEqual([x["kind"] for x in relate(a, b)], ["ONE_TO_N"])
        self.assertEqual([x["kind"] for x in relate(b, a)], ["N_TO_ONE"])
        b[0]["status"] = "REVIEW"
        self.assertTrue(all(x["status"] == "REVIEW" for x in relate(a, b)))

    def test_table_gap_is_not_local_prose_proof(self):
        d = self.doc(["#### 1 Насосы\n\nСЛАБЫЙ ЗАГОЛОВОК\n\nОписание подачи.\n\n| Значение | Единица |\n|---|---|\n| 17 | Па |\n\nОписание удаления.\n\nПродолжение удаления."])
        r = materialize(d)
        anchors = [r["ownership"][i]["source_ref"] for i in (2, 7)]
        self.assertEqual(predict({"kind": "SECTION", "source_anchors": anchors}, r)["answer"], "REVIEW")


if __name__ == "__main__":
    unittest.main()
