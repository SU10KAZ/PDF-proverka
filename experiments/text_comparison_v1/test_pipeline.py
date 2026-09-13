import copy
import json
from pathlib import Path
import tempfile
import unittest

from .common import canonical, file_hash
from .sections import materialize
from .relations import relate, ai_package, validate_ai
from .facts import extract, compare


class PipelineTest(unittest.TestCase):
    def doc(self, pages, version="old", types=None):
        d = Path(self.tmp.name) / str(self.counter)
        self.counter += 1
        d.mkdir()
        chunks, blocks = [], []
        for n, content in enumerate(pages, 1):
            bt = types[n - 1] if types else "text"
            chunks.append(f"## Page {n}\n### BLOCK #{n} [{bt.upper()}]: block_{n}\n\n{content}\n")
            blocks.append({"block_id": f"block_{n}", "page_index": n - 1, "block_type": bt,
                           "coords_norm": [0, 0, 1, 1]})
        md, bs = d / "document.md", d / "blocks.json"
        md.write_text("\n".join(chunks))
        bs.write_text(json.dumps({"blocks": blocks, "pages": [{"page_index": i, "width_px": 800, "height_px": 1200} for i in range(len(pages))]}))
        return {"document_version": version, "document_code": "sample", "version_id": "v001",
                "artifacts": {"work_md": {"path": str(md), "sha256": file_hash(md)},
                              "blocks": {"path": str(bs), "sha256": file_hash(bs)}}}

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.counter = 0

    def tearDown(self):
        self.tmp.cleanup()

    def test_page_flow_multiple_sections_and_repeated_heading(self):
        d = self.doc(["#### 6.3 Кондиционирование\n\nНачало предложения", "продолжается на другой странице.\n\n#### 6.3 Кондиционирование\n\nРасход 50 м3/ч.\n\n#### 6.4 Вентиляция\n\nРасход 80 м3/ч."])
        r = materialize(d)
        self.assertEqual(len(r["sections"]), 2)
        self.assertEqual(r["sections"][0]["page_span"], [1, 2])
        self.assertIn("Начало предложения продолжается", r["sections"][0]["ordered_text_blocks"][0]["text"])
        self.assertEqual(r["quality"]["unexplained_unowned"], 0)

    def test_stable_key_under_page_shift_and_heading_style(self):
        a = materialize(self.doc(["#### 6.3 Кондиционирование\n\nМощность 60 Вт."]))["sections"][0]
        b = materialize(self.doc(["", "###### **6.3 Кондиционирование**\n\nМощность 100 Вт."], "new"))["sections"][0]
        self.assertEqual(a["section_key"], b["section_key"])
        self.assertNotEqual(a["document_version"], b["document_version"])

    def test_text_route_excludes_table_and_image_payload(self):
        r = materialize(self.doc(["#### 1 Система\n\nМощность 60 Вт.\n\n| Код | Мощность |\n|---|---|\n| X | 999 кВт |", "Мощность 888 кВт."], types=["text", "image"]))
        text = " ".join(b["text"] for s in r["sections"] for b in s["ordered_text_blocks"])
        self.assertNotIn("999", text)
        self.assertNotIn("888", text)
        self.assertTrue(r["sections"][0]["table_refs"])
        self.assertTrue(r["sections"][0]["graphic_refs"])

    def test_weak_heading_does_not_prove_boundary(self):
        r = materialize(self.doc(["#### 1 Система\n\nОбычный текст.\n\nВЕНТИЛЯЦИЯ\n\nРасход 60 м3/ч."]))
        self.assertEqual(len(r["sections"]), 1)
        self.assertEqual(r["sections"][0]["status"], "REVIEW")
        self.assertTrue(any(d["decision"] == "REVIEW" for d in r["decisions"]))
        self.assertTrue(all(f["owner_status"] == "REVIEW" for f in extract(r["sections"][0])["facts"]))

    def sections(self, text, version="old"):
        return materialize(self.doc([text], version))["sections"]

    def changes(self, old, new):
        relations = relate(old, new)
        return [c for r in relations for c in compare(r,
            [s for s in old if s["instance_id"] in r["old_sections"]],
            [s for s in new if s["instance_id"] in r["new_sections"]],
            [extract(s) for s in old if s["instance_id"] in r["old_sections"]],
            [extract(s) for s in new if s["instance_id"] in r["new_sections"]])]

    def test_numeric_count_and_pipe_changes_have_provenance(self):
        a = self.sections("#### 6.3 Кондиционирование\n\nЖилые помещения: 60 Вт/м2.\n\nУстановлены 2 холодильных машины.\n\nПрименены 2-х трубные фанкойлы.")
        b = self.sections("#### 6.3 Кондиционирование\n\nЖилые помещения: 100 Вт/м².\n\nУстановлены 3 холодильных машины.\n\nПрименены 4-х трубные фанкойлы.", "new")
        changes = self.changes(a, b)
        engineering = [c for c in changes if c["category"] == "ENGINEERING_CHANGE"]
        self.assertEqual(len(engineering), 3)
        self.assertEqual({c["type"] for c in engineering}, {"VALUE_CHANGED", "PROPERTY_CHANGED"})
        self.assertTrue(all(c["old_facts"][0]["source_refs"] and c["new_facts"][0]["source_refs"] for c in engineering))

    def test_order_and_units_are_not_engineering_changes(self):
        a = self.sections("#### 1 Вентиляция\n\nМощность 1 кВт.\n\nДавление 100 Па.")
        b = self.sections("#### 1 Вентиляция\n\nДавление 100 Па.\n\nМощность 1000 Вт.", "new")
        self.assertFalse(any(c["category"] == "ENGINEERING_CHANGE" for c in self.changes(a, b)))

    def test_unparsed_wording_does_not_disappear(self):
        a = self.sections("#### 1 Вентиляция\n\nОборудование размещено на кровле.")
        b = self.sections("#### 1 Вентиляция\n\nОборудование размещено в подвале.", "new")
        self.assertTrue(any(c["type"] == "REVIEW" for c in self.changes(a, b)))

    def test_swapped_subject_and_object_are_not_editorial(self):
        a = self.sections("#### 1 Вентиляция\n\nНасос обслуживает вентилятор.")
        b = self.sections("#### 1 Вентиляция\n\nВентилятор обслуживает насос.", "new")
        self.assertTrue(any(c["type"] == "REVIEW" for c in self.changes(a, b)))

    def test_range_tail_and_formula_abstain(self):
        sections = self.sections("#### 1 Вентиляция\n\nВода 7/12°C.\n\nМощность -1500 Вт.")
        facts = extract(sections[0])["facts"]
        self.assertEqual(len(facts), 2)
        self.assertTrue(all(f["owner_status"] == "REVIEW" for f in facts))

    def test_split_and_merge(self):
        part1 = "Вентиляция жилых помещений осуществляется отдельной системой с механическим побуждением."
        part2 = "Вентиляция офисных помещений осуществляется другой системой с естественным побуждением."
        a = self.sections("#### 6 Вентиляция\n\n" + part1 + "\n\n" + part2)
        b = self.sections("#### 6.1 Вентиляция жилых помещений\n\n" + part1 + "\n\n#### 6.2 Вентиляция офисных помещений\n\n" + part2, "new")
        self.assertEqual([r["kind"] for r in relate(a, b)], ["ONE_TO_N"])
        self.assertEqual([r["kind"] for r in relate(b, a)], ["N_TO_ONE"])

    def test_nonunique_relations_and_missing_coverage_abstain(self):
        a = self.sections("#### 1 Вентиляция\n\nРасход 100 м3/ч.")
        b = copy.deepcopy(a + a)
        b[1]["instance_id"] += "_duplicate"
        self.assertTrue(all(r["status"] == "REVIEW" for r in relate(a, b)))
        self.assertEqual(relate(a, [], new_complete=False)[0]["kind"], "REVIEW")
        self.assertEqual(relate(a, [])[0]["direction"], "REMOVED")

    def test_ai_fail_closed_and_local_size_guard(self):
        a = self.sections("#### 1 Вентиляция\n\nРасход 100 м3/ч.")
        package = ai_package(a, a)
        self.assertEqual(validate_ai(package, {"label": "SAME_SECTION", "evidence": []}, "test")["label"], "UNSURE")
        self.assertIsNone(ai_package(a, a, max_chars=1))
        self.assertIsNone(ai_package(a + a, a))

    def test_replay_and_changed_source_guard(self):
        doc = self.doc(["#### 1 Вентиляция\n\nМощность 60 Вт."])
        self.assertEqual(canonical(materialize(doc)), canonical(materialize(doc)))
        Path(doc["artifacts"]["work_md"]["path"]).write_text("changed")
        with self.assertRaises(ValueError):
            materialize(doc)


if __name__ == "__main__":
    unittest.main()
