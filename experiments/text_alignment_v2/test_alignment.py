"""Constructed software contracts, explicitly NOT corpus/human truth."""
import copy
import unittest

from .alignment import align
from .facts import compare, analysis


def unit(text,i=0,side="a",heading="Общие технические решения",eligible=True):
    return {"unit_id":f"{side}_{i}","document_version":side,"document_code":side,"ordinal":i,
            "text":text,"source_refs":[{"line_id":i,"page":1,"block_id":str(i),"markdown_line":i+1}],
            "section_context":[{"title":heading,"status":"REVIEW"}] if heading else [],
            "section_ownership_proven":False,"external_refs":[],"review_reasons":[],
            "eligibility":"ELIGIBLE" if eligible else "REVIEW"}


def run(a,b,approach="hybrid"):
    out=align(a,b,approach)
    aa={u["unit_id"]:u for u in a};bb={u["unit_id"]:u for u in b}
    changes=[c for r in out["relations"] for c in compare(r,[aa[x] for x in r["old_unit_ids"]],[bb[x] for x in r["new_unit_ids"]])]
    return out,changes


class Contracts(unittest.TestCase):
    def test_section_uncertainty_not_gate(self):
        a=unit("Расчетная мощность электрического двигателя насосной установки составляет 17 кВт.")
        b=unit(a["text"].replace("17","23"),side="b")
        out,c=run([a],[b])
        self.assertEqual(out["quality"]["aligned_relations"],1)
        self.assertEqual([x["type"] for x in c],["VALUE_CHANGED"])

    def test_no_scope_no_automatic_change(self):
        a=unit("Расчетная мощность электрического двигателя насосной установки составляет 17 кВт.",heading=None)
        b=unit(a["text"].replace("17","23"),side="b",heading=None)
        self.assertEqual(run([a],[b])[1][0]["type"],"REVIEW")

    def test_no_match_never_means_removed(self):
        a=unit("Мощность электрического двигателя насосной установки составляет 17 кВт.")
        self.assertEqual(run([a],[])[1][0]["type"],"REVIEW")

    def test_duplicate_template_abstains(self):
        t="Расчетная мощность электрического двигателя насосной установки составляет {} кВт."
        a=[unit(t.format(17)),unit(t.format(21),1)]
        b=[unit(t.format(23),side="b"),unit(t.format(29),1,"b")]
        self.assertFalse(any(c["category"]=="ENGINEERING_CHANGE" for c in run(a,b)[1]))

    def test_ranges_abstain(self):
        a=unit("Рабочая мощность электрического двигателя насосной установки составляет 17–21 кВт.")
        b=unit(a["text"].replace("21","29"),side="b")
        self.assertFalse(any(c["category"]=="ENGINEERING_CHANGE" for c in run([a],[b])[1]))

    def test_unit_scale_no_false_change(self):
        a=unit("Расчетная мощность электрического двигателя насосной установки составляет 17 кВт.")
        b=unit(a["text"].replace("17 кВт","17000 Вт"),side="b")
        self.assertEqual(run([a],[b])[1][0]["type"],"EDITORIAL_CHANGE")

    def test_multiple_same_property_slots_remain_distinct(self):
        a=unit("Мощность первого двигателя составляет 17 кВт, а второго двигателя составляет 21 кВт.")
        b=unit(a["text"].replace("17","23").replace("21","29"),side="b")
        c=run([a],[b])[1]
        self.assertEqual([x["type"] for x in c],["VALUE_CHANGED","VALUE_CHANGED"])
        self.assertEqual([x["fact_delta"]["slot_index"] for x in c],[0,1])

    def test_negation_engineering(self):
        a=unit("Не допускается устанавливать насосное оборудование непосредственно на деревянное основание.")
        b=unit(a["text"].replace("Не допускается","Допускается"),side="b")
        self.assertEqual(run([a],[b])[1][0]["type"],"REQUIREMENT_CHANGED")

    def test_subject_object_swap_not_editorial(self):
        a=unit("Резервный насос заменяет основной насос при отказе электрического двигателя.")
        b=unit("Основной насос заменяет резервный насос при отказе электрического двигателя.",side="b")
        self.assertFalse(any(c["type"] in {"EDITORIAL_CHANGE","NO_SEMANTIC_CHANGE"} for c in run([a],[b])[1]))

    def test_synonym_modality_no_engineering(self):
        a=unit("Следует устанавливать насосное оборудование непосредственно на бетонное основание.")
        b=unit(a["text"].replace("Следует","Необходимо"),side="b")
        self.assertEqual(run([a],[b])[1][0]["type"],"EDITORIAL_CHANGE")

    def test_equipment_mark_changed_not_value_change(self):
        a=unit("Расчетная мощность электрического двигателя установки П1 составляет 17 кВт.")
        b=unit(a["text"].replace("П1","П2").replace("17","23"),side="b")
        self.assertFalse(any(c["category"]=="ENGINEERING_CHANGE" for c in run([a],[b])[1]))

    def test_split_and_merge(self):
        x="Трубопроводы системы отопления необходимо прокладывать вдоль наружных стен здания."
        y="Насосное оборудование системы устанавливается на прочное бетонное основание."
        a=[unit(x+" "+y)];b=[unit(x,0,"b"),unit(y,1,"b")]
        self.assertEqual(run(a,b)[0]["quality"]["shapes"],{"ONE_TO_N":1})
        self.assertEqual(run(b,a)[0]["quality"]["shapes"],{"N_TO_ONE":1})
        self.assertFalse(any(c["category"]=="ENGINEERING_CHANGE" for c in run(a,b)[1]))

    def test_group_cannot_cross_excluded_source(self):
        x="Трубопроводы системы отопления необходимо прокладывать вдоль наружных стен здания."
        y="Насосное оборудование системы устанавливается на прочное бетонное основание."
        a=[unit(x+" "+y)];b=[unit(x,0,"b"),unit(y,3,"b")]
        self.assertEqual(run(a,b)[0]["quality"]["aligned_relations"],0)

    def test_moved_paragraph(self):
        a=[unit(t,i) for i,t in enumerate([
            "Трубопроводы системы отопления прокладываются вдоль наружных стен здания.",
            "Насосное оборудование устанавливается на прочное бетонное основание.",
            "Кабельные линии системы электроснабжения прокладываются в защитных коробах."])]
        b=[unit(u["text"],i,"b") for i,u in enumerate(reversed(a))]
        self.assertEqual(run(a,b)[0]["quality"]["aligned_old_units"],3)

    def test_normalization_does_not_erase_engineering_symbols(self):
        a=analysis("Давление должно составлять -17 Па.")
        b=analysis("Давление должно составлять 17 Па.")
        self.assertNotEqual(a["canonical"],b["canonical"])
        self.assertNotEqual(a["slots"],b["slots"])

    def test_repeat_deterministic(self):
        a=[unit("Насосное оборудование устанавливается на прочное бетонное основание.")]
        b=[unit(a[0]["text"],side="b")]
        self.assertEqual(run(a,b),run(a,b))

if __name__=="__main__":unittest.main()
