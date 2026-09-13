"""Reuse V1 extraction; guard exact assertion identity and absence claims."""
from experiments.text_comparison_v1.facts import extract
from experiments.text_comparison_v1.facts import compare as compare_proposals
from experiments.text_comparison_v1.common import digest


def compare(relation, old_sections, new_sections, old_extractions, new_extractions):
    result = compare_proposals(relation, old_sections, new_sections, old_extractions, new_extractions)
    for c in result:
        if c["category"] != "ENGINEERING_CHANGE":
            continue
        a, b = c["old_facts"], c["new_facts"]
        reason = None
        if not a or not b:
            reason = "Отсутствие извлечённого факта не доказывает добавление или удаление"
        elif (len(a) != 1 or len(b) != 1 or
              a[0]["context_skeleton"] != b[0]["context_skeleton"]):
            reason = "Совпадение набора слов не доказывает равенство утверждений"
        elif any(s["status"] != "PROVEN" for s in old_sections + new_sections):
            reason = "Состав сопоставленного смыслового раздела требует проверки"
        if reason:
            c["proposal_type"] = c["type"]
            c["type"], c["category"], c["reason"] = "REVIEW", "REVIEW", reason
            c["change_id"] = "text_change_" + digest([relation["relation_id"],
                [f["fact_id"] for f in a], [f["fact_id"] for f in b], reason])[:24]
    return result
