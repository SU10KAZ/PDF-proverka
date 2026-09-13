"""Typed local slots; alignment and fact comparison are separate decisions.

Existing engineering lexicons are reused; no diagnostic-value-specific rule.
The entire assertion outside the typed slots must be identical for automatic
engineering output. Unexplained residuals and inferred absence abstain.
"""
from decimal import Decimal
import re
from experiments.text_comparison_v1.facts import (
    QUANTITY, COUNT, PIPE, FIRE, MATERIAL, MODE, MARK, ENGINEERING,
    normalized_unit, canonical_value,
)
from experiments.text_comparison_v1.common import digest
from .units import canonical_text

MODAL = re.compile(r"\b(?:не\s+допускается|не\s+долж(?:ен|на|но|ны)|допускается|запрещается|долж(?:ен|на|но|ны)|следует|необходимо|требуется)\b", re.I)
MATERIAL_ROOTS = ("полипропилен", "поливинилхлорид", "полимер", "стальн", "медн", "чугунн", "нержавеющ")


def analysis(text):
    raw = canonical_text(text)
    slots = []

    def add(m, prop, kind, value, unit=None, ambiguous=False):
        if any(a["span"][0] < m.end() and a["span"][1] > m.start() for a in slots):
            return
        slots.append({"span": list(m.span()), "quote": m.group(), "property": prop, "kind": kind,
                      "value": value, "unit": unit, "ambiguous": ambiguous})

    for m in PIPE.finditer(raw):
        add(m, "fan_coil_pipe_type", "PROPERTY_CHANGED", {"двух":"2", "четырех":"4"}.get(m["number"],m["number"]), "pipe")
    for m in COUNT.finditer(raw):
        stem = next((x for x in ("холодильн","чиллер","драйкул","фанкойл","насос","вентилятор") if m["entity"].startswith(x)),m["entity"])
        # Entity wording remains in the template: mask only its count.
        if not any(a["span"][0] < m.end() and a["span"][1] > m.start() for a in slots):
            start,end=m.span("number")
            slots.append({"span":[start,end],"quote":m["number"],"property":"equipment_count_"+stem,
                          "kind":"VALUE_CHANGED","value":m["number"],"unit":"count","ambiguous":False})
    for m in QUANTITY.finditer(raw):
        # Prevent tails of document identifiers and range/formula scalars.
        before = raw[max(0,m.start()-14):m.start()]
        if before and re.search(r"[\w.]$", before):
            continue
        unit,factor = normalized_unit(m["unit"])
        qual = canonical_text(m["qual"] or "")
        value = (qual+" "+canonical_value(m["number"],factor)).strip()
        ambiguity = bool(re.search(r"\d\s*[/–−-]\s*$",before) or re.search(r"\\(?:frac|cdot|times)|\$|�",raw))
        add(m, "quantity_"+unit,"VALUE_CHANGED",value,unit,ambiguity)
    for m in FIRE.finditer(raw):
        add(m,"fire_rating","PROPERTY_CHANGED",m["rating"].upper()+m["number"])
    for m in MATERIAL.finditer(raw):
        root = next((x for x in MATERIAL_ROOTS if m.group().startswith(x)),m.group())
        add(m,"material","PROPERTY_CHANGED",root)
    for m in MODE.finditer(raw):
        root = next((x for x in ("ручн","автоматическ","полностью автоматизирован","рециркуляци","прямоток","рекупераци") if m.group().startswith(x)),m.group())
        root = "automatic" if root in {"автоматическ","полностью автоматизирован"} else root
        add(m,"system_mode","SYSTEM_CHANGED",root)
    for m in MODAL.finditer(raw):
        value = "FORBIDDEN" if m.group().startswith(("не ","запрещ")) else "PERMITTED" if m.group()=="допускается" else "REQUIRED"
        add(m,"requirement_modality","REQUIREMENT_CHANGED",value)
    slots.sort(key=lambda s:s["span"])
    template = raw
    for s in reversed(slots):
        a,b=s["span"]
        template = template[:a]+"<"+s["property"]+">"+template[b:]
    return {"canonical":raw,"template":template,"slots":slots,
            "marks": sorted({canonical_text(m.group()) for m in MARK.finditer(raw)}),
            "engineering":bool(ENGINEERING.search(raw)),
            "words":re.findall(r"[а-яёa-z]{2,}",raw)}


def joined(units):
    return " ".join(u["text"] for u in units)


def compare(relation, old, new):
    old_text, new_text = joined(old), joined(new)
    a,b = analysis(old_text), analysis(new_text)
    common = {"alignment_id":relation["alignment_id"],"old_unit_ids":[u["unit_id"] for u in old],
              "new_unit_ids":[u["unit_id"] for u in new],
              "old_quote":old_text,"new_quote":new_text,
              "old_source_refs":[r for u in old for r in u["source_refs"]],
              "new_source_refs":[r for u in new for r in u["source_refs"]],
              "section_context":[s for u in (new or old) for s in u.get("section_context",[])],
              "engineering_entity_refs":sorted(set(a["marks"]+b["marks"])),
              "external_refs":sorted({r for u in old+new for r in u.get("external_refs",[])})}

    def emit(kind,reason,slot=None):
        item = {**common,"type":kind,"category":"ENGINEERING_CHANGE" if kind.endswith("_CHANGED") or kind in {"FACT_ADDED","FACT_REMOVED"} else kind,
                "reason":reason,"fact_delta":slot}
        item["change_id"]="change_"+digest([relation["alignment_id"],kind,reason,slot])[:24]
        return item

    if relation["decision"] != "SAME_ENGINEERING_SUBJECT":
        return [emit("REVIEW","Local alignment unresolved; absence does not establish addition/removal")]
    if a["canonical"] == b["canonical"]:
        return [emit("NO_SEMANTIC_CHANGE" if old_text==new_text else "EDITORIAL_CHANGE","Exact assertion after whitespace/markup/case/ё normalization")]
    # Terminal prose punctuation only. Decimal separators, signs, units, marks,
    # negation and word order are never erased by this editorial certificate.
    if a["canonical"].rstrip(".; ")==b["canonical"].rstrip(".; "):
        return [emit("EDITORIAL_CHANGE","Terminal prose punctuation only")]
    if (a["template"] != b["template"] or len(a["slots"]) != len(b["slots"]) or
            not a["engineering"] or a["marks"] != b["marks"]):
        return [emit("REVIEW","Aligned subject, but complete engineering equivalence/delta is not proven")]
    if any(s["ambiguous"] for s in a["slots"]+b["slots"]):
        return [emit("REVIEW","Range/formula or OCR numeric ambiguity requires source review")]
    if not relation.get("scope_supported"):
        return [emit("REVIEW","Exact assertion template lacks independent local scope support")]
    deltas=[]
    for i,(x,y) in enumerate(zip(a["slots"],b["slots"])):
        if (x["property"],x["unit"],x["kind"]) != (y["property"],y["unit"],y["kind"]):
            return [emit("REVIEW","Typed property mismatch")]
        if x["value"] != y["value"]:
            deltas.append(emit(x["kind"],"Same local subject and complete typed assertion template; explicit field differs",
                               {"slot_index":i,"property":x["property"],"unit":x["unit"],"old":x,"new":y}))
    return deltas or [emit("EDITORIAL_CHANGE","Same typed values and complete assertion template; surface representation differs")]
