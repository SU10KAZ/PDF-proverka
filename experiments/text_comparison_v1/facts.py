"""Provenance-bearing narrative facts and conservative atomic changes.

Reuses Blueprint A's explicit field/owner/provenance discipline, not its table
producer: that producer intentionally excludes narrative and has page-pair scope.
"""
from collections import Counter, defaultdict
from decimal import Decimal
import re

from .common import digest, norm, section_text, similarity

NUMBER = r"[+−-]?\d+(?:[.,]\d+)?"
UNIT = r"(?:кВт|kW|Вт|W)\s*/\s*(?:м|m)[²2]|(?:м|m)[³3]\s*/\s*(?:ч|h|с|s)|(?:кВт|kW|Вт|W|МПа|кПа|Па|MPa|kPa|Pa|мм|mm|м2|м²|м3|м³|°\s*[CС]|кВ|В|А|%)(?![а-яa-z])"
QUANTITY = re.compile(rf"(?P<qual>не\s+(?:менее|более|ниже|выше)\s+|[<>≤≥]\s*)?(?P<number>{NUMBER})\s*(?P<unit>{UNIT})", re.I)
COUNT = re.compile(r"(?P<number>\d+)(?:\s*[-–]?\s*[хx])?\s+(?P<entity>холодильн\w*\s+машин\w*|чиллер\w*|драйкул\w*|фанкойл\w*|насос\w*|вентилятор\w*)", re.I)
PIPE = re.compile(r"(?P<number>[24]|двух|четырех|четырёх)(?:\s*[-–]?\s*[хx])?\s*[-–]?\s*трубн\w*\s+фанкойл\w*", re.I)
FIRE = re.compile(r"\b(?P<rating>REI|EI|RE|R|E)\s*(?P<number>\d{2,3})\b", re.I)
MATERIAL = re.compile(r"\b(?:полипропилен\w*|поливинилхлорид\w*|полимер\w*|стальн\w*|медн\w*|чугунн\w*|нержавеющ\w*\s+стал\w*)\b", re.I)
MODE = re.compile(r"\b(?:ручн\w*\s+режим\w*|автоматическ\w*\s+режим\w*|полностью\s+автоматизирован\w*|рециркуляци\w*|прямоток\w*|рекупераци\w*\s+тепла)\b", re.I)
REQUIREMENT = re.compile(r"\b(?:не\s+допускается|допускается|запрещается|следует|долж\w*|необходимо|требуется|предусмотр\w*|предусматрива\w*)\b", re.I)
MARK = re.compile(r"\b(?:ПВ|П|В|ДВ|ПД|ВРУ|ГРЩ|ФК|FCU|AHU|CH)[-\s]?\d+(?:[.-]\d+)*\b", re.I)
ENGINEERING = re.compile(r"температур|давлен|расход|мощност|нагрузк|потребност|производительност|тепл|холод|вентил|кондицион|фанкойл|труб|насос|кабел|воздух|электр|огнестойк|монтаж|установк|материал|систем", re.I)


def normalized_unit(raw):
    u = norm(raw).replace(" ", "").replace("с", "c")
    mapping = {"квт": ("W", 1000), "kw": ("W", 1000), "вт": ("W", 1), "w": ("W", 1),
               "вт/м2": ("W/m2", 1), "w/m2": ("W/m2", 1), "квт/м2": ("W/m2", 1000),
               "kw/m2": ("W/m2", 1000), "па": ("Pa", 1), "pa": ("Pa", 1),
               "кпа": ("Pa", 1000), "kpa": ("Pa", 1000), "мпа": ("Pa", 1000000),
               "mpa": ("Pa", 1000000), "°c": ("C", 1), "м3/ч": ("m3/h", 1),
               "m3/h": ("m3/h", 1), "м3/c": ("m3/h", 3600), "m3/s": ("m3/h", 3600)}
    return mapping.get(u, (u, 1))


def canonical_value(number, factor=1):
    return format((Decimal(number.replace(",", ".").replace("−", "-")) * factor).normalize(), "f")


def sentences(section):
    """Assemble whitespace-transparent text; map every character interval to ledger refs."""
    text, spans = "", []
    for b in section["ordered_text_blocks"]:
        clean = re.sub(r"[*_]", "", b["text"])
        # Bullets preserve list-item semantic separation. Other line wrapping does not.
        prefix = "\n" if re.match(r"\s*[-•]\s+", clean) else " "
        start = len(text) + len(prefix)
        text += prefix + clean
        spans.append((start, len(text), b))
    separator = re.compile(r"(?<=[.!?;])\s+(?=[А-ЯA-ZЁ-])|\n(?=\s*[-•]\s+)")
    starts = [0] + [m.end() for m in separator.finditer(text)]
    ends = [m.start() for m in separator.finditer(text)] + [len(text)]
    for start, end in zip(starts, ends):
        raw = text[start:end].strip()
        if not raw:
            continue
        sources, status = [], "PROVEN"
        for a, b, block in spans:
            if a < end and b > start:
                sources.extend(block["source_refs"])
                if block["ownership_status"] != "PROVEN":
                    status = "REVIEW"
        yield {"text": raw, "source_refs": sources, "status": status}


def extract(section):
    facts, residuals = [], []
    for sentence in sentences(section):
        raw = sentence["text"]
        context = norm(raw)
        matches = []
        pipe_spans = []
        for m in PIPE.finditer(raw):
            val = {"двух": "2", "четырех": "4", "четырёх": "4"}.get(m["number"].casefold(), m["number"])
            matches.append((m, "fan_coil_pipe_type", val, "pipe", "PROPERTY"))
            pipe_spans.append(m.span())
        for m in COUNT.finditer(raw):
            if not any(a <= m.start() < b for a, b in pipe_spans):
                stem = next((stem for stem in ("холодильн", "чиллер", "драйкул", "фанкойл", "насос", "вентилятор")
                             if m["entity"].casefold().startswith(stem)), "unknown")
                matches.append((m, "equipment_count_" + stem, m["number"], "count", "VALUE"))
        for m in QUANTITY.finditer(raw):
            unit, factor = normalized_unit(m["unit"])
            value = (norm(m["qual"] or "") + " " + canonical_value(m["number"], factor)).strip()
            prop = {"W": "power", "W/m2": "specific_load", "Pa": "pressure", "C": "temperature",
                    "m3/h": "flow"}.get(unit, "quantity_" + unit)
            matches.append((m, prop, value, unit, "VALUE"))
        for m in FIRE.finditer(raw):
            matches.append((m, "fire_rating", m["rating"].upper() + m["number"], None, "PROPERTY"))
        for pattern, prop, kind in ((MATERIAL, "material", "PROPERTY"), (MODE, "system_mode", "SYSTEM")):
            for m in pattern.finditer(raw):
                matches.append((m, prop, norm(m.group()), None, kind))
        # Requirements with recognized values are represented by those facts plus
        # their complete requirement context, avoiding an extra mega-fact.
        requirement = REQUIREMENT.search(raw)
        if not matches and requirement and ENGINEERING.search(raw):
            matches.append((requirement, "installation_requirement", context, None, "REQUIREMENT"))
        if not matches:
            residuals.append(sentence)
            continue
        # A skeleton establishes the same asserted property independent of values.
        # All recognized values are masked together; units normalize separately.
        spans = sorted({m.span() for m, *_ in matches}, reverse=True)
        skeleton = raw
        for a, b in spans:
            skeleton = skeleton[:a] + " <VALUE> " + skeleton[b:]
        for m, prop, value, unit, kind in matches:
            masked = norm(skeleton)
            # Local entity/location remains in the skeleton and exact quotes.
            marks = sorted(set(norm(x.group()) for x in MARK.finditer(raw)))
            fact = {"section_key": section["section_key"], "section_instance": section["instance_id"],
                    "document_version": section["document_version"], "property": prop, "value": value,
                    "unit": unit, "kind": kind, "entity_marks": marks,
                    "scope": "SECTION_LOCAL", "owner_status": sentence["status"],
                    "context_skeleton": masked, "quote": raw, "value_quote": m.group(),
                    "source_refs": sentence["source_refs"], "producer_version": "text-fact-v1.0.0"}
            # A tail of a range/formula is not an independent scalar assertion.
            prefix = raw[max(0, m.start() - 12):m.start()]
            ambiguous_numeric = kind == "VALUE" and (
                bool(re.search(r"\d\s*[/–−-]\s*$", prefix)) or
                bool(re.search(r"\\(?:frac|cdot|times)|\$|[�]", raw)) or
                (prop in {"power", "specific_load", "flow"} and value.startswith("-")))
            if ambiguous_numeric:
                fact["owner_status"] = "REVIEW"
                fact["native_recovery_request"] = "NUMBER_UNIT_OR_SYMBOL_AMBIGUITY"
            fact["fact_key"] = "fact_" + digest([section["section_key"], prop, unit, masked, marks])[:24]
            fact["fact_id"] = "fact_instance_" + digest([section["document_version"], fact["fact_key"], value, raw])[:24]
            facts.append(fact)
    # A repeated sentence is one asserted fact with all its source witnesses.
    dedup = {}
    for f in facts:
        key = (f["fact_key"], f["value"])
        if key not in dedup:
            dedup[key] = f
        else:
            seen = {digest(r) for r in dedup[key]["source_refs"]}
            dedup[key]["source_refs"].extend(r for r in f["source_refs"] if digest(r) not in seen)
            if f["owner_status"] != "PROVEN":
                dedup[key]["owner_status"] = "REVIEW"
    return {"facts": list(dedup.values()), "unparsed_sentences": residuals,
            "quality": {"extracted": len(dedup), "review_facts": sum(f["owner_status"] != "PROVEN" for f in dedup.values()),
                        "unparsed_sentences": len(residuals), "provenance_missing": sum(not f["source_refs"] for f in dedup.values()),
                        "human_precision": None, "human_recall": None}}


def compare(relation, old_sections, new_sections, old_extractions, new_extractions):
    old = [f for s in old_extractions for f in s["facts"]]
    new = [f for s in new_extractions for f in s["facts"]]
    changes = []

    def emit(kind, category, a, b, reason):
        changes.append({"change_id": "text_change_" + digest([relation["relation_id"], kind,
                        [f["fact_id"] for f in a], [f["fact_id"] for f in b], reason])[:24],
                        "type": kind, "category": category, "relation_id": relation["relation_id"],
                        "old_facts": a, "new_facts": b, "reason": reason,
                        "old_section_refs": [{"section_key": s["section_key"], "instance_id": s["instance_id"],
                            "title": s["section_title"], "page_span": s["page_span"], "document_version": s["document_version"]} for s in old_sections],
                        "new_section_refs": [{"section_key": s["section_key"], "instance_id": s["instance_id"],
                            "title": s["section_title"], "page_span": s["page_span"], "document_version": s["document_version"]} for s in new_sections]})

    if relation["status"] != "PROVEN":
        emit("REVIEW", "REVIEW", [], [], "Связь разделов требует проверки")
        return changes
    used_a, used_b = set(), set()
    # Identity is property + context, never proximity or ordinal of a number.
    candidates = {}
    for i, a in enumerate(old):
        for j, b in enumerate(new):
            if (a["property"], a["unit"], a["entity_marks"]) != (b["property"], b["unit"], b["entity_marks"]):
                continue
            score = similarity(a["context_skeleton"], b["context_skeleton"])
            if a["context_skeleton"] == b["context_skeleton"]:
                score = 1.0
            if score >= .72:
                candidates[i, j] = score
    # Equal assertions first: multiple repetitions across split/merged sections
    # cannot consume a changed assertion by greedy fuzzy pairing.
    for i, a in enumerate(old):
        js = [j for j, b in enumerate(new) if j not in used_b and candidates.get((i, j)) == 1
              and a["value"] == b["value"]]
        if len(js) == 1:
            used_a.add(i)
            used_b.add(js[0])
    ranks_a, ranks_b = defaultdict(list), defaultdict(list)
    for (i, j), score in candidates.items():
        if i not in used_a and j not in used_b:
            ranks_a[i].append((score, j))
            ranks_b[j].append((score, i))
    for values in list(ranks_a.values()) + list(ranks_b.values()):
        values.sort(reverse=True)
    for i, ranks in sorted(ranks_a.items()):
        score, j = ranks[0]
        if (ranks_b[j][0][1] != i or (len(ranks) > 1 and score - ranks[1][0] < .12)
                or (len(ranks_b[j]) > 1 and score - ranks_b[j][1][0] < .12)):
            continue
        a, b = old[i], new[j]
        used_a.add(i)
        used_b.add(j)
        if a["value"] == b["value"] and a["context_skeleton"] == b["context_skeleton"]:
            continue
        # Fuzzy association is a candidate, never engineering proof.
        proven = score == 1 and a["owner_status"] == b["owner_status"] == "PROVEN"
        kind = {"VALUE": "VALUE_CHANGED", "PROPERTY": "PROPERTY_CHANGED", "SYSTEM": "SYSTEM_CHANGED",
                "REQUIREMENT": "REQUIREMENT_CHANGED"}[a["kind"]]
        emit(kind if proven else "REVIEW", "ENGINEERING_CHANGE" if proven else "REVIEW", [a], [b],
             "Изменилось явно указанное свойство" if proven else "Нужно подтвердить соответствие фактов и смысл формулировки")
    for side, fs, used, opposite in (("OLD", old, used_a, new_sections), ("NEW", new, used_b, old_sections)):
        for i, f in enumerate(fs):
            if i in used:
                continue
            # A failed match does not prove addition/removal in reworded text.
            proven_absence = relation["kind"] == "NO_RELATION" and f["owner_status"] == "PROVEN"
            kind = "FACT_REMOVED" if side == "OLD" else "FACT_ADDED"
            emit(kind if proven_absence else "REVIEW", "ENGINEERING_CHANGE" if proven_absence else "REVIEW",
                 [f] if side == "OLD" else [], [f] if side == "NEW" else [],
                 "Раздел отсутствует в другой версии" if proven_absence else "Факт без однозначного соответствия; возможны добавление, удаление или переформулировка")
    atext = " ".join(section_text(s) for s in old_sections)
    btext = " ".join(section_text(s) for s in new_sections)
    # Sentence order is editorial; word order within a sentence can change the
    # subject/object and must never be normalized to an unordered word bag.
    bag_a = Counter(norm(x["text"]).strip(" .;!?") for s in old_sections for x in sentences(s))
    bag_b = Counter(norm(x["text"]).strip(" .;!?") for s in new_sections for x in sentences(s))
    if not changes and bag_a == bag_b:
        if atext != btext:
            emit("NO_SEMANTIC_CHANGE", "EDITORIAL_CHANGE", [], [], "Изменились порядок или оформление при том же составе текста")
        else:
            emit("NO_SEMANTIC_CHANGE", "NO_SEMANTIC_CHANGE", [], [], "Текст совпадает без учёта страниц")
    else:
        ra = Counter(norm(x["text"]) for e in old_extractions for x in e["unparsed_sentences"])
        rb = Counter(norm(x["text"]) for e in new_extractions for x in e["unparsed_sentences"])
        if ra != rb:
            emit("REVIEW", "REVIEW", [], [], "Изменён текст, для которого ещё нет надёжного извлечения инженерных фактов")
    return changes
