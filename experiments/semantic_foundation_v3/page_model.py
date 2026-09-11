"""V1 page contract with document-local, once-per-page facts.

The front matter expressions and evidence construction are factored verbatim
from frozen V1; the classifier and SHEET identity functions are imported unchanged.
"""
from collections import Counter, defaultdict
import re

from . import frozen_v1 as v1


def front_matter(number, joined, table_lines):
    types = []
    if re.search(r"\bсодержание(?:\s+тома)?\b|\bоглавление\b", joined) or (
        re.search(r"\bсостав\s+(?:тома|раздела|проектной документации)\b", joined)
        and table_lines >= 3
    ):
        types.append("CONTENTS")
    if re.search(r"\bзаверени[ея]\s+(?:проектной|рабочей)\s+документации\b|\bудостоверяю\b", joined):
        types.append("CERTIFICATION")
    if re.search(r"\b(?:ведомость|таблица|регистрация)\s+(?:регистрации\s+)?изменен", joined) or re.search(r"\bразрешение\s+на\s+внесение\s+изменен", joined):
        types.append("REVISION_REGISTER")
    cover_signal = re.search(r"\b(?:проектная|рабочая)\s+документация\b", joined)
    cover_detail = re.search(r"\bтом\s+\d|\bраздел\s+\d|\bкнига\s+\d|\bглавн(?:ый|ого)\s+(?:инженер|архитектор)\b", joined)
    if number <= 3 and cover_signal and cover_detail and not any(x in types for x in ("CONTENTS", "REVISION_REGISTER")):
        types.append("COVER_OR_TITLE")
    return sorted(set(types))


class PageModel:
    def __init__(self, ledger, raw):
        raw_pages = {int(p["page_index"]) + 1: p for p in raw.get("pages", [])}
        raw_blocks = defaultdict(list)
        for b in raw.get("blocks", []):
            raw_blocks[int(b.get("page_index", -1)) + 1].append(b)
        facts = {}
        self.heading_info = {}
        for number, page in sorted(ledger.pages.items()):
            ids = ledger.page_lines.get(number, [])
            for i in ids:
                self.heading_info[i] = v1.heading_info(ledger.lines[i])
            count = sum(bool(v1.TABLE_ROW_RE.match(ledger.lines[i].text)) for i in ids)
            joined = "\n".join(ledger.columns["normalized_text"][i] for i in ids)
            facts[number] = {"stats": {"content_lines": len(ids), "table_lines": count,
                                      "table_line_ratio": round(count / len(ids), 6) if ids else 0.0},
                             "front": front_matter(number, joined, count), "joined": joined}
        self.pages = {}
        for number, page in sorted(ledger.pages.items()):
            ids = ledger.page_lines.get(number, [])
            raw_page, blocks, fact = raw_pages.get(number, {}), raw_blocks[number], facts[number]
            counts = Counter(str(b.get("block_type", "unknown")) for b in blocks)
            areas = defaultdict(float)
            for b in blocks:
                areas[str(b.get("block_type", "unknown"))] += v1.rect_area(b.get("coords_norm"))
            graphic, total = areas.get("image", 0.0) + areas.get("graphic", 0.0), sum(areas.values())
            width, height = int(raw_page.get("width_px") or 0), int(raw_page.get("height_px") or 0)
            aspect = width / height if height else 0.0
            stamps = [b.stamp for b in page.blocks if b.stamp]
            stamp = max(stamps, key=lambda x: sum(bool(v) for v in x.values()), default={})
            evidence = {
                "physical_page": number, "source_available": True, "front_matter_types": fact["front"],
                "has_stamp_block": bool(counts.get("stamp")), "title_block": stamp,
                "page_geometry": {"width_px": width, "height_px": height, "rotation": int(raw_page.get("rotation") or 0),
                                  "aspect_ratio": round(aspect, 6), "landscape": aspect > v1.THRESHOLDS["landscape_ratio"]},
                "block_composition": {"counts": dict(sorted(counts.items())),
                                      "normalized_rectangle_area": {k: round(v, 6) for k, v in sorted(areas.items())},
                                      "graphic_area_ratio": round(graphic / total if total else 0.0, 6)},
                "markdown_structure": {**fact["stats"],
                                       "headings": [self.heading_info[i]["title"] for i in ids if self.heading_info[i]],
                                       "first_content": [ledger.clean[i] for i in ids[:3]],
                                       "last_content": [ledger.clean[i] for i in ids[-3:]]},
                "graphic_leaf_terms": [t for t in v1.GRAPHIC_LEAF_TERMS if t in fact["joined"]],
                "adjacent_page_signals": [{"physical_page": n, **facts[n]["stats"], "front_matter_types": facts[n]["front"]}
                                          for n in (number - 1, number + 1) if n in facts],
            }
            self.pages[number] = {"evidence": evidence, "classification": v1.classify_evidence(evidence)}
        self.counters = {"page_facts_computed": len(facts), "front_matter_computed": len(facts),
                         "source_normalizations": len(ledger.lines)}

    def eligible(self, number):
        c = self.pages[number]["classification"]
        return c["unit_type"] in {"TEXT_SECTION", "TABLE"} and "FRONT_MATTER_EXCLUDED" not in c["reason_codes"]

    def sheets(self, ledger):
        units, routes = [], []
        d = ledger.document
        for n, model in self.pages.items():
            c, ev, p = model["classification"], model["evidence"], ledger.pages[n]
            if c["unit_type"] != "SHEET":
                continue
            provenance = {"document_code": d["document_code"], "document_version": d["document_version"],
                          "version_id": d["version_id"], "physical_page": n}
            blocks = [b.block_id for b in p.blocks]
            units.append({"unit_id": v1.sheet_identity(d["document_version"], p, ev), "unit_type": "SHEET",
                          "scope": "SHEET_BOUNDED_ARTIFACT", "source_pages": [n], "blocks": blocks,
                          "confidence": c["confidence"], "status": c["status"], "reason_codes": c["reason_codes"],
                          "evidence": ev, "provenance": provenance, "producer_version": v1.PRODUCER_VERSION})
            routes.append({"unit_id": v1.semantic_key("page_route", {"document_version": d["document_version"], "source_page": n}),
                           "unit_type": "SHEET", "page_classifier_type": "SHEET", "scope": "PHYSICAL_PAGE_ROUTING_SURFACE",
                           "source_pages": [n], "blocks": blocks, "child_unit_ids": [], "confidence": c["confidence"],
                           "status": c["status"], "reason_codes": c["reason_codes"],
                           "evidence_refs": {"markdown": d["artifacts"]["work_md"]["path"], "blocks": d["artifacts"]["blocks"]["path"],
                                             "physical_page": n, "evidence_digest": v1.digest(ev)},
                           "page_document_provenance": provenance, "producer_version": v1.PRODUCER_VERSION})
        return {"units": sorted(units, key=lambda x: x["unit_id"]), "page_routing": routes}
