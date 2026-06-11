# -*- coding: utf-8 -*-
"""Pipeline V2 — Mapping-aware Graphic Entity Alignment Preview (mark-only).

Классифицирует пары графических блоков OLD↔NEW по тому, действительно ли это
ОДНА инженерная сущность, или переименование / реорганизация состава /
mismatch. Нужно потому, что обычный entity-aware отбор кандидатов на vision
схлопывает «ВРУ-3 ↔ ВРУ-2» в `mismatch_likely`, не отличая:

  * `same_entity_likely`        — ВРУ-3 ↔ ВРУ-3 (можно в normal enrichment);
  * `possible_rename`           — та же сущность под другим именем + сильные
                                  признаки идентичности (нужна ручная сверка);
  * `scope_reorganized`         — та же family, номер конфликтует, аппараты/
                                  состав разные (реальная переработка проекта);
  * `mismatch_likely`           — разные сущности (ЯК↔ЩО, схема↔план, ОЗДС↔
                                  квартиры) — НЕ брать в enrichment;
  * `link_validation_candidate` — слабая/спорная связь, только для link-валидации.

Это **mark-only** аналитический слой: НЕ запускает vision, НЕ применяет связи,
НЕ создаёт замечаний. Только читает готовые Pipeline V2 артефакты и строит
`entity_alignment_preview_report.json`.

Переиспользует entity-извлечение и базовый scoring из
`pipeline_v2_graphic_vision_enrichment` (единая нормализация маркировок).
Только stdlib + те модули; никаких сетевых/LLM вызовов.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Optional

from .pipeline_v2_graphic_vision_enrichment import (
    extract_entity_ids,
    entity_identity_signal,
    sheet_kind_of,
    score_vision_candidate,
    _descriptor_for,
    _matched_graphic_index,
    _pages_by_number,
    _sheet_name_of,
    _domain_signature,
    _equipment_token_informative,
    _entity_families_of,
    CANDIDATE_SAME,
    CANDIDATE_MISMATCH,
)

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_entity_alignment_preview"

# ─── классы выравнивания ─────────────────────────────────────────────────────
ALIGN_SAME = "same_entity_likely"
ALIGN_RENAME = "possible_rename"
ALIGN_SCOPE = "scope_reorganized"
ALIGN_MISMATCH = "mismatch_likely"
ALIGN_LINK_VALIDATION = "link_validation_candidate"

_ACTION_BY_CLASS = {
    ALIGN_SAME: "use_for_enrichment",
    ALIGN_RENAME: "manual_mapping",
    ALIGN_SCOPE: "manual_mapping",
    ALIGN_MISMATCH: "exclude_from_enrichment",
    ALIGN_LINK_VALIDATION: "link_validation_only",
}
# классы, требующие ручного сопоставления (summary.needs_manual_mapping)
_MANUAL_CLASSES = {ALIGN_RENAME, ALIGN_SCOPE}

# Пороги корроборации rename vs reorg (на numbered_conflict парах). Решает
# сходство аппаратного состава, а не заголовка (заголовки шаблонные).
_RENAME_EQUIP_OVERLAP = 0.4      # информативный equipment-overlap
_RENAME_GROUNDED_OVERLAP = 0.4   # overlap grounded-сущностей блока
# Известные инженерные families (для извлечения primary label).
_KNOWN_FAMILIES = ("ГРЩ", "ВРУ", "ЩАО", "ЩО", "ЩР", "ЯК", "ЯУР", "АВР",
                   "ШК", "ШУ", "ОЗДС", "ИТП", "ТП", "РП", "РУ")

# Стоп-слова заголовка листа (бойлерплейт) для title-similarity.
_TITLE_STOP = {
    "часть", "внутреннее", "электроснабжение", "и", "освещение", "молниезащита",
    "заземление", "в", "т", "ч", "втч", "схема", "однолинейная", "расчетная",
    "расчётная", "принципиальная", "лист", "листов", "стадия", "оздс",
}


# ─── entity labels ───────────────────────────────────────────────────────────


def _label_from_id(eid: str) -> dict:
    """Из нормализованного id («ВРУ-3») → {raw,normalized,family,number}."""
    fam, _, tail = eid.partition("-")
    return {"normalized": eid, "family": fam, "number": (tail or None)}


def extract_entity_labels(*sources: Any) -> dict:
    """Извлечь маркировки сущностей из sheet_name/текста/токенов.

    Возвращает ``{labels: [...], primary, family, number, confidence}``.
    primary — наиболее специфичная нумерованная метка известной family
    (ВРУ-3 предпочтительнее голого ВРУ). confidence отражает наличие
    нумерованной метки известной family.
    """
    ids = extract_entity_ids(*sources)
    labels = sorted(ids)
    numbered = [i for i in labels if "-" in i]
    known_numbered = [i for i in numbered
                      if i.split("-", 1)[0] in _KNOWN_FAMILIES]
    primary_id = None
    if known_numbered:
        primary_id = sorted(known_numbered)[0]
        conf = 0.9
    elif numbered:
        primary_id = sorted(numbered)[0]
        conf = 0.7
    elif labels:
        # голая family (ГРЩ) — известная family как primary
        known_bare = [i for i in labels if i in _KNOWN_FAMILIES]
        primary_id = (sorted(known_bare)[0] if known_bare else sorted(labels)[0])
        conf = 0.6 if known_bare else 0.4
    else:
        return {"labels": [], "primary": None, "family": None,
                "number": None, "confidence": 0.0}
    info = _label_from_id(primary_id)
    return {"labels": labels, "primary": primary_id, "family": info["family"],
            "number": info["number"], "confidence": conf}


def _title_tokens(name: Any) -> set:
    s = str(name or "").lower().replace("ё", "е")
    toks = re.findall(r"[а-яa-z0-9]+", s)
    return {t for t in toks if t not in _TITLE_STOP and len(t) > 1}


def sheet_title_similarity(left: Any, right: Any) -> float:
    """Jaccard информативных токенов заголовков (бойлерплейт отброшен)."""
    a, b = _title_tokens(left), _title_tokens(right)
    if not a or not b:
        return 0.0
    return round(len(a & b) / len(a | b), 3)


# ─── grounded overlap (optional) ─────────────────────────────────────────────


def _grounding_index(grounding_report: Any) -> dict:
    """(left_block_id, right_block_id) → set нормализованных grounded value."""
    out: dict = {}
    rep = grounding_report if isinstance(grounding_report, dict) else {}
    for it in rep.get("items") or []:
        if not isinstance(it, dict):
            continue
        key = (it.get("left_block_id"), it.get("right_block_id"))
        vals: set = set()
        for fld in ("grounded_entities_old", "grounded_entities_new"):
            for e in it.get(fld) or []:
                if isinstance(e, dict) and e.get("status") in ("grounded",
                                                               "weakly_grounded"):
                    nv = str(e.get("normalized") or e.get("value") or "").strip().lower()
                    if nv:
                        vals.add(nv)
        if vals:
            out[key] = vals
    return out


# ─── classification ──────────────────────────────────────────────────────────


def _equip_overlap_informative(matched_entry: Optional[dict],
                               left_desc: dict, right_desc: dict) -> Optional[float]:
    me = matched_entry or {}
    overlap = (me.get("token_overlap") or {}).get("equipment")
    if not isinstance(overlap, (int, float)):
        return None
    lt = left_desc.get("tokens") if isinstance(left_desc.get("tokens"), dict) else {}
    rt = right_desc.get("tokens") if isinstance(right_desc.get("tokens"), dict) else {}
    informative = any(_equipment_token_informative(tok)
                      for tok in (lt.get("equipment") or []) + (rt.get("equipment") or []))
    return float(overlap) if informative else 0.0


def classify_entity_alignment(pair: dict, *, left_desc: dict, right_desc: dict,
                              matched_entry: Optional[dict] = None,
                              grounded_overlap: Optional[float] = None,
                              options: Optional[dict] = None) -> dict:
    """5-классовая классификация выравнивания пары графических блоков."""
    base = score_vision_candidate(pair, left_desc=left_desc, right_desc=right_desc,
                                  matched_entry=matched_entry)
    score = base["candidate_score"]
    reasons = list(base["candidate_reasons"])
    risks = list(base["candidate_risk_flags"])
    base_kind = base["candidate_kind"]

    lt = left_desc.get("tokens") if isinstance(left_desc.get("tokens"), dict) else {}
    rt = right_desc.get("tokens") if isinstance(right_desc.get("tokens"), dict) else {}
    left_ids = extract_entity_ids(left_desc.get("sheet_name"),
                                  lt.get("equipment"), lt.get("raw_key_entities"))
    right_ids = extract_entity_ids(right_desc.get("sheet_name"),
                                   rt.get("equipment"), rt.get("raw_key_entities"))
    identity = entity_identity_signal(left_ids, right_ids)
    primary = entity_identity_signal(extract_entity_ids(left_desc.get("sheet_name")),
                                     extract_entity_ids(right_desc.get("sheet_name")))
    if primary in ("match", "numbered_conflict", "family_conflict"):
        identity = primary

    lk = sheet_kind_of(left_desc.get("sheet_name"))
    rk = sheet_kind_of(right_desc.get("sheet_name"))
    sheet_kind_mismatch = bool(lk and rk and lk != rk)
    family_conflict = (identity == "family_conflict")
    ldom, rdom = _domain_signature(left_desc), _domain_signature(right_desc)
    domain_mismatch = bool(ldom and rdom and not (ldom & rdom))

    title_sim = sheet_title_similarity(left_desc.get("sheet_name"),
                                       right_desc.get("sheet_name"))
    equip_overlap = _equip_overlap_informative(matched_entry, left_desc, right_desc)
    lg_t, rg_t = left_desc.get("graphic_type"), right_desc.get("graphic_type")
    type_match = (matched_entry or {}).get("graphic_type_match")
    if type_match is None and lg_t and rg_t:
        type_match = (lg_t == rg_t)
    disc_match = (matched_entry or {}).get("discipline_match")
    if disc_match is None:
        ld_d, rd_d = left_desc.get("discipline"), right_desc.get("discipline")
        if ld_d and rd_d:
            disc_match = (ld_d == rd_d)

    cls_reasons: list[str] = []
    # ── решение ─────────────────────────────────────────────
    if identity == "match" and not sheet_kind_mismatch:
        cls = ALIGN_SAME
        cls_reasons.append("entity_id_match")
        confidence = max(0.7, score)
    elif family_conflict or sheet_kind_mismatch or (domain_mismatch and identity != "match"):
        cls = ALIGN_MISMATCH
        if family_conflict:
            cls_reasons.append("entity_family_conflict")
        if sheet_kind_mismatch:
            cls_reasons.append(f"sheet_kind_mismatch:{lk}/{rk}")
        if domain_mismatch:
            cls_reasons.append("domain_mismatch")
        confidence = 0.85
    elif identity == "numbered_conflict":
        # та же family, разные номера → rename vs реорганизация.
        # Решает СХОДСТВО АППАРАТНОГО СОСТАВА (equipment/grounded overlap), а НЕ
        # сходство заголовка: заголовки листов шаблонные и отличаются только
        # номером, поэтому title_sim ненадёжен (его держим лишь в evidence).
        corrob = []
        if equip_overlap is not None and equip_overlap >= _RENAME_EQUIP_OVERLAP:
            corrob.append(f"equipment_overlap:{equip_overlap:.2f}")
        if grounded_overlap is not None and grounded_overlap >= _RENAME_GROUNDED_OVERLAP:
            corrob.append(f"grounded_overlap:{grounded_overlap:.2f}")
        type_ok = (type_match is not False)
        disc_ok = (disc_match is not False)
        if corrob and type_ok and disc_ok:
            cls = ALIGN_RENAME
            cls_reasons.append("numbered_conflict_with_identity_corroboration")
            cls_reasons.extend(corrob)
            confidence = 0.55
        else:
            cls = ALIGN_SCOPE
            cls_reasons.append("numbered_conflict_no_strong_corroboration")
            if not corrob:
                cls_reasons.append("apparatus_overlap_low")
            confidence = 0.6
    elif base_kind == CANDIDATE_SAME:
        # family_only_match с высоким score (ГРЩ↔ГРЩ)
        cls = ALIGN_SAME
        cls_reasons.append("entity_family_match_high_score")
        confidence = max(0.6, score)
    elif base_kind == CANDIDATE_MISMATCH:
        cls = ALIGN_MISMATCH
        cls_reasons.append("base_classifier_mismatch")
        confidence = 0.75
    else:
        cls = ALIGN_LINK_VALIDATION
        cls_reasons.append("weak_or_ambiguous_link")
        confidence = 0.4

    return {
        "classification": cls,
        "confidence": round(confidence, 3),
        "candidate_score": score,
        "reasons": cls_reasons + reasons,
        "risk_flags": risks,
        "recommended_action": _ACTION_BY_CLASS[cls],
        "evidence": {
            "sheet_title_similarity": title_sim,
            "entity_id_match": (identity == "match"),
            "entity_family_match": bool(_entity_families_of(left_ids)
                                        & _entity_families_of(right_ids)),
            "numbered_entity_conflict": (identity == "numbered_conflict"),
            "discipline_match": disc_match,
            "graphic_type_match": type_match,
            "visual_status": pair.get("status"),
            "grounded_entities_overlap": (round(grounded_overlap, 3)
                                          if isinstance(grounded_overlap, (int, float))
                                          else None),
            "equipment_overlap_informative": (round(equip_overlap, 3)
                                              if isinstance(equip_overlap, (int, float))
                                              else None),
        },
    }


# ─── report ──────────────────────────────────────────────────────────────────


def _graphic_blocks_with_labels(descriptor_report: Any) -> dict:
    """block_id → {sheet_name, graphic_type, label}. Только нумерованные / known."""
    out: dict = {}
    rep = descriptor_report if isinstance(descriptor_report, dict) else {}
    for d in rep.get("descriptors") or []:
        if not isinstance(d, dict):
            continue
        bid = d.get("block_id")
        if not bid:
            continue
        t = d.get("tokens") if isinstance(d.get("tokens"), dict) else {}
        lab = extract_entity_labels(d.get("sheet_name"), t.get("equipment"),
                                    t.get("raw_key_entities"))
        out[bid] = {"sheet_name": d.get("sheet_name"),
                    "graphic_type": d.get("graphic_type"),
                    "label": lab.get("primary"), "family": lab.get("family")}
    return out


def build_entity_alignment_preview_report(
        left_model: Any, right_model: Any, visual_gate_report: Any, *,
        block_matching_report: Any = None, block_link_preview_report: Any = None,
        left_graphic_report: Any = None, right_graphic_report: Any = None,
        graphic_matched_report: Any = None, grounding_report: Any = None,
        options: Optional[dict] = None) -> dict:
    """Построить ``entity_alignment_preview_report`` (mark-only, fail-soft)."""
    warnings: list[str] = []
    gate = visual_gate_report if isinstance(visual_gate_report, dict) else None
    if gate is None or not isinstance(gate.get("block_pairs"), list):
        return {
            "version": REPORT_VERSION, "kind": REPORT_KIND,
            "status": "completed_with_warnings",
            "summary": {k: 0 for k in (
                "graphic_pairs_total", "same_entity_likely", "possible_rename",
                "scope_reorganized", "mismatch_likely", "link_validation_candidate",
                "needs_manual_mapping")},
            "pairs": [], "unpaired_entities": {"left": [], "right": []},
            "warnings": warnings + ["visual gate report unavailable — entity "
                                    "alignment preview skipped"],
        }

    for name, rep in (("left_graphic_descriptor_report", left_graphic_report),
                      ("right_graphic_descriptor_report", right_graphic_report)):
        if not isinstance(rep, dict) or not rep.get("descriptors"):
            warnings.append(f"{name} missing/empty — labels degrade to gate-only")

    matched_idx = _matched_graphic_index(graphic_matched_report)
    grounding_idx = _grounding_index(grounding_report)
    left_pages = _pages_by_number(left_model)
    right_pages = _pages_by_number(right_model)

    pairs_out: list[dict] = []
    counts = {ALIGN_SAME: 0, ALIGN_RENAME: 0, ALIGN_SCOPE: 0,
              ALIGN_MISMATCH: 0, ALIGN_LINK_VALIDATION: 0}
    matched_left_labels: set = set()
    matched_right_labels: set = set()

    for bp in gate.get("block_pairs"):
        if not isinstance(bp, dict):
            continue
        lid, rid = bp.get("left_block_id"), bp.get("right_block_id")
        ld = _descriptor_for(left_graphic_report, lid)
        rd = _descriptor_for(right_graphic_report, rid)
        # sheet_name fallback из normalized model
        if not ld.get("sheet_name"):
            ld = {**ld, "sheet_name": _sheet_name_of(left_pages, bp.get("left_page_number"))}
        if not rd.get("sheet_name"):
            rd = {**rd, "sheet_name": _sheet_name_of(right_pages, bp.get("right_page_number"))}
        me = matched_idx.get((lid, rid))
        g_vals = grounding_idx.get((lid, rid))
        g_overlap = None
        if g_vals:
            # overlap внутри одного блок-пэйра не информативен (old∪new) —
            # оставляем None, кроме случая будущего cross-block расчёта
            g_overlap = None

        try:
            cls = classify_entity_alignment(bp, left_desc=ld, right_desc=rd,
                                            matched_entry=me, grounded_overlap=g_overlap,
                                            options=options)
        except Exception as exc:  # noqa: BLE001 — одна пара не валит слой
            warnings.append(f"pair {lid}↔{rid}: {type(exc).__name__}: {exc}")
            continue

        l_lab = extract_entity_labels(ld.get("sheet_name"),
                                      (ld.get("tokens") or {}).get("equipment"))
        r_lab = extract_entity_labels(rd.get("sheet_name"),
                                      (rd.get("tokens") or {}).get("equipment"))
        c = cls["classification"]
        counts[c] += 1
        if c in (ALIGN_SAME, ALIGN_RENAME):
            if l_lab.get("primary"):
                matched_left_labels.add(l_lab["primary"])
            if r_lab.get("primary"):
                matched_right_labels.add(r_lab["primary"])

        fam = (l_lab.get("family") or r_lab.get("family"))
        pairs_out.append({
            "pair_key": bp.get("pair_key") or f"{lid}__{rid}",
            "left_block_id": lid, "right_block_id": rid,
            "left_page_number": bp.get("left_page_number"),
            "right_page_number": bp.get("right_page_number"),
            "left_sheet_name": ld.get("sheet_name"),
            "right_sheet_name": rd.get("sheet_name"),
            "left_entity_label": l_lab.get("primary"),
            "right_entity_label": r_lab.get("primary"),
            "entity_family": fam,
            "classification": c,
            "confidence": cls["confidence"],
            "reasons": cls["reasons"],
            "risk_flags": cls["risk_flags"],
            "recommended_action": cls["recommended_action"],
            "evidence": cls["evidence"],
        })

    # unpaired entities (numbered/known labels без пары same/rename)
    left_blocks = _graphic_blocks_with_labels(left_graphic_report)
    right_blocks = _graphic_blocks_with_labels(right_graphic_report)

    def _unpaired(blocks: dict, matched_labels: set) -> list[dict]:
        seen: dict = {}
        for bid, info in blocks.items():
            lab = info.get("label")
            if not lab or lab in matched_labels:
                continue
            if lab not in seen:
                seen[lab] = {"entity_label": lab, "family": info.get("family"),
                             "graphic_type": info.get("graphic_type"),
                             "sheet_name": info.get("sheet_name"),
                             "block_ids": []}
            seen[lab]["block_ids"].append(bid)
        return sorted(seen.values(), key=lambda x: str(x["entity_label"]))

    unpaired = {"left": _unpaired(left_blocks, matched_left_labels),
                "right": _unpaired(right_blocks, matched_right_labels)}

    summary = {
        "graphic_pairs_total": len(pairs_out),
        "same_entity_likely": counts[ALIGN_SAME],
        "possible_rename": counts[ALIGN_RENAME],
        "scope_reorganized": counts[ALIGN_SCOPE],
        "mismatch_likely": counts[ALIGN_MISMATCH],
        "link_validation_candidate": counts[ALIGN_LINK_VALIDATION],
        "needs_manual_mapping": counts[ALIGN_RENAME] + counts[ALIGN_SCOPE],
        "unpaired_left": len(unpaired["left"]),
        "unpaired_right": len(unpaired["right"]),
    }
    status = "completed_with_warnings" if warnings else "ok"
    return {
        "version": REPORT_VERSION, "kind": REPORT_KIND, "status": status,
        "summary": summary, "pairs": pairs_out,
        "unpaired_entities": unpaired, "warnings": warnings,
    }


def entity_alignment_by_pair_key(report: Any) -> dict:
    """Индекс pair_key → классификация (для будущего wiring в selection)."""
    out: dict = {}
    if not isinstance(report, dict):
        return out
    for p in report.get("pairs") or []:
        if isinstance(p, dict) and p.get("pair_key"):
            out[p["pair_key"]] = p
    return out


def write_entity_alignment_preview_report(out_path: str | Path, report: dict) -> Path:
    """Атомарно записать отчёт (tmp + ``os.replace``)."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    fd, tmp_name = tempfile.mkstemp(
        prefix=out_path.name + ".", suffix=".tmp", dir=str(out_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, out_path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return out_path


__all__ = [
    "REPORT_VERSION", "REPORT_KIND",
    "ALIGN_SAME", "ALIGN_RENAME", "ALIGN_SCOPE", "ALIGN_MISMATCH",
    "ALIGN_LINK_VALIDATION",
    "extract_entity_labels", "sheet_title_similarity",
    "classify_entity_alignment", "build_entity_alignment_preview_report",
    "entity_alignment_by_pair_key", "write_entity_alignment_preview_report",
]
