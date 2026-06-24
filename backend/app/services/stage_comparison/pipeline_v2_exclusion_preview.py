# -*- coding: utf-8 -*-
"""Pipeline V2 — Exclusion Preview v2 (mark-only).

Объединяет уже посчитанные Pipeline V2 сигналы (entity alignment, manual
mapping, link validation, visual gate, block link preview, grounded evidence,
delta explanation) в ОДИН предварительный список:

    candidate_exclude          — пару нельзя гонять в block-to-block enrichment
    review_only                — отправить на ручную проверку
    keep                       — можно оставить для enrichment
    link_validation_required   — перед анализом нужна link-validation

Это СТРОГО mark-only слой:

* НЕ запускает модели (Qwen/Gemma/Opus/Claude) — читает готовые артефакты;
* НЕ изменяет входные отчёты;
* НЕ применяет block links, не создаёт замечаний, не делает skip/enforce;
* каждый item: ``auto_apply=False``, ``enforce_allowed=False``,
  ``use_as_grounded_fact=False``.

Отчёт — отправная точка для будущего КОНТРОЛИРУЕМОГО enforce/skip, который
здесь НЕ реализуется.

См. docs/stage_comparison_pipeline_v2_exclusion_preview.md.
"""
from __future__ import annotations

import json
import unicodedata
from pathlib import Path
from typing import Any, Optional

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_exclusion_preview"

# классы решения
CLS_EXCLUDE = "candidate_exclude"
CLS_REVIEW = "review_only"
CLS_KEEP = "keep"
CLS_LINK_VALIDATION = "link_validation_required"
_CLASSES = (CLS_EXCLUDE, CLS_REVIEW, CLS_KEEP, CLS_LINK_VALIDATION)

# recommended_action
ACT_EXCLUDE = "exclude_from_enrichment"
ACT_REVIEW = "manual_review"
ACT_KEEP = "keep_for_enrichment"
ACT_LINK_VALIDATION = "run_link_validation"

# link-validation verdicts
_LV_REJECT = "reject_mapping"
_LV_VALID = "valid_mapping"
_LV_MANUAL_REVIEW = "manual_review"
_REL_DIFFERENT = "different_entity"
_REL_POSITIVE = frozenset({"same_entity", "renamed_same_entity",
                           "reorganized_same_entity"})
_REL_UNCERTAIN = "uncertain"

# manual decisions
_MANUAL_CONFIRM = frozenset({"confirmed_same_entity", "confirmed_rename"})
_MANUAL_REORG = "confirmed_reorganized"
_MANUAL_NEGATIVE = frozenset({"rejected_mapping", "no_match"})

# entity-alignment classifications
_ALIGN_SAME = "same_entity_likely"
_ALIGN_SCOPE = "scope_reorganized"
_ALIGN_MISMATCH = "mismatch_likely"
_ALIGN_LV_CAND = "link_validation_candidate"
_ALIGN_RENAME = "possible_rename"

_STR_CAP = 240
_LIST_CAP = 16

# имена входных артефактов (optional/fail-soft)
_INPUT_FILENAMES = {
    "entity_alignment": "entity_alignment_preview_report.json",
    "overrides": "entity_mapping_overrides.json",
    "link_validation": "link_validation_report.json",
    "visual_gate": "visual_equivalence_gate_report.json",
    "block_link_preview": "block_link_preview_report.json",
    "grounded_evidence": "grounded_evidence_report.json",
    "delta_explanation": "delta_explanation_report.json",
    "graphic_vision": "graphic_vision_enrichment_report.json",
    "graphic_vision_grounding": "graphic_vision_grounding_report.json",
    "entity_diff": "entity_diff_report.json",
}

DEFAULT_OPTIONS = {
    "enabled": False,                  # dry-run stage default OFF
    "repeated_reject_min": 2,          # сколько reject на transition → repeated
    "high_confidence_threshold": 0.9,
}


# ─── helpers ─────────────────────────────────────────────────────────────────


def _clean(value: Any, cap: int = _STR_CAP) -> Optional[str]:
    if value is None:
        return None
    s = value if isinstance(value, str) else str(value)
    s = s.strip()
    if not s:
        return None
    return s if len(s) <= cap else s[: cap - 1] + "…"


def _norm_label(value: Any) -> Optional[str]:
    s = _clean(value, 80)
    if not s:
        return None
    s = unicodedata.normalize("NFKC", s).replace("ё", "е").replace("Ё", "Е")
    return " ".join(s.lower().split())


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _dedup(seq: list) -> list:
    out: list = []
    for x in seq:
        if x is not None and x not in out:
            out.append(x)
    return out[:_LIST_CAP]


def _block_pair_key(left_bid: Any, right_bid: Any,
                    left_label: Any, right_label: Any) -> str:
    lb, rb = _clean(left_bid, 80), _clean(right_bid, 80)
    if lb or rb:
        return f"bp::{lb or '∅'}__{rb or '∅'}"
    return f"ent::{_norm_label(left_label) or '∅'}__{_norm_label(right_label) or '∅'}"


# ─── candidate accumulation ──────────────────────────────────────────────────


def _blank(key: str) -> dict:
    return {
        "key": key, "left_block_id": None, "right_block_id": None,
        "left_entity_label": None, "right_entity_label": None,
        "left_page_number": None, "right_page_number": None,
        "entity_family": None, "target_type": None,
        "source_signals": [], "reasons": [], "risk_flags": [], "evidence_refs": [],
        "manual_mapping": {}, "link_validation": {},
        # raw signals used by classifier
        "_lv_decision": None, "_lv_relation": None, "_lv_confidence": None,
        "_lv_conflict": False, "_manual_decision": None,
        "_align_class": None, "_align_confidence": None,
        "_visual_status": None, "_has_lv": False, "_has_align_lv_cand": False,
    }


def _merge_labels(cand: dict, lb, rb, ll, rl, lp, rp, fam):
    if cand["left_block_id"] is None and _clean(lb, 80):
        cand["left_block_id"] = _clean(lb, 80)
    if cand["right_block_id"] is None and _clean(rb, 80):
        cand["right_block_id"] = _clean(rb, 80)
    if cand["left_entity_label"] is None and _clean(ll, 80):
        cand["left_entity_label"] = _clean(ll, 80)
    if cand["right_entity_label"] is None and _clean(rl, 80):
        cand["right_entity_label"] = _clean(rl, 80)
    if cand["left_page_number"] is None and lp is not None:
        cand["left_page_number"] = lp
    if cand["right_page_number"] is None and rp is not None:
        cand["right_page_number"] = rp
    if cand["entity_family"] is None and _clean(fam, 60):
        cand["entity_family"] = _clean(fam, 60)


def _ingest_link_validation(cands: dict, report: Any):
    if not isinstance(report, dict):
        return
    for it in report.get("items") or []:
        if not isinstance(it, dict):
            continue
        v = it.get("validation") if isinstance(it.get("validation"), dict) else {}
        a = it.get("agreement") if isinstance(it.get("agreement"), dict) else {}
        key = _block_pair_key(it.get("left_block_id"), it.get("right_block_id"),
                              it.get("left_entity_label"), it.get("right_entity_label"))
        c = cands.setdefault(key, _blank(key))
        _merge_labels(c, it.get("left_block_id"), it.get("right_block_id"),
                      it.get("left_entity_label"), it.get("right_entity_label"),
                      it.get("left_page_number"), it.get("right_page_number"), None)
        dec = _clean(v.get("decision"), 40)
        rel = _clean(v.get("entity_relation"), 40)
        c["_has_lv"] = True
        c["_lv_decision"] = dec or c["_lv_decision"]
        c["_lv_relation"] = rel or c["_lv_relation"]
        c["_lv_confidence"] = _num(v.get("confidence")) or c["_lv_confidence"]
        if a.get("conflicts_with_manual_mapping") is True:
            c["_lv_conflict"] = True
        if it.get("manual_decision"):
            c["_manual_decision"] = c["_manual_decision"] or _clean(it.get("manual_decision"), 40)
        c["link_validation"] = {
            "decision": dec, "entity_relation": rel,
            "confidence": _num(v.get("confidence")),
            "conflicts_with_manual_mapping": bool(a.get("conflicts_with_manual_mapping")),
        }
        c["source_signals"].append(f"link_validation:{dec or 'none'}")
        if rel:
            c["reasons"].append(f"vision relation {rel}")


def _ingest_overrides(cands: dict, report: Any):
    if not isinstance(report, dict):
        return
    groups = (("mappings", None), ("rejected", "rejected_mapping"),
              ("no_match", "no_match"))
    for field, forced in groups:
        rows = report.get(field)
        if not isinstance(rows, list):
            continue
        for ov in rows:
            if not isinstance(ov, dict):
                continue
            decision = forced or _clean(ov.get("manual_decision") or ov.get("decision"), 40)
            key = _block_pair_key(ov.get("left_block_id"), ov.get("right_block_id"),
                                  ov.get("left_entity_label"), ov.get("right_entity_label"))
            c = cands.setdefault(key, _blank(key))
            _merge_labels(c, ov.get("left_block_id"), ov.get("right_block_id"),
                          ov.get("left_entity_label"), ov.get("right_entity_label"),
                          ov.get("left_page_number"), ov.get("right_page_number"), None)
            if decision:
                c["_manual_decision"] = decision
                c["manual_mapping"] = {"decision": decision,
                                       "mapping_id": _clean(ov.get("mapping_id"), 60)}
                c["source_signals"].append(f"manual_override:{decision}")


def _ingest_alignment(cands: dict, report: Any):
    if not isinstance(report, dict):
        return
    for p in report.get("pairs") or []:
        if not isinstance(p, dict):
            continue
        cls = _clean(p.get("classification"), 40)
        key = _block_pair_key(p.get("left_block_id"), p.get("right_block_id"),
                              p.get("left_entity_label"), p.get("right_entity_label"))
        c = cands.setdefault(key, _blank(key))
        _merge_labels(c, p.get("left_block_id"), p.get("right_block_id"),
                      p.get("left_entity_label"), p.get("right_entity_label"),
                      p.get("left_page_number"), p.get("right_page_number"),
                      p.get("entity_family"))
        c["_align_class"] = cls or c["_align_class"]
        c["_align_confidence"] = _num(p.get("confidence")) or c["_align_confidence"]
        if cls == _ALIGN_LV_CAND:
            c["_has_align_lv_cand"] = True
        if cls:
            c["source_signals"].append(f"entity_alignment:{cls}")
        for rf in (p.get("risk_flags") or [])[:_LIST_CAP]:
            c["risk_flags"].append(_clean(rf, 60))


def _ingest_visual_gate(cands: dict, report: Any):
    if not isinstance(report, dict):
        return
    for bp in report.get("block_pairs") or []:
        if not isinstance(bp, dict):
            continue
        key = _block_pair_key(bp.get("left_block_id"), bp.get("right_block_id"),
                              None, None)
        c = cands.get(key)
        if c is None:
            continue   # visual gate не создаёт новые candidate'ы сам по себе
        st = _clean(bp.get("status"), 40)
        if st:
            c["_visual_status"] = st
            c["source_signals"].append(f"visual_gate:{st}")


def _ingest_evidence_refs(cands: dict, ge_report: Any, de_report: Any):
    """Best-effort: прикрепить ссылки grounded evidence / delta explanation к
    кандидату по совпадению block_id (только трассировка, не классификатор)."""
    bid_to_keys: dict = {}
    for c in cands.values():
        for bid in (c["left_block_id"], c["right_block_id"]):
            if bid:
                bid_to_keys.setdefault(bid, set()).add(c["key"])

    def _attach(report, origin, list_field, ref_fields):
        if not isinstance(report, dict):
            return
        for row in report.get(list_field) or []:
            if not isinstance(row, dict):
                continue
            blob = json.dumps(row, ensure_ascii=False)
            for bid, keys in bid_to_keys.items():
                if bid and bid in blob:
                    ref = {"source": origin}
                    for f in ref_fields:
                        if row.get(f) is not None:
                            ref[f] = _clean(row.get(f), 120)
                    for k in keys:
                        cands[k]["evidence_refs"].append(ref)
                        cands[k]["source_signals"].append(f"{origin}:present")

    _attach(ge_report, "grounded_evidence", "delta_evidence", ("delta_id", "verdict"))
    _attach(de_report, "delta_explanation", "explanations", ("delta_id", "critic_verdict"))


# ─── classification ──────────────────────────────────────────────────────────


def _classify(c: dict, repeated_reject: bool, high_conf: float) -> dict:
    """Вернуть {classification, severity, recommended_action, confidence,
    reasons[], risk_flags[]} для одного кандидата. mark-only."""
    reasons: list[str] = []
    risk: list[str] = []
    lv_dec = c["_lv_decision"]
    lv_rel = c["_lv_relation"]
    lv_conf = c["_lv_confidence"]
    lv_conflict = c["_lv_conflict"]
    manual = c["_manual_decision"]
    align = c["_align_class"]
    visual = c["_visual_status"]
    has_lv = c["_has_lv"]

    is_lv_reject = (lv_dec == _LV_REJECT) or (lv_rel == _REL_DIFFERENT)
    is_lv_valid = (lv_dec == _LV_VALID) or (lv_rel in _REL_POSITIVE)
    is_lv_unsure = (lv_dec == _LV_MANUAL_REVIEW) or (lv_rel == _REL_UNCERTAIN)

    cls = None
    sev = "medium"
    act = ACT_REVIEW
    conf = lv_conf if lv_conf is not None else (c["_align_confidence"] or 0.5)

    # 1. manual negative (rejected/no_match) — высокий приоритет
    if manual in _MANUAL_NEGATIVE:
        cls = CLS_EXCLUDE
        act = ACT_EXCLUDE
        sev = "high" if (is_lv_reject or manual == "rejected_mapping") else "medium"
        conf = max(conf, 0.85)
        reasons.append(f"manual decision {manual}")
    # 2. link-validation reject / different_entity
    elif has_lv and is_lv_reject:
        cls = CLS_EXCLUDE
        reasons.append(f"link_validation {lv_dec or lv_rel}")
        if lv_conflict or manual in _MANUAL_CONFIRM or manual == _MANUAL_REORG:
            # manual подтверждал, но vision отвергает — НЕ молчаливый exclude
            risk.append("manual_vision_conflict")
            sev = "high"
            act = ACT_REVIEW
            reasons.append(f"manual {manual} conflicts with vision reject")
        else:
            act = ACT_EXCLUDE
            sev = "high" if (repeated_reject or (lv_conf or 0) >= high_conf) else "medium"
    # 3. link-validation valid → keep
    elif has_lv and is_lv_valid:
        cls = CLS_KEEP
        act = ACT_KEEP
        sev = "low"
        reasons.append(f"link_validation {lv_dec or lv_rel}")
    # 4. link-validation uncertain / manual_review → review_only
    elif has_lv and is_lv_unsure:
        cls = CLS_REVIEW
        act = ACT_REVIEW
        sev = "medium"
        reasons.append(f"link_validation {lv_dec or lv_rel or 'uncertain'}")
    # 5. manual confirmed_reorganized без валидации → нужна link-validation
    elif manual == _MANUAL_REORG:
        cls = CLS_LINK_VALIDATION
        act = ACT_LINK_VALIDATION
        sev = "medium"
        reasons.append("manual confirmed_reorganized, no link_validation yet")
    # 6. manual confirmed same/rename без валидации → keep
    elif manual in _MANUAL_CONFIRM:
        cls = CLS_KEEP
        act = ACT_KEEP
        sev = "low"
        reasons.append(f"manual {manual}")
    # 7-11. entity-alignment classification (без vision-результата)
    elif align == _ALIGN_MISMATCH:
        cls = CLS_EXCLUDE
        act = ACT_REVIEW          # детерминированный сигнал без vision → на проверку
        sev = "medium"
        reasons.append("entity_alignment mismatch_likely (no vision confirmation)")
    elif align in (_ALIGN_SCOPE, _ALIGN_LV_CAND, _ALIGN_RENAME):
        cls = CLS_LINK_VALIDATION
        act = ACT_LINK_VALIDATION
        sev = "medium"
        reasons.append(f"entity_alignment {align}, needs link_validation")
    elif align == _ALIGN_SAME:
        cls = CLS_KEEP
        act = ACT_KEEP
        sev = "low"
        reasons.append("entity_alignment same_entity_likely")
    # 12. визуал изменился, связь неясна → review
    elif visual and "changed" in visual:
        cls = CLS_REVIEW
        act = ACT_REVIEW
        sev = "medium"
        reasons.append(f"visual {visual}, entity relation unclear")
    # 13. fallback
    else:
        cls = CLS_REVIEW
        act = ACT_REVIEW
        sev = "low"
        reasons.append("weak/contradictory signals")

    # repeated reject transition boost
    if repeated_reject and cls == CLS_EXCLUDE:
        risk.append("repeated_reject_mapping_transition")
        sev = "high"
        conf = min(0.99, (conf or 0.0) + 0.05)
        reasons.append("same transition rejected on multiple block pairs")

    return {"classification": cls, "severity": sev, "recommended_action": act,
            "confidence": round(conf, 3) if conf is not None else None,
            "reasons": reasons, "risk_flags": risk}


def _target_type(c: dict) -> str:
    if c["_has_lv"]:
        return "block_pair"
    if c["_has_align_lv_cand"]:
        return "vision_candidate"
    return "entity_pair"


# ─── repeated transition aggregation (§6) ────────────────────────────────────


def detect_repeated_reject_transitions(cands: list[dict], *, min_count: int = 2) -> set:
    """Группировка по нормализованному переходу (left→right, family). Возвращает
    множество ключей-кандидатов, попадающих в переход с ≥min_count reject."""
    groups: dict = {}
    for c in cands:
        if not ((c["_lv_decision"] == _LV_REJECT) or (c["_lv_relation"] == _REL_DIFFERENT)):
            continue
        tkey = (_norm_label(c["left_entity_label"]),
                _norm_label(c["right_entity_label"]),
                _norm_label(c["entity_family"]))
        if tkey[0] is None and tkey[1] is None:
            continue
        groups.setdefault(tkey, []).append(c["key"])
    repeated = set()
    for tkey, keys in groups.items():
        if len(keys) >= min_count:
            repeated.update(keys)
    return repeated


# ─── report assembly ─────────────────────────────────────────────────────────


def build_exclusion_preview_report(
        *, session_id: Optional[str], pair_id: Optional[str],
        entity_alignment_report: Any = None, overrides_report: Any = None,
        link_validation_report: Any = None, visual_gate_report: Any = None,
        block_link_preview_report: Any = None, grounded_evidence_report: Any = None,
        delta_explanation_report: Any = None, graphic_vision_report: Any = None,
        graphic_vision_grounding_report: Any = None, entity_diff_report: Any = None,
        created_at: Optional[str] = None, options: Optional[dict] = None,
        warnings: Optional[list[str]] = None) -> dict:
    """Собрать mark-only exclusion-preview из готовых артефактов. Не запускает
    модели, не меняет входы. Отсутствующие артефакты → warning, отчёт строится."""
    opts = {**DEFAULT_OPTIONS, **(options or {})}
    warns = list(warnings or [])
    min_count = int(opts.get("repeated_reject_min") or 2)
    high_conf = float(opts.get("high_confidence_threshold") or 0.9)

    for label, rep in (("entity_alignment_preview", entity_alignment_report),
                       ("link_validation", link_validation_report),
                       ("visual_equivalence_gate", visual_gate_report),
                       ("entity_mapping_overrides", overrides_report)):
        if not isinstance(rep, dict):
            warns.append(f"input artifact missing or unreadable: {label}")

    cands: dict = {}
    _ingest_link_validation(cands, link_validation_report)
    _ingest_overrides(cands, overrides_report)
    _ingest_alignment(cands, entity_alignment_report)
    _ingest_visual_gate(cands, visual_gate_report)
    _ingest_evidence_refs(cands, grounded_evidence_report, delta_explanation_report)

    cand_list = list(cands.values())
    repeated_keys = detect_repeated_reject_transitions(cand_list, min_count=min_count)

    items: list[dict] = []
    for c in cand_list:
        verdict = _classify(c, c["key"] in repeated_keys, high_conf)
        item = {
            "item_id": f"xp_{c['key']}",
            "target_type": _target_type(c),
            "left_block_id": c["left_block_id"], "right_block_id": c["right_block_id"],
            "left_entity_label": c["left_entity_label"],
            "right_entity_label": c["right_entity_label"],
            "left_page_number": c["left_page_number"],
            "right_page_number": c["right_page_number"],
            "entity_family": c["entity_family"],
            "classification": verdict["classification"],
            "confidence": verdict["confidence"],
            "severity": verdict["severity"],
            "recommended_action": verdict["recommended_action"],
            "source_signals": _dedup(c["source_signals"]),
            "reasons": _dedup(c["reasons"] + verdict["reasons"]),
            "risk_flags": _dedup([r for r in c["risk_flags"] if r] + verdict["risk_flags"]),
            "evidence_refs": c["evidence_refs"][:_LIST_CAP],
            "manual_mapping": c["manual_mapping"] or {},
            "link_validation": c["link_validation"] or {},
            # ИНВАРИАНТЫ mark-only слоя
            "use_as_grounded_fact": False,
            "auto_apply": False,
            "enforce_allowed": False,
        }
        items.append(item)

    # сортировка: exclude → review → link_validation → keep; внутри — high severity первым
    cls_order = {CLS_EXCLUDE: 0, CLS_REVIEW: 1, CLS_LINK_VALIDATION: 2, CLS_KEEP: 3}
    sev_order = {"high": 0, "medium": 1, "low": 2}
    items.sort(key=lambda i: (cls_order.get(i["classification"], 9),
                              sev_order.get(i["severity"], 9),
                              -(i["confidence"] or 0.0)))

    def _count(pred):
        return sum(1 for i in items if pred(i))
    summary = {
        "items_total": len(items),
        "candidate_exclude": _count(lambda i: i["classification"] == CLS_EXCLUDE),
        "review_only": _count(lambda i: i["classification"] == CLS_REVIEW),
        "keep": _count(lambda i: i["classification"] == CLS_KEEP),
        "link_validation_required": _count(
            lambda i: i["classification"] == CLS_LINK_VALIDATION),
        "high_confidence_exclude": _count(
            lambda i: i["classification"] == CLS_EXCLUDE and i["severity"] == "high"),
        "manual_override_present": _count(lambda i: bool(i["manual_mapping"])),
        "manual_vision_conflict": _count(
            lambda i: "manual_vision_conflict" in i["risk_flags"]),
        "repeated_reject_transitions": _count(
            lambda i: "repeated_reject_mapping_transition" in i["risk_flags"]),
        "auto_enforce_enabled": False,
    }

    status = "completed_with_warnings" if warns else "ok"
    return {
        "version": REPORT_VERSION, "kind": REPORT_KIND, "status": status,
        "session_id": session_id, "pair_id": pair_id, "created_at": created_at,
        "summary": summary, "items": items,
        "warnings": [w for w in warns if isinstance(w, str)][:40],
    }


# ─── disk I/O (read-only inputs, optional output) ────────────────────────────


def _read_json(path: Path) -> Any:
    try:
        if path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return None
    return None


def load_pipeline_v2_artifacts(out_dir: str | Path) -> tuple[dict, list[str]]:
    """Прочитать входные артефакты из каталога (read-only, fail-soft)."""
    out_dir = Path(out_dir)
    data: dict = {}
    warns: list[str] = []
    for key, name in _INPUT_FILENAMES.items():
        rep = _read_json(out_dir / name)
        data[key] = rep
        if rep is None:
            warns.append(f"input artifact not found/unreadable: {name}")
    return data, warns


def run_pipeline_v2_exclusion_preview(
        out_dir: str | Path, *, session_id: Optional[str] = None,
        pair_id: Optional[str] = None, created_at: Optional[str] = None,
        options: Optional[dict] = None,
        output_path: Optional[str | Path] = None) -> dict:
    """Загрузить артефакты из ``out_dir`` и построить exclusion-preview. Пишет
    отчёт только если задан ``output_path`` (иначе чистый build). НИКОГДА не
    меняет входные артефакты."""
    data, warns = load_pipeline_v2_artifacts(out_dir)
    report = build_exclusion_preview_report(
        session_id=session_id, pair_id=pair_id, created_at=created_at,
        entity_alignment_report=data.get("entity_alignment"),
        overrides_report=data.get("overrides"),
        link_validation_report=data.get("link_validation"),
        visual_gate_report=data.get("visual_gate"),
        block_link_preview_report=data.get("block_link_preview"),
        grounded_evidence_report=data.get("grounded_evidence"),
        delta_explanation_report=data.get("delta_explanation"),
        graphic_vision_report=data.get("graphic_vision"),
        graphic_vision_grounding_report=data.get("graphic_vision_grounding"),
        entity_diff_report=data.get("entity_diff"),
        options=options, warnings=warns)
    if output_path is not None:
        write_exclusion_preview_report(output_path, report)
    return report


def write_exclusion_preview_report(out_path: str | Path, report: dict) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    return out_path


__all__ = [
    "REPORT_VERSION", "REPORT_KIND",
    "CLS_EXCLUDE", "CLS_REVIEW", "CLS_KEEP", "CLS_LINK_VALIDATION",
    "DEFAULT_OPTIONS",
    "build_exclusion_preview_report", "detect_repeated_reject_transitions",
    "load_pipeline_v2_artifacts", "run_pipeline_v2_exclusion_preview",
    "write_exclusion_preview_report",
]
