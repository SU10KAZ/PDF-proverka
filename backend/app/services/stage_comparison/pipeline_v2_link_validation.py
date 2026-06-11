# -*- coding: utf-8 -*-
"""Pipeline V2 — Link Validation Report (mark-only, vision-validation мэппинга).

Отдельный слой ПОВЕРХ manual entity mapping overrides / entity alignment:
проверяет через vision, действительно ли ручная пара OLD↔NEW является одной
реорганизованной сущностью или это РАЗНЫЕ сущности, и сверяет вердикт vision с
ручным решением инженера (agreement / conflict).

```text
entity_mapping_overrides.json (confirmed_reorganized …)
  → select link-validation candidates (selection_mode=link_validation)
  → build_link_validation_prompt (validation-oriented, НЕ enrichment)
  → [injectable runner] vision → parse → agreement vs manual_decision
  → link_validation_report.json (mark-only)
```

Жёсткие инварианты:

* **mark-only**: НЕ применяет block links, НЕ меняет enrichment/grounding/
  grounded_evidence/delta_explanation/findings;
* каждый item: ``use_as_grounded_fact=False`` и ``use_for_delta_explanation=False``
  ВСЕГДА — результат link-validation не является grounded-фактом;
* runner ИНЪЕКТИРУЕТСЯ (контракт ``runner(prompt, left_image_path,
  right_image_path, options) -> dict``); модуль сам НЕ создаёт vision-моделей и
  НЕ делает сетевых вызовов. ``runner=None`` → ``status=skipped_no_runner``
  (кандидаты построены, модель не вызвана);
* модуль НЕ пишет runtime сам по себе — только если явно передан ``output_path``;
* fail-soft: битый ответ одного item → ``failed`` item, не исключение.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Callable, Optional

from backend.app.services.stage_comparison.pipeline_v2_graphic_vision_enrichment import (
    CANDIDATE_MANUAL_REORG,
    select_vision_candidates_v2,
)
from backend.app.services.stage_comparison.pipeline_v2_entity_mapping_overrides import (
    index_overrides_for_lookup,
)

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_link_validation"

LinkValidationRunner = Callable[[str, Optional[str], Optional[str], dict], Any]

# контракт vision-ответа
_VALID_RELATIONS = ("same_entity", "renamed_same_entity",
                    "reorganized_same_entity", "different_entity", "uncertain")
_VALID_DECISIONS = ("valid_mapping", "manual_review", "reject_mapping")
_POSITIVE_RELATIONS = frozenset(
    {"same_entity", "renamed_same_entity", "reorganized_same_entity"})

# ручные решения, которые УТВЕРЖДАЮТ «это одна сущность»
_CONFIRM_DECISIONS = frozenset(
    {"confirmed_same_entity", "confirmed_rename", "confirmed_reorganized"})
# ручные решения, которые УТВЕРЖДАЮТ «это НЕ валидная пара»
_NEGATIVE_DECISIONS = frozenset({"rejected_mapping", "no_match"})

# recommended_action
ACTION_KEEP = "keep_mapping"
ACTION_REVIEW = "manual_review_mapping"
ACTION_REJECT = "reject_mapping_candidate"

_STR_CAP = 300
_LIST_CAP = 12

DEFAULT_LINK_VALIDATION_OPTIONS = {
    "enabled": False,                       # dry-run: default OFF (нужен runner)
    "candidate_kinds": ["manual_confirmed_reorganized"],  # MVP scope
    "max_items": 8,
}


# ─── prompt ──────────────────────────────────────────────────────────────────

LINK_VALIDATION_PROMPT_TEMPLATE = """Ты сравниваешь два графических блока OLD и NEW из проектной документации.
Задача — НЕ искать все отличия, а определить, являются ли эти блоки одной и той же \
инженерной сущностью после переименования/реорганизации, либо это разные сущности.

Контекст:
- OLD (старая стадия): {left_label}{left_page}
- NEW (новая стадия): {right_label}{right_page}

Правила:
- Не выдумывать номиналы и маркировки.
- Не делать вывод только по названию листа.
- Сохранять разделение OLD и NEW (не путать стороны).
- Если аппараты/цепи явно разные — manual_review или reject_mapping.
- Если видно, что это реорганизованная связанная сущность — reorganized_same_entity.
- Если качество чтения недостаточно — uncertain/manual_review.
- Не использовать результат как grounded fact для delta explanation.

Ответ — СТРОГО один JSON-объект без пояснений вокруг:
{{
  "old_new_orientation_ok": true,
  "entity_relation": "same_entity|renamed_same_entity|reorganized_same_entity|different_entity|uncertain",
  "confidence": 0.0,
  "decision": "valid_mapping|manual_review|reject_mapping",
  "old_entity_label": "...",
  "new_entity_label": "...",
  "supporting_visual_evidence": [],
  "conflicting_visual_evidence": [],
  "key_devices_old": [],
  "key_devices_new": [],
  "notable_changes": [],
  "risks": [],
  "do_not_use_as_fact": true
}}"""


def build_link_validation_prompt(*, left_label: Any = None, right_label: Any = None,
                                 left_page: Any = None, right_page: Any = None) -> str:
    """Validation-oriented prompt (НЕ enrichment)."""
    return LINK_VALIDATION_PROMPT_TEMPLATE.format(
        left_label=_clean(left_label) or "[метка не указана]",
        right_label=_clean(right_label) or "[метка не указана]",
        left_page=f", стр. {left_page}" if left_page is not None else "",
        right_page=f", стр. {right_page}" if right_page is not None else "",
    )


# ─── helpers ─────────────────────────────────────────────────────────────────


def _clean(value: Any, cap: int = _STR_CAP) -> Optional[str]:
    if value is None:
        return None
    s = value if isinstance(value, str) else str(value)
    s = s.strip()
    if not s:
        return None
    return s if len(s) <= cap else s[: cap - 1] + "…"


def _str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for v in value[:_LIST_CAP]:
        s = _clean(v)
        if s:
            out.append(s)
    return out


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return default


def _coerce_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return round(float(value), 3)
    if isinstance(value, str):
        try:
            return round(float(value.strip()), 3)
        except ValueError:
            return None
    return None


def _extract_json(text: str) -> Optional[dict]:
    """Достать первый JSON-объект из текста (без сетевых зависимостей)."""
    if not isinstance(text, str):
        return None
    s = text.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?", "", s).strip()
        s = re.sub(r"```$", "", s).strip()
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:  # noqa: BLE001
        pass
    start = s.find("{")
    if start < 0:
        return None
    depth = 0
    for i in range(start, len(s)):
        c = s[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                try:
                    obj = json.loads(s[start:i + 1])
                    return obj if isinstance(obj, dict) else None
                except Exception:  # noqa: BLE001
                    return None
    return None


def parse_link_validation_response(raw: Any) -> Optional[dict]:
    """Нормализовать ответ runner'а в validation-dict (или None при сбое).

    ``raw`` может быть: уже распарсенный dict; dict с полем
    ``content``/``raw``/``text`` (строка-JSON); или строка-JSON.
    """
    payload: Optional[dict] = None
    if isinstance(raw, dict):
        if any(k in raw for k in ("entity_relation", "decision",
                                  "old_new_orientation_ok")):
            payload = raw
        else:
            for k in ("content", "raw", "text", "response"):
                v = raw.get(k)
                if isinstance(v, str):
                    payload = _extract_json(v)
                    if payload:
                        break
                elif isinstance(v, dict) and "entity_relation" in v:
                    payload = v
                    break
    elif isinstance(raw, str):
        payload = _extract_json(raw)
    if not isinstance(payload, dict):
        return None

    relation = _clean(payload.get("entity_relation"))
    relation = relation if relation in _VALID_RELATIONS else "uncertain"
    decision = _clean(payload.get("decision"))
    decision = decision if decision in _VALID_DECISIONS else "manual_review"
    return {
        "old_new_orientation_ok": _coerce_bool(
            payload.get("old_new_orientation_ok"), default=True),
        "entity_relation": relation,
        "decision": decision,
        "confidence": _coerce_float(payload.get("confidence")),
        "old_entity_label": _clean(payload.get("old_entity_label")),
        "new_entity_label": _clean(payload.get("new_entity_label")),
        "supporting_visual_evidence": _str_list(payload.get("supporting_visual_evidence")),
        "conflicting_visual_evidence": _str_list(payload.get("conflicting_visual_evidence")),
        "key_devices_old": _str_list(payload.get("key_devices_old")),
        "key_devices_new": _str_list(payload.get("key_devices_new")),
        "notable_changes": _str_list(payload.get("notable_changes")),
        "risks": _str_list(payload.get("risks")),
        # link-validation НИКОГДА не является grounded-фактом
        "do_not_use_as_fact": True,
    }


# ─── agreement ───────────────────────────────────────────────────────────────


def compute_agreement(manual_decision: Optional[str], validation: dict) -> dict:
    """Сверить ручное решение и vision-вердикт → agreement + recommended_action.

    Возвращает ``{agrees_with_manual_mapping, conflicts_with_manual_mapping,
    orientation_failed, reason, recommended_action}``.
    """
    relation = validation.get("entity_relation")
    decision = validation.get("decision")
    orientation_ok = validation.get("old_new_orientation_ok") is not False

    is_positive = relation in _POSITIVE_RELATIONS and decision == "valid_mapping"
    is_negative = relation == "different_entity" or decision == "reject_mapping"
    is_uncertain = relation == "uncertain" or decision == "manual_review"

    agrees = conflicts = False
    if not orientation_ok:
        # перепутанные стороны → доверять вердикту нельзя, только на ручную проверку
        reason = "vision orientation OLD/NEW failed — verdict unreliable"
        action = ACTION_REVIEW
    elif manual_decision in _CONFIRM_DECISIONS:
        if is_negative:
            conflicts = True
            reason = (f"manual {manual_decision} but validation "
                      f"{decision}/{relation}")
            action = ACTION_REVIEW
        elif is_positive:
            agrees = True
            reason = f"manual {manual_decision} confirmed by validation {decision}/{relation}"
            action = ACTION_KEEP
        else:   # uncertain / manual_review
            reason = f"validation {decision}/{relation} — inconclusive"
            action = ACTION_REVIEW
    elif manual_decision in _NEGATIVE_DECISIONS:
        if is_negative:
            agrees = True
            reason = f"manual {manual_decision} confirmed by validation {decision}/{relation}"
            action = ACTION_REJECT
        elif is_positive:
            conflicts = True
            reason = (f"manual {manual_decision} but validation says "
                      f"{decision}/{relation}")
            action = ACTION_REVIEW
        else:
            reason = f"validation {decision}/{relation} — inconclusive"
            action = ACTION_REVIEW
    else:
        reason = f"no/unknown manual decision ({manual_decision!r})"
        action = ACTION_REVIEW

    return {
        "agrees_with_manual_mapping": agrees,
        "conflicts_with_manual_mapping": conflicts,
        "orientation_failed": not orientation_ok,
        "reason": reason,
        "recommended_action": action,
    }


# ─── candidate selection ─────────────────────────────────────────────────────


def _override_labels_index(overrides_report: Any) -> dict:
    """mapping_id → {left_entity_label, right_entity_label, manual_decision}."""
    out: dict = {}
    idx = index_overrides_for_lookup(overrides_report)
    for m in (idx.get("by_id") or {}).values():
        if isinstance(m, dict) and m.get("mapping_id"):
            out[m["mapping_id"]] = m
    return out


def select_link_validation_candidates(
        visual_gate_report: Any, overrides_report: Any, *,
        left_graphic_report: Any = None, right_graphic_report: Any = None,
        graphic_matched_report: Any = None,
        options: Optional[dict] = None) -> tuple[list[dict], dict, list[str]]:
    """Отобрать кандидатов link-validation (MVP: manual confirmed_reorganized).

    Переиспользует ``select_vision_candidates_v2`` в режиме link_validation с
    ``use_entity_mapping_overrides=true`` и фильтрует по candidate_kinds.
    Возвращает (candidates_с_метками, stats, warnings).
    """
    options = options or {}
    kinds = options.get("candidate_kinds") or DEFAULT_LINK_VALIDATION_OPTIONS["candidate_kinds"]
    kinds = set(kinds)
    sel_options = {
        "candidate_selection": "entity_aware",
        "use_entity_mapping_overrides": True,
        "manual_mapping_mode": "link_validation",
        "selection_mode": "link_validation",
        "include_manual_review": True,
        "include_exclude_from_vision": True,
        "max_items": 0,
    }
    selected, sel_stats, warnings = select_vision_candidates_v2(
        visual_gate_report, left_graphic_report=left_graphic_report,
        right_graphic_report=right_graphic_report,
        graphic_matched_report=graphic_matched_report,
        overrides_report=overrides_report, options=sel_options)

    labels_idx = _override_labels_index(overrides_report)
    out: list[dict] = []
    for c in selected:
        if c.get("candidate_kind") not in kinds:
            continue
        mm = c.get("manual_mapping") or {}
        ov = labels_idx.get(mm.get("mapping_id"), {})
        out.append({
            "left_block_id": c.get("left_block_id"),
            "right_block_id": c.get("right_block_id"),
            "left_page_number": c.get("left_page_number"),
            "right_page_number": c.get("right_page_number"),
            "left_entity_label": _clean(ov.get("left_entity_label")),
            "right_entity_label": _clean(ov.get("right_entity_label")),
            "manual_decision": mm.get("decision") or ov.get("manual_decision"),
            "mapping_id": mm.get("mapping_id"),
            "candidate_kind": c.get("candidate_kind"),
            "candidate_rank": c.get("candidate_rank"),
            "candidate_score": c.get("candidate_score"),
            "candidate_reasons": c.get("candidate_reasons") or [],
            "candidate_risk_flags": c.get("candidate_risk_flags") or [],
            "left_crop_ref": c.get("left_crop_ref"),
            "right_crop_ref": c.get("right_crop_ref"),
            "manual_mapping": mm,
        })
    stats = {"link_validation_candidates": len(out),
             "selection_mode": "link_validation",
             "candidate_kinds": sorted(kinds),
             "selection_total": sel_stats.get("candidates_total"),
             "manual_mapping_applied": sel_stats.get("manual_mapping_applied")}
    return out, stats, warnings


# ─── report assembly ─────────────────────────────────────────────────────────


def _item_id(cand: dict) -> str:
    return f"lv_{cand.get('left_block_id')}__{cand.get('right_block_id')}"


def _build_item(cand: dict, validation: Optional[dict], *,
                status: str, error: Optional[str] = None) -> dict:
    item = {
        "item_id": _item_id(cand),
        "mapping_id": cand.get("mapping_id"),
        "left_block_id": cand.get("left_block_id"),
        "right_block_id": cand.get("right_block_id"),
        "left_page_number": cand.get("left_page_number"),
        "right_page_number": cand.get("right_page_number"),
        "left_entity_label": cand.get("left_entity_label"),
        "right_entity_label": cand.get("right_entity_label"),
        "manual_decision": cand.get("manual_decision"),
        "candidate_kind": cand.get("candidate_kind"),
        "candidate_rank": cand.get("candidate_rank"),
        "status": status,
        "validation": validation,
        # ИНВАРИАНТ: link-validation никогда не grounded-факт
        "use_as_grounded_fact": False,
        "use_for_delta_explanation": False,
    }
    if validation is not None:
        agreement = compute_agreement(cand.get("manual_decision"), validation)
        item["agreement"] = {
            "agrees_with_manual_mapping": agreement["agrees_with_manual_mapping"],
            "conflicts_with_manual_mapping": agreement["conflicts_with_manual_mapping"],
            "reason": agreement["reason"],
        }
        item["recommended_action"] = agreement["recommended_action"]
        item["_orientation_failed"] = agreement["orientation_failed"]
    else:
        item["agreement"] = None
        item["recommended_action"] = ACTION_REVIEW
        item["_orientation_failed"] = False
    if error:
        item["error"] = error
    return item


def build_link_validation_report(session_id: Optional[str], pair_id: Optional[str],
                                 items: list[dict], *, created_at: Optional[str],
                                 status: str, warnings: Optional[list[str]] = None,
                                 stats: Optional[dict] = None) -> dict:
    succeeded = [i for i in items if i.get("status") == "done"]
    failed = [i for i in items if i.get("status") == "failed"]
    def _count(pred):
        return sum(1 for i in succeeded if pred(i))
    summary = {
        "candidates_total": len(items),
        "attempted": len(succeeded) + len(failed),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "valid_mapping": _count(lambda i: (i["validation"] or {}).get("decision") == "valid_mapping"),
        "manual_review": _count(lambda i: (i["validation"] or {}).get("decision") == "manual_review"),
        "reject_mapping": _count(lambda i: (i["validation"] or {}).get("decision") == "reject_mapping"),
        "agrees_with_manual_mapping": _count(
            lambda i: (i.get("agreement") or {}).get("agrees_with_manual_mapping")),
        "conflicts_with_manual_mapping": _count(
            lambda i: (i.get("agreement") or {}).get("conflicts_with_manual_mapping")),
        "orientation_failed": _count(lambda i: i.get("_orientation_failed")),
    }
    # вычистить транзиентный флаг
    clean_items = []
    for i in items:
        i = dict(i)
        i.pop("_orientation_failed", None)
        clean_items.append(i)
    out = {
        "version": REPORT_VERSION, "kind": REPORT_KIND, "status": status,
        "session_id": session_id, "pair_id": pair_id, "created_at": created_at,
        "summary": summary, "items": clean_items,
        "warnings": [w for w in (warnings or []) if isinstance(w, str)][:30],
    }
    if stats:
        out["selection_stats"] = stats
    return out


# ─── orchestrator ────────────────────────────────────────────────────────────


def run_pipeline_v2_link_validation(
        visual_gate_report: Any, overrides_report: Any, *,
        session_id: Optional[str] = None, pair_id: Optional[str] = None,
        left_graphic_report: Any = None, right_graphic_report: Any = None,
        graphic_matched_report: Any = None, options: Optional[dict] = None,
        runner: Optional[LinkValidationRunner] = None,
        created_at: Optional[str] = None,
        output_path: Optional[str | Path] = None,
        diagnostics_dir: Optional[str | Path] = None) -> dict:
    """Построить link_validation_report (+ опц. validation через runner).

    ``runner=None`` → ``status=skipped_no_runner`` (кандидаты построены, модель
    НЕ вызвана). Пишет на диск ТОЛЬКО если передан ``output_path``. Реальные
    vision-модели НЕ создаются — runner инъектируется.
    """
    warnings: list[str] = []
    try:
        candidates, stats, sel_warnings = select_link_validation_candidates(
            visual_gate_report, overrides_report,
            left_graphic_report=left_graphic_report,
            right_graphic_report=right_graphic_report,
            graphic_matched_report=graphic_matched_report, options=options)
    except Exception as exc:  # noqa: BLE001 — слой не должен падать наружу
        report = build_link_validation_report(
            session_id, pair_id, [], created_at=created_at, status="error",
            warnings=[f"candidate selection failed: {type(exc).__name__}: {exc}"])
        if output_path is not None:
            write_link_validation_report(output_path, report)
        return report
    warnings.extend(sel_warnings)

    max_items = int((options or {}).get("max_items",
                                        DEFAULT_LINK_VALIDATION_OPTIONS["max_items"]) or 0)
    if max_items > 0 and len(candidates) > max_items:
        warnings.append(f"link validation truncated by max_items={max_items}: "
                        f"{len(candidates) - max_items} dropped")
        candidates = candidates[:max_items]

    diag = Path(diagnostics_dir) if diagnostics_dir else None
    if diag is not None:
        diag.mkdir(parents=True, exist_ok=True)

    items: list[dict] = []
    if runner is None:
        for cand in candidates:
            items.append(_build_item(cand, None, status="skipped_no_runner"))
        status = "skipped_no_runner"
    else:
        for cand in candidates:
            prompt = build_link_validation_prompt(
                left_label=cand.get("left_entity_label"),
                right_label=cand.get("right_entity_label"),
                left_page=cand.get("left_page_number"),
                right_page=cand.get("right_page_number"))
            try:
                raw = runner(prompt, cand.get("left_crop_ref"),
                             cand.get("right_crop_ref"), dict(options or {}))
                validation = parse_link_validation_response(raw)
                if diag is not None:
                    _write_item_diag(diag, cand, prompt, raw, validation)
                if validation is None:
                    items.append(_build_item(
                        cand, None, status="failed",
                        error="link validation response could not be parsed"))
                else:
                    items.append(_build_item(cand, validation, status="done"))
            except Exception as exc:  # noqa: BLE001 — один item не валит отчёт
                items.append(_build_item(
                    cand, None, status="failed",
                    error=f"{type(exc).__name__}: {exc}"))
        status = "ok" if items else "ok"

    report = build_link_validation_report(
        session_id, pair_id, items, created_at=created_at, status=status,
        warnings=warnings, stats=stats)
    if output_path is not None:
        write_link_validation_report(output_path, report)
    return report


def _write_item_diag(diag: Path, cand: dict, prompt: str, raw: Any,
                     validation: Optional[dict]) -> None:
    try:
        base = diag / _item_id(cand)
        base.mkdir(parents=True, exist_ok=True)
        (base / "prompt.txt").write_text(prompt, encoding="utf-8")
        (base / "raw_response.json").write_text(
            json.dumps(raw, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8")
        if validation is not None:
            (base / "parsed_response.json").write_text(
                json.dumps(validation, ensure_ascii=False, indent=2),
                encoding="utf-8")
    except Exception:  # noqa: BLE001 — диагностика не критична
        pass


def write_link_validation_report(out_path: str | Path, report: dict) -> Path:
    """Атомарно записать отчёт (tmp + ``os.replace``)."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, ensure_ascii=False, indent=2)
    fd, tmp = tempfile.mkstemp(prefix=out_path.name + ".", suffix=".tmp",
                               dir=str(out_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, out_path)
    finally:
        if os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass
    return out_path


__all__ = [
    "REPORT_VERSION", "REPORT_KIND", "DEFAULT_LINK_VALIDATION_OPTIONS",
    "LINK_VALIDATION_PROMPT_TEMPLATE", "build_link_validation_prompt",
    "parse_link_validation_response", "compute_agreement",
    "select_link_validation_candidates", "build_link_validation_report",
    "run_pipeline_v2_link_validation", "write_link_validation_report",
    "ACTION_KEEP", "ACTION_REVIEW", "ACTION_REJECT",
]
