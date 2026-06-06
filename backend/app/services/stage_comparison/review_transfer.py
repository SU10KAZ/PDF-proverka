"""Перенос экспертных решений из классических «Расхождений» в V2.

Источник — `expert_review.json`, ключи `<pair>::<raw_id>`, где `raw_id` —
слаг находки из `unified_findings` (`chg_…`/`uf_…`). Это решения, которые
эксперт проставил во вкладке «Расхождения». Цель — те же находки в
представлении V2, у которых ключ решения `<pair>::<v2_id>`
(см. `v2_review.make_v2_id`). Поскольку V2 и «Расхождения» строятся из одного
`comparison_result.json`, в норме это буквально одни и те же находки — просто
с разными id-схемами.

Двухуровневый матчинг (hybrid):

  1. **Точный по `raw_id`** (детерминированный, без LLM): v2-находка несёт
     `raw_id`; если он совпадает со слагом исходного решения — это буквально
     та же находка. Здесь судить нечего.

  2. **Семантический (Claude)**: остаток исходных решений, чей слаг больше не
     встречается среди текущих v2-находок (перепрогон Opus переименовал/слил
     находки). Claude сопоставляет «та же реальная находка?» по смыслу — это
     та часть, где нужен ум, а не строковый скрипт.

Политики (заданы пользователем):
  * конфликт (у v2-находки уже есть решение) → ПОМЕТИТЬ, не перезаписывать;
  * неуверенное семантическое совпадение → перенести с флагом `needs_review`
    («проверить»);
  * запуск — на всю сессию (одной кнопкой).

Fail-soft: если Claude недоступен/упал — точные переносы всё равно
применяются, остаток уходит в отчёт как `unmatched`.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

from . import expert_review as expert_review_mod
from . import store as store_mod
from . import text_llm_provider as text_llm_provider_mod
from . import v2_review as v2_review_mod

logger = logging.getLogger(__name__)

# Порог уверенности Claude: >= CONFIDENCE → перенос «чисто», ниже но >= FLOOR →
# перенос с флагом needs_review («проверить»), ниже FLOOR → не переносим.
_CONFIDENCE_DEFAULT = 0.75
_FLOOR_DEFAULT = 0.4


def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _confidence_threshold() -> float:
    return _env_float("STAGE_COMPARISON_REVIEW_TRANSFER_CONFIDENCE", _CONFIDENCE_DEFAULT)


def _floor_threshold() -> float:
    return _env_float("STAGE_COMPARISON_REVIEW_TRANSFER_FLOOR", _FLOOR_DEFAULT)


def _claude_model() -> str:
    return (os.environ.get("STAGE_COMPARISON_REVIEW_TRANSFER_MODEL") or "sonnet").strip() or "sonnet"


def _claude_timeout() -> int:
    raw = (os.environ.get("STAGE_COMPARISON_REVIEW_TRANSFER_TIMEOUT_SEC") or "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return 300


# ─── Source decisions ────────────────────────────────────────────────────


def _humanize_raw_id(raw_id: str) -> str:
    """`chg_vru2_scheme_overhaul` → `vru2 scheme overhaul` (подсказка для Claude)."""
    s = str(raw_id or "")
    for pref in ("chg_", "uf_"):
        if s.startswith(pref):
            s = s[len(pref):]
            break
    return s.replace("_", " ").strip()


def build_source_decisions(decisions: dict) -> dict[str, dict[str, dict]]:
    """Сгруппировать классические решения по паре: `{pair_id: {raw_id: entry}}`.

    В источник попадают только НЕ-v2 ключи (raw_id не начинается с `v2_`) —
    то есть решения, проставленные во вкладке «Расхождения». Учитываются только
    записи с реальным вердиктом accepted/rejected.
    """
    out: dict[str, dict[str, dict]] = {}
    for key, entry in (decisions or {}).items():
        if not isinstance(entry, dict):
            continue
        if (entry.get("decision") or "").lower() not in ("accepted", "rejected"):
            continue
        pair_id, _, raw_id = str(key).partition("::")
        if not pair_id or not raw_id:
            continue
        if raw_id.startswith("v2_"):
            continue
        out.setdefault(pair_id, {})[raw_id] = entry
    return out


# ─── Claude semantic matching (residue only) ─────────────────────────────


_SYSTEM_PROMPT = (
    "Ты сопоставляешь инженерные находки изменений между двумя прогонами "
    "сравнения стадий проектной документации (старый прогон ↔ текущий V2).\n"
    "Тебе дают:\n"
    "  (A) СТАРЫЕ находки, по которым эксперт уже вынес решение "
    "(принято/отклонено). У каждой — короткий идентификатор-слаг и комментарий "
    "эксперта (содержимое старой находки могло не сохраниться, опирайся на слаг "
    "и комментарий).\n"
    "  (B) ТЕКУЩИЕ находки V2 с полным содержимым (заголовок, было/стало, лист).\n"
    "Задача: для КАЖДОЙ старой находки определить, какая ТЕКУЩАЯ находка описывает "
    "ТО ЖЕ САМОЕ реальное изменение, либо что соответствия нет.\n"
    "Правила:\n"
    "  • сопоставляй по смыслу изменения, а не по похожести слов;\n"
    "  • одной старой находке соответствует не более одной текущей;\n"
    "  • если уверенного соответствия нет — верни v2_id=null;\n"
    "  • confidence: 1.0 — точно та же находка, 0.5 — возможно, 0.0 — нет.\n"
    "Верни СТРОГО JSON без пояснений:\n"
    '{\"matches\":[{\"source_id\":\"<слаг>\",\"v2_id\":\"<v2_id|null>\",'
    '\"confidence\":0.0,\"reason\":\"кратко\"}]}'
)


def _compact_v2_for_prompt(item: dict) -> dict:
    return {
        "v2_id": item.get("id"),
        "title": str(item.get("title") or "")[:200],
        "summary": str(item.get("summary") or "")[:300],
        "old": str(item.get("old_value") or "")[:200],
        "new": str(item.get("new_value") or "")[:200],
        "sheet": item.get("sheet") or "",
        "page": item.get("page"),
    }


def _compact_source_for_prompt(raw_id: str, entry: dict) -> dict:
    return {
        "source_id": raw_id,
        "about": _humanize_raw_id(raw_id),
        "decision": (entry.get("decision") or "").lower(),
        "comment": str(entry.get("rejection_reason") or "")[:300],
    }


def _parse_claude_matches(raw_response: str) -> tuple[list[dict], Optional[str]]:
    """Достать matches[] из stdout `claude -p --output-format json`."""
    if not raw_response or not raw_response.strip():
        return [], "empty_response"
    text = raw_response.strip()
    # claude -p --output-format json → {"result": "...inner..."}
    try:
        obj = json.loads(text)
        if isinstance(obj, dict) and isinstance(obj.get("result"), str):
            text = obj["result"].strip()
    except json.JSONDecodeError:
        pass
    # Достаём JSON-объект из тела (возможны fences / окружающий текст).
    payload: Optional[dict] = None
    try:
        cand = json.loads(text)
        if isinstance(cand, dict):
            payload = cand
    except json.JSONDecodeError:
        first = text.find("{")
        last = text.rfind("}")
        if first != -1 and last > first:
            try:
                cand = json.loads(text[first:last + 1])
                if isinstance(cand, dict):
                    payload = cand
            except json.JSONDecodeError as exc:
                return [], f"json_decode_error: {exc}"
    if not isinstance(payload, dict):
        return [], "no_json_object"
    matches = payload.get("matches")
    if not isinstance(matches, list):
        return [], "no_matches_array"
    out: list[dict] = []
    for m in matches:
        if not isinstance(m, dict):
            continue
        out.append(m)
    return out, None


def _claude_match_residue(
    provider: Any,
    *,
    pair_label: str,
    sources: list[dict],
    v2_items: list[dict],
    model: str,
    timeout_sec: int,
) -> tuple[list[dict], dict]:
    """Вызвать Claude для сопоставления остатка. Возвращает (matches, diag).

    matches: [{source_id, v2_id|None, confidence, reason}]. Fail-soft: при любой
    ошибке возвращает ([], diag) — точные переносы не пострадают.
    """
    diag: dict[str, Any] = {"called": True, "status": None, "n_sources": len(sources), "n_v2": len(v2_items)}
    user_prompt = json.dumps(
        {
            "pair": pair_label,
            "old_findings": sources,
            "current_v2_findings": v2_items,
        },
        ensure_ascii=False,
    )
    try:
        result = provider.invoke(
            system_prompt=_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            model=model,
            timeout_sec=timeout_sec,
        )
    except Exception as exc:  # noqa: BLE001 — провайдер не должен валить перенос
        diag["status"] = "exception"
        diag["error"] = str(exc)[:300]
        return [], diag
    diag["status"] = result.status
    diag["duration_sec"] = round(getattr(result, "duration_sec", 0.0) or 0.0, 1)
    if result.status != "done":
        diag["error"] = (result.error or "")[:300]
        return [], diag
    matches, parse_err = _parse_claude_matches(result.raw_response)
    if parse_err:
        diag["parse_error"] = parse_err
    diag["matches_returned"] = len(matches)
    return matches, diag


# ─── Per-pair planning ───────────────────────────────────────────────────


def _plan_pair(session_id: str, pair_id: str, src_for_pair: dict[str, dict]) -> dict:
    """Построить v2-находки и развести source-решения на exact / residue.

    Возвращает {items, by_rawid, v2_by_id, exact, residue_raw_ids}.
    """
    built = v2_review_mod.build_pair_v2_changes(session_id, pair_id)
    items = built.get("items") or []
    by_rawid: dict[str, dict] = {}
    v2_by_id: dict[str, dict] = {}
    for it in items:
        v2_by_id[str(it.get("id"))] = it
        raw = str(it.get("raw_id") or "")
        if raw and raw not in by_rawid:
            by_rawid[raw] = it

    exact: list[tuple[dict, dict]] = []   # (v2_item, src_entry)
    residue_raw_ids: list[str] = []
    for raw_id, entry in src_for_pair.items():
        item = by_rawid.get(raw_id)
        if item is not None:
            exact.append((item, entry))
        else:
            residue_raw_ids.append(raw_id)
    return {
        "items": items,
        "by_rawid": by_rawid,
        "v2_by_id": v2_by_id,
        "exact": exact,
        "residue_raw_ids": residue_raw_ids,
    }


# ─── Session-wide transfer ───────────────────────────────────────────────


def transfer_session(session_id: str, *, use_claude: bool = True) -> dict:
    """Перенести классические решения в V2 по всем парам сессии.

    Бросает KeyError, если сессия не найдена.
    """
    session = store_mod.get_session(session_id)
    if not session:
        raise KeyError(session_id)
    decisions = expert_review_mod.load(session_id).get("decisions") or {}
    src_by_pair = build_source_decisions(decisions)

    conf_threshold = _confidence_threshold()
    floor = _floor_threshold()

    # Provider — прямой ClaudeCodeProvider (не зависит от kill-switch
    # text_llm): перенос «с умом» это осознанное действие пользователя.
    provider = None
    provider_reason = None
    if use_claude:
        provider = text_llm_provider_mod.ClaudeCodeProvider()
        ok, reason = provider.check_availability()
        if not ok:
            provider = None
            provider_reason = reason

    model = _claude_model()
    timeout_sec = _claude_timeout()

    transfers: list[dict] = []          # ops, exact-first
    per_pair_report: list[dict] = []
    claude_diags: list[dict] = []

    pairs = [p for p in (session.get("pairs") or []) if isinstance(p, dict)]
    for p in pairs:
        if p.get("status") == "disabled":
            continue
        pair_id = str(p.get("id") or "")
        if not pair_id:
            continue
        src_for_pair = src_by_pair.get(pair_id) or {}
        if not src_for_pair:
            continue
        try:
            plan = _plan_pair(session_id, pair_id, src_for_pair)
        except Exception as exc:  # noqa: BLE001
            per_pair_report.append({"pair_id": pair_id, "error": str(exc)[:200]})
            continue

        pair_label = str(p.get("label") or pair_id)
        exact_ops: list[dict] = []
        for item, entry in plan["exact"]:
            exact_ops.append({
                "key": expert_review_mod.make_key(pair_id, str(item.get("id"))),
                "decision": (entry.get("decision") or "").lower(),
                "rejection_reason": entry.get("rejection_reason") or "",
                "method": "exact",
                "source_raw_id": str(item.get("raw_id") or ""),
                "confidence": 1.0,
                "needs_review": False,
            })

        residue = plan["residue_raw_ids"]
        semantic_ops: list[dict] = []
        unmatched: list[str] = []
        pair_claude_diag: Optional[dict] = None

        if residue and provider is not None and plan["items"]:
            sources = [_compact_source_for_prompt(r, src_for_pair[r]) for r in residue]
            v2_compact = [_compact_v2_for_prompt(it) for it in plan["items"]]
            matches, diag = _claude_match_residue(
                provider,
                pair_label=pair_label,
                sources=sources,
                v2_items=v2_compact,
                model=model,
                timeout_sec=timeout_sec,
            )
            diag["pair_id"] = pair_id
            claude_diags.append(diag)
            pair_claude_diag = diag
            matched_src: set[str] = set()
            for m in matches:
                source_id = str(m.get("source_id") or "")
                v2_id = m.get("v2_id")
                if source_id not in src_for_pair:
                    continue
                if not v2_id or str(v2_id) not in plan["v2_by_id"]:
                    continue
                try:
                    conf = float(m.get("confidence") or 0.0)
                except (TypeError, ValueError):
                    conf = 0.0
                if conf < floor:
                    continue
                entry = src_for_pair[source_id]
                semantic_ops.append({
                    "key": expert_review_mod.make_key(pair_id, str(v2_id)),
                    "decision": (entry.get("decision") or "").lower(),
                    "rejection_reason": entry.get("rejection_reason") or "",
                    "method": "semantic",
                    "source_raw_id": source_id,
                    "confidence": conf,
                    # Неуверенное совпадение → «проверить».
                    "needs_review": conf < conf_threshold,
                })
                matched_src.add(source_id)
            unmatched = [r for r in residue if r not in matched_src]
        else:
            # Claude не звали (нет остатка / недоступен) — весь остаток unmatched.
            unmatched = list(residue)

        transfers.extend(exact_ops)
        transfers.extend(semantic_ops)
        per_pair_report.append({
            "pair_id": pair_id,
            "pair_label": pair_label,
            "v2_items": len(plan["items"]),
            "source_decisions": len(src_for_pair),
            "exact": len(exact_ops),
            "semantic": len(semantic_ops),
            "unmatched_source": len(unmatched),
            "unmatched_source_ids": unmatched[:50],
            "claude": pair_claude_diag is not None,
        })

    apply_report = expert_review_mod.apply_transfer(session_id, transfers)

    return {
        "session_id": session_id,
        "use_claude": use_claude,
        "claude_available": provider is not None,
        "claude_unavailable_reason": provider_reason,
        "totals": {
            "applied": apply_report.get("applied", 0),
            "consistent_existing": apply_report.get("consistent_existing", 0),
            "conflicts": len(apply_report.get("conflicts", [])),
            "needs_review": apply_report.get("needs_review", 0),
            "exact": sum(r.get("exact", 0) for r in per_pair_report),
            "semantic": sum(r.get("semantic", 0) for r in per_pair_report),
            "unmatched_source": sum(r.get("unmatched_source", 0) for r in per_pair_report),
            "pairs_processed": len(per_pair_report),
        },
        "conflicts": apply_report.get("conflicts", []),
        "per_pair": per_pair_report,
        "claude_diagnostics": claude_diags,
    }


__all__ = ["transfer_session", "build_source_decisions"]
