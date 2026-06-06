"""Политика безопасного авто-применения штамп-сопоставления (batch auto-match).

Решает, какие предложенные пары (`suggested_items` из `match_sheet_indexes`)
можно применить АВТОМАТИЧЕСКИ в `page_alignment`, а какие оставить на ручную
проверку. precision > recall: рискованное/низкоуверенное НЕ применяем.

Чистый модуль: без I/O и сети. Используется и пакетным job'ом, и (потенциально)
ручным режимом. Сам alignment не сохраняет — только строит очищенные items.
"""
from __future__ import annotations

import os
from typing import Optional


# ─── env (безопасные дефолты) ──────────────────────────────────────────────

def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def auto_apply_min_score() -> float:
    return _env_float("STAGE_COMPARISON_STAMP_AUTO_APPLY_MIN_SCORE", 0.80)


def auto_apply_llm_min_confidence() -> float:
    return _env_float("STAGE_COMPARISON_STAMP_AUTO_APPLY_LLM_MIN_CONFIDENCE", 0.85)


def auto_apply_text_layer() -> bool:
    return _env_bool("STAGE_COMPARISON_STAMP_AUTO_APPLY_TEXT_LAYER", False)


def auto_overwrite_existing_default() -> bool:
    return _env_bool("STAGE_COMPARISON_STAMP_AUTO_OVERWRITE_EXISTING", False)


# Risk-флаги, которые САМИ ПО СЕБЕ требуют ручной проверки (auto-apply запрещён).
_BLOCKING_RISK = {"low_margin"}
# Сильные признаки: при их наличии duplicate_sheet_name перестаёт быть блокером.
_STRONG_EVIDENCE_PREFIXES = ("оборуд:", "этаж:", "корпус:", "система:")

_EXACT_TYPES = {"exact_name", "exact_canonical_name", "exact_multipart_group"}
_FUZZY_TYPES = {"fuzzy_name", "fuzzy_structural"}


def _has_strong_evidence(item: dict) -> bool:
    for ev in (item.get("positive_evidence") or []):
        if any(str(ev).startswith(p) for p in _STRONG_EVIDENCE_PREFIXES):
            return True
    return False


def should_auto_apply_stamp_match(item: dict) -> tuple[bool, str]:
    """Можно ли применить ЭТУ предложенную пару автоматически.

    Возвращает (apply: bool, reason: str). reason — короткий код причины
    (для применённых — тип; для отклонённых — почему оставили на ручную).
    Применяет ТОЛЬКО matched-пары; односторонние сюда не передаём.
    """
    if not item.get("match"):
        return False, "not_a_match"

    mt = str(item.get("match_type") or "")
    risk = list(item.get("risk_flags") or [])
    score = float(item.get("score") or 0.0)
    conf = item.get("confidence")
    conf = float(conf) if conf is not None else score

    # Блокирующие risk-флаги (low_margin) → всегда на ручную.
    if _BLOCKING_RISK & set(risk):
        return False, "low_margin"

    # text_layer-источник: по умолчанию НЕ применяем автоматически.
    if mt == "text_layer" or "text_layer_fallback" in risk:
        if not auto_apply_text_layer():
            return False, "text_layer"
        # Флаг включён: text_layer-тип применяем как fuzzy (по порогу score);
        # для прочих типов с флагом text_layer_fallback продолжаем обычную логику.
        if mt == "text_layer":
            if score < auto_apply_min_score():
                return False, "low_score"
            return True, "text_layer"

    # duplicate_sheet_name без сильных признаков → на ручную.
    if "duplicate_sheet_name" in risk and not _has_strong_evidence(item):
        return False, "duplicate_sheet_name"

    # Структурные совпадения — безопасны.
    if mt in _EXACT_TYPES:
        return True, "exact"
    if mt == "multipart_group":
        # дополнительные risk-флаги (кроме уже отсеянных) → на ручную
        if set(risk) - {"llm_semantic"}:
            return False, "multipart_risky"
        return True, "multipart"
    if mt in _FUZZY_TYPES:
        if score < auto_apply_min_score():
            return False, "low_score"
        return True, "fuzzy"
    if mt == "llm_semantic":
        if conf < auto_apply_llm_min_confidence():
            return False, "llm_low_confidence"
        # любой нетривиальный risk-флаг (кроме самого llm_semantic) → на ручную
        if set(risk) - {"llm_semantic"}:
            return False, "llm_risky"
        return True, "llm"

    return False, "unknown_type"


def build_auto_apply_items(suggested_items: list[dict]) -> dict:
    """Преобразовать suggested_items в очищенные `items` для PUT page-alignment.

    Безопасные matched-пары применяются как пара; небезопасные — расцепляются на
    два односторонних слота (остаются на ручную проверку); односторонние слоты
    (включая multipart_continuation) сохраняются как есть. В сохранённый
    alignment попадают ТОЛЬКО канонические поля (slot/left_page/right_page/mode/
    note) — display-поля (match_type/score/…) не копируются.

    Возвращает {items, applied, review, reasons:{reason: count}}.
    """
    items: list[dict] = []
    applied = 0
    review = 0
    reasons: dict[str, int] = {}

    def _note(reason: str) -> str:
        reasons[reason] = reasons.get(reason, 0) + 1
        return reason

    for it in (suggested_items or []):
        lp = it.get("left_page")
        rp = it.get("right_page")
        if it.get("match"):
            ok, reason = should_auto_apply_stamp_match(it)
            if ok:
                items.append({"left_page": lp, "right_page": rp, "mode": "manual",
                              "note": f"auto:{_note('applied:' + reason)}"})
                applied += 1
            else:
                # небезопасно → два односторонних слота (на ручную проверку)
                _note("review:" + reason)
                if lp is not None:
                    items.append({"left_page": lp, "right_page": None, "mode": "manual",
                                  "note": f"auto-review:{reason}"})
                if rp is not None:
                    items.append({"left_page": None, "right_page": rp, "mode": "manual",
                                  "note": f"auto-review:{reason}"})
                review += 1
        else:
            # односторонний (left_only/right_only/multipart_continuation) → как есть
            note = str(it.get("match_type") or it.get("note") or "")
            items.append({"left_page": lp, "right_page": rp, "mode": "manual",
                          "note": note[:80]})

    for i, it in enumerate(items):
        it["slot"] = i + 1
    return {"items": items, "applied": applied, "review": review, "reasons": reasons}


__all__ = [
    "should_auto_apply_stamp_match",
    "build_auto_apply_items",
    "auto_apply_min_score",
    "auto_apply_llm_min_confidence",
    "auto_apply_text_layer",
    "auto_overwrite_existing_default",
]
