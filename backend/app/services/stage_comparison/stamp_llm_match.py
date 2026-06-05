"""Haiku-семантическое доматчивание листов поверх детерминированного штамп-матчинга.

Зачем
=====
`stamp_matching.match_sheet_indexes` матчит листы по имени из штампа точным
совпадением + IDF-косинусом с margin-гейтом (precision > recall). Многие листы
остаются непарными, хотя по смыслу это ОДИН И ТОТ ЖЕ лист — у них слегка разное
название/обрывки OCR. Классический пример:

    «Однолинейная расчетная схема ГРЩ»  ==  «Однолинейная схема ГРЩ»

Детерминированный матчер этого не ловит (разный набор токенов + неоднозначность
рядом стоящих похожих имён). Этот модуль добавляет лёгкий LLM-слой (Haiku через
Claude Code subscription), который смотрит ТОЛЬКО на НЕсматченный остаток и
предлагает дополнительные пары «это один и тот же лист».

Принципы
========
* **Только остаток.** Точные/нечёткие детерминированные совпадения не трогаем —
  LLM работает по `left_only` ∩ `right_only`. Высокая precision сохраняется.
* **Дёшево и узко.** В промпт идут только имена листов (+ номер листа, тип),
  не весь MD. Haiku хватает, ответ быстрый.
* **Advisory.** Пары возвращаются как обычные `suggested_items` с
  `match_type="llm_semantic"` и `needs_review=true` — пользователь подтверждает
  их галочкой перед «Применить». LLM ничего не применяет сам.
* **Fail-soft.** Любая проблема (CLI нет, таймаут, мусорный JSON) → пустой
  список пар + статус. Поведение деградирует ровно до сегодняшнего
  детерминированного результата.
* **Чистое ядро.** Построение промпта и парсинг — без I/O и сети (тестируются
  напрямую). Сетевой вызов изолирован в `llm_match_sheets` через
  инъектируемый provider.

Модуль НЕ импортирует stamp_matching, чтобы избежать циклов: типы листов
передаются как «утиные» объекты с атрибутами `page`, `sheet_no`, `sheet_name`,
`is_graphic` (или dict с теми же ключами).
"""
from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

logger = logging.getLogger(__name__)


# ─── Тюнинг (env override, безопасные дефолты) ─────────────────────────────

def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def stamp_llm_enabled() -> bool:
    """Главный kill-switch. Default ON — фича доступна сразу, но реально
    срабатывает только когда фронт прислал use_llm=true И provider доступен."""
    return _env_bool("STAGE_COMPARISON_STAMP_LLM_ENABLED", True)


def stamp_llm_model() -> str:
    return os.environ.get("STAGE_COMPARISON_STAMP_LLM_MODEL", "haiku").strip() or "haiku"


def stamp_llm_timeout_sec() -> int:
    return _env_int("STAGE_COMPARISON_STAMP_LLM_TIMEOUT_SEC", 90)


def stamp_llm_max_sheets() -> int:
    """Верхняя граница числа листов на сторону, отдаваемых в промпт (защита от
    гигантских списков). 0/отрицательное → без лимита."""
    return _env_int("STAGE_COMPARISON_STAMP_LLM_MAX_SHEETS", 150)


def stamp_llm_min_confidence() -> float:
    return _env_float("STAGE_COMPARISON_STAMP_LLM_MIN_CONFIDENCE", 0.6)


_NAME_MAX_CHARS = 160


# ─── Доступ к полям листа (dataclass SheetRec или dict) ────────────────────

def _attr(rec: Any, key: str, default: Any = None) -> Any:
    if isinstance(rec, dict):
        return rec.get(key, default)
    return getattr(rec, key, default)


def _sheet_line(rec: Any) -> Optional[str]:
    """Одна строка листа для промпта или None, если у листа нет имени."""
    page = _attr(rec, "page")
    name = (_attr(rec, "sheet_name", "") or "").strip()
    if page is None or not name:
        return None
    name = name.replace("\n", " ").replace("\r", " ")
    name = re.sub(r"\s+", " ", name).strip()[:_NAME_MAX_CHARS]
    if not name:
        return None
    sheet_no = (str(_attr(rec, "sheet_no", "") or "")).strip()
    kind = "графический" if _attr(rec, "is_graphic") else "текстовый"
    extra = f", лист {sheet_no}" if sheet_no else ""
    return f"[page {int(page)}{extra}, {kind}] {name}"


# ─── Промпт ─────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = (
    "Ты — инженер-эксперт по проектной документации (МКД). Тебе дают список "
    "НЕсопоставленных листов СТАРОЙ стадии и список НЕсопоставленных листов "
    "НОВОЙ стадии одного и того же проекта. Названия листов взяты из штампа "
    "(возможны опечатки и обрывки OCR).\n\n"
    "Задача: найти пары «это ОДИН И ТОТ ЖЕ лист», даже если название "
    "сформулировано немного по-разному.\n"
    "Пример эквивалентных названий: «Однолинейная расчетная схема ГРЩ» и "
    "«Однолинейная схема ГРЩ» — это один и тот же лист.\n\n"
    "ЖЁСТКИЕ ПРАВИЛА:\n"
    "1. Сопоставляй по СМЫСЛУ/НАЗНАЧЕНИЮ листа, а не по точному совпадению слов.\n"
    "2. Только один и тот же объект. НЕ путай разные объекты: ВРУ-1 ≠ ВРУ-2, "
    "ГРЩ ≠ ВРУ, «План 1 этажа» ≠ «План 2 этажа», «Корпус 1» ≠ «Корпус 2».\n"
    "3. Каждый старый page и каждый новый page используется НЕ БОЛЕЕ ОДНОГО РАЗА.\n"
    "4. Если не уверен, что это один и тот же лист — НЕ создавай пару (оставь "
    "лист непарным). Лучше пропустить, чем ошибиться.\n"
    "5. Используй только page-номера из данных. Ничего не выдумывай.\n\n"
    "Ответ — СТРОГО JSON без пояснений:\n"
    '{\"pairs\": [{\"old_page\": <int>, \"new_page\": <int>, '
    '\"confidence\": <0.0-1.0>, \"reason\": \"<кратко>\"}]}\n'
    "Если ни одной пары — верни {\"pairs\": []}."
)


def build_llm_match_prompt(
    left_recs: Sequence[Any],
    right_recs: Sequence[Any],
    *,
    max_sheets: Optional[int] = None,
) -> tuple[str, str, dict]:
    """Собрать (system_prompt, user_prompt, meta).

    meta содержит фактическое число строк каждой стороны (для диагностики и
    early-exit, если одна сторона пуста после фильтра).
    """
    cap = stamp_llm_max_sheets() if max_sheets is None else max_sheets
    left_lines = [s for s in (_sheet_line(r) for r in left_recs) if s]
    right_lines = [s for s in (_sheet_line(r) for r in right_recs) if s]
    if cap and cap > 0:
        left_lines = left_lines[:cap]
        right_lines = right_lines[:cap]

    user = (
        "СТАРАЯ стадия — несопоставленные листы:\n"
        + ("\n".join(left_lines) if left_lines else "(нет)")
        + "\n\nНОВАЯ стадия — несопоставленные листы:\n"
        + ("\n".join(right_lines) if right_lines else "(нет)")
        + "\n\nВерни JSON с парами одинаковых по смыслу листов."
    )
    meta = {"left_lines": len(left_lines), "right_lines": len(right_lines)}
    return SYSTEM_PROMPT, user, meta


# ─── Парсинг ответа ──────────────────────────────────────────────────────────

_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)
_CLAUDE_TEXT_FIELDS = ("result", "text", "content", "response")


def _extract_model_text(raw_response: str) -> str:
    """Достать тело ответа модели из stdout `claude -p --output-format json`.

    Если stdout — это {"result": "..."} обёртка Claude Code, берём поле result.
    Иначе возвращаем как есть.
    """
    if not raw_response:
        return ""
    raw = raw_response.strip()
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if isinstance(obj, dict):
        # Уже распарсенный объект с нашим контрактом?
        if "pairs" in obj:
            return raw
        for k in _CLAUDE_TEXT_FIELDS:
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v
    return raw


def _loads_json_object(text: str) -> Optional[dict]:
    text = (text or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass
    m = _FENCE_RE.search(text)
    if m:
        try:
            parsed = json.loads(m.group(1))
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            pass
    first = text.find("{")
    last = text.rfind("}")
    if first != -1 and last > first:
        try:
            parsed = json.loads(text[first:last + 1])
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def parse_llm_match_pairs(
    raw_response: str,
    *,
    min_confidence: Optional[float] = None,
) -> list[dict]:
    """Распарсить ответ Haiku в список пар.

    Возвращает [{"old_page": int, "new_page": int, "confidence": float,
    "reason": str}], отфильтрованных по min_confidence. Никогда не падает.
    """
    thr = stamp_llm_min_confidence() if min_confidence is None else min_confidence
    obj = _loads_json_object(_extract_model_text(raw_response))
    if not obj:
        return []
    raw_pairs = obj.get("pairs")
    if not isinstance(raw_pairs, list):
        return []
    out: list[dict] = []
    seen_old: set[int] = set()
    seen_new: set[int] = set()
    for p in raw_pairs:
        if not isinstance(p, dict):
            continue
        op = p.get("old_page", p.get("left_page"))
        np_ = p.get("new_page", p.get("right_page"))
        try:
            op_i = int(op)
            np_i = int(np_)
        except (TypeError, ValueError):
            continue
        if op_i in seen_old or np_i in seen_new:
            continue  # каждый page не более одного раза (берём первый)
        try:
            conf = float(p.get("confidence", 0.0))
        except (TypeError, ValueError):
            conf = 0.0
        conf = max(0.0, min(1.0, conf))
        if conf < thr:
            continue
        reason = str(p.get("reason", "") or "")[:200]
        seen_old.add(op_i)
        seen_new.add(np_i)
        out.append({"old_page": op_i, "new_page": np_i,
                    "confidence": conf, "reason": reason})
    return out


# ─── Оркестрация вызова ──────────────────────────────────────────────────────

def llm_match_sheets(
    left_recs: Sequence[Any],
    right_recs: Sequence[Any],
    *,
    provider: Any,
    model: Optional[str] = None,
    timeout_sec: Optional[int] = None,
    max_sheets: Optional[int] = None,
    min_confidence: Optional[float] = None,
) -> dict:
    """Вызвать LLM-провайдер на остатке и вернуть пары + диагностику.

    provider — объект с методом invoke(system_prompt, user_prompt, model,
    timeout_sec, work_dir) → ProviderResult (status/raw_response/error/...).
    Сетевой вызов изолирован здесь; в тестах provider мокается.

    Никогда не бросает наружу: любая ошибка → pairs=[] + status.
    """
    result = {"pairs": [], "status": "ok", "error": None,
              "duration_sec": 0.0, "left_lines": 0, "right_lines": 0,
              "raw_pairs": 0, "model": model or stamp_llm_model()}
    try:
        system, user, meta = build_llm_match_prompt(
            left_recs, right_recs, max_sheets=max_sheets)
        result["left_lines"] = meta["left_lines"]
        result["right_lines"] = meta["right_lines"]
        if not meta["left_lines"] or not meta["right_lines"]:
            result["status"] = "no_unmatched"
            return result

        # ВАЖНО: запускаем provider в ИЗОЛИРОВАННОМ temp-каталоге (вне дерева
        # проекта). Иначе `claude -p` поднимется в CWD backend'а (корень
        # проекта), подхватит огромный аудиторский CLAUDE.md + docs/skills и
        # начнёт вести себя как «эксперт-аудитор» (спрашивать документы) вместо
        # строгого JSON-матчера. Temp-каталог под /tmp обрывает discovery
        # project-CLAUDE.md и даёт чистый контекст. Полный system prompt при
        # этом идёт через --append-system-prompt-file (без обрезки 4000).
        with tempfile.TemporaryDirectory(prefix="stamp_llm_") as wd:
            pr = provider.invoke(
                system_prompt=system,
                user_prompt=user,
                model=model or stamp_llm_model(),
                timeout_sec=timeout_sec or stamp_llm_timeout_sec(),
                work_dir=Path(wd),
            )
        result["duration_sec"] = round(getattr(pr, "duration_sec", 0.0) or 0.0, 2)
        result["model"] = getattr(pr, "model", None) or result["model"]
        status = getattr(pr, "status", "error")
        if status != "done":
            result["status"] = status
            result["error"] = getattr(pr, "error", None)
            return result

        pairs = parse_llm_match_pairs(
            getattr(pr, "raw_response", "") or "", min_confidence=min_confidence)
        result["raw_pairs"] = len(pairs)
        result["pairs"] = pairs
    except Exception as exc:  # fail-soft — никакая ошибка не валит сравнение
        logger.warning("stamp_llm_match: failed: %s", exc)
        result["status"] = "exception"
        result["error"] = str(exc)
        result["pairs"] = []
    return result


def make_llm_match_fn(
    provider: Any,
    *,
    model: Optional[str] = None,
    timeout_sec: Optional[int] = None,
    max_sheets: Optional[int] = None,
    min_confidence: Optional[float] = None,
    diagnostics: Optional[dict] = None,
) -> Callable[[Sequence[Any], Sequence[Any]], list[tuple[int, int, float, str]]]:
    """Построить инъектируемую в `match_sheet_indexes` функцию доматчинга.

    Возвращаемая функция: (rem_left, rem_right) → [(old_page, new_page, score,
    match_type), ...]. match_type всегда "llm_semantic". Если передан dict
    `diagnostics`, в него пишется отчёт о вызове (для result.json).
    """
    def _fn(rem_left: Sequence[Any], rem_right: Sequence[Any]):
        rep = llm_match_sheets(
            rem_left, rem_right, provider=provider, model=model,
            timeout_sec=timeout_sec, max_sheets=max_sheets,
            min_confidence=min_confidence)
        if diagnostics is not None:
            diagnostics.clear()
            diagnostics.update({k: v for k, v in rep.items() if k != "pairs"})
            diagnostics["pairs_added"] = len(rep.get("pairs") or [])
        return [
            (p["old_page"], p["new_page"], float(p["confidence"]), "llm_semantic")
            for p in rep.get("pairs", [])
        ]
    return _fn


__all__ = [
    "stamp_llm_enabled",
    "stamp_llm_model",
    "build_llm_match_prompt",
    "parse_llm_match_pairs",
    "llm_match_sheets",
    "make_llm_match_fn",
    "SYSTEM_PROMPT",
]
