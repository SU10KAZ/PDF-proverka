"""
Детерминированный corrector оптимизаций.

Зачем
-----
Замер 07-07 (92 проекта, 916 предложений) вскрыл два дефекта агентного
`optimization_corrector` (`claude -p`):

1. **Тихая потеря данных.** Агентный critic обрывается на больших входах
   (`reviews` < `items`: напр. `133_23-ГК-ЭО1` — 14 предложений, отрецензировано
   7). Агентный corrector затем ПЕРЕЗАПИСЫВАЕТ `optimization.json` только
   отрецензированной частью → 7 валидных предложений (в т.ч. устранявшее
   КРИТИЧЕСКОЕ замечание) молча удаляются.

2. **Corrector удаляет item'ы.** Всего 41 удаление, из них 11 — по item'ам,
   которые критик вообще не рецензировал. Потеря легитимной экономии.

Решение — по образцу findings deterministic_corrector: применяем по вердиктам
critic **консервативные детерминированные** действия. Главный инвариант:
**ни одно предложение не удаляется** и **ни одно не теряется**.

| Вердикт | Действие |
|---|---|
| `pass` | без изменений |
| `vendor_violation` | оставить + `requires_review` + note (эксперт решает по вендор-листу) |
| `conflicts_with_finding` | оставить + `blocked_by_finding` + savings→0 + note (замечание в приоритете) |
| `unrealistic_savings` | срезать `savings_pct` до потолка (сохранив исходное) + note |
| `no_traceability` | оставить + `requires_review` + note |
| `too_vague` | оставить + `requires_review` + note |
| `technical_issue` | оставить + `requires_review` + note |
| `wrong_page` | оставить + `requires_review` + note |
| нет вердикта | считать `pass`, СОХРАНИТЬ (guard против потери неотрецензированных) |

Каждому исправленному item'у проставляется `corrector_note` и
`corrected_by="deterministic"`. Прочие поля сохраняются, правки идемпотентны.
"""
from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

# Вердикты, требующие только пометки на ручную проверку (item сохраняется как есть).
_REVIEW_VERDICTS = {
    "vendor_violation", "no_traceability", "too_vague",
    "technical_issue", "wrong_page",
}
_ALL_VERDICTS = _REVIEW_VERDICTS | {"pass", "conflicts_with_finding", "unrealistic_savings"}


@dataclass
class DeterministicOptCorrectorResult:
    items_total: int = 0
    reviewed: int = 0
    unreviewed_kept: int = 0     # guard: item'ы без вердикта, сохранённые как pass
    corrected: int = 0
    savings_capped: int = 0
    conflicts_blocked: int = 0
    flagged_review: int = 0
    deleted: int = 0             # инвариант: всегда 0
    error: Optional[str] = None
    actions: Counter = field(default_factory=Counter)

    def to_meta(self) -> dict:
        return {
            "corrector": "deterministic",
            "corrected_at": datetime.now().isoformat(),
            "items_total": self.items_total,
            "reviewed": self.reviewed,
            "unreviewed_kept": self.unreviewed_kept,
            "corrected": self.corrected,
            "savings_capped": self.savings_capped,
            "conflicts_blocked": self.conflicts_blocked,
            "flagged_review": self.flagged_review,
            "deleted": self.deleted,
            "actions": dict(self.actions),
        }


# ═══════════════════════════════════════════════════════════════════════════
# Разбор входных структур
# ═══════════════════════════════════════════════════════════════════════════

def iter_opt_items(data) -> list:
    """Список предложений из optimization.json (list или dict-обёртка)."""
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("items", "optimizations", "scenarios"):
            value = data.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
    return []


def _items_key(data) -> str:
    if isinstance(data, dict):
        for key in ("items", "optimizations", "scenarios"):
            if isinstance(data.get(key), list):
                return key
    return "items"


def _item_id(item: dict):
    return item.get("id") or item.get("item_id") or item.get("opt_id")


def build_verdict_map(review_data) -> dict:
    """{item_id: {verdict, details, conflicting_finding_id, suggested_action}}."""
    out = {}
    reviews = []
    if isinstance(review_data, dict):
        reviews = review_data.get("reviews") or []
    elif isinstance(review_data, list):
        reviews = review_data
    for r in reviews:
        if not isinstance(r, dict):
            continue
        rid = r.get("item_id") or r.get("id") or r.get("opt_id")
        if rid is None:
            continue
        verdict = (r.get("verdict") or r.get("status") or "pass").lower()
        if verdict not in _ALL_VERDICTS:
            verdict = "pass"
        out[str(rid)] = {
            "verdict": verdict,
            "details": r.get("details") or r.get("reason") or "",
            "conflicting_finding_id": r.get("conflicting_finding_id"),
            "suggested_action": r.get("suggested_action"),
        }
    return out


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Ядро (без I/O) — удобно тестировать
# ═══════════════════════════════════════════════════════════════════════════

def correct_items(items: list, verdict_map: dict, *, savings_cap: int = 50):
    """Применить детерминированные правки. Возвращает (new_items, result).

    Инвариант: len(new_items) == len(items) (ничего не удаляется/не теряется)."""
    result = DeterministicOptCorrectorResult(items_total=len(items))
    new_items = []

    for item in items:
        iid = _item_id(item)
        rec = verdict_map.get(str(iid))
        if rec is None:
            # guard: критик не отрецензировал → сохраняем как pass, НЕ теряем
            result.unreviewed_kept += 1
            result.actions["unreviewed_kept"] += 1
            new_items.append(item)
            continue

        result.reviewed += 1
        verdict = rec["verdict"]
        result.actions[verdict] += 1

        if verdict == "pass":
            new_items.append(item)
            continue

        fixed = dict(item)  # копия — правки идемпотентны, оригинал не мутируем
        note = rec.get("details") or ""

        if verdict == "unrealistic_savings":
            cur = _as_float(fixed.get("savings_pct"))
            if cur is not None and cur > savings_cap:
                fixed.setdefault("savings_pct_original", fixed.get("savings_pct"))
                fixed["savings_pct"] = savings_cap
                result.savings_capped += 1
            _add_note(fixed, f"savings срезан до {savings_cap}% (unrealistic_savings): {note}")

        elif verdict == "conflicts_with_finding":
            fid = rec.get("conflicting_finding_id")
            if fid:
                fixed["blocked_by_finding"] = fid
            # экономию на позиции, которую всё равно надо менять по замечанию,
            # заявлять нельзя → savings→0, но предложение сохраняем (эксперт видит связь)
            if _as_float(fixed.get("savings_pct")):
                fixed.setdefault("savings_pct_original", fixed.get("savings_pct"))
                fixed["savings_pct"] = 0
            fixed["requires_review"] = True
            result.conflicts_blocked += 1
            _add_note(fixed, f"конфликт с замечанием {fid or ''} (conflicts_with_finding): {note}")

        else:  # vendor_violation / no_traceability / too_vague / technical_issue / wrong_page
            fixed["requires_review"] = True
            result.flagged_review += 1
            _add_note(fixed, f"{verdict}: {note}")

        fixed["corrected_by"] = "deterministic"
        result.corrected += 1
        new_items.append(fixed)

    return new_items, result


def _add_note(item: dict, text: str) -> None:
    text = (text or "").strip()
    if not text:
        return
    prev = item.get("corrector_note") or ""
    # идемпотентность: не дублируем ту же пометку
    if text in prev:
        return
    item["corrector_note"] = (prev + " | " + text).strip(" |") if prev else text


# ═══════════════════════════════════════════════════════════════════════════
# Главная функция (I/O)
# ═══════════════════════════════════════════════════════════════════════════

async def run_deterministic_corrector(
    output_dir: Path,
    *,
    optimization_filename: str = "optimization.json",
    review_filename: str = "optimization_review.json",
    pre_review_filename: str = "optimization_pre_review.json",
    savings_cap: int = 50,
    on_log: Optional[Callable[[str], Awaitable[None]]] = None,
    write: bool = True,
) -> DeterministicOptCorrectorResult:
    """Прогнать детерминированный корректор и (опц.) записать optimization.json.

    Бэкапит исходник в optimization_pre_review.json перед записью.
    Любая ошибка чтения → fail-soft: возвращает result с error, файл не трогаем.
    """
    output_dir = Path(output_dir)
    opt_data = _load_json(output_dir / optimization_filename)
    if opt_data is None:
        return DeterministicOptCorrectorResult(error=f"{optimization_filename} не найден/невалиден")
    review_data = _load_json(output_dir / review_filename)
    if review_data is None:
        return DeterministicOptCorrectorResult(error=f"{review_filename} не найден/невалиден")

    items = iter_opt_items(opt_data)
    verdict_map = build_verdict_map(review_data)
    new_items, result = correct_items(items, verdict_map, savings_cap=savings_cap)

    # инвариант: ничего не удалено
    assert len(new_items) == len(items), "deterministic_corrector потерял item'ы"

    if write:
        # бэкап оригинала (если ещё не сделан вызывающей стороной)
        pre_path = output_dir / pre_review_filename
        if not pre_path.exists():
            pre_path.write_text(
                json.dumps(opt_data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        key = _items_key(opt_data)
        if isinstance(opt_data, dict):
            out = dict(opt_data)
            out[key] = new_items
            meta = dict(out.get("meta") or {})
            meta["corrector"] = result.to_meta()
            out["meta"] = meta
        else:
            out = new_items
        (output_dir / optimization_filename).write_text(
            json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    if on_log:
        await on_log(
            f"Optimization Corrector (детерм.): {result.items_total} предложений — "
            f"{result.corrected} исправлено, {result.savings_capped} savings срезано, "
            f"{result.conflicts_blocked} конфликтов, {result.flagged_review} на проверку, "
            f"{result.unreviewed_kept} без вердикта сохранено, удалено {result.deleted}"
        )
    return result


def _load_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("deterministic_opt_corrector: не прочитан %s: %s", path, exc)
        return None
