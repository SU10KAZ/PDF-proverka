"""
Детерминированный структурный критик оптимизаций (аугментация агентного).

Зачем
-----
Замер 07-07 (92 проекта): агентный критик оптимизаций даёт КАЧЕСТВЕННЫЕ
семантические вердикты (vendor_violation / conflicts_with_finding /
technical_issue — 21/21 конфликтов реальны). НО он агентный (`claude -p`) и
обрывается на больших входах: `reviews` < `items` (ЭО1: 14 предложений,
отрецензировано 7). Неотрецензированные предложения остаются БЕЗ вердикта → в
отчёте невидимы, а старый corrector их молча удалял.

Плюс `unrealistic_savings` у агентного критика — грубое мнение модели (порог
>50% без учёта основания расчёта).

Решение
-------
НЕ заменяем агентный критик (его семантика ценна), а **аугментируем** его
детерминированными СТРУКТУРНЫМИ проверками поверх результата:

  · no_traceability     — spec_items пуст ИЛИ page не задан (чистый Python);
  · unrealistic_savings — savings_pct > потолка И basis НЕ «расчёт»
                          (basis-aware, надёжнее агентного порога).

Правило слияния (консервативное, семантика в приоритете):
  1. агентный НЕ-pass вердикт (vendor/conflict/technical/too_vague/wrong_page) →
     оставляем как есть (семантику не перебиваем);
  2. иначе структурный вердикт (если есть);
  3. иначе pass.

Инвариант ПОКРЫТИЯ: КАЖДЫЙ item получает вердикт (в т.ч. неотрецензированные
агентным — закрытие дыры от обрыва). Формат optimization_review.json сохранён.
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

# Негативные вердикты агентного критика, которые структурка НЕ перебивает.
_SEMANTIC_NEGATIVE = {
    "vendor_violation", "conflicts_with_finding",
    "technical_issue", "too_vague", "wrong_page",
}
_STRUCTURAL = {"no_traceability", "unrealistic_savings"}
_ALL_VERDICTS = _SEMANTIC_NEGATIVE | _STRUCTURAL | {"pass"}


@dataclass
class DeterministicOptCriticResult:
    items_total: int = 0
    had_agentic_review: int = 0       # item'ы, у которых был агентный вердикт
    coverage_added: int = 0           # неотрецензированные, получившие вердикт
    structural_added: int = 0         # агентный pass → структурный не-pass
    error: Optional[str] = None
    reviews: list = field(default_factory=list)

    def verdict_counts(self) -> dict:
        c: Counter = Counter()
        for r in self.reviews:
            c[r.get("verdict", "pass")] += 1
        out = {"pass": c.get("pass", 0)}
        for v in sorted(_ALL_VERDICTS - {"pass"}):
            if c.get(v):
                out[v] = c[v]
        return out

    def to_review_dict(self, project_id: str = "") -> dict:
        counts = self.verdict_counts()
        return {
            "meta": {
                "project_id": project_id,
                "review_date": datetime.now().isoformat(),
                "stage": "optimization_critic",
                "mode": "agentic+deterministic",
                "total_reviewed": self.items_total,
                "verdicts": counts,
                "coverage_added": self.coverage_added,
                "structural_added": self.structural_added,
            },
            "reviews": self.reviews,
        }


# ═══════════════════════════════════════════════════════════════════════════
# Разбор входа
# ═══════════════════════════════════════════════════════════════════════════

def iter_opt_items(data) -> list:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("items", "optimizations", "scenarios"):
            value = data.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
    return []


def _item_id(item: dict):
    return item.get("id") or item.get("item_id") or item.get("opt_id")


def _existing_reviews(review_data) -> dict:
    out = {}
    reviews = []
    if isinstance(review_data, dict):
        reviews = review_data.get("reviews") or []
    elif isinstance(review_data, list):
        reviews = review_data
    for r in reviews:
        if isinstance(r, dict):
            rid = r.get("item_id") or r.get("id")
            if rid is not None:
                out[str(rid)] = r
    return out


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Структурные проверки
# ═══════════════════════════════════════════════════════════════════════════

def structural_verdict(item: dict, savings_cap: int = 50):
    """Вердикт по структурным проверкам или None (структурно чисто)."""
    spec = item.get("spec_items") or []
    page = item.get("page")
    has_trace = bool(spec) and page not in (None, "", 0)
    if not has_trace:
        return (
            "no_traceability",
            "spec_items пуст или page не задан — предложение не привязано к позиции.",
        )
    s = _as_float(item.get("savings_pct"))
    basis = (item.get("savings_basis") or "").lower()
    # basis-aware: не флагуем расчётную экономию, только экспертную оценку/«не определено»
    if s is not None and s > savings_cap and "расч" not in basis:
        return (
            "unrealistic_savings",
            f"savings_pct={s:g}% > {savings_cap}% при основании «{item.get('savings_basis') or 'не указано'}» (не расчёт).",
        )
    return None


def _mk_review(iid, verdict, details, source, agentic=None) -> dict:
    r = {
        "item_id": iid,
        "verdict": verdict,
        "details": details,
        "conflicting_finding_id": (agentic or {}).get("conflicting_finding_id"),
        "suggested_action": (agentic or {}).get("suggested_action"),
        "source": source,
    }
    return r


# ═══════════════════════════════════════════════════════════════════════════
# Ядро (без I/O)
# ═══════════════════════════════════════════════════════════════════════════

def augment_reviews(items: list, existing: dict, *, savings_cap: int = 50):
    """Слить агентные вердикты со структурными. Возвращает (reviews, result).

    Инвариант: len(reviews) == len(items) — вердикт у КАЖДОГО предложения."""
    result = DeterministicOptCriticResult(items_total=len(items))
    reviews = []
    for item in items:
        iid = _item_id(item)
        ag = existing.get(str(iid))
        ag_verdict = (ag.get("verdict") or "pass").lower() if ag else None
        struct = structural_verdict(item, savings_cap)

        if ag is not None:
            result.had_agentic_review += 1

        # 1) семантический не-pass агентного критика — не перебиваем
        if ag_verdict in _SEMANTIC_NEGATIVE:
            reviews.append(_mk_review(iid, ag_verdict, ag.get("details") or "",
                                      "agentic", ag))
            continue
        # 2) структурный вердикт
        if struct is not None:
            verdict, details = struct
            if ag is None:
                result.coverage_added += 1        # неотрецензированный → закрыт
            elif ag_verdict == "pass":
                result.structural_added += 1       # агентный pass → структурный не-pass
            reviews.append(_mk_review(iid, verdict, details, "deterministic", ag))
            continue
        # 3) pass
        if ag is not None:
            reviews.append(_mk_review(iid, "pass", ag.get("details") or "",
                                      "agentic", ag))
        else:
            result.coverage_added += 1             # неотрецензированный, структурно чист
            reviews.append(_mk_review(iid, "pass",
                                      "Не отрецензирован агентным критиком; структурно чист.",
                                      "deterministic-default"))
    result.reviews = reviews
    return reviews, result


# ═══════════════════════════════════════════════════════════════════════════
# Главная функция (I/O)
# ═══════════════════════════════════════════════════════════════════════════

async def run_deterministic_critic_augment(
    output_dir: Path,
    *,
    project_id: str = "",
    optimization_filename: str = "optimization.json",
    review_filename: str = "optimization_review.json",
    savings_cap: int = 50,
    on_log: Optional[Callable[[str], Awaitable[None]]] = None,
    write: bool = True,
) -> DeterministicOptCriticResult:
    """Аугментировать optimization_review.json структурными вердиктами.

    Читает optimization.json (все item'ы) + optimization_review.json (агентные
    вердикты, опционально) → пишет обратно review со 100% покрытием.
    """
    output_dir = Path(output_dir)
    opt_data = _load_json(output_dir / optimization_filename)
    if opt_data is None:
        return DeterministicOptCriticResult(error=f"{optimization_filename} не найден/невалиден")
    review_data = _load_json(output_dir / review_filename)  # может отсутствовать

    items = iter_opt_items(opt_data)
    existing = _existing_reviews(review_data)
    reviews, result = augment_reviews(items, existing, savings_cap=savings_cap)
    assert len(reviews) == len(items), "augment потерял/добавил вердикты"

    if write:
        (output_dir / review_filename).write_text(
            json.dumps(result.to_review_dict(project_id), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if on_log:
        await on_log(
            f"Optimization Critic (структурная аугментация): {result.items_total} предложений — "
            f"покрытие +{result.coverage_added}, новых структурных +{result.structural_added}"
        )
    return result


def _load_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("deterministic_opt_critic: не прочитан %s: %s", path, exc)
        return None
