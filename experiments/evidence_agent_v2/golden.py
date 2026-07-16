"""EV2 golden-set sampler — сбалансированная диверсифицированная выборка.

Читает knowledge_base/evidence_golden_set.json (его генерит существующий
build_evidence_golden_set.py — общий артефакт, не Cursor-specific).

Проблема исходного бенчмарка Cursor: берёт первые N graphic-кейсов в порядке
файла => перекос (3286 rejected vs 175 confirmed) + все из одного проекта.
Здесь:
  - берём РАВНОЕ число confirmed и rejected (баланс классов);
  - round-robin по проектам (диверсификация, не один чертёж);
  - только кейсы, где реально резолвится PNG (иначе vision-проба бессмысленна).
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Optional

from .context import load_context

ROOT = Path(__file__).resolve().parents[2]
GOLDEN_FILE = ROOT / "knowledge_base" / "evidence_golden_set.json"
DECISIONS_FILE = ROOT / "knowledge_base" / "decisions_log.json"

# Порог Jaccard по токенам между summary@decision и current finding.problem.
# Ниже него считаем, что F-ID осыпался (пере-аудит регенерил finding) и пара
# битая — такие кейсы исключаем, иначе бенчмарк меряет шум (8% golden-set).
CONSISTENCY_JACCARD_MIN = 0.2

_decision_summary_cache: dict[str, str] | None = None


def _decision_summaries() -> dict[str, str]:
    """decision_id -> summary замечания НА МОМЕНТ экспертного решения."""
    global _decision_summary_cache
    if _decision_summary_cache is not None:
        return _decision_summary_cache
    out: dict[str, str] = {}
    if DECISIONS_FILE.is_file():
        data = json.load(DECISIONS_FILE.open(encoding="utf-8"))
        entries = data.get("entries", data) if isinstance(data, dict) else data
        for e in entries or []:
            did = e.get("id")
            if did:
                out[did] = e.get("summary") or ""
    _decision_summary_cache = out
    return out


def _toks(s: str) -> set:
    return {w for w in " ".join((s or "").lower().split()).split() if len(w) > 3}


def is_consistent(case: dict) -> bool:
    """True, если метка эксперта относится к ТОМУ ЖЕ замечанию, что в снапшоте."""
    summ = _decision_summaries().get(case.get("decision_id"))
    if not summ:
        return False  # нет summary@decision — не можем подтвердить связь, не берём
    cur = case["finding"].get("problem") or case["finding"].get("description") or ""
    a, b = _toks(summ), _toks(cur)
    if not (a | b):
        return False
    return len(a & b) / len(a | b) >= CONSISTENCY_JACCARD_MIN

# expert_decision -> ожидаемое поведение EV2 на graphic-кейсе
#   accepted  => замечание реальное   => EV2 НЕ должен reject (ideal: accept)
#   rejected  => ложное срабатывание  => EV2 должен reject
GRAPHIC_CLASSES = ("graphic_confirmed", "graphic_rejected", "graphic_mixed")


def _load() -> list[dict]:
    if not GOLDEN_FILE.is_file():
        raise FileNotFoundError(f"golden set not found: {GOLDEN_FILE}")
    data = json.load(GOLDEN_FILE.open(encoding="utf-8"))
    return data.get("cases", [])


def _norm_project(name: str) -> str:
    """Схлопнуть формы одного физического проекта: name и name.pdf — один ключ.
    (6 проектов в golden-set существуют в обеих формах — разные прогоны F-ID.)"""
    n = (name or "?").strip()
    return n[:-4] if n.lower().endswith(".pdf") else n


def _round_robin_by_project(cases: list[dict], limit: int) -> list[dict]:
    by_proj: dict[str, list[dict]] = defaultdict(list)
    for c in cases:
        by_proj[_norm_project(c.get("source_project", "?"))].append(c)
    projects = list(by_proj.keys())
    out: list[dict] = []
    idx = 0
    while len(out) < limit and any(by_proj[p] for p in projects):
        p = projects[idx % len(projects)]
        if by_proj[p]:
            out.append(by_proj[p].pop(0))
        idx += 1
    return out


def build_balanced_sample(
    *,
    per_class: int = 20,
    require_png: bool = True,
    consistency_gate: bool = True,
    classes: tuple[str, ...] = ("graphic_confirmed", "graphic_rejected"),
    png_scan_cap: int = 600,
    alia_only: bool = False,
) -> list[dict]:
    """Вернуть до per_class кейсов на каждый класс (диверсиф. по проектам, с PNG).

    consistency_gate=True (по умолчанию) отбрасывает кейсы с осыпавшимся F-ID
    (метка эксперта про другое замечание) — даёт ДОВЕРЕННЫЕ метки для бенчмарка.
    alia_only=True оставляет только проект 214 ASTERUS (Alia) — префикс «13АВ».
    """
    all_cases = _load()
    by_class: dict[str, list[dict]] = defaultdict(list)
    for c in all_cases:
        if c.get("case_class") in classes:
            if alia_only and not str(c.get("source_project", "")).startswith("13АВ"):
                continue
            if consistency_gate and not is_consistent(c):
                continue
            by_class[c["case_class"]].append(c)

    result: list[dict] = []
    for cls in classes:
        bucket = by_class.get(cls, [])
        if cls == "graphic_rejected":
            # визуальные misread вперёд: их vision-модель реально может опровергнуть
            misread = [c for c in bucket if is_visual_misread_reject(c)]
            other = [c for c in bucket if not is_visual_misread_reject(c)]
            pool = (_round_robin_by_project(misread, png_scan_cap)
                    + _round_robin_by_project(other, png_scan_cap))
        else:
            pool = _round_robin_by_project(bucket, png_scan_cap)
        picked: list[dict] = []
        for c in pool:
            if len(picked) >= per_class:
                break
            if require_png:
                finding = {**c["finding"], "id": c["item_id"]}
                try:
                    ctx = load_context(c["source_project"], finding, section=c.get("section") or "")
                except Exception:
                    ctx = None
                if ctx is None or not ctx.has_png:
                    continue
                c = {**c, "_primary_png": str(ctx.primary_png)}
            picked.append(c)
        result.extend(picked)
    return result


def expected_should_reject(case: dict) -> bool:
    """True, если EV2 ДОЛЖЕН вернуть reject (замечание = ложное срабатывание)."""
    return (case.get("expert_decision") or "").strip().lower() == "rejected"


# Маркеры того, что эксперт отклонил замечание именно как ВИЗУАЛЬНЫЙ misread —
# то, что vision-модель может опровергнуть по чертежу. Нормативные отклонения
# («требование не обязательно» и т.п.) сюда не попадают: их по картинке не поймать,
# и спрашивать с модели за них нечестно.
_MISREAD_MARKERS = (
    "прочит", "неверно прочит", "ошибся", "ошибка ии", "реальн", "на чертеже",
    "на самом деле", "указан", "значени", "ocr", "видно", "масштаб", "размер",
    "правильн", "фактическ", "корректн", "верное значение", "не соответствует факт",
)


def is_visual_misread_reject(case: dict) -> bool:
    """True, если отклонение — визуальный/числовой misread (vision может опровергнуть)."""
    if not expected_should_reject(case):
        return False
    reason = (case.get("expert_reason") or "").lower()
    return any(m in reason for m in _MISREAD_MARKERS)
