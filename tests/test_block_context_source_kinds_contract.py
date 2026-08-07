"""SOURCE_KINDS обязан покрывать всё, что реально производит роутер.

Инцидент 06.08.2026: профиль «Условные обозначения» начал отдавать
`source_kind="structured_legend"` (block_source_router.py), а в allowlist
контракта его не добавили. Любой проект, где нашёлся блок легенды, падал на
подготовке с «Канонический block_context summary невалиден: unknown
source_kind» — при этом сам блок был разобран корректно.

Класс ошибки: производитель и валидатор значения живут в разных файлах и
расходятся молча. Тест сводит их принудительно.
"""
from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
ROUTER = ROOT / "backend/app/pipeline/stages/block_grounding/block_source_router.py"
BUILDER = ROOT / "backend/app/pipeline/stages/block_context/builder.py"


def _emitted_source_kinds() -> set[str]:
    """Строковые литералы, которые роутер присваивает source_kind."""
    src = ROUTER.read_text(encoding="utf-8")
    return set(re.findall(r'source_kind\s*=\s*"([a-z0-9_]+)"', src))


def test_router_emits_only_known_source_kinds():
    from backend.app.pipeline.stages.block_context.contract import SOURCE_KINDS

    emitted = _emitted_source_kinds()
    assert emitted, "не нашли ни одного литерала — regex устарел, почини тест"
    unknown = sorted(emitted - set(SOURCE_KINDS))
    assert not unknown, (
        f"роутер отдаёт source_kind, которых нет в SOURCE_KINDS: {unknown}. "
        "Проект с таким блоком упадёт на валидации контракта. Добавь значение "
        "в contract.SOURCE_KINDS."
    )


def test_legend_profile_is_accepted():
    """Регрессия на конкретный инцидент."""
    from backend.app.pipeline.stages.block_context.contract import SOURCE_KINDS

    assert "structured_legend" in SOURCE_KINDS


def test_no_vector_text_kinds_are_either_known_or_normalized_away():
    """Второй список не должен содержать значений-призраков.

    `gemma_fallback` в SOURCE_KINDS отсутствует законно: builder.py:248
    переписывает его в `image_only` ДО записи сводки. Если нормализация
    исчезнет, значение начнёт доходить до валидатора и ронять стадию.
    """
    from backend.app.pipeline.stages.block_context.contract import (
        NO_VECTOR_TEXT_SOURCE_KINDS,
        SOURCE_KINDS,
    )

    builder_src = BUILDER.read_text(encoding="utf-8")
    for kind in sorted(NO_VECTOR_TEXT_SOURCE_KINDS):
        if kind in SOURCE_KINDS:
            continue
        assert f'source == "{kind}"' in builder_src, (
            f"{kind!r} нет ни в SOURCE_KINDS, ни в нормализации builder.py — "
            "значит он дойдёт до валидатора и уронит подготовку"
        )


def test_all_summaries_on_disk_pass_the_allowlist():
    """Живая проверка: ни один существующий проект не заблокирован контрактом.

    Пропускается, если данных проектов нет (чистая машина, CI).
    """
    import json

    from backend.app.pipeline.stages.block_context.contract import SOURCE_KINDS

    base = ROOT / "projects_v2"
    if not base.exists():
        import pytest

        pytest.skip("нет projects_v2 — нечего проверять")

    offenders: dict[str, set[str]] = {}
    checked = 0
    for path in base.glob("**/block_context_summary.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        checked += 1
        bad = {
            str(b.get("source_kind"))
            for b in (data.get("blocks") or [])
            if isinstance(b, dict) and b.get("source_kind") not in SOURCE_KINDS
        }
        if bad:
            offenders[str(path)] = bad
    assert not offenders, (
        f"проверено {checked} сводок; заблокированы контрактом: "
        + "; ".join(f"{k} → {sorted(v)}" for k, v in list(offenders.items())[:5])
    )
