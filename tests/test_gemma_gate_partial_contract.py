"""reserc.md #17 — мёртвый partial-гейт удалён, контракт «partial всегда ready».

partial_gemma_allowed вычислялся, но результат игнорировался (gate всегда отдавал
ready на partial). Функция и config-ключи allow_partial/partial_mode убраны;
поле state['partial_allowed'] зафиксировано как контракт = True.
"""
from __future__ import annotations

from backend.app.pipeline.stages.gemma_enrichment import gemma_gate as gg


def test_dead_partial_flag_function_removed():
    # функция-пустышка удалена (была 0 эффективных потребителей)
    assert not hasattr(gg, "partial_gemma_allowed")


def test_evaluate_still_exposed():
    assert callable(gg.evaluate_gemma_enrichment)


def test_partial_allowed_is_contract_true_ignores_config(tmp_path):
    """Даже при явном allow_partial=False состояние отдаёт partial_allowed=True —
    флаг больше не влияет (no-op), это и есть зафиксированный контракт #17."""
    st = gg.evaluate_gemma_enrichment(
        tmp_path, project_info={"gemma_enrichment": {"allow_partial": False}}
    )
    assert st["partial_allowed"] is True
