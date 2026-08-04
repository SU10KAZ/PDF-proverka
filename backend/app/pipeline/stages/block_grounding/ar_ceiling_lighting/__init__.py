"""Shadow-пилот профиля Вектографа «АР. План потолков и освещения».

Детерминированное извлечение семантического графа квартир (помещения,
потолочные зоны, световые выводы, группы освещения, выключатели,
мастер-выключатели, размерные привязки) из ВЕКТОРНОГО слоя PDF:
без LLM, без OCR, без растрового распознавания.

Пилот изолирован: в боевой Stage 01/02 и block_source_router НЕ включён.
Формат узлов/рёбер совместим с домовым конвертом block_grounding
({id,label,node_type,x,y,bbox_page,field_state} + аддитивные поля
provenance/tier), чтобы последующая интеграция не требовала перестройки.

Точка входа: :func:`build_ar_ceiling_lighting_result` (см. runner.py) и
CLI ``scripts/build_ar_ceiling_lighting_description.py``.
"""
from .runner import (PROFILE_ID, PROFILE_VERSION, build_ar_ceiling_lighting_result,
                     compact_fixture, run_profile, write_artifacts)

__all__ = ["PROFILE_ID", "PROFILE_VERSION", "build_ar_ceiling_lighting_result",
           "compact_fixture", "run_profile", "write_artifacts"]
