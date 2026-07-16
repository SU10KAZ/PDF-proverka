from __future__ import annotations

from backend.app.pipeline.stages.block_grounding.architecture_geometry import (
    PROFILE_FLOOR_WALL_JUNCTION,
    PROFILE_OPENING_DRAWING,
    classify_ar_profile,
)
from backend.app.pipeline.stages.block_grounding.alia_scheme_geometry import (
    PROFILE_CCTV,
    classify_alia_scheme_profile,
)
from backend.app.pipeline.stages.block_grounding.block_source_router import (
    _classification_context,
    _classification_metadata,
)
from backend.app.pipeline.stages.block_grounding.alia_remaining_geometry import (
    PROFILE_INSTALL,
    PROFILE_PLAN,
    classify_remaining_profile,
)
from backend.app.pipeline.stages.block_grounding.electrical_geometry import (
    PROFILE_SINGLELINE,
    classify_electrical_profile,
)
from backend.app.pipeline.stages.block_grounding.general_plan_geometry import (
    PROFILE_GENERAL,
    classify_gp_profile,
)
from backend.app.pipeline.stages.block_grounding.hvac_geometry import (
    PROFILE_PLAN as HVAC_PLAN,
    classify_hvac_profile,
)
from backend.app.pipeline.stages.block_grounding.structural_geometry import (
    KJ_SECTION,
    KM_MEMBER,
    classify_kj_profile,
    classify_km_profile,
)
from backend.app.pipeline.stages.block_grounding.technology_geometry import (
    PARKING,
    classify_tx_profile,
)
from backend.app.pipeline.stages.block_grounding.water_geometry import (
    PROFILE_PLAN as WATER_PLAN,
    classify_water_profile,
)
from backend.app.pipeline.stages.crop_blocks.block_markdown import (
    extract_chandra_block_description,
)


SAMPLE = """\
## СТРАНИЦА 1

### BLOCK [IMAGE]: DETAIL-1
**[ИЗОБРАЖЕНИЕ]** | Тип: Схема

**Краткое описание:** Узел примыкания плавающего пола к стене.

**Описание:** Две схемы устройства гидроизоляции пола и стены.

**Текст на чертеже:** ЯД-ИЗ-ТЕКСТА-ЧЕРТЕЖА. Маркировочный план.

**Сущности:** ЯД-ИЗ-СУЩНОСТЕЙ

**[ENRICHED google/gemma-test @ 2026-07-01T00:00:00]**
- **Тип блока:** ЯД-ИЗ-ОБОГАЩЕНИЯ

### BLOCK [IMAGE]: OTHER-2
**[ИЗОБРАЖЕНИЕ]** | Тип: План
**Краткое описание:** Другой блок.
"""


def test_chandra_contract_uses_only_semantic_description_fields():
    description = extract_chandra_block_description(SAMPLE, "DETAIL-1")

    assert description is not None
    assert description.block_type == "Схема"
    assert description.short_description == "Узел примыкания плавающего пола к стене."
    assert description.description == "Две схемы устройства гидроизоляции пола и стены."
    assert "ЯД-" not in description.classification_text
    assert "Маркировочный план" not in description.classification_text


def test_chandra_context_is_not_polluted_by_vector_text_or_page_stamp():
    description = extract_chandra_block_description(SAMPLE, "DETAIL-1")
    context = _classification_context(
        description,
        "См. кладочный план и разрез лестничной клетки",
        "Архитектурные решения. Маркировочный план",
    )

    assert "схемы устройства гидроизоляции" in context
    assert "кладочный план" not in context
    assert "лестничной клетки" not in context
    assert "Маркировочный план" not in context
    assert classify_ar_profile(context) == PROFILE_FLOOR_WALL_JUNCTION


def test_classification_metadata_records_ignored_chandra_sources():
    description = extract_chandra_block_description(SAMPLE, "DETAIL-1")
    metadata = _classification_metadata(description, PROFILE_FLOOR_WALL_JUNCTION)

    assert metadata["source"] == "chandra_md"
    assert metadata["confidence"] == "high"
    assert metadata["chandra_drawing_text_used"] is False
    assert metadata["ignored_chandra_fields"] == [
        "Текст на чертеже", "Сущности", "ENRICHED"
    ]


def test_chandra_wording_maps_firestop_and_metal_profile_drawings():
    low_voltage_profile, _ = classify_remaining_profile(
        "Изометрическая схема проходки кабеля с применением огнезащитных материалов."
    )

    assert low_voltage_profile == PROFILE_INSTALL
    assert classify_km_profile(
        "Спецификация с геометрическими схемами профильных элементов и сечений."
    ) == KM_MEMBER


def test_chandra_semantics_have_priority_in_every_discipline():
    # АР: это конкретный конфликт 9TU6-AR4J-CAP. Разрезы и кладочные планы
    # упомянуты лишь как дополнительные сведения и ссылки.
    assert classify_ar_profile(
        "Схема. На фрагменте представлены фронтальные виды дверных блоков и люков "
        "с габаритами и маркировкой. Дополнительно показаны разрезы люков; марки "
        "проёмов приведены на кладочных планах рядом с лестничной клеткой."
    ) == PROFILE_OPENING_DRAWING

    assert classify_gp_profile(
        "План благоустройства и озеленения территории с проездами и площадками."
    ) == PROFILE_GENERAL
    assert classify_electrical_profile(
        "Однолинейная электрическая схема распределительного щита с аппаратами QF."
    ) == PROFILE_SINGLELINE
    assert classify_kj_profile(
        "Схемы армирования стены и сечения армирования с позициями стержней."
    ) == KJ_SECTION
    assert classify_km_profile(
        "Спецификация и геометрические схемы профильных металлических элементов."
    ) == KM_MEMBER
    assert classify_tx_profile(
        "План подземной автостоянки с машиноместами; показаны входы в лифтовые холлы."
    ) == PARKING

    hvac, _ = classify_hvac_profile(
        "План систем вентиляции этажа с воздуховодами; рядом дана ссылка на аксонометрию.",
        block_id="4MDN-MDUE-J3J",
        prefer_block_hint=False,
    )
    assert hvac == HVAC_PLAN
    water, _ = classify_water_profile(
        "План систем водоснабжения и канализации этажа со стояками.",
        block_id="6TC7-T7MM-CHH",
        prefer_block_hint=False,
    )
    assert water == WATER_PLAN

    assert classify_alia_scheme_profile(
        "Структурная схема системы видеонаблюдения с камерами и линиями связи."
    ) == PROFILE_CCTV
    low_voltage, _ = classify_remaining_profile(
        "План размещения камер видеонаблюдения на этаже.",
        block_id="93XF-ML4D-VL3",
        prefer_block_hint=False,
    )
    assert low_voltage == PROFILE_PLAN
