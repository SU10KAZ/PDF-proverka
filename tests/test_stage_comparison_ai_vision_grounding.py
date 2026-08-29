"""Визуальное доказательство: своя сторона, свой отпечаток, не выше текста.

Три вещи, которых у наблюдения по чертежу раньше не было.

Первая — сторона. Увиденное дописывалось в контекст обычной строкой, и после
этого «слева» и «справа» различались только тем, в какой список её положили.

Вторая — отпечаток изображения. Ключ кэша зависел от текстовых доказательств
элемента, поэтому перерисованный кроп возвращал ответ, данный про другую
картинку.

Третья — приоритет текста. На реальном листе модель читала «Корпус 1» вместо
«Корпус 4»; молча заменить прочитанное увиденным значит поверить худшему
источнику.
"""
from __future__ import annotations

import json

from backend.app.services.stage_comparison.ai import (
    cache as cache_module,
    evidence as evidence_module,
    vision as vision_module,
)

_STAMP = {"buildings": ["4"], "floors": ["3"], "sheet_kind": "PLAN"}


def _crop(side: str, page: int = 29, digest: str = "aa", **kwargs) -> vision_module.Crop:
    return vision_module.Crop(
        side=side, page=page, path=f"/tmp/{side}.png", digest=digest,
        document_digest="doc-" + side.lower(),
        bbox=kwargs.pop("bbox", (0.1, 0.2, 0.4, 0.3)),
        **kwargs,
    )


# ── Сторона наблюдения ────────────────────────────────────────────────────

def test_an_observation_is_bound_to_the_side_of_its_answer_key():
    lines = evidence_module.vision_lines(
        evidence_module.EvidenceItem(item_id="ureview_1"),
        {"observed_left": "EI 60", "observed_right": "EI 90", "model": "m"},
        crops=[
            {"side": "LEFT", "page": 29, "crop_ref": "LV:29", "digest": "aa"},
            {"side": "RIGHT", "page": 8, "crop_ref": "RV:8", "digest": "bb"},
        ],
    )

    assert [line["side"] for line in lines["LEFT"]] == ["LEFT"]
    assert [line["side"] for line in lines["RIGHT"]] == ["RIGHT"]
    assert "EI 60" in lines["LEFT"][0]["text"]
    assert "EI 90" in lines["RIGHT"][0]["text"]
    # Строка помечена источником: аналитик и верификатор видят, что это
    # прочитано с чертежа, а не из текста документа.
    assert {line["source"] for line in lines["LEFT"] + lines["RIGHT"]} == {"VISION"}
    assert lines["LEFT"][0]["crop_digests"] == ["aa"]
    assert lines["RIGHT"][0]["crop_digests"] == ["bb"]


def test_an_observation_about_a_side_that_was_never_shown_is_dropped():
    # Единственный детерминированный признак перепутанных сторон: модель
    # описала лист, изображения которого ей не давали.
    observations, problems = vision_module.observations_by_side(
        {"observed_left": "EI 60", "observed_right": "EI 90"},
        [_crop("LEFT")],
    )

    assert observations == {"LEFT": "EI 60"}
    assert problems == [f"{vision_module.SIDE_WITHOUT_IMAGE}:RIGHT"]


def test_swapped_observations_never_reach_the_opposite_side():
    observations, problems = vision_module.observations_by_side(
        {"observed_left": None, "observed_right": "виден левый фрагмент"},
        [_crop("LEFT")],
    )

    assert observations == {}
    assert problems == [f"{vision_module.SIDE_WITHOUT_IMAGE}:RIGHT"]


# ── Отпечаток изображения ─────────────────────────────────────────────────

def _vision_key(crops: list[vision_module.Crop]) -> str:
    from backend.app.services.stage_comparison.production_artifacts import (
        content_signature,
    )

    return cache_module.cache_key(
        evidence_digest=content_signature({
            "evidence_digest": "same-text-evidence",
            "role": "vision",
            **vision_module.cache_identity(crops),
        }),
        model="gpt-5.6-sol",
        reasoning_level="medium",
        prompt_version="vision.v3",
        schema_version="ai.v2",
        role="vision",
    )


def test_the_same_images_hit_the_same_cache_key():
    assert _vision_key([_crop("LEFT")]) == _vision_key([_crop("LEFT")])


def test_a_redrawn_image_misses_the_cache():
    assert _vision_key([_crop("LEFT", digest="aa")]) != _vision_key(
        [_crop("LEFT", digest="bb")]
    )


def test_moving_the_crop_misses_the_cache():
    assert _vision_key([_crop("LEFT")]) != _vision_key(
        [_crop("LEFT", bbox=(0.11, 0.2, 0.4, 0.3))]
    )


def test_another_page_misses_the_cache():
    assert _vision_key([_crop("LEFT")]) != _vision_key([_crop("LEFT", page=30)])


def test_another_source_document_misses_the_cache():
    other = _crop("LEFT")
    other.document_digest = "doc-other"

    assert _vision_key([_crop("LEFT")]) != _vision_key([other])


def test_the_whole_sheet_and_a_fragment_of_it_are_different_images():
    whole = _crop("LEFT", bbox=None)
    whole.whole_sheet = True

    assert _vision_key([_crop("LEFT")]) != _vision_key([whole])


def test_the_cache_identity_names_every_input_it_depends_on():
    identity = vision_module.cache_identity([_crop("LEFT")])
    blob = json.dumps(identity, ensure_ascii=False)

    for marker in ("side", "page", "bbox", "image_digest", "document_digest"):
        assert marker in blob


# ── Приоритет текстового штампа ───────────────────────────────────────────

def test_a_drawing_reading_another_building_contradicts_the_proven_stamp():
    assert vision_module.contradicts_text_stamp(
        "В штампе указан Корпус 1, план 3 этажа", _STAMP
    ) is True


def test_a_drawing_agreeing_with_the_stamp_is_not_a_contradiction():
    assert vision_module.contradicts_text_stamp(
        "Корпус 4, план 3 этажа", _STAMP
    ) is False


def test_a_drawing_saying_nothing_about_identity_is_not_a_contradiction():
    assert vision_module.contradicts_text_stamp("Видна перегородка EI 60", _STAMP) is False


def test_without_a_proven_stamp_there_is_nothing_to_contradict():
    assert vision_module.contradicts_text_stamp("Корпус 1", None) is False
    assert vision_module.contradicts_text_stamp("Корпус 1", {}) is False


def test_a_different_floor_also_contradicts_the_stamp():
    assert vision_module.contradicts_text_stamp("План 5 этажа", _STAMP) is True
