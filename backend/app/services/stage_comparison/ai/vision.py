"""Визуальный резерв: чертёж как подтверждение, а не как истина.

Текстовые режимы всегда первичны. Vision вызывается только там, где
детерминированный слой честно сказал, что вектора не хватает
(route = VISION_REQUIRED), или где аналитик отказался именно из-за отсутствия
графики. Это не «второй аналитик на всякий случай»: каждая картинка стоит
дороже целой партии текста.

И Vision НЕ является источником истины. На реальном листе модель уже прочитала
«Корпус 1» вместо «Корпус 4» — поэтому текстовый штамп при наличии остаётся
первичным доказательством, а увиденное на картинке лишь ДОБАВЛЯЕТСЯ в пакет
как наблюдение с явной пометкой. Итоговый вывод после этого всё равно делает
текстовый аналитик, и всё равно через тот же верификатор.
"""
from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

#: Насколько расширить рамку находки, чтобы на кропе был виден контекст.
CROP_MARGIN = 0.06
#: Разрешение рендера. Выше — дороже и медленнее без выигрыша в читаемости
#: чертёжного шрифта.
CROP_DPI = 200
#: Пометка, с которой наблюдение попадает в пакет доказательств.
OBSERVATION_PREFIX = "по чертежу:"

VISION_REASONS = frozenset({"GRAPHIC_EVIDENCE_REQUIRED", "EVIDENCE_TRUNCATED"})


@dataclass
class Crop:
    side: str
    page: int
    path: str


def _fitz():
    import fitz  # noqa: PLC0415 — тяжёлый импорт только по требованию

    return fitz


def _bounds(
    locations: Iterable[Mapping[str, Any]],
) -> tuple[float, float, float, float] | None:
    """Общая рамка находки в долях страницы, расширенная на поля."""
    boxes = [
        box
        for location in locations or ()
        for box in (location.get("bboxes") or [])
        if isinstance(box, Mapping)
    ]
    if not boxes:
        return None
    x0 = min(float(box.get("x", 0)) for box in boxes)
    y0 = min(float(box.get("y", 0)) for box in boxes)
    x1 = max(float(box.get("x", 0)) + float(box.get("width", 0)) for box in boxes)
    y1 = max(float(box.get("y", 0)) + float(box.get("height", 0)) for box in boxes)
    return (
        max(0.0, x0 - CROP_MARGIN),
        max(0.0, y0 - CROP_MARGIN),
        min(1.0, x1 + CROP_MARGIN),
        min(1.0, y1 + CROP_MARGIN),
    )


def render_crops(
    *,
    pdf_paths: Mapping[str, str],
    locations: Mapping[str, Sequence[Mapping[str, Any]]],
    out_dir: Path,
    dpi: int = CROP_DPI,
) -> list[Crop]:
    """Отрисовать по одному фрагменту на сторону: сначала место находки.

    Полная страница — крайний случай: на листе формата А1 находка занимает
    доли процента площади, и целая страница в разрешении, где её видно, весит
    столько, что смысл резерва теряется.
    """
    fitz = _fitz()
    out_dir.mkdir(parents=True, exist_ok=True)
    crops: list[Crop] = []
    for side in ("LEFT", "RIGHT"):
        path = pdf_paths.get(side)
        side_locations = list(locations.get(side) or [])
        if not path or not side_locations:
            continue
        page_number = int(side_locations[0].get("page") or 0)
        if page_number < 1:
            continue
        with fitz.open(path) as document:
            if page_number > document.page_count:
                continue
            page = document[page_number - 1]
            rect = page.rect
            box = _bounds(side_locations)
            clip = None
            if box is not None:
                clip = fitz.Rect(
                    rect.x0 + box[0] * rect.width,
                    rect.y0 + box[1] * rect.height,
                    rect.x0 + box[2] * rect.width,
                    rect.y0 + box[3] * rect.height,
                )
                if clip.is_empty or clip.width < 4 or clip.height < 4:
                    clip = None
            pixmap = page.get_pixmap(dpi=dpi, clip=clip)
            target = out_dir / f"{side.lower()}_p{page_number}.png"
            pixmap.save(str(target))
        crops.append(Crop(side=side, page=page_number, path=str(target)))
    return crops


def needs_vision(
    *,
    resolution: Mapping[str, Any] | None,
    graphic_route: str | None,
) -> bool:
    """Строго два повода: так сказал детерминированный роутер или аналитик."""
    if isinstance(resolution, Mapping):
        # Разобранный текстом элемент картинке не нужен, какой бы маршрут ни
        # выбрал детерминированный роутер: резерв — это резерв.
        if resolution.get("resolution_status") == "AI_RESOLVED":
            return False
        if str(resolution.get("human_reason") or "") in VISION_REASONS:
            return True
    return graphic_route == "VISION_REQUIRED"


def observations_to_context(payload: Mapping[str, Any]) -> dict[str, list[str]]:
    """Наблюдения с картинки — в пакет доказательств, с явной пометкой.

    Пометка обязательна: после неё верификатор по-прежнему требует дословного
    совпадения, но инженеру видно, что это прочитано с чертежа, а не из текста.
    """
    output: dict[str, list[str]] = {"LEFT": [], "RIGHT": []}
    for side, key in (("LEFT", "observed_left"), ("RIGHT", "observed_right")):
        value = " ".join(str(payload.get(key) or "").split())
        if value:
            output[side].append(f"{OBSERVATION_PREFIX} {value}")
    return output


def crop_workdir() -> Path:
    return Path(tempfile.mkdtemp(prefix="sc_ai_vision_"))


__all__ = [
    "CROP_DPI",
    "CROP_MARGIN",
    "Crop",
    "OBSERVATION_PREFIX",
    "VISION_REASONS",
    "crop_workdir",
    "needs_vision",
    "observations_to_context",
    "render_crops",
]
