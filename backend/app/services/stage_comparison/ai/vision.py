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

import hashlib
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

#: Насколько расширить рамку находки, чтобы на кропе был виден контекст.
CROP_MARGIN = 0.06
#: Разрешение рендера. Выше — дороже и медленнее без выигрыша в читаемости
#: чертёжного шрифта.
CROP_DPI = 200
#: Разрешение листа целиком. Лист А2 при 200 dpi — это 4700 пикселей по
#: длинной стороне, которые провайдер всё равно ужмёт; 150 достаточно, чтобы
#: увидеть, есть ли на листе искомая строка.
SHEET_DPI = 150
#: Пометка, с которой наблюдение попадает в пакет доказательств.
OBSERVATION_PREFIX = "по чертежу:"

VISION_REASONS = frozenset({"GRAPHIC_EVIDENCE_REQUIRED", "EVIDENCE_TRUNCATED"})


@dataclass
class Crop:
    side: str
    page: int
    path: str
    #: Лист целиком, а не место находки. Крайний случай: применяется, когда у
    #: элемента с этой стороны нет координат вообще.
    whole_sheet: bool = False
    #: Рамка кропа в долях страницы: (x0, y0, x1, y1). Лист целиком — None.
    bbox: tuple[float, float, float, float] | None = None
    #: Отпечаток самого изображения. Ключ кэша обязан от него зависеть: иначе
    #: перерисованный кроп с теми же координатами вернёт чужой ответ.
    digest: str = ""
    #: Отпечаток исходного PDF: страница 9 старой и новой редакции — разные
    #: факты, и различать их по номеру страницы нельзя.
    document_digest: str = ""

    @property
    def crop_ref(self) -> str:
        """Стабильный адрес изображения внутри одного разрешения."""
        return f"{'LV' if self.side == 'LEFT' else 'RV'}:{self.page}"

    def identity(self) -> dict[str, Any]:
        """Всё, что делает это изображение именно этим изображением."""
        return {
            "side": self.side,
            "page": self.page,
            "whole_sheet": self.whole_sheet,
            "bbox": [round(value, 6) for value in self.bbox] if self.bbox else None,
            "image_digest": self.digest,
            "document_digest": self.document_digest,
        }

    def caption(self) -> str:
        side = "левая (старая) редакция" if self.side == "LEFT" else "правая (новая) редакция"
        what = "ЛИСТ ЦЕЛИКОМ" if self.whole_sheet else "фрагмент вокруг места находки"
        return f"{side}, стр. PDF {self.page}, {what}"


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
    sheet_pages: Mapping[str, Sequence[int]] | None = None,
) -> list[Crop]:
    """Отрисовать по одному изображению на сторону: сначала место находки.

    Если с одной стороны координат нет вовсе — а это ровно случай «строка
    добавлена» или «строка удалена», — то на месте находки рисовать нечего, и
    без противоположной стороны вопрос принципиально неразрешим: на первом же
    боевом прогоне все 15 обращений к резерву вернули INSUFFICIENT_IMAGE
    именно потому, что модели показывали одну сторону из двух. Поэтому для
    пустой стороны берётся лист целиком из пары листов: увидеть, что искомой
    строки на листе нет, можно только глядя на весь лист.
    """
    fitz = _fitz()
    out_dir.mkdir(parents=True, exist_ok=True)
    sheet_pages = sheet_pages or {}
    crops: list[Crop] = []
    for side in ("LEFT", "RIGHT"):
        path = pdf_paths.get(side)
        if not path:
            continue
        side_locations = list(locations.get(side) or [])
        whole_sheet = not side_locations
        if whole_sheet:
            pages = [int(page) for page in sheet_pages.get(side) or ()]
            page_number = pages[0] if pages else 0
        else:
            page_number = int(side_locations[0].get("page") or 0)
        if page_number < 1:
            continue
        with fitz.open(path) as document:
            if page_number > document.page_count:
                continue
            page = document[page_number - 1]
            rect = page.rect
            clip = None
            if not whole_sheet:
                box = _bounds(side_locations)
                if box is not None:
                    clip = fitz.Rect(
                        rect.x0 + box[0] * rect.width,
                        rect.y0 + box[1] * rect.height,
                        rect.x0 + box[2] * rect.width,
                        rect.y0 + box[3] * rect.height,
                    )
                    if clip.is_empty or clip.width < 4 or clip.height < 4:
                        clip = None
            pixmap = page.get_pixmap(
                dpi=SHEET_DPI if whole_sheet else dpi, clip=clip
            )
            suffix = "_sheet" if whole_sheet else ""
            target = out_dir / f"{side.lower()}_p{page_number}{suffix}.png"
            pixmap.save(str(target))
            bbox = None
            if clip is not None and rect.width and rect.height:
                bbox = (
                    (float(clip.x0) - rect.x0) / rect.width,
                    (float(clip.y0) - rect.y0) / rect.height,
                    (float(clip.x1) - rect.x0) / rect.width,
                    (float(clip.y1) - rect.y0) / rect.height,
                )
        crops.append(Crop(
            side=side, page=page_number, path=str(target), whole_sheet=whole_sheet,
            bbox=bbox,
            digest=file_digest(target),
            document_digest=file_digest(Path(path)),
        ))
    return crops


def file_digest(path: Path | str) -> str:
    """Отпечаток файла. Пустая строка — если файла нет: кэш тогда промахнётся."""
    try:
        digest = hashlib.sha256()
        with open(path, "rb") as stream:
            for chunk in iter(lambda: stream.read(1 << 20), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return ""


def cache_identity(crops: Sequence[Crop]) -> dict[str, Any]:
    """Часть ключа кэша, описывающая ИМЕННО ЭТИ изображения.

    Без неё ключ зависел только от текстовых доказательств элемента, и
    перерисованный кроп — другой отступ, другое разрешение, исправленный
    исходный PDF — возвращал ответ, данный про другую картинку.
    """
    return {
        "vision_images": [crop.identity() for crop in crops],
        "crop_dpi": CROP_DPI,
        "sheet_dpi": SHEET_DPI,
        "crop_margin": CROP_MARGIN,
    }


def needs_vision(
    *,
    resolution: Mapping[str, Any] | None,
    graphic_route: str | None,
    source: str = "TEXT",
) -> bool:
    """Строго два повода: так сказал аналитик или так сказал роутер графики.

    Второй повод действует ТОЛЬКО на графические элементы. `VISION_REQUIRED`
    означает «геометрию этого блока вектором не сравнить», а не «строку
    таблицы нельзя прочитать»: на паре АР маршрут графики был VISION_REQUIRED,
    а все 423 нерешённых элемента — текстовые, и резерв уходил рисовать кропы
    вокруг строки «203,26», пока не упирался в собственный предел.
    """
    if isinstance(resolution, Mapping):
        # Разобранный текстом элемент картинке не нужен, какой бы маршрут ни
        # выбрал детерминированный роутер: резерв — это резерв.
        if resolution.get("resolution_status") == "AI_RESOLVED":
            return False
        if str(resolution.get("human_reason") or "") in VISION_REASONS:
            return True
    return (
        graphic_route == "VISION_REQUIRED"
        and str(source or "TEXT").upper() == "GRAPHIC"
    )


#: Наблюдение о стороне, которую модели не показывали. Единственный признак,
#: по которому перепутанные местами observed_left/observed_right видно
#: детерминированно: модель описала лист, изображения которого не было.
SIDE_WITHOUT_IMAGE = "VISION_SIDE_WITHOUT_IMAGE"

_BUILDING_RE = re.compile(r"корпус\w*\s*(\d+(?:\.\d+)?)", re.I)
_FLOOR_RE = re.compile(r"(\d+)\s*этаж", re.I)


def observations_by_side(
    payload: Mapping[str, Any],
    crops: Sequence[Crop] = (),
) -> tuple[dict[str, str], list[str]]:
    """Наблюдения, привязанные к своей стороне, и найденные несоответствия.

    Сторона наблюдения задаётся не текстом, а ключом ответа: observed_left —
    только LEFT, observed_right — только RIGHT. Наблюдение о стороне, кроп
    которой не отрисовывался, отбрасывается: модель либо перепутала стороны,
    либо описала то, чего ей не показывали, и в обоих случаях это не
    доказательство.
    """
    shown = {crop.side for crop in crops or ()}
    output: dict[str, str] = {}
    problems: list[str] = []
    for side, key in (("LEFT", "observed_left"), ("RIGHT", "observed_right")):
        value = " ".join(str(payload.get(key) or "").split())
        if not value:
            continue
        if crops and side not in shown:
            problems.append(f"{SIDE_WITHOUT_IMAGE}:{side}")
            continue
        output[side] = value
    return output, problems


def contradicts_text_stamp(
    observation: str,
    stamp_identity: Mapping[str, Any] | None,
) -> bool:
    """Спорит ли увиденное на чертеже с ДОКАЗАННЫМ текстовым штампом.

    Текстовый штамп прочитан из вектор-слоя и первичен. На реальном листе
    модель уже читала «Корпус 1» вместо «Корпус 4» — если такое повторяется,
    это повод показать инженеру оба доказательства, а не молча переписать
    текст картинкой.
    """
    if not isinstance(stamp_identity, Mapping) or not observation:
        return False
    buildings = {
        str(value).strip() for value in stamp_identity.get("buildings") or ()
    }
    floors = {str(value).strip() for value in stamp_identity.get("floors") or ()}
    seen_buildings = set(_BUILDING_RE.findall(observation))
    seen_floors = set(_FLOOR_RE.findall(observation))
    if buildings and seen_buildings and not (seen_buildings & buildings):
        return True
    return bool(floors and seen_floors and not (seen_floors & floors))


def observations_to_context(payload: Mapping[str, Any]) -> dict[str, list[str]]:
    """Наблюдения с картинки в виде помеченных строк.

    Оставлено для совместимости чтения старых артефактов; новый путь строит
    типизированные строки доказательств через ``evidence.vision_lines``.
    """
    observations, _problems = observations_by_side(payload)
    return {
        side: ([f"{OBSERVATION_PREFIX} {observations[side]}"] if side in observations else [])
        for side in ("LEFT", "RIGHT")
    }


def crop_workdir() -> Path:
    return Path(tempfile.mkdtemp(prefix="sc_ai_vision_"))


__all__ = [
    "CROP_DPI",
    "CROP_MARGIN",
    "SHEET_DPI",
    "SIDE_WITHOUT_IMAGE",
    "Crop",
    "OBSERVATION_PREFIX",
    "VISION_REASONS",
    "cache_identity",
    "contradicts_text_stamp",
    "crop_workdir",
    "file_digest",
    "needs_vision",
    "observations_by_side",
    "observations_to_context",
    "render_crops",
]
