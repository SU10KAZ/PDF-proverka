"""Чтение PDF для сравнения версий.

Исходник открывается ТОЛЬКО как копия в памяти (``fitz.open(stream=...)``).
Все правки — удаление аннотаций для извлечения текста, отделение
предупреждений — делаются в этой копии и никогда не сохраняются. Тест
``test_version_comparison_sources_immutable`` дополнительно проверяет, что в
пакете нет вызовов сохранения документа.

Координаты: PyMuPDF отдаёт слова, векторы, аннотации и картинки в
НЕповёрнутой системе страницы, а рендер — в повёрнутой (как видит человек).
Здесь всё переводится в повёрнутую систему через ``page.rotation_matrix``.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from pathlib import Path
from typing import Any, Iterable

import fitz
import numpy as np

SHX_ANNOT_TITLE = "AutoCAD SHX Text"


def open_memory(data: bytes) -> fitz.Document:
    return fitz.open(stream=data, filetype="pdf")


def read_bytes(path: Path) -> bytes:
    return Path(path).read_bytes()


def to_display(page: fitz.Page, rect: Iterable[float], matrix: fitz.Matrix | None = None) -> list[float]:
    """Прямоугольник из неповёрнутой системы страницы в повёрнутую.

    ``matrix`` — заранее взятая ``page.rotation_matrix``: её вычисление в
    PyMuPDF дорогое, на сотнях тысяч объектов страницы это главный расход.
    """
    r = fitz.Rect(*rect) * (matrix if matrix is not None else page.rotation_matrix)
    r.normalize()
    return [float(r.x0), float(r.y0), float(r.x1), float(r.y1)]


def normalize_token(text: str) -> str:
    """Нормализация для сопоставления листов (не для показа отличий)."""
    text = unicodedata.normalize("NFKC", text or "").strip().lower()
    return re.sub(r"\s+", " ", text)


# ---------------------------------------------------------------- аннотации

def annotation_records(page: fitz.Page) -> list[dict[str, Any]]:
    out = []
    for annot in page.annots() or []:
        info = annot.info or {}
        title = str(info.get("title") or "")
        out.append({
            "type": annot.type[1] if annot.type else "",
            "subject": str(info.get("subject") or ""),
            "author": title,
            "content": str(info.get("content") or ""),
            "rect": to_display(page, annot.rect),
            # «AutoCAD SHX Text» — скрытый текст SHX-надписей: источник текста,
            # а не пометка (план §1.2).
            "role": "shx_text" if title == SHX_ANNOT_TITLE else "markup",
        })
    return out


def strip_annotations(doc: fitz.Document) -> None:
    """Удалить аннотации во всех страницах КОПИИ В ПАМЯТИ.

    Нужно для текста: ``get_text`` включает текст FreeText-аннотаций
    (проверено на ЭМ2 (1) PDF 23), а аннотации сравниваются отдельно.
    """
    for page in doc:
        for annot in list(page.annots() or []):
            page.delete_annot(annot)


def page_words(page: fitz.Page) -> list[list[Any]]:
    """Слова страницы в повёрнутой системе: [x0, y0, x1, y1, text]."""
    out = []
    m = page.rotation_matrix
    for w in page.get_text("words"):
        text = str(w[4]).strip()
        if not text:
            continue
        out.append(to_display(page, w[:4], m) + [text])
    return out


def image_records(doc: fitz.Document, page: fitz.Page) -> list[dict[str, Any]]:
    area = max(1.0, page.rect.width * page.rect.height)
    smasks = {img[0]: img[1] for img in page.get_images(full=True)}
    out = []
    for info in page.get_image_info(xrefs=True):
        xref = int(info.get("xref") or 0)
        bbox = to_display(page, info.get("bbox") or (0, 0, 0, 0))
        w = max(0.0, bbox[2] - bbox[0])
        h = max(0.0, bbox[3] - bbox[1])
        digest = ""
        if xref:
            try:
                digest = hashlib.sha256(doc.xref_stream_raw(xref) or b"").hexdigest()[:16]
            except Exception:
                digest = ""
        out.append({
            "xref": xref,
            "bbox": bbox,
            "area_share": round(w * h / area, 4),
            "smask": bool(smasks.get(xref)),
            "has_alpha": bool(info.get("has-mask")),
            "digest": digest,
            "width_px": int(info.get("width") or 0),
            "height_px": int(info.get("height") or 0),
        })
    return out


# ------------------------------------------------------------ хэш содержимого

_STREAM_META = re.compile(r"/(Length|Filter|DecodeParms)\s+(\[[^\]]*\]|<<.*?>>|/\w+|\d+(\s+0\s+R)?)", re.S)
_REF = re.compile(r"(\d+)\s+0\s+R")


class _Merkle:
    """Хэш объекта PDF, не зависящий от номеров объектов.

    Ссылки «N 0 R» заменяются хэшами объектов, на которые они указывают; у
    потоков сравниваются распакованные байты, а /Length и /Filter
    отбрасываются. Поэтому два файла с одинаковым содержимым дают одинаковый
    хэш, даже если PDF пересохранён с другой нумерацией. Обратное не нужно:
    разный хэш просто ведёт на полный путь сравнения.
    """

    def __init__(self, doc: fitz.Document):
        self.doc = doc
        self.memo: dict[int, str] = {}
        self.active: set[int] = set()

    def obj(self, xref: int) -> str:
        if xref in self.memo:
            return self.memo[xref]
        if xref in self.active:
            return "cycle"
        self.active.add(xref)
        try:
            text = self.doc.xref_object(xref, compressed=False)
            stream_digest = ""
            if self.doc.xref_is_stream(xref):
                text = _STREAM_META.sub("", text)
                stream_digest = hashlib.sha256(self.doc.xref_stream(xref) or b"").hexdigest()
            text = self.refs(text)
            digest = hashlib.sha256((text + "|" + stream_digest).encode("utf-8", "replace")).hexdigest()
        finally:
            self.active.discard(xref)
        self.memo[xref] = digest
        return digest

    def refs(self, text: str) -> str:
        return _REF.sub(lambda m: "#" + self.obj(int(m.group(1)))[:24], text)


def _page_key(doc: fitz.Document, page: fitz.Page, key: str) -> tuple[str, str]:
    """Ключ страницы с учётом наследования от /Parent."""
    xref = page.xref
    for _ in range(32):
        kind, value = doc.xref_get_key(xref, key)
        if kind != "null":
            return kind, value
        kind, value = doc.xref_get_key(xref, "Parent")
        if kind != "xref":
            break
        xref = int(value.split()[0])
    return "null", "null"


def content_hash(doc: fitz.Document, page: fitz.Page, merkle: _Merkle | None = None) -> str | None:
    """Хэш того, что рисуется на странице, без аннотаций (план §5 шаг 1).

    Включает: потоки содержимого, рекурсивно ресурсы, геометрию страницы,
    конфигурацию видимости слоёв документа. Равенство хэшей двух страниц
    означает одинаковый рендер по построению. None — хэш не вычислен.
    """
    merkle = merkle or _Merkle(doc)
    try:
        h = hashlib.sha256()
        geom = (
            f"mb={tuple(round(v, 3) for v in page.mediabox)};cb={tuple(round(v, 3) for v in page.cropbox)};"
            f"rot={page.rotation};"
        )
        uu_kind, uu = _page_key(doc, page, "UserUnit")
        h.update((geom + f"uu={uu if uu_kind != 'null' else 1}").encode())
        h.update(page.read_contents() or b"")
        kind, value = _page_key(doc, page, "Resources")
        h.update(merkle.refs(value if kind != "null" else "").encode("utf-8", "replace"))
        cat = doc.pdf_catalog()
        ok, oc = doc.xref_get_key(cat, "OCProperties")
        if ok != "null":
            h.update(merkle.refs(oc).encode("utf-8", "replace"))
        return h.hexdigest()
    except Exception:
        return None


def text_hash(words: list[list[Any]]) -> str:
    items = sorted(f"{round(w[0])},{round(w[1])},{w[4]}" for w in words)
    return hashlib.sha256("\n".join(items).encode("utf-8")).hexdigest()


def annots_hash(annots: list[dict[str, Any]]) -> str:
    items = sorted(
        f"{a['type']}|{a['subject']}|{a['author']}|{a['content']}|{[round(v) for v in a['rect']]}"
        for a in annots
    )
    return hashlib.sha256("\n".join(items).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------- рендер

def render(page: fitz.Page, matrix: fitz.Matrix, *, clip: fitz.Rect | None = None,
           gray: bool = False) -> tuple[np.ndarray, int, int]:
    """Рендер без аннотаций. Возвращает (массив, x0, y0) — x0/y0 = угол irect."""
    cs = fitz.csGRAY if gray else fitz.csRGB
    pix = page.get_pixmap(matrix=matrix, colorspace=cs, alpha=False, annots=False, clip=clip)
    arr = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    if gray:
        arr = arr[:, :, 0]
    return arr.copy(), int(pix.x), int(pix.y)
