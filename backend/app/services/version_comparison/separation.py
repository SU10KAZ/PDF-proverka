"""Безопасное отделение наложенных объектов (план §5.1).

Наложенные поверх чертежа объекты — отметка выпуска «В производство работ»
с QR, предупреждения «НЕ СОГЛАСОВАНО», водяные знаки — отделяются, чтобы
сравнить чертёж под ними, и сравниваются отдельно (служебные зоны).

Правила:
* только копия в памяти, без сохранения;
* только объекты из белого списка: последний поток содержимого страницы
  (так PDF-редакторы дописывают штампы) и крупная полупрозрачная картинка,
  которой нет в другой версии. Отделение по цвету или области запрещено;
* контроль: рендер с объектом и без него различается только там, где объект
  рисует сам (включая белые подложки); исчезнувшие слова лежат там же; новых
  слов не появилось. Иначе объект не отделяется;
* поток отделяется, только если его текст — отметка выпуска или
  предупреждение (или он рисует только QR без текста). Любой другой
  дописанный поток остаётся в основном сравнении: это может быть правка.
"""
from __future__ import annotations

import hashlib
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import cv2
import fitz
import numpy as np

from . import pdfio
from .settings import WARNING_MIN_AREA_SHARE
from .stamp import RELEASE_PATTERNS

CHECK_DPI = 72
# Операторы, которые что-то рисуют (а не только сохраняют состояние или сдвигают систему координат).
_PAINT_OPS = re.compile(rb"(?<![A-Za-z])(Do|Tj|TJ|'|re|f\*?|F|S|s|B\*?|b\*?|BI|sh)(?![A-Za-z])")
WARNING_PATTERNS = (
    re.compile(r"не\s*согласован", re.I),
    re.compile(r"строить\s*опасно", re.I),
    re.compile(r"аннулирован", re.I),
    re.compile(r"для\s*согласования", re.I),
    re.compile(r"не\s*для\s*строительства", re.I),
    re.compile(r"\bкопия\b", re.I),
)


@dataclass
class Separation:
    doc: fitz.Document                      # копия в памяти без отделённых объектов
    page_index: int
    separated: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)


def _render_gray(page: fitz.Page, dpi: int = CHECK_DPI) -> np.ndarray:
    arr, _, _ = pdfio.render(page, fitz.Matrix(dpi / 72, dpi / 72), gray=True)
    return arr


def _painted_mask(data: bytes, pno: int, stream_xrefs: list[int], dpi: int = CHECK_DPI) -> np.ndarray:
    """Где рисует сам поток (включая белые заливки): альфа рендера только этого потока."""
    doc = pdfio.open_memory(data)
    page = doc[pno]
    doc.xref_set_key(page.xref, "Contents", "[" + " ".join(f"{x} 0 R" for x in stream_xrefs) + "]")
    page = doc[pno]
    pix = page.get_pixmap(matrix=fitz.Matrix(dpi / 72, dpi / 72), alpha=True, annots=False)
    arr = np.frombuffer(pix.samples, np.uint8).reshape(pix.height, pix.width, pix.n)
    return (arr[:, :, 3] > 0).astype(np.uint8)


def _words(page: fitz.Page) -> Counter:
    return Counter((w[4], round(w[0], 1), round(w[1], 1), round(w[2], 1), round(w[3], 1)) for w in pdfio.page_words(page))


def _bbox_of(mask: np.ndarray, dpi: int) -> list[float] | None:
    ys, xs = np.nonzero(mask)
    if not len(xs):
        return None
    k = 72.0 / dpi
    return [float(xs.min() * k), float(ys.min() * k), float((xs.max() + 1) * k), float((ys.max() + 1) * k)]


def _classify_text(text: str) -> str | None:
    if any(p.search(text) for p in RELEASE_PATTERNS):
        return "release_stamp"
    if any(p.search(text) for p in WARNING_PATTERNS):
        return "warning"
    return None


def _check_removal(before: fitz.Page, after: fitz.Page, allowed: np.ndarray,
                   base: tuple[np.ndarray, Counter] | None = None) -> tuple[bool, dict[str, Any]]:
    """Контроль §5.1: разница рендеров только внутри разрешённой маски.

    ``base`` — готовые рендер и слова ``before``: исходная страница одна для всех кандидатов.
    """
    g0 = base[0] if base else _render_gray(before)
    g1 = _render_gray(after)
    if g0.shape != g1.shape:
        return False, {"reason": "размер рендера изменился"}
    diff = (np.abs(g0.astype(np.int16) - g1.astype(np.int16)) > 0).astype(np.uint8)
    allowed = cv2.dilate(allowed, np.ones((5, 5), np.uint8)) if allowed.any() else allowed
    if allowed.shape != diff.shape:
        allowed = cv2.resize(allowed, (diff.shape[1], diff.shape[0]), interpolation=cv2.INTER_NEAREST)
    outside = int((diff & (1 - allowed)).sum())
    w0, w1 = (base[1] if base else _words(before)), _words(after)
    added = w1 - w0
    removed = w0 - w1
    k = CHECK_DPI / 72.0
    stray = []
    for (text, x0, y0, x1, y1), _n in removed.items():
        cx0, cy0 = max(0, int(x0 * k)), max(0, int(y0 * k))
        cx1, cy1 = min(allowed.shape[1], int(x1 * k) + 1), min(allowed.shape[0], int(y1 * k) + 1)
        if cx1 <= cx0 or cy1 <= cy0 or not allowed[cy0:cy1, cx0:cx1].any():
            stray.append(text)
    ok = outside == 0 and not added and not stray
    return ok, {
        "pixels_outside": outside,
        "words_added": sum(added.values()),
        "words_removed_outside": stray[:20],
        "removed_text": " ".join(t for (t, *_), n in sorted(removed.items(), key=lambda i: (i[0][2], i[0][1]))
                                 for _ in range(n)),
    }


_TEXT_OPS = re.compile(rb"(?<![A-Za-z])(Tj|TJ|'|\")(?![A-Za-z])")


def _own_text(data: bytes, pno: int, xref: int, doc: fitz.Document, xobjects: list[dict[str, Any]]) -> str | None:
    """Текст, который рисует сам поток (в порядке чтения), — без рендера всей страницы.

    Поток без текстовых операторов и без форм (картинка) текста не рисует — "". ``None`` —
    текстовые операторы есть, а отдельно поток слов не дал (шрифт или состояние из соседнего
    потока): судить по содержимому нельзя, нужна полная проверка.
    """
    has_text = bool(_TEXT_OPS.search(doc.xref_stream(xref) or b""))
    if not has_text and not any(x["subtype"] == "/Form" for x in xobjects):
        return ""
    single = pdfio.open_memory(data)
    words = _words(_set_contents(single, pno, [xref]))
    if not words and has_text:
        return None
    return " ".join(t for (t, *_), n in sorted(words.items(), key=lambda i: (i[0][2], i[0][1])) for _ in range(n))


def _set_contents(doc: fitz.Document, pno: int, xrefs: list[int]) -> fitz.Page:
    page = doc[pno]
    doc.xref_set_key(page.xref, "Contents", "[" + " ".join(f"{x} 0 R" for x in xrefs) + "]")
    return doc[pno]


def _stream_xobjects(doc: fitz.Document, page: fitz.Page, stream_xref: int) -> list[dict[str, Any]]:
    """XObject-ы, которые рисует поток: тип, маска прозрачности, отпечаток."""
    raw = doc.xref_stream(stream_xref) or b""
    out = []
    for name in sorted(set(re.findall(rb"/([^\s/\[\]<>()]+)\s+Do\b", raw))):
        kind, value = doc.xref_get_key(page.xref, "Resources/XObject/" + name.decode("latin1"))
        if kind != "xref":
            continue
        xref = int(value.split()[0])
        subtype = doc.xref_get_key(xref, "Subtype")[1]
        smask = doc.xref_get_key(xref, "SMask")[0] != "null"
        try:
            digest = hashlib.sha256(doc.xref_stream_raw(xref) or b"").hexdigest()[:16]
        except Exception:
            digest = ""
        out.append({"name": name.decode("latin1"), "xref": xref, "subtype": subtype, "smask": smask, "digest": digest})
    return out


def _classify_stream(text: str, xobjects: list[dict[str, Any]], painted_share: float,
                     other_image_digests: set[str]) -> str | None:
    kind = _classify_text(text)
    if kind:
        return kind
    if text.strip():
        return None  # текст, но не отметка и не предупреждение: возможно, правка — не отделяем
    images = [x for x in xobjects if x["subtype"] == "/Image"]
    if images and painted_share >= WARNING_MIN_AREA_SHARE and all(
            x["smask"] and x["digest"] not in other_image_digests for x in images):
        return "warning"   # полупрозрачная картинка-предупреждение без текста
    if xobjects and painted_share < 0.05:
        # Мелкая графика без текста (QR, логотип штампа): принимается, только
        # если рядом отделена отметка выпуска или предупреждение (см. separate).
        return "qr_candidate"
    return None


def _near(a: list[float] | None, b: list[float] | None, gap_pt: float) -> bool:
    if not a or not b:
        return False
    return max(a[0] - b[2], b[0] - a[2], a[1] - b[3], b[1] - a[3]) <= gap_pt


def separate(data: bytes, pno: int, *, other_image_digests: set[str] | None = None) -> Separation:
    """Отделить наложенные объекты страницы ``pno`` в копии в памяти.

    Кандидаты — потоки содержимого, дописанные после первого рисующего потока
    (так PDF-редакторы добавляют штампы и предупреждения поверх исходного
    содержимого). Каждый кандидат проверяется отдельно (§5.1); отметки
    выпуска и предупреждения принимаются по тексту или картинке, мелкая
    графика без текста (QR) — только рядом с принятой отметкой. Затем все
    принятые потоки снимаются вместе, и общий результат проверяется ещё раз.
    """
    other_image_digests = other_image_digests or set()
    doc = pdfio.open_memory(data)
    result = Separation(doc=doc, page_index=pno)
    streams = doc[pno].get_contents()
    painting = [i for i, x in enumerate(streams) if _PAINT_OPS.search(doc.xref_stream(x) or b"")]
    candidates: list[tuple[int, bool, np.ndarray, dict[str, Any]]] = []
    deferred_qr: list[tuple[int, np.ndarray, dict[str, Any]]] = []
    base: tuple[np.ndarray, Counter] | None = None

    def full_check(xref: int, painted: np.ndarray) -> tuple[int, bool, np.ndarray, dict[str, Any]]:
        nonlocal base
        if base is None:
            base = (_render_gray(doc[pno]), _words(doc[pno]))
        trial = pdfio.open_memory(data)
        tpage = _set_contents(trial, pno, [x for x in streams if x != xref])
        ok, info = _check_removal(doc[pno], tpage, painted, base)
        xobjects = _stream_xobjects(trial, tpage, xref)
        share = float(painted.mean()) if painted.size else 0.0
        kind = _classify_stream(info.get("removed_text", ""), xobjects, share, other_image_digests)
        record = {"object": "content_stream", "stream_xref": xref, "kind": kind,
                  "bbox": _bbox_of(painted, CHECK_DPI), "painted_share": round(share, 4),
                  "text": info.get("removed_text", "")[:400],
                  "xobjects": [{k: v for k, v in x.items() if k != "xref"} for x in xobjects],
                  "check": {k: v for k, v in info.items() if k != "removed_text"}}
        if kind == "warning" and not record["text"]:
            record["note"] = "картинка-предупреждение: текст не извлекается"
        return xref, ok, painted, record

    if len(streams) >= 2 and painting:
        # Дорогая проверка (два рендера листа и слова) — только для потоков, которые по своему
        # содержимому могут быть отметкой, предупреждением или QR. Лист AutoCAD бывает из сотен
        # потоков-картинок (ВК4 л.3: 507 потоков, 46 мин при проверке каждого).
        for xref in streams[painting[0] + 1:]:
            painted = _painted_mask(data, pno, [xref])
            share = float(painted.mean()) if painted.size else 0.0
            xobjects = _stream_xobjects(doc, doc[pno], xref)
            own = _own_text(data, pno, xref, doc, xobjects)
            pre = None if own is None else _classify_stream(own, xobjects, share, other_image_digests)
            if own is None or pre in ("release_stamp", "warning"):
                candidates.append(full_check(xref, painted))
                continue
            record = {"object": "content_stream", "stream_xref": xref, "kind": pre,
                      "bbox": _bbox_of(painted, CHECK_DPI), "painted_share": round(share, 4), "text": own[:400],
                      "xobjects": [{k: v for k, v in x.items() if k != "xref"} for x in xobjects],
                      "check": {"skipped": "по содержимому потока не отметка и не предупреждение"}}
            if pre == "qr_candidate":
                deferred_qr.append((xref, painted, record))
            else:
                candidates.append((xref, False, painted, record))
    accepted = [c for c in candidates if c[1] and c[3]["kind"] in ("release_stamp", "warning")]
    anchors = [c[3]["bbox"] for c in accepted]
    for xref, painted, record in deferred_qr:
        if not any(_near(record["bbox"], a, 40 * 72 / 25.4) for a in anchors):
            record["check"] = {"skipped": "мелкая графика без текста не рядом с отметкой выпуска"}
            candidates.append((xref, True, painted, record))
            continue
        c = full_check(xref, painted)
        candidates.append(c)
        if c[1] and c[3]["kind"] == "qr_candidate":
            c[3]["kind"] = "qr"
            accepted.append(c)
    accepted_x = {c[0] for c in accepted}
    for c in candidates:
        if c[0] not in accepted_x:
            c[3]["safety_check"] = "not_service" if c[1] or "skipped" in c[3]["check"] else "failed"
            result.rejected.append(c[3])
    if accepted:
        final = pdfio.open_memory(data)
        fpage = _set_contents(final, pno, [x for x in streams if x not in accepted_x])
        union = np.zeros_like(accepted[0][2])
        for c in accepted:
            union |= c[2]
        ok, info = _check_removal(doc[pno], fpage, union)
        for c in accepted:
            c[3]["joint_check"] = {k: v for k, v in info.items() if k != "removed_text"}
            if ok:
                c[3]["safety_check"] = "passed"
                result.separated.append(c[3])
            else:
                c[3]["safety_check"] = "failed"
                result.rejected.append(c[3])
        if ok:
            result.doc = final
    _separate_images(data, result, other_image_digests)
    return result


def _drawings_signature(page: fitz.Page) -> list[tuple]:
    try:
        drawings = page.get_drawings()
    except Exception:
        return []
    return sorted((d.get("type"), tuple(round(v, 1) for v in d["rect"]), len(d.get("items", []))) for d in drawings)


def _separate_images(data: bytes, result: Separation, other_image_digests: set[str]) -> None:
    """Отделить полупрозрачную картинку, нарисованную ОДНОЙ операцией ``Do``.

    Удаляется только эта операция в копии в памяти. Контроль: текст и все
    векторы страницы не изменились — значит, разница рендеров создана самой
    картинкой. Картинка, которая есть и в другой версии, не трогается.
    """
    pno = result.page_index
    doc = result.doc
    page = doc[pno]
    # Слова и векторы исходного листа считаются один раз: принятое отделение их не меняет
    # (это его условие), отклонённое не меняет лист. Рендер «до» обновляется при принятии.
    # ГП3 л.2: 183 картинки × (векторы 1,7 с ×2 + слова + рендер) ≈ 14 мин на версию.
    base: dict[str, Any] = {}
    for img in page.get_images(full=True):
        xref, smask, name, referencer = img[0], img[1], img[7], img[9]
        if not smask or not name:
            continue
        try:
            digest = hashlib.sha256(doc.xref_stream_raw(xref) or b"").hexdigest()[:16]
        except Exception:
            continue
        if digest in other_image_digests:
            continue
        holders = [referencer] if referencer else page.get_contents()
        pattern = re.compile(rb"/" + re.escape(name.encode("latin1")) + rb"\s+Do\b")
        hits = [(h, len(pattern.findall(doc.xref_stream(h) or b""))) for h in holders]
        total = sum(n for _, n in hits)
        record = {"object": "image_op", "image_xref": xref, "kind": "warning", "text": "",
                  "note": "картинка-предупреждение: текст не извлекается"}
        if total != 1:
            if total > 1:
                record.update(safety_check="failed", check={"reason": f"картинка рисуется {total} раз"})
                result.rejected.append(record)
            continue
        holder = next(h for h, n in hits if n == 1)
        # Копия текущего состояния: исходные байты + уже принятые отделения потоков.
        trial = pdfio.open_memory(data)
        for sep in result.separated:
            if sep["object"] == "content_stream":
                tp = trial[pno]
                _set_contents(trial, pno, [x for x in tp.get_contents() if x != sep["stream_xref"]])
        trial.update_stream(holder, pattern.sub(b"", trial.xref_stream(holder) or b"", count=1))
        before, after = doc[pno], trial[pno]
        if not base:
            base.update(words=_words(before), vectors=_drawings_signature(before), gray=_render_gray(before))
        same_text = base["words"] == _words(after)
        same_vectors = base["vectors"] == _drawings_signature(after)
        g0, g1 = base["gray"], _render_gray(after)
        diff = (np.abs(g0.astype(np.int16) - g1.astype(np.int16)) > 0).astype(np.uint8) if g0.shape == g1.shape else None
        record.update(bbox=_bbox_of(diff, CHECK_DPI) if diff is not None else None,
                      painted_share=round(float(diff.mean()), 4) if diff is not None else None,
                      check={"text_unchanged": same_text, "vectors_unchanged": same_vectors})
        if same_text and same_vectors and diff is not None and diff.any():
            record["safety_check"] = "passed"
            result.doc = trial
            doc = trial
            base["gray"] = g1
            page = doc[pno]
            result.separated.append(record)
        else:
            record["safety_check"] = "failed"
            result.rejected.append(record)


def overlay_render(data: bytes, pno: int, stream_xref: int, matrix: fitz.Matrix) -> tuple[np.ndarray, int, int]:
    """Рендер только отделённого потока (для сравнения служебной зоны)."""
    doc = pdfio.open_memory(data)
    page = doc[pno]
    doc.xref_set_key(page.xref, "Contents", f"[{stream_xref} 0 R]")
    return pdfio.render(doc[pno], matrix, gray=True)


def overlay_words(data: bytes, pno: int, stream_xref: int) -> list[list[Any]]:
    doc = pdfio.open_memory(data)
    page = doc[pno]
    doc.xref_set_key(page.xref, "Contents", f"[{stream_xref} 0 R]")
    pdfio.strip_annotations(doc)
    return pdfio.page_words(doc[pno])
