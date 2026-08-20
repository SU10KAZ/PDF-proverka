# -*- coding: utf-8 -*-
"""Universal block-PDF source helper for image/imagine blocks.

This helper belongs to the shared audit pipeline and is independent from the
documentation-comparison section.

    block (result.json)
      → resolve_block_pdf_source()   # crop_url PDF > image_file PDF > none
      → extract_block_text_layer()   # pdfplumber_text (result.json) > PyMuPDF > pdfplumber
      → render_block_pdf()           # PyMuPDF high-res PNG render of the block PDF
      → (Qwen по PNG / tiles)

Принцип:
    text layer / pdfplumber_text = точные буквальные значения (словарь),
    Qwen = визуальная структура / связи / группировка,
    backend = validation / merge / recall / anti-hallucination.

Модуль НЕ зависит от Qwen/Opus и не делает сетевых вызовов, кроме скачивания
block-PDF по `crop_url` (обычный HTTP GET к R2/хранилищу Chandra, инжектируемый
для тестов). Приватные ссылки в логи целиком не выводятся.

По умолчанию helper доступен всем режимам; «использовать ли его в живом
pipeline» решает вызывающий код по своим feature-флагам.
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


# ─── Dataclasses ──────────────────────────────────────────────────────────


@dataclass
class BlockPdfSource:
    """Откуда взят PDF-фрагмент блока."""

    block_id: str
    source: str = "none"           # "crop_url" | "source_pdf" | "image_file" | "cache" | "none"
    pdf_path: Optional[Path] = None
    crop_url_status: Optional[int] = None
    content_type: Optional[str] = None
    fallback_used: bool = False    # True → block-PDF недоступен, нужен page-crop
    cache_hit: bool = False        # True → взято из локального кэша без http/render
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        # source_pdf (рендер из локального исходного PDF) — полноценный block-PDF,
        # downstream работает так же, как с crop_url.
        return self.source in ("crop_url", "source_pdf", "image_file") and self.pdf_path is not None


@dataclass
class BlockTextLayer:
    """Извлечённый текст-слой блока (словарь буквальных значений)."""

    source: str = "none"           # "result_json" | "pymupdf" | "pdfplumber" | "none"
    ok: bool = False
    text: str = ""
    words: list[dict] = field(default_factory=list)  # [{text,bbox,page}]
    quality: dict = field(default_factory=dict)       # {chars,word_count,garbled_ratio,usable}

    @property
    def usable(self) -> bool:
        return bool(self.ok and self.quality.get("usable"))


@dataclass
class RenderedBlock:
    """Результат рендера block-PDF в PNG."""

    png_path: Optional[Path] = None
    source: str = "block_pdf"      # "block_pdf"
    long_side: int = 0
    width: int = 0
    height: int = 0
    ok: bool = False
    error: Optional[str] = None


# ─── Helpers ──────────────────────────────────────────────────────────────


def _safe_block_id(block_id: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in str(block_id or "block"))[:64]


def _host_only(url: str) -> str:
    """Хост из URL без пути/query (чтобы не светить приватную ссылку целиком)."""
    m = re.match(r"^[a-zA-Z]+://([^/]+)", str(url or ""))
    return m.group(1) if m else "<url>"


def _block_field(block: dict, *names: str) -> Any:
    """Достать поле из блока: сначала top-level, затем из block['raw']."""
    for n in names:
        if isinstance(block, dict) and block.get(n) not in (None, ""):
            return block.get(n)
    raw = block.get("raw") if isinstance(block, dict) else None
    if isinstance(raw, dict):
        for n in names:
            if raw.get(n) not in (None, ""):
                return raw.get(n)
    return None


# ─── 1. resolve_block_pdf_source ──────────────────────────────────────────


def _coords_px_from_block(block: dict) -> Optional[list[float]]:
    """Bbox блока в пикселях (в системе ``page_width``×``page_height``)."""
    cx = _block_field(block, "coords_px", "bbox", "coords")
    if isinstance(cx, (list, tuple)) and len(cx) >= 4:
        try:
            return [float(cx[0]), float(cx[1]), float(cx[2]), float(cx[3])]
        except (TypeError, ValueError):
            return None
    return None


def _page_number_from_block(block: dict) -> Optional[int]:
    for n in ("page", "page_number", "page_index", "page_idx"):
        v = _block_field(block, n)
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                continue
    return None


def _page_px_from_block(block: dict) -> Optional[tuple[int, int]]:
    pw = _block_field(block, "page_width")
    ph = _block_field(block, "page_height")
    try:
        pw, ph = int(pw), int(ph)
        if pw > 0 and ph > 0:
            return (pw, ph)
    except (TypeError, ValueError):
        pass
    return None


def _source_identity(source_pdf_path: Optional[str | Path]) -> Optional[str]:
    """mtime:size исходного PDF — часть cache-key (rebuild при смене источника)."""
    if not source_pdf_path:
        return None
    try:
        st = Path(source_pdf_path).stat()
        return f"{int(st.st_mtime)}:{st.st_size}"
    except OSError:
        return None


def _block_pdf_cache_key(block_id: str, page: Optional[int],
                         coords_px: Optional[list[float]], src_identity: str) -> str:
    payload = f"{block_id}|{page}|{coords_px}|{src_identity}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def _build_block_pdf_from_source(
    *, source_pdf_path: str | Path, page_number: int,
    coords_px: list[float], page_px_size: tuple[int, int], out_path: str | Path,
) -> Optional[Path]:
    """Собрать standalone 1-page block-PDF из локального исходного PDF.

    coords_px (в px системе ``page_px_size``) → координаты страницы (pt) →
    clip → ``show_pdf_page`` в новую страницу размером с clip (векторно,
    без растеризации). Возвращает out_path или None (fail-soft)."""
    try:
        import fitz  # PyMuPDF
    except ImportError:  # pragma: no cover
        return None
    try:
        sp = Path(source_pdf_path)
        if not sp.exists():
            return None
        doc = fitz.open(str(sp))
        try:
            pno = int(page_number) - 1
            if pno < 0 or pno >= doc.page_count:
                return None
            page = doc[pno]
            rw, rh = float(page.rect.width), float(page.rect.height)
            w_px, h_px = float(page_px_size[0]), float(page_px_size[1])
            if rw <= 0 or rh <= 0 or w_px <= 0 or h_px <= 0:
                return None
            sx, sy = rw / w_px, rh / h_px
            x0, y0, x1, y1 = coords_px
            clip = fitz.Rect(min(x0, x1) * sx, min(y0, y1) * sy,
                             max(x0, x1) * sx, max(y0, y1) * sy) & page.rect
            if clip.width < 2 or clip.height < 2:
                return None
            new = fitz.open()
            npage = new.new_page(width=clip.width, height=clip.height)
            npage.show_pdf_page(fitz.Rect(0, 0, clip.width, clip.height), doc, pno, clip=clip)
            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            new.save(str(out_path), garbage=3, deflate=True)
            new.close()
            return out_path
        finally:
            doc.close()
    except Exception as exc:  # noqa: BLE001 — fail-soft → следующий fallback
        logger.warning("block_pdf_source: source-PDF build failed: %s", type(exc).__name__)
        return None


def resolve_block_pdf_source(
    block: dict,
    *,
    cache_dir: str | Path,
    http_get: Optional[Callable[[str], tuple[int, Optional[str], Optional[bytes]]]] = None,
    allow_download: bool = True,
    source_pdf_path: Optional[str | Path] = None,
    prefer_source_pdf: bool = False,
) -> BlockPdfSource:
    """Найти лучший PDF-фрагмент блока (устойчиво к истёкшим/удалённым crop_url).

    Приоритет источников:
      0. **локальный кэш block-PDF** (по cache-key) — без http/render;
      1. ``crop_url`` PDF из result.json (R2/хранилище Chandra);
      2. **source-PDF fallback** — если ``crop_url`` 404/недоступен И есть
         локальный ``source_pdf_path`` + ``coords_px`` + ``page`` +
         ``page_width/height``: вырезать block-region из локального PDF
         (иммунно к чистке R2 — публичный объект crop_url может быть удалён);
      3. ``image_file`` PDF, если локально существует;
      4. иначе ``source="none"`` + ``fallback_used=True`` (caller → page-crop).

    Cache-key = block_id + page + coords_px + identity(source_pdf | crop_url host).
    Повторные прогоны берут block-PDF из кэша и не зависят от доступности
    crop_url. ``http_get`` инжектируется для тестов. Приватный URL целиком не
    логируется (только host).
    """
    bid = str(block.get("id") or block.get("block_id") or "block")
    cache_dir = Path(cache_dir)
    crop_url = _block_field(block, "crop_url")
    image_file = _block_field(block, "image_file")

    coords_px = _coords_px_from_block(block)
    page = _page_number_from_block(block)
    page_px = _page_px_from_block(block)
    src_identity = _source_identity(source_pdf_path)
    can_source_fallback = bool(source_pdf_path and coords_px and page is not None and page_px)

    # cache-key (если есть стабильные координаты блока)
    ck: Optional[str] = None
    cached_pdf: Optional[Path] = None
    sidecar: Optional[Path] = None
    if coords_px is not None and page is not None:
        identity = src_identity or ("crop:" + _host_only(str(crop_url or "")))
        ck = _block_pdf_cache_key(bid, page, coords_px, identity)
        cached_pdf = cache_dir / f"{_safe_block_id(bid)}__{ck}.pdf"
        sidecar = cache_dir / f"{_safe_block_id(bid)}__{ck}.src"
        # 0. cache-first
        try:
            if cached_pdf.exists() and cached_pdf.stat().st_size > 0:
                cached_source = "crop_url"
                try:
                    cached_source = (sidecar.read_text(encoding="utf-8").strip() or "crop_url")
                except OSError:
                    pass
                return BlockPdfSource(
                    block_id=bid, source=cached_source, pdf_path=cached_pdf,
                    crop_url_status=200 if cached_source == "crop_url" else None,
                    content_type="application/pdf", cache_hit=True)
        except OSError:
            pass

    def _cache_paths() -> tuple[Path, Optional[Path]]:
        if ck:
            return cache_dir / f"{_safe_block_id(bid)}__{ck}.pdf", sidecar
        return cache_dir / f"{_safe_block_id(bid)}.pdf", None

    partial_status: Optional[int] = None
    partial_ctype: Optional[str] = None

    # Autonomous retrieval can already have the authoritative PDF and exact
    # block geometry. In that workflow, trying thousands of expired public
    # crop URLs first only adds latency; use the same-version local PDF before
    # the network and retain crop_url as a fail-soft fallback.
    if prefer_source_pdf and can_source_fallback:
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            out, side = _cache_paths()
            if not ck:
                out = cache_dir / f"{_safe_block_id(bid)}_src.pdf"
            built = _build_block_pdf_from_source(
                source_pdf_path=source_pdf_path, page_number=int(page),
                coords_px=coords_px, page_px_size=page_px, out_path=out)
            if built is not None:
                if side is not None:
                    side.write_text("source_pdf", encoding="utf-8")
                logger.info("block_pdf_source: %s ← preferred source_pdf (page %s)", bid, page)
                return BlockPdfSource(block_id=bid, source="source_pdf", pdf_path=built,
                                      crop_url_status=None, content_type="application/pdf")
        except Exception as exc:  # noqa: BLE001 — fail-soft → crop_url
            logger.warning("block_pdf_source: %s preferred source-PDF failed: %s",
                           bid, type(exc).__name__)

    # 1. crop_url PDF
    if crop_url and allow_download:
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            status, ctype, body = (http_get or _default_http_get)(str(crop_url))
            if status == 200 and body and "pdf" in (ctype or "").lower():
                out, side = _cache_paths()
                out.write_bytes(body)
                if side is not None:
                    side.write_text("crop_url", encoding="utf-8")
                logger.info("block_pdf_source: %s ← crop_url (%s, %d bytes)",
                            bid, _host_only(str(crop_url)), len(body))
                return BlockPdfSource(block_id=bid, source="crop_url", pdf_path=out,
                                      crop_url_status=status, content_type=ctype)
            logger.warning("block_pdf_source: %s crop_url not usable (status=%s ctype=%s)",
                           bid, status, ctype)
            partial_status, partial_ctype = status, ctype
        except Exception as exc:  # noqa: BLE001 — fail-soft → следующий источник
            logger.warning("block_pdf_source: %s crop_url fetch failed: %s",
                           bid, type(exc).__name__)

    # 2. source-PDF fallback (crop_url 404/недоступен → рендер из локального PDF)
    if can_source_fallback and not prefer_source_pdf:
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            out, side = _cache_paths()
            if not ck:
                out = cache_dir / f"{_safe_block_id(bid)}_src.pdf"
            built = _build_block_pdf_from_source(
                source_pdf_path=source_pdf_path, page_number=int(page),
                coords_px=coords_px, page_px_size=page_px, out_path=out)
            if built is not None:
                if side is not None:
                    side.write_text("source_pdf", encoding="utf-8")
                logger.info("block_pdf_source: %s ← source_pdf fallback (page %s)", bid, page)
                return BlockPdfSource(block_id=bid, source="source_pdf", pdf_path=built,
                                      crop_url_status=partial_status,
                                      content_type="application/pdf")
        except Exception as exc:  # noqa: BLE001 — fail-soft → следующий источник
            logger.warning("block_pdf_source: %s source-PDF fallback failed: %s",
                           bid, type(exc).__name__)

    # 3. image_file PDF (локальный)
    if image_file:
        p = Path(str(image_file))
        if p.suffix.lower() == ".pdf" and p.exists():
            return BlockPdfSource(block_id=bid, source="image_file", pdf_path=p,
                                  crop_url_status=partial_status, content_type=partial_ctype)

    # 4. нет block-PDF — caller использует page-crop
    return BlockPdfSource(block_id=bid, source="none", pdf_path=None,
                          crop_url_status=partial_status, content_type=partial_ctype,
                          fallback_used=True)


def _default_http_get(url: str) -> tuple[int, Optional[str], Optional[bytes]]:
    import httpx
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        r = client.get(url)
        return r.status_code, r.headers.get("content-type"), r.content


# ─── 2. extract_block_text_layer ──────────────────────────────────────────

# «Полезные» классы символов для оценки читаемости текст-слоя.
_READABLE_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]")
_MEANINGFUL_RE = re.compile(r"\S")


def _text_quality(text: str, word_count: int) -> dict:
    chars = len(text or "")
    meaningful = _MEANINGFUL_RE.findall(text or "")
    readable = _READABLE_RE.findall(text or "")
    n_meaning = len(meaningful)
    garbled_ratio = round(1.0 - (len(readable) / n_meaning), 3) if n_meaning else 1.0
    usable = bool(chars >= 20 and garbled_ratio <= 0.5)
    return {"chars": chars, "word_count": word_count,
            "garbled_ratio": garbled_ratio, "usable": usable}


def extract_block_text_layer(
    pdf_path: Optional[str | Path] = None,
    *,
    result_json_text: Optional[str] = None,
    prefer_result_json: bool = True,
) -> BlockTextLayer:
    """Извлечь текст-слой блока как словарь буквальных значений.

    Порядок:
      1. ``result_json_text`` (``pdfplumber_text`` из result.json) — самый
         дешёвый путь, текст уже извлечён upstream Chandra-сервисом;
      2. PyMuPDF ``get_text("words")`` по block-PDF (даёт words+bbox);
      3. pdfplumber, если установлен (fallback);
      4. иначе ``ok=False`` (caller → Chandra raw OCR).

    Если результирующий текст «битый» (garbled_ratio>0.5 / chars<20) →
    ``usable=False``, но caller всё равно может использовать block-PDF render.
    """
    # 1. result.json pdfplumber_text
    if prefer_result_json and result_json_text and result_json_text.strip():
        q = _text_quality(result_json_text, len(result_json_text.split()))
        if q["usable"]:
            return BlockTextLayer(source="result_json", ok=True,
                                  text=result_json_text, words=[], quality=q)

    # 2. PyMuPDF
    if pdf_path and Path(pdf_path).exists():
        try:
            import fitz  # PyMuPDF
            doc = fitz.open(str(pdf_path))
            try:
                words: list[dict] = []
                parts: list[str] = []
                for pno in range(doc.page_count):
                    page = doc[pno]
                    for w in page.get_text("words") or []:
                        # w = (x0, y0, x1, y1, "word", block_no, line_no, word_no)
                        if len(w) >= 5 and str(w[4]).strip():
                            words.append({"text": str(w[4]),
                                          "bbox": [round(float(w[0]), 1), round(float(w[1]), 1),
                                                   round(float(w[2]), 1), round(float(w[3]), 1)],
                                          "page": pno + 1})
                    t = page.get_text("text") or ""
                    if t.strip():
                        parts.append(t)
                text = "\n".join(parts).strip()
            finally:
                doc.close()
            if text or words:
                q = _text_quality(text, len(words) or len(text.split()))
                return BlockTextLayer(source="pymupdf", ok=bool(text or words),
                                      text=text, words=words, quality=q)
        except Exception as exc:  # noqa: BLE001
            logger.debug("extract_block_text_layer: PyMuPDF failed: %s", exc)

        # 3. pdfplumber (optional)
        try:
            import pdfplumber  # type: ignore
            words = []
            parts = []
            with pdfplumber.open(str(pdf_path)) as pdf:
                for pno, page in enumerate(pdf.pages):
                    for w in page.extract_words() or []:
                        if str(w.get("text", "")).strip():
                            words.append({"text": str(w["text"]),
                                          "bbox": [round(float(w["x0"]), 1), round(float(w["top"]), 1),
                                                   round(float(w["x1"]), 1), round(float(w["bottom"]), 1)],
                                          "page": pno + 1})
                    t = page.extract_text() or ""
                    if t.strip():
                        parts.append(t)
            text = "\n".join(parts).strip()
            if text or words:
                q = _text_quality(text, len(words) or len(text.split()))
                return BlockTextLayer(source="pdfplumber", ok=bool(text or words),
                                      text=text, words=words, quality=q)
        except ImportError:
            pass
        except Exception as exc:  # noqa: BLE001
            logger.debug("extract_block_text_layer: pdfplumber failed: %s", exc)

    # 4. last resort: result.json text even if not "usable"
    if result_json_text and result_json_text.strip():
        q = _text_quality(result_json_text, len(result_json_text.split()))
        return BlockTextLayer(source="result_json", ok=True,
                              text=result_json_text, words=[], quality=q)

    return BlockTextLayer(source="none", ok=False, text="", words=[],
                          quality={"chars": 0, "word_count": 0, "garbled_ratio": 1.0, "usable": False})


# ─── 3. render_block_pdf ──────────────────────────────────────────────────


def render_block_pdf(
    pdf_path: str | Path,
    *,
    long_side: int,
    out_path: str | Path,
    max_scale: float = 12.0,
) -> RenderedBlock:
    """Отрендерить (одностраничный) block-PDF в PNG с заданной длинной стороной."""
    try:
        import fitz  # PyMuPDF
    except ImportError as exc:  # pragma: no cover
        return RenderedBlock(ok=False, error=f"no_pymupdf:{exc}", long_side=long_side)
    pdf_path = Path(pdf_path)
    out_path = Path(out_path)
    if not pdf_path.exists():
        return RenderedBlock(ok=False, error="pdf_not_found", long_side=long_side)
    try:
        doc = fitz.open(str(pdf_path))
        try:
            page = doc[0]
            long_pt = max(page.rect.width, page.rect.height)
            if long_pt < 1:
                return RenderedBlock(ok=False, error="zero_page_size", long_side=long_side)
            scale = max(0.5, min(max_scale, long_side / long_pt))
            mat = fitz.Matrix(scale, scale)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            pix.save(str(out_path))
            return RenderedBlock(png_path=out_path, source="block_pdf", long_side=long_side,
                                 width=pix.width, height=pix.height, ok=True)
        finally:
            doc.close()
    except Exception as exc:  # noqa: BLE001
        return RenderedBlock(ok=False, error=f"render_error:{type(exc).__name__}:{exc}",
                             long_side=long_side)


# ─── 4. OCR-literal anchors + anti-hallucination validation ───────────────

_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9][A-Za-zА-Яа-яЁё0-9\.\,\-х/×()]*")
_LAT2CYR = str.maketrans({
    'A': 'А', 'B': 'В', 'C': 'С', 'E': 'Е', 'H': 'Н', 'K': 'К', 'M': 'М', 'O': 'О',
    'P': 'Р', 'T': 'Т', 'X': 'Х', 'Y': 'У', 'U': 'У', 'V': 'В', 'D': 'Д', 'R': 'Р',
    'a': 'а', 'c': 'с', 'e': 'е', 'o': 'о', 'p': 'р', 'x': 'х', 'y': 'у'})


def _norm_token(s: str) -> str:
    s = str(s or "").upper().translate(_LAT2CYR)
    s = re.sub(r"[ .,·．]", "", s)
    s = re.sub(r"[–—‒-]+", "-", s)
    return s.strip("-")


def build_ocr_literal_anchors(text_layer: BlockTextLayer) -> dict:
    """Список буквальных значений из текст-слоя (для prompt vocabulary и
    валидации Qwen-якорей). Возвращает {tokens:set-like list, normalized:set}."""
    text = text_layer.text or ""
    tokens = []
    seen = set()
    for m in _TOKEN_RE.findall(text):
        t = m.strip()
        if len(t) >= 2 and t.lower() not in seen:
            seen.add(t.lower())
            tokens.append(t)
    normalized = {_norm_token(t) for t in tokens if len(_norm_token(t)) >= 3}
    return {"tokens": tokens, "normalized": sorted(normalized), "count": len(tokens)}


def validate_anchors_against_text_layer(
    qwen_labels: list[str],
    text_layer: BlockTextLayer,
    *,
    expected_anchors: Optional[list[str]] = None,
) -> dict:
    """Анти-галлюцинационная сверка Qwen-меток с текст-слоем block-PDF.

    - метка Qwen есть в текст-слое → ``verified``;
    - метки Qwen нет в текст-слое → ``visual_unverified`` (не удалять, пометить);
    - значение текст-слоя (``expected_anchors``), которого Qwen не извлёк →
      ``missing_text_layer_anchor`` (сигнал ``not_extracted``, НЕ «удалено»);
    - искусственные ряды (``A-1..A-N`` с номерами, которых нет в текст-слое) →
      ``rejected_artificial_series``.
    """
    anchors = build_ocr_literal_anchors(text_layer)
    layer_norm = set(anchors["normalized"])

    verified, visual_unverified = [], []
    for lbl in qwen_labels or []:
        n = _norm_token(lbl)
        if not n:
            continue
        hit = n in layer_norm or any((n in a or a in n) and len(a) >= 5 for a in layer_norm)
        (verified if hit else visual_unverified).append(lbl)

    missing = []
    if expected_anchors:
        got = {_norm_token(x) for x in (qwen_labels or [])}
        for a in expected_anchors:
            na = _norm_token(a)
            if na and na not in got and not any((na in g or g in na) and len(na) >= 5 for g in got):
                missing.append(a)

    # искусственные ряды среди visual_unverified (номер не подтверждён слоем)
    rejected = []
    for lbl in visual_unverified:
        n = _norm_token(lbl)
        if re.search(r"-\d+$", n) and n not in layer_norm:
            rejected.append(lbl)

    return {
        "verified_by_text_layer": verified,
        "visual_unverified": visual_unverified,
        "missing_text_layer_anchors": missing,
        "rejected_artificial_series": rejected,
    }


# ─── Diagnostics bundle ───────────────────────────────────────────────────


def build_block_source_diagnostics(
    src: BlockPdfSource,
    text_layer: BlockTextLayer,
    rendered: Optional[RenderedBlock],
    validation: Optional[dict] = None,
) -> dict:
    """Собрать per-block диагностику (для image_descriptions.json)."""
    return {
        "block_source": {
            "pdf_source": src.source,                       # crop_url|source_pdf|image_file|none
            "used_crop_url_pdf": src.source == "crop_url",
            "used_source_pdf_fallback": src.source == "source_pdf",
            "cache_hit": src.cache_hit,
            "crop_url_status": src.crop_url_status,
            "text_layer_source": text_layer.source,
            "text_layer_usable": text_layer.usable,
            "render_source": (rendered.source if (rendered and rendered.ok) else "page_crop_fallback"),
            "fallback_used": src.fallback_used or (rendered is None or not rendered.ok),
        },
        "text_layer_stats": {
            "chars": text_layer.quality.get("chars", 0),
            "words": text_layer.quality.get("word_count", 0),
            "garbled_ratio": text_layer.quality.get("garbled_ratio", 1.0),
        },
        "qwen_validation": validation or {
            "verified_by_text_layer": [],
            "visual_unverified": [],
            "missing_text_layer_anchors": [],
            "rejected_artificial_series": [],
        },
    }
