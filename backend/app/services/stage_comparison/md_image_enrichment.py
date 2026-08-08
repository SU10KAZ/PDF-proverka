"""Чтение и сборка enriched MD для раздела «Сравнение стадий».

Производство описаний графики локальной VLM (Qwen 35B через LM Studio) с
платформы удалено — здесь остались только офлайн-операции над УЖЕ готовыми
артефактами:

    * разбор MD на блоки (`parse_md_blocks`);
    * чтение сохранённых описаний (`read_summary_only`, `_read_image_descriptions`);
    * пересборка enriched MD из сохранённых описаний
      (`build_enriched_md`, `rebuild_enriched_md_from_descriptions`);
    * определение формата enriched MD (`detect_enriched_md_format`);
    * индекс графических отличий (`build_image_diff_index`).

Никаких сетевых вызовов модуль не делает.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from . import graphic_profiles as graphic_profiles_mod
from . import grsh_core_systems as grsh_core_systems_mod
from . import paths as paths_mod

logger = logging.getLogger(__name__)


# Версия формата enriched MD (left_enriched.md / right_enriched.md):
#   "append_v0"               — legacy: <!-- original_imagine_start --> wrapper +
#                                #### QWEN_IMAGE_DESCRIPTION рядом со старым блоком.
#                                В таком формате Opus видел и старое OCR-описание,
#                                и Qwen-описание одного и того же блока — это
#                                раздувало enriched MD и могло конфликтовать.
#   "replace_image_blocks_v1" — НЫНЕШНИЙ default: image/imagine-блок ПОЛНОСТЬЮ
#                                заменяется на структурированное Qwen-описание
#                                в HTML-обёртке <!-- QWEN_IMAGE_DESCRIPTION_START
#                                ... QWEN_IMAGE_DESCRIPTION_END -->. Старое OCR
#                                из исходного MD физически отсутствует в основном
#                                enriched.md (debug-метаданные сохранены в
#                                image_descriptions.json).
ENRICHED_MD_FORMAT_VERSION = "replace_image_blocks_v1"

# Маркеры старого формата — используются для детекции outdated enriched.md
# при rebuild без повторного Qwen.
_LEGACY_ENRICHED_MARKER = "<!-- original_imagine_start -->"
# Маркер нового формата
_REPLACE_ENRICHED_MARKER = "QWEN_IMAGE_DESCRIPTION_START"


_DOMAIN_FIELD_ABSENT = "не указано"


# ─── Парсер MD ────────────────────────────────────────────────────────────


_HEADING_RE = re.compile(r"^(\s{0,3})(#{1,6})\s+(.*?)\s*$")

# Image markers (см. text_llm_input.py — оставляем совместимыми с одним
# набором правил).
_IMG_TOKENS = (
    r"\bBLOCK\s*\[\s*IMAGE\s*\]",
    r"\[\s*IMAGE\s*\]",
    r"\bIMAGE\b",
    r"\bIMAGEN\b",
    r"\bIMAGINE\b",
    r"Изображени[ея]",
    r"Графический\s+блок",
    r"Графика",
    r"Иллюстрация",
)
_IMG_TOKEN_RE = re.compile(r"(?:" + r"|".join(_IMG_TOKENS) + r")", re.IGNORECASE)

_TEXT_BLOCK_HEADING_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+.*?(?:BLOCK\s*\[\s*TEXT\s*\]|\[\s*TEXT\s*\])",
    re.IGNORECASE,
)

_PAGE_HEADING_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+(?:СТРАНИЦА|Страница|PAGE|Лист)\b",
    re.IGNORECASE,
)

# Извлечь block_id из заголовка вида `### BLOCK [IMAGE]: <id>` или `[IMAGE] id`
_BLOCK_ID_FROM_HEADING_RE = re.compile(
    r"\[\s*(?:IMAGE|IMAGEN|IMAGINE)\s*\]\s*[:\-]?\s*([A-Za-z0-9_\-]+)",
    re.IGNORECASE,
)

# Извлечь номер страницы из заголовка-страницы Chandra
_PAGE_NUM_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+(?:СТРАНИЦА|Страница|PAGE|Лист)\s*[:№#]?\s*(\d+)",
    re.IGNORECASE,
)


def _is_image_heading(stripped: str) -> bool:
    m = _HEADING_RE.match(stripped)
    if not m:
        return False
    if _TEXT_BLOCK_HEADING_RE.match(stripped):
        return False
    if _PAGE_HEADING_RE.match(stripped):
        return False
    return bool(_IMG_TOKEN_RE.search(m.group(3) or ""))


def _heading_level(stripped: str) -> Optional[int]:
    m = _HEADING_RE.match(stripped)
    if not m:
        return None
    return len(m.group(2))


def _extract_block_id(stripped_heading: str) -> Optional[str]:
    m = _BLOCK_ID_FROM_HEADING_RE.search(stripped_heading)
    if not m:
        return None
    raw = m.group(1).strip()
    return raw or None


def _extract_page_from_heading(stripped: str) -> Optional[int]:
    m = _PAGE_NUM_RE.match(stripped)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


@dataclass
class MdBlock:
    """Логический блок MD после парсинга.

    Не привязан к нумерации страниц/листов проектной документации; это
    просто отрезок исходного текста между двумя image/text boundary'ами.
    """

    kind: str               # "text" | "image"
    text: str               # исходный текст блока, как он был в MD (с переводами строк)
    page: Optional[int]     # текущая открытая `### СТРАНИЦА N`/`### Лист N`, если известна
    block_id: Optional[str] = None  # для image-блока — id из заголовка, если он там был
    order: int = 0          # порядковый номер блока этого kind среди блоков того же типа
    image_order_on_page: Optional[int] = None  # для image-блока — индекс среди image-блоков той же страницы

    @property
    def is_image(self) -> bool:
        return self.kind == "image"


def parse_md_blocks(md_text: str) -> list[MdBlock]:
    """Разбить MD на упорядоченный список text/image-блоков.

    Парсер совместим с Chandra MD-форматом (`### BLOCK [IMAGE]: ...`,
    `### СТРАНИЦА N`, `<image>...</image>`). Никаких lossy-преобразований
    с самим текстом не делается — оригинал собирается обратно конкатенацией
    `block.text`.
    """
    if not isinstance(md_text, str):
        md_text = "" if md_text is None else str(md_text)

    blocks: list[MdBlock] = []
    cur_page: Optional[int] = None
    page_image_counter: dict[int, int] = {}
    block_image_counter = 0
    block_text_counter = 0

    # Буфер для накапливания обычного текста
    text_buf: list[str] = []

    def flush_text():
        nonlocal block_text_counter
        if not text_buf:
            return
        joined = "".join(text_buf)
        # Полностью пустой буфер из одних переводов строк — оставляем для
        # точного восстановления, но не плодим лишний text-блок.
        if joined.strip() == "" and not blocks:
            text_buf.clear()
            return
        if joined.strip() == "":
            # Пустота между блоками — приклеим к последнему блоку, чтобы при
            # сборке enriched MD не терялись разделители.
            if blocks:
                blocks[-1].text += joined
            text_buf.clear()
            return
        block_text_counter += 1
        blocks.append(MdBlock(
            kind="text",
            text=joined,
            page=cur_page,
            block_id=None,
            order=block_text_counter,
        ))
        text_buf.clear()

    def append_image(text: str, block_id: Optional[str]):
        nonlocal block_image_counter
        page_image_counter[cur_page or 0] = page_image_counter.get(cur_page or 0, 0) + 1
        block_image_counter += 1
        blocks.append(MdBlock(
            kind="image",
            text=text,
            page=cur_page,
            block_id=block_id,
            order=block_image_counter,
            image_order_on_page=page_image_counter[cur_page or 0],
        ))

    lines = md_text.splitlines(keepends=True)
    i = 0
    in_image_fence = False
    fence_buf: list[str] = []

    in_image_tag = False
    tag_buf: list[str] = []

    in_image_block_by_heading = False
    image_heading_level: Optional[int] = None
    image_heading_buf: list[str] = []
    image_heading_block_id: Optional[str] = None

    while i < len(lines):
        raw_line = lines[i]
        stripped = raw_line.rstrip("\n").rstrip("\r")

        # ── Внутри ```image fence ─────────────────────────────────────
        if in_image_fence:
            fence_buf.append(raw_line)
            m_close = re.match(r"^(\s*)```\s*(\S+)?", stripped)
            if m_close and (m_close.group(2) is None or m_close.group(2) == ""):
                in_image_fence = False
                flush_text()
                append_image("".join(fence_buf), None)
                fence_buf = []
            i += 1
            continue

        # ── Внутри <image>...</image> / [IMAGE]...[/IMAGE] ─────────────
        if in_image_tag:
            tag_buf.append(raw_line)
            if re.match(
                r"^\s*(?:<\s*/\s*(?:image|imagen|imagine)\s*>|\[\s*/\s*(?:IMAGE|IMAGEN|IMAGINE)\s*\])\s*$",
                stripped, flags=re.IGNORECASE,
            ):
                in_image_tag = False
                flush_text()
                append_image("".join(tag_buf), None)
                tag_buf = []
            i += 1
            continue

        # ── Внутри image-блока, открытого заголовком ─────────────────
        if in_image_block_by_heading:
            # Что-то заканчивает блок?
            new_level = _heading_level(stripped)
            if new_level is not None:
                ends = False
                if image_heading_level is not None and new_level <= image_heading_level:
                    ends = True
                if _TEXT_BLOCK_HEADING_RE.match(stripped) or _PAGE_HEADING_RE.match(stripped):
                    ends = True
                if ends:
                    # Закрыть текущий image-блок и продолжить общую обработку строки
                    in_image_block_by_heading = False
                    image_heading_level = None
                    flush_text()
                    append_image("".join(image_heading_buf), image_heading_block_id)
                    image_heading_buf = []
                    image_heading_block_id = None
                    # не делаем i += 1 — fall-through
                else:
                    image_heading_buf.append(raw_line)
                    i += 1
                    continue
            else:
                image_heading_buf.append(raw_line)
                i += 1
                continue

        # ── Открывающий image-fence ─────────────────────────────────
        m_fence = re.match(r"^(\s*)```\s*(\S+)?", stripped)
        if m_fence:
            lang = (m_fence.group(2) or "").strip().lower()
            if lang in ("image", "imagen", "imagine"):
                in_image_fence = True
                fence_buf.append(raw_line)
                i += 1
                continue
            # обычный fence — fall-through (он попадёт в text buffer)

        # ── Открывающий image-tag ────────────────────────────────────
        if re.match(
            r"^\s*(?:<\s*(?:image|imagen|imagine)\b[^>]*>|\[\s*(?:IMAGE|IMAGEN|IMAGINE)\s*\])\s*$",
            stripped, flags=re.IGNORECASE,
        ):
            in_image_tag = True
            tag_buf.append(raw_line)
            i += 1
            continue

        # ── Page heading? ───────────────────────────────────────────
        page_num = _extract_page_from_heading(stripped)
        if page_num is not None:
            cur_page = page_num
            # Не делаем flush — page heading это часть текста, идёт в text-блок
            text_buf.append(raw_line)
            i += 1
            continue

        # ── Image heading? ──────────────────────────────────────────
        if _is_image_heading(stripped):
            in_image_block_by_heading = True
            image_heading_level = _heading_level(stripped)
            image_heading_buf.append(raw_line)
            image_heading_block_id = _extract_block_id(stripped)
            i += 1
            continue

        # ── Standalone markdown image-line (![](...)) ────────────────
        if re.match(r"^\s*(?:!\[[^\]]*\]\([^)]*\)\s*)+[\s.,;:!?]*$", stripped) and stripped.strip().startswith("!"):
            flush_text()
            append_image(raw_line, None)
            i += 1
            continue

        # ── Обычная строка → в text buffer ───────────────────────────
        text_buf.append(raw_line)
        i += 1

    # Хвосты — на случай неполных блоков в конце файла
    if in_image_fence and fence_buf:
        flush_text()
        append_image("".join(fence_buf), None)
    elif in_image_tag and tag_buf:
        flush_text()
        append_image("".join(tag_buf), None)
    elif in_image_block_by_heading and image_heading_buf:
        flush_text()
        append_image("".join(image_heading_buf), image_heading_block_id)

    flush_text()
    return blocks


# ─── Связь image-блока с реальной картинкой ──────────────────────────────






# ─── Кеш ──────────────────────────────────────────────────────────────────










# ─── Сборка enriched MD ───────────────────────────────────────────────────




def _format_grsh_sections(lines: list[str], desc_payload: dict) -> None:
    """Отрендерить GRSH-секции (verified/ocr_only/visual/rejected + структура).

    rejected_anchors помечены «НЕ evidence», чтобы Opus не строил по ним diff.
    """
    va = desc_payload.get("verified_anchors")
    if isinstance(va, dict):
        verified_labels = [str(x).strip() for x in (va.get("labels") or []) if str(x).strip()]
        if verified_labels:
            lines.append("GRSH_VERIFIED_ANCHORS — подтверждены словарём Chandra (evidence):")
            for x in verified_labels:
                lines.append(f"- {x}")
            lines.append("")
        for title, key in (("Кабели", "cables"), ("Номиналы", "ratings"),
                           ("Оборудование", "equipment")):
            arr = [str(x).strip() for x in (va.get(key) or []) if str(x).strip()]
            if arr:
                lines.append(f"GRSH_VERIFIED — {title}:")
                for x in arr:
                    lines.append(f"- {x}")
                lines.append("")

    ocr_only = [str(x).strip() for x in (desc_payload.get("ocr_only_anchors") or []) if str(x).strip()]
    if ocr_only:
        lines.append("GRSH_OCR_ONLY_ANCHORS — есть в Chandra-OCR, Qwen не описал (слабое evidence):")
        for x in ocr_only:
            lines.append(f"- {x}")
        lines.append("")

    visual = [_grsh_anchor_text(x).strip() for x in (desc_payload.get("visual_unverified_anchors") or [])]
    visual = [x for x in visual if x]
    if visual:
        lines.append("GRSH_VISUAL_UNVERIFIED — видно на картинке, нет в Chandra (НЕ evidence в одиночку):")
        for x in visual:
            lines.append(f"- {x}")
        lines.append("")

    rejected = [_grsh_anchor_text(x).strip() for x in (desc_payload.get("rejected_anchors") or [])]
    rejected = [x for x in rejected if x]
    if rejected:
        lines.append("GRSH_REJECTED — отброшены как достроенный ряд / нечитаемое (НЕ evidence, НЕ использовать):")
        for x in rejected:
            lines.append(f"- {x}")
        lines.append("")

    panels = desc_payload.get("panels")
    if isinstance(panels, list) and panels:
        lines.append("GRSH_PANELS — секции ГРЩ / вводы:")
        for p in panels:
            if not isinstance(p, dict):
                continue
            name = str(p.get("name") or "").strip()
            fed = str(p.get("fed_from") or "").strip()
            inp = p.get("input")
            busbar = str((inp or {}).get("busbar") or "").strip() if isinstance(inp, dict) else ""
            seg = f"- {name or '?'}"
            if fed:
                seg += f" ← {fed}"
            if busbar:
                seg += f" [{busbar}]"
            lines.append(seg)
        lines.append("")

    circuits = desc_payload.get("circuits")
    if isinstance(circuits, list) and circuits:
        lines.append("GRSH_CIRCUITS — отходящие линии (источник → автомат → кабель → потребитель):")
        for c in circuits:
            if not isinstance(c, dict):
                continue
            src = str(c.get("source") or "").strip()
            br = str(c.get("breaker") or "").strip()
            cab = str(c.get("cable") or "").strip()
            cons = str(c.get("consumer") or "").strip()
            chain = " → ".join(x for x in (src, br, cab, cons) if x)
            if chain:
                lines.append(f"- {chain}")
        lines.append("")

    connections = desc_payload.get("connections")
    if isinstance(connections, list) and connections:
        lines.append("GRSH_CONNECTIONS — связи:")
        for c in connections:
            if not isinstance(c, dict):
                continue
            f_ = str(c.get("from") or "?").strip()
            t_ = str(c.get("to") or "?").strip()
            via = str(c.get("via") or c.get("relation") or "").strip()
            seg = f"- {f_} → {t_}"
            if via:
                seg += f" ({via})"
            lines.append(seg)
        lines.append("")

    uncertainties = [_grsh_anchor_text(x).strip() for x in (desc_payload.get("uncertainties") or [])]
    uncertainties = [x for x in uncertainties if x]
    if uncertainties:
        lines.append("GRSH_UNCERTAIN — нечитаемое / сомнительное:")
        for x in uncertainties:
            lines.append(f"- {x}")
        lines.append("")


def _format_qwen_description_md(desc_payload: dict, *, model: str, page: Optional[int], block_id: Optional[str]) -> str:
    """Сформировать тело markdown-блока Qwen-описания для enriched MD (без HTML-обёртки).

    Используется новым builder'ом `build_enriched_md`: HTML-обёртка
    `<!-- QWEN_IMAGE_DESCRIPTION_START ... -->` строится наружу, а это тело —
    структурированное описание (заголовок «Графический блок / схема», секции
    «Краткое описание», «Видимый текст», «Оборудование», «Материалы»,
    «Числовые параметры», «Схема», «Неопределённости»).

    `desc_payload` — это либо `{"status": "done", ...}` (parsed JSON-ответ
    модели), либо `{"status": "error", "error": "..."}`.
    """
    lines: list[str] = []
    status = (desc_payload.get("status") or "").strip()
    if status == "error":
        err = (desc_payload.get("error") or "unknown").strip()
        lines.append("### Графический блок не распознан")
        lines.append("")
        lines.append("Описание графического блока отсутствует из-за ошибки распознавания.")
        lines.append(f"Этот блок требует повторного Qwen-enrichment / ручной проверки.")
        lines.append(f"Причина: {err}")
        lines.append("")
        return "\n".join(lines)

    lines.append("### Графический блок / схема")
    lines.append("")
    if model:
        lines.append(f"Модель: {model}")
    if page is not None:
        lines.append(f"Страница: {page}")
    if block_id:
        lines.append(f"Block ID: {block_id}")
    if desc_payload.get("_salvaged"):
        lines.append("Salvaged: yes (partial JSON, восстановлен с пропусками — модель оборвалась многоточием)")
    chunks_count = desc_payload.get("chunks_count")
    continued = desc_payload.get("continued")
    if isinstance(chunks_count, int) and chunks_count > 1:
        lines.append(f"Chunks: {chunks_count}")
    if continued is True:
        lines.append("Continued: yes (Qwen вернул несколько chunk'ов с continuation_prompt)")
    elif isinstance(chunks_count, int):
        lines.append("Continued: no")
    cont_warnings = desc_payload.get("continuation_warnings")
    if isinstance(cont_warnings, list) and cont_warnings:
        # Любая запись `*_cap_reached*` означает: модель ещё хотела продолжать,
        # но мы упёрлись в лимит → явное предупреждение.
        cap_hit = any("cap_reached" in str(w) for w in cont_warnings)
        if cap_hit:
            lines.append("⚠ Continuation cap reached — описание может быть неполным, увеличьте STAGE_COMPARISON_GRAPHIC_LLM_MAX_CONTINUATIONS")
        for w in cont_warnings:
            lines.append(f"  · continuation warning: {w}")
    continues_flag = desc_payload.get("continues")
    if continues_flag is True:
        nxt = (desc_payload.get("next_chunk_hint") or "").strip()
        lines.append(f"Продолжение требуется: yes — {nxt}" if nxt else "Продолжение требуется: yes")
    cov = (desc_payload.get("coverage_notes") or "").strip()
    if cov:
        lines.append(f"Покрытие: {cov}")
    lines.append("")

    # ── DOMAIN_FIELDS (r1): фиксированные доменные слоты с явным «не указано» ──
    # Рендерятся ВСЕГДА, когда присутствуют (включая «не указано»), чтобы Opus
    # механически отличал «поля нет» от «не описано». Пустые слоты не скрываются.
    domain_fields = desc_payload.get("domain_fields")
    if isinstance(domain_fields, dict) and domain_fields:
        lines.append("DOMAIN_FIELDS — фиксированные доменные поля (отсутствующее = «не указано»):")
        for slot, val in domain_fields.items():
            if isinstance(val, list):
                rendered = "; ".join(str(x) for x in val if str(x).strip()) or _DOMAIN_FIELD_ABSENT
            elif isinstance(val, dict):
                rendered = json.dumps(val, ensure_ascii=False) if val else _DOMAIN_FIELD_ABSENT
            else:
                rendered = str(val).strip() or _DOMAIN_FIELD_ABSENT
            lines.append(f"- {slot}: {rendered}")
        lines.append("")

    # ── GRSH (dense_grsh_singleline): verified / ocr_only / visual_unverified /
    #    rejected якоря + panels/circuits/connections. rejected помечены явно
    #    «НЕ evidence», чтобы Opus не использовал их как доказательство. ──
    if is_grsh_payload(desc_payload):
        _format_grsh_sections(lines, desc_payload)

    # ── GRSH_FEEDERS (контур B): пофидерная таблица из tiled-извлечения ──
    # Идёт ДО summary — Opus читает буквальные пофидерные строки раньше прозы.
    grsh_feeder_table = desc_payload.get("grsh_feeder_table")
    if isinstance(grsh_feeder_table, str) and grsh_feeder_table.strip():
        lines.append(grsh_feeder_table.strip())
        lines.append("")

    # ── GRSH_CORE_SYSTEMS (B1): ядро ГРЩ (вводы/шины/вводные QF/секц-АВР-ПСВ/
    #    УЗИП-ОПН/ТТ-ТШП/учёт/АУКРМ/ГЗШ-ДСУП/штамп) из structured + connections +
    #    block-PDF text-layer. Идёт ДО summary; «not_extracted» ≠ removed. ──
    try:
        if isinstance(desc_payload.get("core_systems"), dict):
            # production: ядро построено штатно в enrich_side (build_core_systems)
            core_md = grsh_core_systems_mod.render_core_systems_md(desc_payload["core_systems"])
        elif grsh_core_systems_mod.is_grsh_core_payload(desc_payload):
            # legacy / offline rebuild: построить на лету из structured + text-layer
            core_md = grsh_core_systems_mod.render_grsh_core_systems_md(
                desc_payload.get("structured"),
                desc_payload.get("grsh_connections"),
                desc_payload.get("_core_text_layer") or "",
                source_side=str(desc_payload.get("_source_side") or desc_payload.get("source_side") or "?"),
            )
        else:
            core_md = ""
        if core_md.strip():
            lines.append(core_md.strip())
            lines.append("")
    except Exception:  # noqa: BLE001 — core render must never break enriched MD
        logger.debug("GRSH_CORE_SYSTEMS render failed", exc_info=True)

    # ── DIFF_ANCHORS: буквальные маркировки/номиналы/связи для diff'а ──
    # Эта секция идёт ДО summary, чтобы Opus видел сырые ЩР-1а / ВРУ-2 с.ш.1
    # / QF3 / 4х185 раньше, чем плавный текст. Это критично для схем —
    # текст summary часто нормализован, anchors — нет.
    diff_anchors = desc_payload.get("diff_anchors")
    if isinstance(diff_anchors, dict):
        labels = diff_anchors.get("labels")
        ratings = diff_anchors.get("ratings")
        connections = diff_anchors.get("connections")
        uncertain = diff_anchors.get("uncertain_text")

        if isinstance(labels, list) and labels:
            lines.append("DIFF_ANCHORS — буквальные маркировки:")
            for lab in labels:
                if not isinstance(lab, dict):
                    continue
                raw = (lab.get("raw_text") or "").strip()
                if not raw:
                    continue
                ntype = (lab.get("normalized_type") or "").strip()
                comment = (lab.get("comment") or "").strip()
                lconf = lab.get("confidence")
                parts = [f"- {raw}"]
                if ntype:
                    parts.append(f" [{ntype}]")
                if isinstance(lconf, (int, float)):
                    try:
                        parts.append(f" (уверенность: {float(lconf):.2f})")
                    except (TypeError, ValueError):
                        pass
                if comment:
                    parts.append(f" — {comment}")
                lines.append("".join(parts))
            lines.append("")

        if isinstance(ratings, list) and ratings:
            lines.append("DIFF_ANCHORS — кабели, номиналы, мощности:")
            for r in ratings:
                if not isinstance(r, dict):
                    continue
                raw = (r.get("raw_text") or "").strip()
                if not raw:
                    continue
                vtype = (r.get("value_type") or "").strip()
                related = (r.get("related_to") or "").strip()
                rconf = r.get("confidence")
                parts = [f"- {raw}"]
                if vtype:
                    parts.append(f" [{vtype}]")
                if related:
                    parts.append(f" → {related}")
                if isinstance(rconf, (int, float)):
                    try:
                        parts.append(f" (уверенность: {float(rconf):.2f})")
                    except (TypeError, ValueError):
                        pass
                lines.append("".join(parts))
            lines.append("")

        if isinstance(connections, list) and connections:
            lines.append("DIFF_ANCHORS — связи:")
            for c in connections:
                if not isinstance(c, dict):
                    continue
                f_raw = (c.get("from_raw") or "?").strip()
                t_raw = (c.get("to_raw") or "?").strip()
                relation = (c.get("relation") or "").strip()
                cconf = c.get("confidence")
                parts = [f"- {f_raw} → {t_raw}"]
                if relation:
                    parts.append(f" ({relation})")
                if isinstance(cconf, (int, float)):
                    try:
                        parts.append(f" [уверенность: {float(cconf):.2f}]")
                    except (TypeError, ValueError):
                        pass
                lines.append("".join(parts))
            lines.append("")

        if isinstance(uncertain, list) and uncertain:
            lines.append("Неуверенно прочитанные надписи:")
            for u in uncertain:
                if not isinstance(u, dict):
                    continue
                possible = (u.get("possible_text") or "").strip()
                if not possible:
                    continue
                alts = u.get("alternatives") or []
                alts_clean = [str(a).strip() for a in alts if str(a).strip()]
                why = (u.get("why_uncertain") or "").strip()
                uconf = u.get("confidence")
                parts = [f"- {possible}"]
                if alts_clean:
                    parts.append(f" (варианты: {', '.join(alts_clean)})")
                if isinstance(uconf, (int, float)):
                    try:
                        parts.append(f" [уверенность: {float(uconf):.2f}]")
                    except (TypeError, ValueError):
                        pass
                if why:
                    parts.append(f" — {why}")
                lines.append("".join(parts))
            lines.append("")

    summary = (desc_payload.get("summary") or "").strip()
    if summary:
        lines.append("Краткое описание:")
        lines.append(summary)
        lines.append("")

    def _bullets(label: str, items: Any):
        if not isinstance(items, list) or not items:
            return
        lines.append(label)
        for it in items:
            if isinstance(it, str) and it.strip():
                lines.append(f"- {it.strip()}")
        lines.append("")

    _bullets("Видимый текст:", desc_payload.get("visible_text"))
    _bullets("Оборудование и элементы:", desc_payload.get("equipment"))
    _bullets("Материалы:", desc_payload.get("materials"))
    _bullets("Проектные решения:", desc_payload.get("design_solutions"))

    nums = desc_payload.get("numeric_parameters")
    if isinstance(nums, list) and nums:
        lines.append("Числовые параметры:")
        for n in nums:
            if not isinstance(n, dict):
                continue
            name = (n.get("name") or "").strip()
            value = (n.get("value") or "").strip()
            unit = (n.get("unit") or "").strip()
            context = (n.get("context") or "").strip()
            entry = f"- {name}: {value}".rstrip(": ").rstrip()
            if unit:
                entry += f" {unit}"
            if context:
                entry += f"  ({context})"
            if entry.strip() == "-":
                continue
            lines.append(entry)
        lines.append("")

    _bullets("Требования / примечания:", desc_payload.get("requirements"))
    _bullets("Таблицы:", desc_payload.get("tables"))
    _bullets("Существенно для сравнения стадий:", desc_payload.get("comparison_relevant_facts"))

    image_kind = (desc_payload.get("image_kind") or "").strip()
    if image_kind:
        lines.append(f"Тип изображения: {image_kind}")
    conf = desc_payload.get("confidence")
    if isinstance(conf, (int, float)):
        try:
            lines.append(f"Уверенность модели: {float(conf):.2f}")
        except (TypeError, ValueError):
            pass

    scheme = desc_payload.get("scheme_analysis")
    if isinstance(scheme, dict):
        is_scheme = bool(scheme.get("is_scheme"))
        if not is_scheme:
            # Кратко отметим, чтобы было видно в enriched MD, что модель проверила.
            lines.append("")
            lines.append("Схемный анализ: не применимо (изображение не является схемой)")
        else:
            lines.append("")
            lines.append("Схемный анализ:")
            scheme_type = (scheme.get("scheme_type") or "").strip()
            if scheme_type:
                lines.append(f"- Тип схемы: {scheme_type}")
            flow_medium = (scheme.get("flow_medium") or "").strip()
            if flow_medium:
                lines.append(f"- Среда / поток: {flow_medium}")
            lines.append("")

            nodes = scheme.get("nodes")
            if isinstance(nodes, list) and nodes:
                lines.append("Узлы:")
                for n in nodes:
                    if not isinstance(n, dict):
                        continue
                    nid = (n.get("id") or "").strip() or "?"
                    label = (n.get("label") or "").strip()
                    ntype = (n.get("type") or "").strip()
                    mark = (n.get("visible_mark") or "").strip()
                    parts = [f"- {nid}"]
                    if label:
                        parts.append(f": {label}")
                    if ntype:
                        parts.append(f", тип: {ntype}")
                    if mark:
                        parts.append(f", маркировка: {mark}")
                    params = n.get("parameters")
                    if isinstance(params, list) and params:
                        params_clean = [str(p).strip() for p in params if str(p).strip()]
                        if params_clean:
                            parts.append(f", параметры: {', '.join(params_clean)}")
                    nconf = n.get("confidence")
                    if isinstance(nconf, (int, float)):
                        try:
                            parts.append(f", уверенность: {float(nconf):.2f}")
                        except (TypeError, ValueError):
                            pass
                    lines.append("".join(parts))
                lines.append("")

            conns = scheme.get("connections")
            if isinstance(conns, list) and conns:
                lines.append("Связи:")
                for c in conns:
                    if not isinstance(c, dict):
                        continue
                    src = (c.get("from") or "?").strip()
                    dst = (c.get("to") or "?").strip()
                    direction = (c.get("direction") or "unknown").strip()
                    line_label = (c.get("line_label") or "").strip()
                    evidence = (c.get("evidence") or "").strip()
                    parts = [f"- {src} → {dst}", f", направление: {direction}"]
                    if line_label:
                        parts.append(f", линия: {line_label}")
                    params = c.get("parameters")
                    if isinstance(params, list) and params:
                        params_clean = [str(p).strip() for p in params if str(p).strip()]
                        if params_clean:
                            parts.append(f", параметры: {', '.join(params_clean)}")
                    if evidence:
                        parts.append(f", основание: {evidence}")
                    cconf = c.get("confidence")
                    if isinstance(cconf, (int, float)):
                        try:
                            parts.append(f", уверенность: {float(cconf):.2f}")
                        except (TypeError, ValueError):
                            pass
                    lines.append("".join(parts))
                lines.append("")

            sequence = scheme.get("sequence_summary")
            if isinstance(sequence, list) and sequence:
                lines.append("Последовательность:")
                for s in sequence:
                    if isinstance(s, str) and s.strip():
                        lines.append(f"- {s.strip()}")
                lines.append("")

            circuits = scheme.get("independent_circuits")
            if isinstance(circuits, list) and circuits:
                lines.append("Независимые контуры:")
                for circ in circuits:
                    if not isinstance(circ, dict):
                        continue
                    name = (circ.get("name") or "").strip()
                    seq = (circ.get("sequence") or "").strip()
                    notes = (circ.get("notes") or "").strip()
                    entry = "- "
                    if name:
                        entry += f"{name}: "
                    if seq:
                        entry += seq
                    if notes:
                        entry += f"  ({notes})"
                    if entry.strip() != "-":
                        lines.append(entry)
                lines.append("")

            _bullets("Существенно для сравнения (схема):", scheme.get("comparison_relevant_scheme_facts"))
            _bullets("Неопределённости (схема):", scheme.get("uncertainties"))

    # Top-level Неопределённости — в самом конце, после scheme_analysis,
    # по новому формату replace_image_blocks_v1.
    _bullets("Неопределённости:", desc_payload.get("uncertainties"))

    return "\n".join(lines).rstrip() + "\n"


def _format_image_block_header(
    *,
    status: str,
    source_kind: str,
    block_id: Optional[str],
    page: Optional[int],
    desc_item: Optional[dict] = None,
    error: Optional[str] = None,
) -> str:
    """Сформировать HTML-комментарий <!-- QWEN_IMAGE_DESCRIPTION_START ... -->.

    Используется как обёртка вокруг тела Qwen-описания в новом формате
    `replace_image_blocks_v1`. В метаданных сохраняется:
      - format_version (как маркер для preflight);
      - block_id / page (из исходного MD-блока);
      - source (image / imagine);
      - status (done / done_with_salvage / error / pending / no_image);
      - prompt_version / model / confidence — если описание есть;
      - original_block_id (для debug).
    """
    lines = ["<!-- QWEN_IMAGE_DESCRIPTION_START"]
    lines.append(f"format_version: {ENRICHED_MD_FORMAT_VERSION}")
    if block_id:
        lines.append(f"block_id: {block_id}")
    if page is not None:
        lines.append(f"page: {page}")
    lines.append(f"source: {source_kind or 'image'}")
    lines.append(f"status: {status}")
    if desc_item:
        prompt_version = desc_item.get("used_prompt_version") or desc_item.get("prompt_version")
        if prompt_version:
            lines.append(f"prompt_version: {prompt_version}")
        model = (desc_item.get("model_used") or desc_item.get("model") or "").strip()
        if model:
            lines.append(f"model: {model}")
        # Per-block metadata: тип, рендер/inference sizing, usable_for_diff и
        # warnings — это нужно Opus'у, чтобы судить, насколько верить блоку.
        block_type = (desc_item.get("block_type") or "").strip()
        if block_type:
            lines.append(f"block_type: {block_type}")
        for fk in ("prompt_family",):
            fv = (desc_item.get(fk) or "").strip()
            if fv:
                lines.append(f"{fk}: {fv}")
        for nk in ("render_target_long_side", "image_input_long_side"):
            nv = desc_item.get(nk)
            if isinstance(nv, (int, float)) and int(nv) > 0:
                lines.append(f"{nk}: {int(nv)}")
        if "usable_for_diff" in desc_item:
            lines.append(f"usable_for_diff: {'true' if desc_item.get('usable_for_diff') else 'false'}")
        warnings_list = desc_item.get("warnings")
        if isinstance(warnings_list, list) and warnings_list:
            safe_w = [str(w).replace("\n", " ").replace("--", "—") for w in warnings_list if w]
            if safe_w:
                # Чтобы HTML-комментарий не сломался от слишком длинных значений
                lines.append("warnings: " + ", ".join(safe_w)[:600])
        payload = desc_item.get("description")
        if isinstance(payload, dict):
            conf = payload.get("confidence")
            if isinstance(conf, (int, float)):
                try:
                    lines.append(f"confidence: {float(conf):.2f}")
                except (TypeError, ValueError):
                    pass
        if block_id:
            lines.append(f"original_block_id: {block_id}")
    if error:
        # экранируем переводы строк, чтобы HTML-комментарий не сломался
        safe_err = str(error).replace("\n", " ").replace("--", "—")
        lines.append(f"error: {safe_err[:200]}")
    lines.append("-->")
    return "\n".join(lines) + "\n"


def build_enriched_md(blocks: list[MdBlock], descriptions: list[dict]) -> str:
    """Собрать enriched MD из блоков + сопоставленных описаний.

    Формат `replace_image_blocks_v1`:
      - text-блоки переносятся как есть, под заголовком `### BLOCK [TEXT]`;
      - image/imagine-блоки ПОЛНОСТЬЮ ЗАМЕНЯЮТСЯ структурированным
        Qwen-описанием в обёртке
        `<!-- QWEN_IMAGE_DESCRIPTION_START … --> … <!-- QWEN_IMAGE_DESCRIPTION_END -->`.
        Старое OCR из исходного MD физически отсутствует в основном
        enriched.md (debug сохранён в image_descriptions.json: original_block_id,
        original_page, original_kind, original_order).

    `descriptions` — список dict'ов по индексу, соответствующему `block.order`
    для image-блоков. Каждый элемент: один блок image, со всеми полями.
    """
    desc_by_image_order: dict[int, dict] = {}
    for d in descriptions:
        order = d.get("order")
        if isinstance(order, int):
            desc_by_image_order[order] = d

    out_parts: list[str] = []
    # Документ-уровневый header — упрощает детекцию формата в preflight.
    out_parts.append(f"<!-- ENRICHED_MD_FORMAT: {ENRICHED_MD_FORMAT_VERSION} -->\n\n")

    # IMAGE_DIFF_INDEX: компактный список буквальных diff-якорей. Ставим
    # сразу после format-header'а, чтобы Opus при сравнении стадий видел
    # raw маркировки/номиналы РАНЬШЕ длинных markdown-блоков.
    try:
        diff_index = build_image_diff_index(descriptions)
    except Exception:  # noqa: BLE001
        logger.debug("build_enriched_md: build_image_diff_index failed", exc_info=True)
        diff_index = (
            _IMAGE_DIFF_INDEX_START
            + "\nimage_diff_index_parse_failed: yes\n"
            + _IMAGE_DIFF_INDEX_END
            + "\n"
        )
    out_parts.append(diff_index)
    out_parts.append("\n")

    for block in blocks:
        if block.kind == "text":
            out_parts.append("### BLOCK [TEXT]\n")
            out_parts.append(block.text)
            if not block.text.endswith("\n"):
                out_parts.append("\n")
            out_parts.append("\n")
            continue

        # ── image-блок: REPLACE (не append) ─────────────────────────
        d = desc_by_image_order.get(block.order)

        def _wrap(body: str, *, status: str, error: Optional[str] = None) -> None:
            header = _format_image_block_header(
                status=status,
                source_kind="image",
                block_id=block.block_id,
                page=block.page,
                desc_item=d,
                error=error,
            )
            out_parts.append(header)
            out_parts.append("\n")
            out_parts.append(body)
            if not body.endswith("\n"):
                out_parts.append("\n")
            out_parts.append("\n<!-- QWEN_IMAGE_DESCRIPTION_END -->\n\n")

        if d is None:
            body = (
                "### Графический блок не распознан\n\n"
                "Описание ещё не сформировано (dry-run или модель не запущена).\n"
            )
            _wrap(body, status="pending")
            continue

        item_status = (d.get("status") or "").lower()

        # Large Sheet Enrichment: вставляем готовую компактную сводку вместо
        # обычного Qwen-описания (тело уже сформировано в large_sheet_md).
        if d.get("source") == "large_sheet_enrichment":
            body = d.get("large_sheet_md") or "### Большой лист\n\n(сводка отсутствует)\n"
            _wrap(body, status=("done" if item_status == "done" else item_status or "pending"))
            continue

        if item_status in ("pending", "no_image"):
            note = (
                d.get("error")
                or (
                    "Описание ещё не сформировано (dry-run)."
                    if item_status == "pending"
                    else "Для блока не найдено изображения."
                )
            )
            body = (
                "### Графический блок не распознан\n\n"
                f"{note}\n"
            )
            _wrap(body, status=item_status, error=str(note))
            continue

        if item_status == "error":
            err_msg = str(d.get("error") or "unknown")
            body = (
                "### Графический блок не распознан\n\n"
                "Описание графического блока отсутствует из-за ошибки распознавания.\n"
                "Этот блок требует повторного Qwen-enrichment / ручной проверки.\n"
                f"Причина: {err_msg}\n"
            )
            _wrap(body, status="error", error=err_msg)
            continue

        payload = d.get("description") or {"status": "error", "error": d.get("error") or "unknown"}
        model = (d.get("model_used") or d.get("model") or "").strip()
        body = _format_qwen_description_md(
            payload,
            model=model,
            page=block.page,
            block_id=block.block_id,
        )
        # «status» отражает реальный per-item статус (done / partial → done_with_salvage).
        wrap_status = "done_with_salvage" if (item_status == "partial" or d.get("salvaged")) else (
            item_status or "done"
        )
        _wrap(body, status=wrap_status)

    return "".join(out_parts)


def detect_enriched_md_format(text: str | bytes | None) -> str:
    """Определить формат enriched MD: `replace_image_blocks_v1` / `append_v0` / `unknown`.

    Используется preflight'ом для решения «можно ли запускать Opus» и
    «нужна ли пересборка enriched.md без повторного Qwen».
    """
    if not text:
        return "unknown"
    sample = text if isinstance(text, str) else text.decode("utf-8", errors="replace")
    sample = sample[:4096]  # достаточно для header'а
    if _REPLACE_ENRICHED_MARKER in sample or ENRICHED_MD_FORMAT_VERSION in sample:
        return ENRICHED_MD_FORMAT_VERSION
    if _LEGACY_ENRICHED_MARKER in sample:
        return "append_v0"
    # Edge case: совсем пустой файл / без image-блоков. Считаем legacy, чтобы
    # rebuild пересобрал в новом формате (это безопасно — image-блоков нет).
    return "append_v0"


# ─── Quality-эвристики (hallucination / usable_for_diff) ─────────────────


# Маркеры искусственных рядов: модели иногда выдумывают «ВРП-1 ... ВРП-50»
# или «ЩА-1.1 ... ЩА-1.40» (subindex format).
# Считаем подозрительным ряд из ≥6 идущих по возрастанию маркировок одной
# из этих серий, если нет локальных доказательств (другой evidence в
# original_md_excerpt / scheme).
#
# Префиксы для top-level номеров: ЩР-1, ЩР-2, ВРП-N, QF-N etc.

# Каталожно-генерики сечения и номиналы. Если ratings полностью совпадают
# с этим списком (или очень близко) и при этом labels пустые/generic, это
# почти наверняка не наблюдение, а каталог.





# Универсальный парсер маркировки серии: ЩР-1, ЩА-1.5, ЩР-2.10, ЩО-1-12, QF12, QF-3.7.
# Возвращает (series_key, seq_num) или None.
#
#   - "ЩР-1"      → ("ЩР", 1)            # top-level номер
#   - "ЩР-2.10"   → ("ЩР-2", 10)         # subindex
#   - "ЩА-1.5"    → ("ЩА-1", 5)
#   - "ЩО-1-12"   → ("ЩО-1", 12)
#   - "QF-3.7"    → ("QF-3", 7)
#   - "QF12"      → ("QF", 12)
#   - "ВРУ-2"     → ("ВРУ", 2)
#   - "ВРУ-2 с.ш.1" → ("ВРУ", 2)         # игнорируем суффиксы вроде "с.ш.1"
#
# Без literal-text fallback: если raw не совпадает с pref → не парсится.


















# ─── GRSH validation / dedup layer ────────────────────────────────────────
#
# Qwen на GRSH-схеме сам не партиционирует бакеты (копирует реальные labels
# и в verified, и в visual_unverified, и в rejected) и иногда достраивает
# числовые ряды. Этот детерминированный слой:
#   * сверяет каждую verified-маркировку со словарём Chandra (grounding);
#   * отбрасывает достроенные ряды (ТП3…ТП22, ГРЩ1-РП1-8…15) в rejected_anchors;
#   * negrounded не-серии → visual_unverified_anchors;
#   * не теряет важные Chandra-only маркировки → ocr_only_anchors;
#   * делает бакеты взаимоисключающими (verified > ocr_only > visual_unverified
#     > rejected > uncertainties).
# Перенесено из controlled-эксперимента (exp_qwen.dedup_buckets / detect_artificial_series).

# Серия = «префикс + хвостовое число»: ТП1, ВРУ2, ГРЩ1-РП1-8 → (key, num).




def _grsh_anchor_text(x: Any) -> str:
    if isinstance(x, str):
        return x
    if isinstance(x, dict):
        for k in ("raw_text", "label", "text", "possible_text", "name"):
            if x.get(k):
                return str(x[k])
    return str(x)


def is_grsh_payload(payload: Any) -> bool:
    """True, если payload похож на GRSH-описание (verified_anchors+бакеты)."""
    if not isinstance(payload, dict):
        return False
    if isinstance(payload.get("verified_anchors"), dict):
        return True
    return (payload.get("sheet_kind") == "electrical_single_line"
            and ("panels" in payload or "circuits" in payload))
















# ─── IMAGE_DIFF_INDEX builder ────────────────────────────────────────────


_IMAGE_DIFF_INDEX_START = "<!-- IMAGE_DIFF_INDEX_START -->"
_IMAGE_DIFF_INDEX_END = "<!-- IMAGE_DIFF_INDEX_END -->"


def _extract_grsh_anchors_from_payload(payload: dict) -> dict[str, list[str]]:
    """GRSH-форма → {labels (verified+ocr_only), ratings, connections,
    visual_unverified, rejected}. rejected отдаётся отдельно — НЕ как evidence."""
    out: dict[str, list[str]] = {
        "labels": [], "ratings": [], "connections": [],
        "visual_unverified": [], "rejected": [],
    }
    va = payload.get("verified_anchors") or {}
    if isinstance(va, dict):
        out["labels"].extend(str(x).strip() for x in (va.get("labels") or []) if str(x).strip())
        out["ratings"].extend(str(x).strip() for x in (va.get("ratings") or []) if str(x).strip())
    # ocr_only — Chandra-grounded, считаем verified-уровнем evidence.
    out["labels"].extend(str(x).strip() for x in (payload.get("ocr_only_anchors") or []) if str(x).strip())
    out["visual_unverified"].extend(
        _grsh_anchor_text(x).strip() for x in (payload.get("visual_unverified_anchors") or [])
        if _grsh_anchor_text(x).strip()
    )
    out["rejected"].extend(
        _grsh_anchor_text(x).strip() for x in (payload.get("rejected_anchors") or [])
        if _grsh_anchor_text(x).strip()
    )
    for c in (payload.get("connections") or []):
        if isinstance(c, dict):
            f = str(c.get("from") or "").strip()
            t = str(c.get("to") or "").strip()
            if f or t:
                out["connections"].append(f"{f or '?'} -> {t or '?'}")
    # dedup сохраняя порядок
    for k in out:
        seen: set[str] = set()
        uniq: list[str] = []
        for v in out[k]:
            if v and v not in seen:
                seen.add(v)
                uniq.append(v)
        out[k] = uniq
    return out


def _extract_anchors_from_description(d: dict) -> dict[str, list[str]]:
    """Извлечь labels/ratings/connections из item.description.

    Сначала GRSH-форма (verified_anchors + ocr_only/visual/rejected), затем
    diff_anchors (v5 prompt), иначе fallback на
    visible_text/numeric_parameters/scheme_analysis.nodes/connections от
    v4-блоков. Результат всегда плоский: list[str]. Для GRSH дополнительно
    возвращаются ключи visual_unverified / rejected.
    """
    out = {"labels": [], "ratings": [], "connections": []}
    if not isinstance(d, dict):
        return out
    payload = d.get("description")
    if not isinstance(payload, dict):
        return out

    if is_grsh_payload(payload):
        return _extract_grsh_anchors_from_payload(payload)

    da = payload.get("diff_anchors")
    if isinstance(da, dict):
        for raw in (da.get("labels") or []):
            if isinstance(raw, dict):
                txt = (raw.get("raw_text") or "").strip()
                if txt:
                    out["labels"].append(txt)
        for raw in (da.get("ratings") or []):
            if isinstance(raw, dict):
                txt = (raw.get("raw_text") or "").strip()
                if txt:
                    out["ratings"].append(txt)
        for raw in (da.get("connections") or []):
            if isinstance(raw, dict):
                f = (raw.get("from_raw") or "").strip()
                t = (raw.get("to_raw") or "").strip()
                if f or t:
                    out["connections"].append(f"{f or '?'} -> {t or '?'}")
        if out["labels"] or out["ratings"] or out["connections"]:
            return out

    # Fallback для v4-блоков: пытаемся вытащить хоть что-то.
    vt = payload.get("visible_text") or []
    if isinstance(vt, list):
        for x in vt:
            if isinstance(x, str) and x.strip():
                out["labels"].append(x.strip())
    np = payload.get("numeric_parameters") or []
    if isinstance(np, list):
        for x in np:
            if isinstance(x, dict):
                val = (x.get("value") or "").strip()
                unit = (x.get("unit") or "").strip()
                if val:
                    out["ratings"].append((val + " " + unit).strip())
    scheme = payload.get("scheme_analysis") or {}
    if isinstance(scheme, dict):
        for n in (scheme.get("nodes") or []):
            if isinstance(n, dict):
                mark = (n.get("visible_mark") or n.get("label") or "").strip()
                if mark:
                    out["labels"].append(mark)
        for c in (scheme.get("connections") or []):
            if isinstance(c, dict):
                f = (c.get("from") or "").strip()
                t = (c.get("to") or "").strip()
                if f or t:
                    out["connections"].append(f"{f or '?'} -> {t or '?'}")
    # Deduplicate, сохраняя порядок.
    for k in out:
        seen: set[str] = set()
        uniq: list[str] = []
        for v in out[k]:
            key = v.strip()
            if key and key not in seen:
                seen.add(key)
                uniq.append(key)
        out[k] = uniq
    return out


def build_image_diff_index(descriptions: list[dict]) -> str:
    """Сформировать компактный IMAGE_DIFF_INDEX для enriched MD.

    Индекс ставится в начало enriched MD сразу после
    `<!-- ENRICHED_MD_FORMAT: replace_image_blocks_v1 -->`. Opus получает
    плоский список:

        ## Page 24 / block ... / scheme / confidence 0.74 / usable_for_diff=true
        labels:
        - ЩР-1а
        ratings:
        - 1000А
        connections:
        - ВРУ-2 с.ш.1 -> ЩР-1а
        warnings:
        - none

    Это нужно, чтобы при сравнении двух стадий буквальные маркировки были
    видны Opus'у ДО любого markdown'а — снижает риск, что image_enrichment
    источник вообще не сработает.
    """
    if not descriptions:
        return _IMAGE_DIFF_INDEX_START + "\n_no image blocks_\n" + _IMAGE_DIFF_INDEX_END + "\n"

    lines = [_IMAGE_DIFF_INDEX_START]
    for d in descriptions:
        if not isinstance(d, dict):
            continue
        item_status = (d.get("status") or "").lower()
        if item_status in ("pending", "no_image"):
            continue  # пустые блоки — не индексируем
        try:
            anchors = _extract_anchors_from_description(d)
        except Exception:  # noqa: BLE001
            logger.debug("build_image_diff_index: extract failed", exc_info=True)
            continue

        page = d.get("page") or d.get("original_page")
        block_id = (d.get("md_block_id") or d.get("original_block_id")
                    or d.get("side_block_id") or "").strip()
        block_type = (d.get("block_type") or "photo_or_general").strip()
        usable = bool(d.get("usable_for_diff", True))
        warnings_list = list(d.get("warnings") or [])

        # confidence из description.confidence, не из item.
        conf_text = ""
        payload = d.get("description")
        if isinstance(payload, dict):
            try:
                conf_text = f" / confidence {float(payload.get('confidence') or 0.0):.2f}"
            except (TypeError, ValueError):
                pass

        header_parts = [
            f"## Page {page if page is not None else '?'}",
            f"block {block_id or '?'}",
            block_type,
        ]
        header = " / ".join(header_parts) + conf_text + f" / usable_for_diff={'true' if usable else 'false'}"
        lines.append(header)
        lines.append("")

        # labels / ratings / connections (если есть хотя бы по одной строке).
        # Для GRSH labels = verified+ocr_only (это evidence).
        for section_name, key in (("labels", "labels"), ("ratings", "ratings"), ("connections", "connections")):
            arr = anchors.get(key) or []
            if not arr:
                continue
            lines.append(f"{section_name}:")
            # Ограничиваем размер на блок, чтобы index оставался компактным.
            for v in arr[:30]:
                lines.append(f"- {v}")
            lines.append("")

        # GRSH: visual_unverified и rejected — РАЗДЕЛЬНО. rejected явно помечены
        # «(NOT evidence)» и НЕ должны использоваться Opus как доказательство.
        visual_unverified = anchors.get("visual_unverified") or []
        if visual_unverified:
            lines.append("visual_unverified (weak, not evidence alone):")
            for v in visual_unverified[:30]:
                lines.append(f"- {v}")
            lines.append("")
        rejected = anchors.get("rejected") or []
        if rejected:
            lines.append("rejected (NOT evidence — hallucinated/unreadable, do not use):")
            for v in rejected[:30]:
                lines.append(f"- {v}")
            lines.append("")

        # GRSH core anchors (ядро ГРЩ): вводные QF 3200/50кА, шинопровод, АВР/ПСВ,
        # УЗИП/ОПН, ТТ/ТШП, учёт(Меркурий/TS), АУКРМ, ГЗШ/ДСУП, штамп.
        # not_extracted перечисляется явно (absence ≠ removed).
        if isinstance(payload, dict) and isinstance(payload.get("core_systems"), dict):
            try:
                core_lines = grsh_core_systems_mod.core_diff_index_lines(payload["core_systems"])
            except Exception:  # noqa: BLE001
                core_lines = []
            if core_lines:
                lines.extend(core_lines)
                lines.append("")

        # warnings: всегда показываем хотя бы «none», чтобы Opus понимал.
        lines.append("warnings:")
        if warnings_list:
            for w in warnings_list[:8]:
                lines.append(f"- {w}")
        else:
            lines.append("- none")
        lines.append("")

    lines.append(_IMAGE_DIFF_INDEX_END)
    return "\n".join(lines) + "\n"




# ─── Подготовка карты блоков из result.json ──────────────────────────────




# ─── Image resolution ────────────────────────────────────────────────────






# ─── Высокоуровневый enrich для одной стороны ────────────────────────────




def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _read_image_descriptions(session_id: str, pair_id: str, side: str) -> Optional[dict]:
    p = paths_mod.text_enrichment_descriptions_path(session_id, pair_id, side)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_image_descriptions(session_id: str, pair_id: str, side: str, payload: dict) -> Path:
    p = paths_mod.text_enrichment_descriptions_path(session_id, pair_id, side)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return p






def _read_side_md(md_path: Optional[str | Path]) -> Optional[str]:
    if not md_path:
        return None
    p = Path(md_path)
    if not p.exists():
        return None
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def discover_image_blocks_for_side(md_text: Optional[str]) -> tuple[list[MdBlock], list[MdBlock]]:
    """Распарсить MD и разбить на (все_блоки, image_only_блоки)."""
    if not md_text:
        return [], []
    blocks = parse_md_blocks(md_text)
    image_blocks = [b for b in blocks if b.is_image]
    return blocks, image_blocks








def read_summary_only(session_id: str, pair_id: str, side: str) -> dict:
    """Лёгкое read-only представление для GET md-enrichment.

    Не запускает парсер и не читает MD — только подхватывает существующий
    JSON, чтобы быстро отрисовать статус в UI.
    """
    data = _read_image_descriptions(session_id, pair_id, side)
    md_path_resolved = paths_mod.text_enrichment_md_path(session_id, pair_id, side)
    md_format = "unknown"
    if md_path_resolved.exists():
        try:
            head = md_path_resolved.read_text(encoding="utf-8", errors="replace")[:4096]
            md_format = detect_enriched_md_format(head)
        except OSError:
            md_format = "unknown"
    if not data:
        return {
            "side": side,
            "status": "not_run",
            "image_blocks": 0,
            "described": 0,
            "from_cache": 0,
            "errors": 0,
            "pending": 0,
            "salvaged": 0,
            "enriched_md_path": None,
            "enriched_md_format_version": md_format,
            "replacement_mode": md_format == ENRICHED_MD_FORMAT_VERSION,
            "original_image_blocks": 0,
            "replaced_image_blocks": 0,
            "qwen_description_blocks": 0,
        }
    salvaged = int(data.get("salvaged") or 0)
    described = int(data.get("described") or 0)
    image_blocks_total = int(data.get("image_blocks_total") or 0)
    errors_n = int(data.get("errors") or 0)
    pending_n = int(data.get("pending") or 0)
    if image_blocks_total and described == image_blocks_total and errors_n == 0 and pending_n == 0 and salvaged == 0:
        status = "done"
    elif (image_blocks_total
          and described == image_blocks_total
          and errors_n == 0
          and pending_n == 0
          and salvaged > 0):
        # Все блоки описаны, часть восстановлена salvage'ом — это
        # backward-совместимое представление старых artifact'ов,
        # которые писали status="partial" по той же ситуации.
        status = "done_with_salvage"
    elif described > 0:
        status = "partial"
    else:
        status = "not_run"
    # Replacement-mode metadata. JSON может быть legacy (без полей) — тогда
    # source-of-truth — формат enriched.md на диске.
    json_format = (data.get("enriched_md_format_version") or "").strip() or None
    if json_format:
        format_version = json_format
    else:
        format_version = md_format
    return {
        "side": side,
        "status": status,
        "image_blocks": image_blocks_total,
        "described": described,
        "from_cache": int(data.get("from_cache") or 0),
        "errors": errors_n,
        "pending": pending_n,
        "salvaged": salvaged,
        "enriched_md_path": data.get("enriched_md_path"),
        "model": data.get("model"),
        "provider": data.get("provider"),
        "updated_at": data.get("updated_at"),
        "enriched_md_format_version": format_version,
        "replacement_mode": format_version == ENRICHED_MD_FORMAT_VERSION,
        "original_image_blocks": int(data.get("original_image_blocks") or image_blocks_total),
        "replaced_image_blocks": int(data.get("replaced_image_blocks") or 0),
        "qwen_description_blocks": int(data.get("qwen_description_blocks") or described),
        "md_format_on_disk": md_format,
    }


def rebuild_enriched_md_from_descriptions(
    session_id: str,
    pair_id: str,
    side: str,
    *,
    md_path: Optional[str | Path] = None,
) -> dict:
    """Пересобрать `<side>_enriched.md` из существующего image_descriptions.json,
    не вызывая Qwen повторно.

    Используется когда:
      - Qwen descriptions уже готовы и валидны;
      - но enriched.md лежит в старом `append_v0` формате (с
        `<!-- original_imagine_start -->` обёрткой).

    Возвращает dict с counts: `original_image_blocks`,
    `replaced_image_blocks`, `qwen_description_blocks`, `enriched_md_path`,
    `enriched_md_format_version`, `status`.

    Cache key включает PROMPT_VERSION (не ENRICHED_MD_FORMAT_VERSION) — потому
    что при rebuild мы не дёргаем модель, мы только пересобираем enriched.md
    из уже готовых items. Cache-инвалидация формата не нужна.
    """
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")

    data = _read_image_descriptions(session_id, pair_id, side)
    if not data:
        return {"status": "no_descriptions", "side": side}

    items = data.get("items") if isinstance(data.get("items"), list) else []

    md_resolved: Optional[str | Path] = md_path or data.get("md_path")
    md_text = _read_side_md(md_resolved)
    if md_text is None:
        return {"status": "md_not_found", "side": side, "md_path": str(md_resolved) if md_resolved else None}

    blocks, image_blocks = discover_image_blocks_for_side(md_text)

    enriched_md = build_enriched_md(blocks, items)
    md_out = paths_mod.text_enrichment_md_path(session_id, pair_id, side)
    try:
        md_out.write_text(enriched_md, encoding="utf-8")
    except OSError as exc:
        return {"status": f"write_failed:{type(exc).__name__}", "side": side, "error": str(exc)[:200]}

    original_image_blocks = len(image_blocks)
    replaced_image_blocks = sum(
        1 for d in items
        if (d.get("status") or "").lower() in ("done", "partial", "no_image", "error", "pending")
    )
    qwen_description_blocks = sum(
        1 for d in items
        if (d.get("status") or "").lower() in ("done", "partial")
    )

    data["enriched_md_format_version"] = ENRICHED_MD_FORMAT_VERSION
    data["replacement_mode"] = True
    data["enriched_md_path"] = str(md_out)
    data["original_image_blocks"] = original_image_blocks
    data["replaced_image_blocks"] = replaced_image_blocks
    data["qwen_description_blocks"] = qwen_description_blocks
    data["updated_at"] = _now_iso()
    try:
        _write_image_descriptions(session_id, pair_id, side, data)
    except OSError:
        pass

    return {
        "status": "rebuilt",
        "side": side,
        "enriched_md_path": str(md_out),
        "enriched_md_format_version": ENRICHED_MD_FORMAT_VERSION,
        "original_image_blocks": original_image_blocks,
        "replaced_image_blocks": replaced_image_blocks,
        "qwen_description_blocks": qwen_description_blocks,
        "size_bytes": len(enriched_md.encode("utf-8")),
        "size_chars": len(enriched_md),
    }


