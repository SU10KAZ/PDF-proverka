"""Нативный парсер `*_results.md` — нового MD-формата портала vibe (2026-07).

Новый формат = СТАНДАРТ (решение Андрея Ивановича 14.07.2026: конвертер в
старый Chandra-вид отменён). Этот модуль — единственная точка разбора нового
формата; потребители старого формата получают ветку
«новый формат → results_md, старый → прежние regex».

Грамматика снята с 4 реальных выгрузок (АР/ПС/ЭО/АПТ, 316 блоков, 0 отклонений
скелета) — см. experiments/новая структура./ОБСЛЕДОВАНИЕ_выгрузка_2026-07-16.md:

    # Document: <имя>.pdf
    Path: <дисциплина> / <папка> / <имя>.pdf
    Generated: YYYY-MM-DD HH:MM:SS UTC
    **Stamp:** Code: … | Stage: … | Object: … | Organization: …
    ---
    ## Page 1
    ### BLOCK #1 [TEXT|IMAGE]: blk_<32hex>
    > **Created:** YYYY-MM-DD HH:MM:SS UTC
    > **Crop:** [Crop](https://…/api/crops/<token>)
    > **Stamp:** Code: … | Stage: … | Sheet: … | Object: … | Name: … | Organization: … | Revisions: …
    <тело блока: markdown; у IMAGE — строка **[IMAGE]** | Type: … и секции
     **Summary:** / **Description:** / **Entities:** / **Verification:**>

Известные грабли, учтённые здесь:
- Stamp парсится по ИЗВЕСТНЫМ ключам, не по двоеточию (Object содержит
  «по адресу: г. Москва», Axes — «Оси:»); split по « | » небезопасен только
  теоретически, поэтому режем по позициям ключей.
- Пустые значения: «Sheet:  |» (двойной пробел) и хвостовой «Revisions: ».
- Sheet НЕуникален в документе (до 5 страниц на один Sheet) и бывает пуст —
  ключ листа = страница PDF, sheet лишь подпись.
- Created немонотонен; канонический порядок блоков = порядок в файле
  (совпадает с (page_index, ordinal) из *_blocks.json).
- `## Page N` и `### BLOCK` — единственные разделители; `---` встречается
  только после шапки. Терпимость к будущим типам ([STAMP]/[FAILED]) заложена:
  тип блока — любой [A-Z]+.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

# ── Регэкспы скелета ─────────────────────────────────────────────────────────

# Заголовок блока: "### BLOCK #12 [TEXT]: blk_8db07fd5e0a24e12b549745faa1ad4f0".
# Тип — любой [A-Z]+ (терпимость к будущим [STAMP]/[FAILED]); id — blk_<hex>.
BLOCK_HEADER_RE = re.compile(
    r"^###\s+BLOCK\s+#(?P<ordinal>\d+)\s+\[(?P<type>[A-Z]+)\]:\s*(?P<block_id>blk_[0-9a-f]{8,40})\s*$",
    re.MULTILINE,
)
PAGE_HEADER_RE = re.compile(r"^##\s+Page\s+(?P<page>\d+)\s*$", re.MULTILINE)
_DOC_HEADER_RE = re.compile(r"^#\s+Document:\s*(?P<name>.+?)\s*$", re.MULTILINE)
_PATH_RE = re.compile(r"^Path:\s*(?P<path>.+?)\s*$", re.MULTILINE)
_GENERATED_RE = re.compile(r"^Generated:\s*(?P<generated>.+?)\s*$", re.MULTILINE)

_META_CREATED_RE = re.compile(r"^>\s*\*\*Created:\*\*\s*(?P<created>.*?)\s*$")
_META_CROP_RE = re.compile(r"^>\s*\*\*Crop:\*\*\s*\[[^\]]*\]\((?P<url>[^)\s]+)\)\s*$")
_META_STAMP_RE = re.compile(r"^>\s*\*\*Stamp:\*\*\s*(?P<stamp>.*)$")
_DOC_STAMP_RE = re.compile(r"^\*\*Stamp:\*\*\s*(?P<stamp>.*)$", re.MULTILINE)

_CROP_TOKEN_RE = re.compile(r"/api/crops/(?P<token>[\w-]+)")

# Под-ключи штампа в порядке появления в строке (все 7 присутствуют всегда,
# но парсер не требует полного набора).
STAMP_KEYS = ("Code", "Stage", "Sheet", "Object", "Name", "Organization", "Revisions")
_STAMP_KEY_RE = re.compile(r"(?:^|\|)\s*(Code|Stage|Sheet|Object|Name|Organization|Revisions):\s?")

# Строка-шапка IMAGE-блока: "**[IMAGE]** | Type: План | Axes: … | Zone: … | Level: …"
_IMAGE_LINE_RE = re.compile(r"^\*\*\[IMAGE\]\*\*\s*(?P<rest>\|.*)?$")
IMAGE_META_KEYS = ("Type", "Axes", "Zone", "Level")
_IMAGE_KEY_RE = re.compile(r"(?:^|\|)\s*(Type|Axes|Zone|Level):\s?")

# Секции описания IMAGE-блока (в фиксированном порядке, но парсим терпимо).
IMAGE_SECTION_KEYS = ("Summary", "Description", "Entities", "Verification")
_IMAGE_SECTION_RE = re.compile(r"^\*\*(Summary|Description|Entities|Verification):\*\*\s*(?P<inline>.*)$")


# ── Модель ───────────────────────────────────────────────────────────────────

@dataclass
class ResultsMdBlock:
    """Один блок `### BLOCK #N [TYPE]: blk_…` нового формата."""
    block_id: str
    ordinal: int
    block_type: str                      # "text" | "image" (нижний регистр; терпимо к новым)
    page: int                            # 1-based страница PDF (из `## Page N`)
    created: Optional[str] = None        # как в MD: "2026-07-07 15:22:34 UTC"
    crop_url: Optional[str] = None
    crop_token: Optional[str] = None
    stamp: dict = field(default_factory=dict)   # {code, stage, sheet, object, name, organization, revisions}
    body: str = ""                       # содержимое без заголовка и мета-цитат
    image_meta: dict = field(default_factory=dict)  # IMAGE: {type, axes, zone, level}
    image_sections: dict = field(default_factory=dict)  # IMAGE: {summary, description, entities, verification}
    header_line: int = 0                 # 1-based строка заголовка блока в файле
    body_start_line: int = 0             # 1-based строка начала тела

    @property
    def sheet(self) -> Optional[str]:
        """Номер листа из штампа (подпись, НЕ ключ — Sheet неуникален)."""
        return self.stamp.get("sheet") or None

    @property
    def sheet_name(self) -> Optional[str]:
        return self.stamp.get("name") or None

    @property
    def is_image(self) -> bool:
        return self.block_type == "image"

    @property
    def is_text(self) -> bool:
        return self.block_type == "text"


@dataclass
class ResultsMdPage:
    """Секция `## Page N` (N = физическая страница PDF, 1-based)."""
    number: int
    blocks: list[ResultsMdBlock] = field(default_factory=list)
    start_line: int = 0

    @property
    def sheet(self) -> Optional[str]:
        """Sheet страницы: все блоки одной страницы несут одинаковый Sheet."""
        for b in self.blocks:
            if b.sheet:
                return b.sheet
        return None

    @property
    def sheet_name(self) -> Optional[str]:
        for b in self.blocks:
            if b.sheet_name:
                return b.sheet_name
        return None

    def text(self) -> str:
        """Плоский текст страницы (тела всех блоков по порядку)."""
        return "\n\n".join(b.body for b in self.blocks if b.body)


@dataclass
class ResultsMdDocument:
    """Разобранный `*_results.md`."""
    document_name: Optional[str] = None      # "ПД-00542664-АР1.2-1_V1.pdf"
    path: Optional[str] = None               # "АР / ПД-00542664-АР1.2-1 / <имя>.pdf"
    generated: Optional[str] = None          # "2026-07-15 05:51:33 UTC"
    stamp: dict = field(default_factory=dict)  # документный штамп (без Sheet/Name)
    pages: list[ResultsMdPage] = field(default_factory=list)
    blocks: list[ResultsMdBlock] = field(default_factory=list)  # сквозной порядок файла

    @property
    def discipline_hint(self) -> Optional[str]:
        """Первый сегмент Path («АР», «ЭОМ», «СС», «ВК»…) — подсказка дисциплины."""
        if not self.path:
            return None
        seg = self.path.split("/", 1)[0].strip()
        return seg or None

    @property
    def page_numbers(self) -> list[int]:
        return [p.number for p in self.pages]

    def page(self, number: int) -> Optional[ResultsMdPage]:
        for p in self.pages:
            if p.number == number:
                return p
        return None

    def blocks_by_id(self) -> dict[str, ResultsMdBlock]:
        return {b.block_id: b for b in self.blocks}

    def sheet_map(self) -> dict[int, dict]:
        """{страница PDF (1-based) → {"sheet": …, "name": …}} по штампам блоков.

        Ключ листа = страница PDF (решение АИ: Sheet неуникален); sheet/name —
        подписи, могут быть None (титулы, сертификаты, нераспознанные штампы).
        """
        return {
            p.number: {"sheet": p.sheet, "name": p.sheet_name}
            for p in self.pages
        }

    def full_page_numbers(self, pdf_page_count: Optional[int] = None) -> list[int]:
        """Полный список страниц 1..K.

        В выгрузках с 07-15 `## Page N` покрывает все страницы PDF, но ранние
        генерации пропускали страницы без блоков — при известном числе страниц
        PDF дополняем диапазон (страховка остаётся навсегда).
        """
        have = set(self.page_numbers)
        top = max([pdf_page_count or 0, *have]) if (have or pdf_page_count) else 0
        return list(range(1, top + 1))


# ── Детект формата ───────────────────────────────────────────────────────────

def is_results_md_text(text: str) -> bool:
    """Текст похож на новый формат портала (results.md)?

    Признак — заголовок блока нового формата ИЛИ пара «# Document:» + «## Page N».
    Старый Chandra-формат (`## СТРАНИЦА N`) не матчится никогда.
    """
    if not text:
        return False
    head = text[:20000]
    if BLOCK_HEADER_RE.search(head) or BLOCK_HEADER_RE.search(text):
        return True
    return bool(_DOC_HEADER_RE.search(head) and PAGE_HEADER_RE.search(text))


def is_results_md_name(name: str) -> bool:
    """Имя файла указывает на новый формат (*_results.md)."""
    return (name or "").lower().endswith("_results.md")


# ── Разбор значений ──────────────────────────────────────────────────────────

def parse_stamp_line(raw: str) -> dict:
    """Разобрать строку штампа по ИЗВЕСТНЫМ ключам.

    «Sheet:  |» (пустое значение) → "", «Object: …по адресу: г. Москва…» не
    ломает разбор (режем по позициям ключей, не по двоеточиям). Ключи в
    результате — в нижнем регистре: code/stage/sheet/object/name/organization/
    revisions; отсутствующие ключи не включаются.
    """
    out: dict = {}
    raw = (raw or "").strip()
    if not raw:
        return out
    matches = list(_STAMP_KEY_RE.finditer(raw))
    for i, m in enumerate(matches):
        key = m.group(1).lower()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(raw)
        value = raw[m.end():end].strip()
        # значение заканчивается разделителем « |» перед следующим ключом
        if value.endswith("|"):
            value = value[:-1].rstrip()
        out[key] = value
    return out


def _parse_image_line(rest: str) -> dict:
    """Разобрать хвост строки `**[IMAGE]** | Type: … | Axes: … | Zone: … | Level: …`.

    Axes/Level могут содержать вложенные двоеточия («Оси: А, Б…») — режем по
    позициям известных ключей. Ключи результата в нижнем регистре.
    """
    out: dict = {}
    rest = (rest or "").strip()
    if not rest:
        return out
    matches = list(_IMAGE_KEY_RE.finditer(rest))
    for i, m in enumerate(matches):
        key = m.group(1).lower()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(rest)
        value = rest[m.end():end].strip()
        if value.endswith("|"):
            value = value[:-1].rstrip()
        out[key] = value
    return out


def _parse_image_sections(body_lines: list[str]) -> dict:
    """Секции **Summary:**/**Description:**/**Entities:**/**Verification:** IMAGE-блока."""
    sections: dict = {}
    current: Optional[str] = None
    buf: list[str] = []

    def _flush() -> None:
        nonlocal buf, current
        if current is not None:
            sections[current] = "\n".join(buf).strip()
        buf = []

    for line in body_lines:
        m = _IMAGE_SECTION_RE.match(line.strip())
        if m:
            _flush()
            current = m.group(1).lower()
            inline = (m.group("inline") or "").strip()
            buf = [inline] if inline else []
        elif current is not None:
            buf.append(line)
    _flush()
    return sections


# ── Основной парсер ──────────────────────────────────────────────────────────

def parse_results_md(text: str) -> ResultsMdDocument:
    """Разобрать текст `*_results.md` в ResultsMdDocument.

    Терпимость: неизвестные типы блоков сохраняются (block_type в нижнем
    регистре), отсутствующие мета-строки → None, текст до первого `## Page`
    вне шапки игнорируется. Порядок блоков = порядок файла.
    """
    doc = ResultsMdDocument()
    if not text:
        return doc

    m = _DOC_HEADER_RE.search(text)
    if m:
        doc.document_name = m.group("name").strip()
    m = _PATH_RE.search(text)
    if m:
        doc.path = m.group("path").strip()
    m = _GENERATED_RE.search(text)
    if m:
        doc.generated = m.group("generated").strip()
    # документный штамп — ПЕРВЫЙ `**Stamp:**` до первой страницы
    first_page = PAGE_HEADER_RE.search(text)
    head_end = first_page.start() if first_page else len(text)
    m = _DOC_STAMP_RE.search(text, 0, head_end)
    if m:
        doc.stamp = parse_stamp_line(m.group("stamp"))

    lines = text.splitlines()
    # индексы строк-границ: (line_idx, kind, match)
    boundaries: list[tuple[int, str, re.Match]] = []
    for i, line in enumerate(lines):
        pm = PAGE_HEADER_RE.match(line)
        if pm:
            boundaries.append((i, "page", pm))
            continue
        bm = BLOCK_HEADER_RE.match(line)
        if bm:
            boundaries.append((i, "block", bm))

    current_page: Optional[ResultsMdPage] = None
    for bi, (line_idx, kind, m) in enumerate(boundaries):
        if kind == "page":
            current_page = ResultsMdPage(number=int(m.group("page")), start_line=line_idx + 1)
            doc.pages.append(current_page)
            continue

        # заголовок блока
        block = ResultsMdBlock(
            block_id=m.group("block_id"),
            ordinal=int(m.group("ordinal")),
            block_type=m.group("type").lower(),
            page=current_page.number if current_page else 0,
            header_line=line_idx + 1,
        )
        # конец содержимого блока — следующая граница или конец файла
        next_idx = boundaries[bi + 1][0] if bi + 1 < len(boundaries) else len(lines)

        # мета-цитаты `> **…**` сразу после заголовка (пустые строки допустимы)
        j = line_idx + 1
        while j < next_idx:
            line = lines[j]
            if not line.strip():
                j += 1
                continue
            if not line.lstrip().startswith(">"):
                break
            mm = _META_CREATED_RE.match(line)
            if mm:
                block.created = mm.group("created").strip() or None
                j += 1
                continue
            mm = _META_CROP_RE.match(line)
            if mm:
                block.crop_url = mm.group("url").strip()
                tm = _CROP_TOKEN_RE.search(block.crop_url)
                block.crop_token = tm.group("token") if tm else None
                j += 1
                continue
            mm = _META_STAMP_RE.match(line)
            if mm:
                block.stamp = parse_stamp_line(mm.group("stamp"))
                j += 1
                continue
            # незнакомая цитата — не мета, дальше тело
            break

        body_lines = lines[j:next_idx]
        # убрать хвостовые/ведущие пустые строки, сохранив внутренние
        while body_lines and not body_lines[0].strip():
            body_lines.pop(0)
            j += 1
        while body_lines and not body_lines[-1].strip():
            body_lines.pop()
        block.body_start_line = j + 1
        block.body = "\n".join(body_lines)

        if block.is_image and body_lines:
            im = _IMAGE_LINE_RE.match(body_lines[0].strip())
            if im:
                block.image_meta = _parse_image_line(im.group("rest") or "")
            block.image_sections = _parse_image_sections(body_lines)

        doc.blocks.append(block)
        if current_page is not None:
            current_page.blocks.append(block)

    return doc


def parse_results_md_file(path) -> ResultsMdDocument:
    """Прочитать и разобрать файл `*_results.md` (utf-8, errors=replace)."""
    from pathlib import Path as _P
    return parse_results_md(_P(path).read_text(encoding="utf-8", errors="replace"))
