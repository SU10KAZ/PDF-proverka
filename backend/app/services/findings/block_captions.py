"""Человекочитаемые подписи блоков для текстов замечаний.

Сторонний эксперт не знает, что такое block_id («6L97-3VTH-XTC»), поэтому
внутренние идентификаторы в видимых текстах замечаний бесполезны. Модуль:

1. строит карту block_id → подпись («Перечень отклонений…» (лист 1, стр. PDF 5))
   из уже существующих артефактов (01_blocks_analysis.json, document_graph.json);
2. детерминированно заменяет упоминания ID в текстовых полях замечаний
   (problem/description/solution/risk/recommendation) на такие подписи,
   а слово «блок*» перед ID — на «фрагмент*» с тем же падежным окончанием.

Вызывается ПОСЛЕДНЕЙ post-merge операцией findings_merge: группировка
merge_similar_findings/дедуп и якоря text-layer highlights работают по сырым
текстам, гуманизация ничего из этого не сдвигает.

Структурные поля не чистятся — наоборот: ID, встречавшиеся только в тексте
problem/description, переносятся в структуру (image-блоки → related_block_ids,
текст-блоки → selected_text_block_ids), потому что их читают
deterministic_critic (_referenced_block_ids) и compute_finding_block_map
(привязка кропов в UI). ID из solution/risk/recommendation в структуру не
переносятся: критик эти поля никогда не сканировал, а перенос блока-примера
с другой страницы в related_block_ids давал бы ложный page_mismatch.

Замена выполняется только для ID с содержательной подписью; неизвестные и
вырожденные остаются как есть (лучше сырой ID, чем неверная подпись).
Названия санируются (_clean_name): вычищаются сами ID-токены (эхо-label
«Блок 3C6E-… — схема» вернул бы ID в текст и сломал идемпотентность),
JSON-огрызки, generic-метки («image», «Схема»), хедж-хвосты («, вероятно…»),
внутренние «ёлочки» → „лапки“. Если в одном поле заменяется > 4 разных ID,
подписи рендерятся компактно «(лист N, стр. PDF M)» — иначе текст раздувается
втрое. Преобразование идемпотентно: после замены известных ID в тексте не
остаётся, повторный прогон — no-op.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Формат портальных block_id: 6L97-3VTH-XTC / RUXD-WP4R-6C3 (см. также
# _BLOCK_ID_RE в findings_review/deterministic_critic.py — там шире, включая
# legacy block_007_1; здесь только портальный формат, для legacy ID подписи
# всё равно не построить).
_ID_TOKEN_RE = re.compile(r"\b[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{2,4}\b")

# Слово «блок» в любом падеже непосредственно перед уже заменённой подписью.
_BLOCK_WORD_RE = re.compile(
    r"\b([Бб])лок(ами|ах|ам|ов|ом|а|у|е|и)?(?=\s*[«\"']?\x00)"
)
# Англицизм «BLOCK 49RV-UJKL-6QT» из LLM-текстов (живой паттерн в description).
_BLOCK_WORD_LAT_RE = re.compile(r"\b[Bb][Ll][Oo][Cc][Kk](?P<pl>[Ss])?(?=\s*[«\"']?\x00)")

# «блоки … » — им. падеж мн.ч.: у «фрагмент» окончание другое (и → ы).
_ENDING_MAP = {"и": "ы"}

# Хедж-хвосты OCR-описаний: «…, вероятно с указанием размеров…» — не название.
_HEDGE_TAIL_RE = re.compile(
    r",?\s*(?:вероятно|возможно|по-видимому|скорее всего|судя по|"
    r"представляющ\w+ соб\w+)\b.*$",
    re.IGNORECASE,
)

# Generic-метки без содержательной ценности (extract_ocr_label отдаёт "image").
_GENERIC_NAMES = {
    "image", "изображение", "схема", "план", "таблица", "узел",
    "чертеж", "чертёж", "фрагмент", "текст", "рисунок",
}

# Текстовые поля замечания, которые видит человек (в UI, Excel, БЗ).
TEXT_FIELDS = ("problem", "description", "solution", "risk", "recommendation")
# Поля, из которых найденные ID переносятся в структуру (паритет с критиком:
# deterministic_critic._referenced_block_ids сканировал description/problem/title).
ANCHOR_FIELDS = ("problem", "description")

_MAX_NAME_LEN = 60
# Больше стольких разных ID в одном поле → компактный рендер без названий.
_COMPACT_THRESHOLD = 4


@dataclass
class BlockCaption:
    block_id: str
    name: str = ""            # короткое название содержимого
    sheet_no: str = ""        # номер листа из штампа («18», «1 (из 2)»)
    page: Optional[int] = None
    kind: str = "image"       # image | text — судьба ID при переносе в структуру

    @property
    def is_degenerate(self) -> bool:
        """Ни названия, ни листа, ни страницы — подписи не выйдет."""
        return not (self.name or self.sheet_no or self.page is not None)

    def render(self, compact: bool = False) -> str:
        """«Название» (лист 18, стр. PDF 24); compact — «(лист 18, стр. PDF 24)»."""
        where = []
        if self.sheet_no:
            where.append(f"лист {self.sheet_no}")
        if self.page is not None:
            where.append(f"стр. PDF {self.page}")
        where_s = ", ".join(where)
        if not compact and self.name:
            return f"«{self.name}» ({where_s})" if where_s else f"«{self.name}»"
        if where_s:
            return f"({where_s})"
        # вырожденный случай: humanize_text такие подписи не подставляет
        # (иначе сырой ID вернулся бы в текст и сломал идемпотентность)
        return f"фрагмент {self.block_id}"


def _shorten(text: str, limit: int = _MAX_NAME_LEN) -> str:
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    # Длинный label часто = «Название. Рассуждение модели…» — первое
    # предложение целиком лучше обрубка. Но не резать по точке сокращения
    # («Развертка Пом. 123»): последнее слово перед точкой должно быть ≥ 4 букв.
    first_sentence = text.split(". ", 1)[0].rstrip(".")
    last_word = re.search(r"[\wА-Яа-яЁё]+$", first_sentence)
    if (
        12 <= len(first_sentence) <= limit
        and last_word is not None
        and len(last_word.group(0)) >= 4
    ):
        return first_sentence
    cut = text[:limit]
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut.rstrip(" ,;:.") + "…"


def _clean_name(name: str) -> str:
    """Санация названия: ID-токены, хедж-хвосты, мусор, generic-метки → вон.

    Пустой результат означает «содержательного названия нет» — подпись
    деградирует до «(лист N, стр. PDF M)» через render().
    """
    # ID внутри названия (эхо-label «Блок 3C6E-… — схема») вернул бы сырой ID
    # в текст и сломал идемпотентность повторного прогона.
    name = _ID_TOKEN_RE.sub(" ", name)
    name = _HEDGE_TAIL_RE.sub("", name)
    # Внутренние «ёлочки» → „лапки“: render() оборачивает name в «…»
    name = name.replace("«", "„").replace("»", "“")
    name = " ".join(name.split()).strip(" :—–-„“\"'.,;")
    if not name:
        return ""
    if name.lower() in _GENERIC_NAMES:
        return ""
    # Остатки JSON/техномусора («H{…», '"location": {')
    if "{" in name or "}" in name or '":' in name:
        return ""
    if not re.search(r"[А-Яа-яA-Za-zЁё]{3}", name):
        return ""
    return _shorten(name)


def _name_from_label(label: object) -> str:
    """Название из label/ocr_label Stage 02; guard от битых JSON-огрызков."""
    if not isinstance(label, str):
        return ""
    label = label.strip()
    if not label:
        return ""
    if label.startswith(("{", "[", '"', "'", "`")):
        # Огрызок JSON-ответа OCR («{\n "location": …»): пробуем достать
        # человекочитаемые поля, иначе название не строим вовсе.
        for key in ("content_summary", "zone_name", "project_name"):
            m = re.search(rf'"{key}"\s*:\s*"([^"\\]+)"', label)
            if m and m.group(1).strip():
                return _clean_name(m.group(1))
        return ""
    return _clean_name(label)


def _name_from_text(text: object) -> str:
    """Название текст-блока: первая содержательная строка его текста."""
    if not isinstance(text, str):
        return ""
    for line in text.splitlines():
        line = " ".join(line.replace("\t", " ").split())
        # короткие строки — ячейки шапки таблицы («№ п/п»), не название
        if len(line) < 12:
            continue
        if not re.search(r"[А-Яа-яA-Za-z]{3}", line):
            continue
        return _clean_name(line)
    return ""


def _graph_page_sheet_map(graph: dict) -> dict[int, str]:
    from backend.app.pipeline.stages.prepare.graph_builder import get_page_sheet_no

    psm: dict[int, str] = {}
    for pg in graph.get("pages", []) or []:
        try:
            page_num = int(pg.get("page"))
        except (TypeError, ValueError):
            continue
        sheet_no = get_page_sheet_no(pg) or pg.get("sheet_no_normalized")
        if sheet_no:
            psm[page_num] = str(sheet_no)
    return psm


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _atomic_write_json(path: Path, data: dict) -> None:
    """tmp-файл + os.replace: kill посреди записи не оставит усечённый мастер."""
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=path.name + ".", suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(data, ensure_ascii=False, indent=2))
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def build_block_caption_map(output_dir: Path) -> dict[str, BlockCaption]:
    """Карта block_id → подпись из артефактов версии.

    Источники (по убыванию качества названия):
      1. 01_blocks_analysis.json → block_analyses[].label (+page);
      2. document_graph.json → image_blocks (ocr) и text_blocks (первая строка);
    Номер листа — всегда из document_graph (get_page_sheet_no, v1/v2).
    """
    captions: dict[str, BlockCaption] = {}

    graph = _read_json(output_dir / "document_graph.json") or {}
    psm = _graph_page_sheet_map(graph)

    def _sheet_for(page: Optional[int]) -> str:
        if page is None:
            return ""
        return psm.get(int(page), "")

    def _coerce_page(value: object) -> Optional[int]:
        try:
            return int(value)  # в v2-графе page бывает строкой
        except (TypeError, ValueError):
            return None

    # 2) document_graph: покрывает и text-блоки (их нет в Stage 02 вовсе)
    for pg in graph.get("pages", []) or []:
        page = _coerce_page(pg.get("page"))
        for tb in pg.get("text_blocks", []) or []:
            bid = tb.get("id") or tb.get("block_id")
            if bid:
                captions[bid] = BlockCaption(
                    block_id=bid,
                    name=_name_from_text(tb.get("text")),
                    sheet_no=_sheet_for(page),
                    page=page,
                    kind="text",
                )
        for ib in pg.get("image_blocks", []) or []:
            bid = ib.get("id") or ib.get("block_id")
            if not bid:
                continue
            ocr = (
                ib.get("ocr_text_normalized")
                or ib.get("ocr")
                or ib.get("ocr_raw")
                or ""
            )
            name = _name_from_label(ocr) or _name_from_text(ocr)
            captions[bid] = BlockCaption(
                block_id=bid, name=name, sheet_no=_sheet_for(page), page=page,
                kind="image",
            )

    # 1) Stage 02 label — качественнее OCR-огрызков, перекрывает graph-запись
    from backend.app.services.storage.stage_artifacts import (
        BLOCKS_ANALYSIS_FILENAME,
        resolve_existing,
    )

    blocks_path = resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME)
    data02 = _read_json(blocks_path) if blocks_path.exists() else None
    for ba in (data02 or {}).get("block_analyses", []) or []:
        bid = ba.get("block_id")
        if not bid:
            continue
        page = _coerce_page(ba.get("page"))
        prev = captions.get(bid)
        name = _name_from_label(ba.get("label"))
        if not name and prev:
            name = prev.name
        if page is None and prev:
            page = prev.page
        captions[bid] = BlockCaption(
            block_id=bid,
            name=name,
            sheet_no=_sheet_for(page) or (prev.sheet_no if prev else ""),
            page=page,
            kind="image",  # Stage 02 анализирует только image-блоки
        )

    return captions


def humanize_text(text: str, captions: dict[str, BlockCaption]) -> tuple[str, list[str]]:
    """Заменить известные block_id в тексте на подписи.

    Возвращает (новый текст, список заменённых ID). Неизвестные ID не трогаем.
    Замена через маркер \\x00: слово «блок*» меняем на «фрагмент*» только
    перед реально заменёнными ID.
    """
    # Посторонний U+0000 из входа (валиден в JSON) коллидировал бы с маркером.
    text = text.replace("\x00", "")

    substitutable = [
        t for t in dict.fromkeys(_ID_TOKEN_RE.findall(text))
        if t in captions and not captions[t].is_degenerate
    ]
    if not substitutable:
        return text, []
    # Стена из десятков подписей раздувает текст втрое — компактный рендер.
    compact = len(substitutable) > _COMPACT_THRESHOLD

    replaced: list[str] = []

    def _sub_id(m: re.Match) -> str:
        cap = captions.get(m.group(0))
        if cap is None or cap.is_degenerate:
            return m.group(0)
        rendered = cap.render(compact=compact)
        if _ID_TOKEN_RE.search(rendered):
            # подпись сама содержит ID-подобный токен — не подставляем,
            # иначе ID вернётся в текст и повторный прогон снова его обернёт
            return m.group(0)
        replaced.append(m.group(0))
        return "\x00" + rendered

    new_text = _ID_TOKEN_RE.sub(_sub_id, text)
    if not replaced:
        return text, []

    def _sub_word(m: re.Match) -> str:
        first = "Ф" if m.group(1) == "Б" else "ф"
        ending = m.group(2) or ""
        return first + "рагмент" + _ENDING_MAP.get(ending, ending)

    new_text = _BLOCK_WORD_RE.sub(_sub_word, new_text)
    new_text = _BLOCK_WORD_LAT_RE.sub(
        lambda m: "фрагменты" if m.group("pl") else "фрагмент", new_text,
    )
    new_text = new_text.replace("\x00", "")
    return new_text, replaced


def humanize_findings(
    findings: list[dict], captions: dict[str, BlockCaption],
) -> dict:
    """Пройти по замечаниям: заменить ID в текстах, сохранив привязку.

    ID из problem/description переносятся в структуру — иначе после чистки
    текста замечание с пустыми структурными полями потеряет fallback-привязку
    к кропам (compute_finding_block_map) и evidence-проверку критика
    (_referenced_block_ids). Судьба по типу блока: image → related_block_ids,
    text → selected_text_block_ids (текст-блоков нет в 01_blocks_analysis,
    в related они дали бы ложный phantom_block у критика).
    """
    stats = {
        "findings_changed": 0,
        "ids_replaced": 0,
        "related_added": 0,
        "text_refs_added": 0,
    }
    for f in findings:
        changed = False
        anchor_ids: list[str] = []
        for field in TEXT_FIELDS:
            value = f.get(field)
            if not isinstance(value, str) or not value:
                continue
            new_value, replaced = humanize_text(value, captions)
            if replaced:
                f[field] = new_value
                if field in ANCHOR_FIELDS:
                    anchor_ids.extend(replaced)
                stats["ids_replaced"] += len(replaced)
                changed = True
        # sub_findings — состав merged-группы, показывается в UI
        for sub in f.get("sub_findings") or []:
            if not isinstance(sub, dict):
                continue
            value = sub.get("problem")
            if not isinstance(value, str) or not value:
                continue
            new_value, replaced = humanize_text(value, captions)
            if replaced:
                sub["problem"] = new_value
                stats["ids_replaced"] += len(replaced)
                changed = True
        if not changed:
            continue
        stats["findings_changed"] += 1
        image_ids = [
            b for b in dict.fromkeys(anchor_ids)
            if captions[b].kind != "text"
        ]
        text_ids = [
            b for b in dict.fromkeys(anchor_ids)
            if captions[b].kind == "text"
        ]
        if image_ids:
            related = f.get("related_block_ids")
            if not isinstance(related, list):
                related = []
            for bid in image_ids:
                if bid not in related:
                    related.append(bid)
                    stats["related_added"] += 1
            f["related_block_ids"] = related
        if text_ids:
            selected = f.get("selected_text_block_ids")
            if not isinstance(selected, list):
                selected = []
            for bid in text_ids:
                if bid not in selected:
                    selected.append(bid)
                    stats["text_refs_added"] += 1
            f["selected_text_block_ids"] = selected
    return stats


def humanize_findings_file(output_dir: Path) -> Optional[dict]:
    """Прочитать 03_findings.json, гуманизировать тексты, записать при изменениях.

    Fail-soft: любая проблема чтения → None, файл не трогаем. Запись атомарная.
    """
    findings_path = output_dir / "03_findings.json"
    data = _read_json(findings_path)
    if not data:
        return None
    findings = data.get("findings")
    if not isinstance(findings, list):
        return None

    captions = build_block_caption_map(output_dir)
    if not captions:
        return None

    stats = humanize_findings(findings, captions)
    if stats["findings_changed"]:
        _atomic_write_json(findings_path, data)
    return stats
