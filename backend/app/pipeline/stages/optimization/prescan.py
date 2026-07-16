"""Deterministic pre-scan for optimization opportunities in project MD.

The pre-scan is intentionally conservative. It does not create final
``optimization.json`` items. Instead, it builds a short checklist of
high-signal specification rows and repeated product families that the LLM must
review before writing optimization proposals.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from backend.app.services.common.results_md import is_results_md_text, parse_results_md


_UNIT_THRESHOLDS = {
    "шт": 20.0,
    "компл": 5.0,
    "м": 100.0,
    "пог.м": 100.0,
    "м2": 50.0,
    "м3": 15.0,
    "кг": 100.0,
    "т": 1.0,
}

_MOUNTING_KEYWORDS = (
    "креп", "кроншт", "анкер", "опор", "подвес", "хомут", "лоток",
    "гильз", "проход", "врезк", "свар", "болт", "муфт", "фитинг",
    "зажим", "соедин", "гофр", "короб",
)

_LIFECYCLE_KEYWORDS = (
    "насос", "вентилятор", "частот", "акб", "батар", "ибп", "источник",
    "счетчик", "счётчик", "регулятор", "фильтр", "изоляц", "компенсатор",
    "клапан", "теплосчетчик", "теплосчётчик", "привод", "двигател",
)

_DISCIPLINE_KEYWORDS = {
    "AI": (
        "сантех", "смесител", "мойк", "мебель", "панел", "плитк",
        "покрыт", "светиль", "двер", "люк", "камень",
    ),
    "AR": (
        "фасад", "нвф", "панел", "плитк", "керамогран", "краск",
        "штукатур", "двер", "люк", "огражд", "анкер", "креп", "камень",
    ),
    "EOM": (
        "кабель", "лоток", "щит", "автомат", "выключател", "светиль",
        "шина", "труба", "гофр", "счетчик", "счётчик", "акб", "ибп",
    ),
    "GP": (
        "плитк", "борт", "бордюр", "решет", "решёт", "покрыт",
        "асфальт", "бетон", "газон", "камень", "креп",
    ),
    "KJ": (
        "арматур", "бетон", "каркас", "стерж", "анкер", "закладн",
        "опалуб", "свар", "гильз", "плита", "балка",
    ),
    "KM": (
        "сталь", "профил", "лист", "балка", "колонн", "анкер", "креп",
        "закладн", "свар", "фасад", "нвф", "кроншт",
    ),
    "OV": (
        "воздуховод", "вентилятор", "клапан", "труба", "трубопровод",
        "изоляц", "насос", "радиатор", "конвектор", "компенсатор", "кран",
        "фильтр", "фитинг", "коллектор",
    ),
    "PT": (
        "труба", "трубопровод", "оросител", "шкаф", "клапан", "врезк",
        "отвод", "гильз", "креп", "насос", "изоляц",
    ),
    "SS": (
        "кабель", "извещател", "оповещател", "шкаф", "блок", "акб",
        "ивэп", "контроллер", "датчик", "лоток", "короб", "проходк",
    ),
    "TX": (
        "мебель", "оборудован", "машин", "знак", "разметк", "покрыт",
        "отбой", "демпфер", "шлагбаум", "креп",
    ),
    "VK": (
        "труба", "трубопровод", "клапан", "кран", "насос", "фильтр",
        "счетчик", "счётчик", "изоляц", "регулятор", "гильз", "фитинг",
    ),
}

_CRITICAL_FINDING_SEVERITIES = {"КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ"}

_STOPWORDS = {
    "для", "или", "при", "тип", "марка", "исполнение", "правое", "левое",
    "нижним", "верхним", "подключением", "техническая", "характеристика",
    "оборудование", "изделия", "материалы", "наименование",
}

_ROOM_AREA_WORDS = (
    "помещение", "вестибюль", "кладовая", "санузел", "с/у", "коридор",
    "автостоянка", "насосная", "лестничная", "тамбур", "холл", "лобби",
    "техническое пространство", "машиноместо", "квартира",
)

_AREA_MATERIAL_KEYWORDS = (
    "отделк", "плитк", "покрыт", "разметк", "штукатур", "краск",
    "панел", "бетон", "керамогран", "фасад", "изоляц", "огнезащ",
    "крошк", "борт", "лоток", "решет", "решёт", "облицов",
    "стяжк", "потол", "пол", "стен", "клей", "камень",
)


@dataclass(frozen=True)
class _MdPage:
    page: int
    sheet: str
    text: str


@dataclass(frozen=True)
class _SpecRow:
    page: int
    sheet: str
    pos: str
    name: str
    model: str
    code: str
    supplier: str
    unit: str
    qty: float

    @property
    def text(self) -> str:
        return " ".join(x for x in (self.name, self.model, self.code, self.supplier) if x)

    @property
    def spec_item(self) -> str:
        label = f"Поз. {self.pos} — " if self.pos else ""
        return _truncate(label + self.name, 180)


@dataclass
class OptimizationOpportunity:
    candidate_id: str
    type: str
    lens: str
    title: str
    why: str
    spec_items: list[str] = field(default_factory=list)
    related_pages: list[int] = field(default_factory=list)
    related_sheets: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    confidence: str = "medium"
    opportunity_score: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _norm(value: Any) -> str:
    text = _clean_text(value).lower().replace("ё", "е")
    text = text.replace("×", "x")
    text = re.sub(r"[^0-9a-zа-я.+/ -]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _truncate(value: Any, limit: int = 220) -> str:
    text = _clean_text(value)
    return text if len(text) <= limit else text[: limit - 3].rstrip() + "..."


def _split_pages_results_md(md_text: str) -> list[_MdPage]:
    """Страницы нового формата портала (`*_results.md`) в структуре _MdPage.

    Номер страницы = физическая страница PDF (1-based, из ``## Page N``);
    sheet — подпись из штампа блоков (может быть пустой: титулы, нераспознанные
    штампы). ``text`` — плоский текст страницы (тела блоков), как и в старом
    пути, где страница = сырой текст между заголовками ``## СТРАНИЦА``.
    """
    doc = parse_results_md(md_text)
    return [
        _MdPage(page=page.number, sheet=_clean_text(page.sheet or ""), text=page.text())
        for page in doc.pages
    ]


def _split_pages(md_text: str) -> list[_MdPage]:
    # Новый формат портала vibe (*_results.md) разбирается единым парсером
    # results_md; старый Chandra-формат («## СТРАНИЦА N») — прежним кодом.
    if is_results_md_text(md_text):
        results_pages = _split_pages_results_md(md_text)
        if results_pages:
            return results_pages
    headers = list(re.finditer(r"(?m)^##\s+СТРАНИЦА\s+(\d+)\s*$", md_text))
    if not headers:
        return [_MdPage(page=0, sheet="", text=md_text)]

    pages: list[_MdPage] = []
    for idx, header in enumerate(headers):
        start = header.end()
        end = headers[idx + 1].start() if idx + 1 < len(headers) else len(md_text)
        body = md_text[start:end]
        sheet_match = re.search(r"\*\*Лист:\*\*\s*([^\n]+)", body)
        sheet = _clean_text(sheet_match.group(1)) if sheet_match else ""
        pages.append(_MdPage(page=int(header.group(1)), sheet=sheet, text=body))
    return pages


def _split_table_cells(line: str) -> list[str]:
    if "|" not in line:
        return []
    raw = line.strip()
    if not raw.startswith("|"):
        return []
    cells = [cell.strip() for cell in raw.strip("|").split("|")]
    return cells


def _is_separator_row(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-{3,}:?", c.strip()) for c in cells)


def _is_numbering_row(cells: list[str]) -> bool:
    values = [c.strip() for c in cells if c.strip()]
    return len(values) >= 3 and all(re.fullmatch(r"\d{1,2}", c) for c in values)


def _looks_like_spec_header(cells: list[str]) -> bool:
    normed = [_norm(c) for c in cells]
    if any(
        "подпись" in c or "наименование объекта" in c or "содержание изменения" in c
        for c in normed
    ):
        return False
    has_name = any("наименование" in c or "описание" in c for c in normed)
    has_qty = any(
        c.startswith("кол")
        or " кол" in c
        or "кол-во" in c
        or "площад" in c
        or "масса" in c
        or c.startswith("всего")
        for c in normed
    )
    return has_name and has_qty


def _header_index(header: list[str], *needles: str) -> int | None:
    normed = [_norm(c) for c in header]
    for needle in needles:
        for idx, cell in enumerate(normed):
            if needle in cell:
                return idx
    return None


def _canonical_unit(value: str) -> str:
    text = _norm(value).replace(".", "")
    text = text.replace("м²", "м2").replace("м³", "м3")
    if "пог" in text and "м" in text:
        return "пог.м"
    if text in {"шт", "штука", "штук"}:
        return "шт"
    if text.startswith("комп"):
        return "компл"
    if text in {"м2", "кв м", "м 2"}:
        return "м2"
    if text in {"м3", "куб м", "м 3"}:
        return "м3"
    if text in {"кг", "килограмм"}:
        return "кг"
    if text in {"т", "тонн", "тонна"}:
        return "т"
    if text == "м":
        return "м"
    return text


def _unit_from_header(value: str) -> str:
    text = _norm(value)
    if "пог" in text and "м" in text:
        return "пог.м"
    if "м2" in text or "м²" in value.lower() or "площад" in text:
        return "м2"
    if "м3" in text or "м³" in value.lower() or "куб" in text:
        return "м3"
    if "кг" in text or "масса" in text:
        return "кг"
    if re.search(r"\bшт\b", text):
        return "шт"
    if "комп" in text:
        return "компл"
    if re.search(r"\bм\b", text):
        return "м"
    return ""


def _parse_qty(value: str) -> float:
    text = _clean_text(value).replace(" ", "").replace(",", ".")
    if not re.fullmatch(r"\d+(?:\.\d+)?", text):
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _parse_qty_with_unit(value: str) -> tuple[float, str]:
    text = _clean_text(value).replace(",", ".")
    match = re.search(
        r"(\d+(?:\.\d+)?)\s*(пог\.?\s*м|м²|м2|м³|м3|куб\.?\s*м|шт\.?|кг|т)\b",
        text,
        flags=re.I,
    )
    if not match:
        return 0.0, ""
    qty = _parse_qty(match.group(1))
    unit = _canonical_unit(match.group(2).replace("куб", "м3"))
    return qty, unit


def _extract_qty_unit_from_cells(cells: list[str]) -> tuple[float, str]:
    for cell in reversed(cells):
        qty, unit = _parse_qty_with_unit(cell)
        if qty > 0 and unit in _UNIT_THRESHOLDS:
            return qty, unit
    return 0.0, ""


def _cell(cells: list[str], idx: int | None) -> str:
    if idx is None or idx < 0 or idx >= len(cells):
        return ""
    return _clean_text(cells[idx])


def _extract_spec_rows(md_text: str) -> list[_SpecRow]:
    rows: list[_SpecRow] = []
    for page in _split_pages(md_text):
        header: list[str] | None = None
        indexes: dict[str, int | None] = {}
        for line in page.text.splitlines():
            cells = _split_table_cells(line)
            if not cells or _is_separator_row(cells):
                continue
            if _looks_like_spec_header(cells):
                header = cells
                indexes = {
                    "pos": _header_index(header, "поз"),
                    "name": _header_index(header, "наименование", "описание"),
                    "model": _header_index(header, "тип", "марка", "обозначение"),
                    "code": _header_index(header, "код"),
                    "supplier": _header_index(header, "поставщик", "производитель", "изготовитель"),
                    "unit": _header_index(header, "ед"),
                    "qty": _header_index(header, "кол", "площад", "масса", "всего"),
                }
                continue
            if header is None or _is_numbering_row(cells):
                continue

            name = _cell(cells, indexes.get("name"))
            unit = _canonical_unit(_cell(cells, indexes.get("unit")))
            qty = _parse_qty(_cell(cells, indexes.get("qty")))
            if not unit and indexes.get("qty") is not None:
                unit = _unit_from_header(header[indexes["qty"] or 0])
            if qty <= 0:
                embedded_qty, embedded_unit = _parse_qty_with_unit(_cell(cells, indexes.get("qty")))
                if embedded_qty > 0:
                    qty, unit = embedded_qty, embedded_unit or unit
            if qty <= 0 or unit not in _UNIT_THRESHOLDS:
                embedded_qty, embedded_unit = _extract_qty_unit_from_cells(cells)
                if embedded_qty > 0:
                    qty, unit = embedded_qty, embedded_unit
            if not name or qty <= 0 or unit not in _UNIT_THRESHOLDS:
                continue
            if len(_norm(name)) < 8:
                continue
            rows.append(
                _SpecRow(
                    page=page.page,
                    sheet=page.sheet,
                    pos=_cell(cells, indexes.get("pos")),
                    name=name,
                    model=_cell(cells, indexes.get("model")),
                    code=_cell(cells, indexes.get("code")),
                    supplier=_cell(cells, indexes.get("supplier")),
                    unit=unit,
                    qty=qty,
                )
            )
    return rows


def _extract_finding_blockers(findings_data: Any) -> list[dict[str, Any]]:
    if isinstance(findings_data, str):
        try:
            findings_data = json.loads(findings_data)
        except Exception:
            findings_data = None
    if isinstance(findings_data, dict):
        items = findings_data.get("findings") or findings_data.get("items") or []
    elif isinstance(findings_data, list):
        items = findings_data
    else:
        items = []

    result: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        severity = _clean_text(item.get("severity")).upper()
        if severity not in _CRITICAL_FINDING_SEVERITIES:
            continue
        problem = _clean_text(item.get("problem") or item.get("finding") or item.get("description"))
        raw_page = item.get("page") or 0
        if isinstance(raw_page, list):
            raw_page = next((p for p in raw_page if p), 0)
        try:
            page = int(raw_page or 0)
        except (TypeError, ValueError):
            page = 0
        result.append({
            "id": _clean_text(item.get("id") or "finding"),
            "severity": severity,
            "page": page,
            "text": problem,
            "norm_text": _norm(problem),
        })
    return result


def _row_blockers(row: _SpecRow, blockers: list[dict[str, Any]]) -> list[str]:
    row_norm = _norm(row.text)
    result: list[str] = []
    row_tokens = {t for t in row_norm.split() if len(t) >= 5}
    for blocker in blockers:
        same_page = row.page and int(blocker.get("page") or 0) == row.page
        blocker_tokens = {t for t in _clean_text(blocker.get("norm_text")).split() if len(t) >= 5}
        overlap = len(row_tokens & blocker_tokens)
        if same_page or overlap >= 2:
            result.append(f"{blocker.get('id')}:{blocker.get('severity')}")
    return result[:4]


def _finding_type(text: str) -> str:
    norm = _norm(text)
    if any(token in norm for token in (
        "огнестой", "пожар", "корроз", "долговеч", "ремонтопригод",
        "эксплуатац", "обслужив", "ресурс",
    )):
        return "lifecycle"
    if any(token in norm for token in (
        "монтаж", "свар", "вязк", "нахлест", "нахлест", "опалуб",
        "проход", "гильз", "муфт", "каркас", "бетонирован",
    )):
        return "faster_install"
    if any(token in norm for token in (
        "расхожд", "не совпад", "неоднознач", "маркиров", "унифиц",
        "типоразмер", "дублир", "количеств", "спецификац",
    )):
        return "simpler_design"
    if any(token in norm for token in (
        "бетон", "морозостой", "водонепрониц", "плотност", "псб",
        "пенополистирол", "бренд", "импорт", "дорог",
    )):
        return "cheaper_analog"
    return "simpler_design"


def _finding_score(text: str, severity: str) -> int:
    norm = _norm(text)
    score = 62
    if severity == "ЭКОНОМИЧЕСКОЕ":
        score += 18
    if severity == "КРИТИЧЕСКОЕ":
        score += 10
    if re.search(r"\b\d+[.,]?\d*\s*(шт|м3|м2|м\.п|мм|этаж|%)\b", norm):
        score += 10
    if any(token in norm for token in (
        "повтор", "массов", "десятк", "суммар", "типов", "типоразмер",
        "унифиц", "спецификац", "ведомост",
    )):
        score += 8
    return min(100, score)


def _extract_finding_candidates(findings_data: Any) -> list[OptimizationOpportunity]:
    blockers = _extract_finding_blockers(findings_data)
    candidates: list[OptimizationOpportunity] = []
    seen_topics: set[str] = set()
    for blocker in blockers:
        text = _clean_text(blocker.get("text"))
        if len(text) < 80:
            continue
        norm = _norm(text)
        topic_tokens = [
            token for token in norm.split()
            if len(token) >= 6 and token not in _STOPWORDS
        ][:12]
        topic_key = " ".join(topic_tokens[:8])
        if topic_key in seen_topics:
            continue
        seen_topics.add(topic_key)

        page = int(blocker.get("page") or 0)
        severity = _clean_text(blocker.get("severity")).upper()
        finding_id = _clean_text(blocker.get("id") or "finding")
        candidates.append(
            OptimizationOpportunity(
                candidate_id="OPT-PRE-FINDING",
                type=_finding_type(text),
                lens="audit_finding",
                title=f"Проверить оптимизацию по замечанию {finding_id}: {_truncate(text, 150)}",
                why=(
                    "03_findings уже зафиксировал проблему; в baseline Claude такие темы часто становятся "
                    "отдельными OPT-пунктами: обязательное исправление, унификация, ускорение монтажа "
                    "или оптимизация жизненного цикла."
                ),
                spec_items=[],
                related_pages=[page] if page else [],
                related_sheets=[],
                evidence=[_truncate(text, 220)],
                blockers=[f"{finding_id}:{severity}"],
                confidence="medium",
                opportunity_score=_finding_score(text, severity),
            )
        )
    candidates.sort(key=lambda item: item.opportunity_score, reverse=True)
    return candidates


def _matches_discipline(row: _SpecRow, section: str) -> bool:
    terms = _DISCIPLINE_KEYWORDS.get((section or "").upper(), ())
    text = _norm(row.text)
    return bool(terms and any(term in text for term in terms))


def _row_type(row: _SpecRow) -> str:
    text = _norm(row.text)
    if any(keyword in text for keyword in _MOUNTING_KEYWORDS):
        return "faster_install"
    if any(keyword in text for keyword in _LIFECYCLE_KEYWORDS):
        return "lifecycle"
    return "cheaper_analog"


def _row_score(row: _SpecRow, section: str, vendor_list_text: str, blockers: list[str]) -> int:
    threshold = _UNIT_THRESHOLDS.get(row.unit, 20.0)
    ratio = min(4.0, row.qty / threshold)
    score = 25 + int(ratio * 15)
    if row.supplier:
        score += 8
    if vendor_list_text and "ограничений заказчика по вендорам нет" not in vendor_list_text.lower():
        score += 6
    if _matches_discipline(row, section):
        score += 12
    elif row.unit == "м2":
        score -= 18
    if _row_type(row) == "faster_install":
        score += 8
    if blockers:
        score -= 12
    return max(0, min(100, score))


def _looks_like_room_area(row: _SpecRow) -> bool:
    if row.unit != "м2":
        return False
    text = _norm(row.name)
    if any(keyword in text for keyword in _AREA_MATERIAL_KEYWORDS):
        return False
    if any(keyword in text for keyword in _ROOM_AREA_WORDS):
        return True
    tokens = [t for t in text.split() if len(t) > 2 and t not in _STOPWORDS]
    return len(tokens) <= 3 and not row.model and not row.supplier


def _family_key(row: _SpecRow) -> str:
    text = _norm(row.name)
    text = re.sub(r"\b(?:dn|ду|d|l|h|b)\s*\d+(?:[.,]\d+)?\b", " ", text)
    text = re.sub(r"\b\d+(?:[.,]\d+)?(?:x\d+(?:[.,]\d+)?)*\b", " ", text)
    text = re.sub(r"\b(?:мм|м|шт|тип|гост|ту|pn|в|квт)\b", " ", text)
    tokens = [t for t in text.split() if len(t) >= 4 and t not in _STOPWORDS]
    return " ".join(tokens[:5])


def _family_type(rows: list[_SpecRow]) -> str:
    text = _norm(" ".join(row.text for row in rows[:8]))
    if any(keyword in text for keyword in _MOUNTING_KEYWORDS):
        return "faster_install"
    return "simpler_design"


def _make_row_candidate(
    *,
    candidate_id: str,
    row: _SpecRow,
    section: str,
    vendor_list_text: str,
    blockers: list[str],
) -> OptimizationOpportunity:
    item_type = _row_type(row)
    score = _row_score(row, section, vendor_list_text, blockers)
    title = f"Проверить оптимизацию крупной позиции: {row.spec_item}"
    if item_type == "faster_install":
        why = "Большой объём монтажно-чувствительной позиции: стоит проверить сборные/быстромонтируемые решения и унификацию крепежа."
    elif item_type == "lifecycle":
        why = "Позиция влияет на эксплуатационные затраты или ремонтопригодность; стоит проверить более надёжное/энергоэффективное решение."
    else:
        why = (
            "Крупная спецификационная позиция с поставщиком/маркой: стоит проверить допустимый аналог "
            "по вендор-листу и закупочную альтернативу. Если точных цен нет, не отбрасывай кандидат: "
            "ставь savings_pct=0 и savings_basis='экспертная оценка' или 'не определено'."
        )
    supplier = f", поставщик: {row.supplier}" if row.supplier else ""
    return OptimizationOpportunity(
        candidate_id=candidate_id,
        type=item_type,
        lens="large_spec_position",
        title=title,
        why=why,
        spec_items=[row.spec_item],
        related_pages=[row.page] if row.page else [],
        related_sheets=[row.sheet] if row.sheet else [],
        evidence=[f"{row.unit} {row.qty:g}{supplier}; {row.model}".strip("; ")],
        blockers=blockers,
        confidence="high" if score >= 75 and not blockers else "medium",
        opportunity_score=score,
    )


def _make_family_candidate(
    *,
    candidate_id: str,
    key: str,
    rows: list[_SpecRow],
    blockers: list[str],
) -> OptimizationOpportunity:
    total_qty = sum(row.qty for row in rows)
    pages = sorted({row.page for row in rows if row.page})[:8]
    sheets = sorted({row.sheet for row in rows if row.sheet})[:8]
    item_type = _family_type(rows)
    sample_items = [row.spec_item for row in rows[:8]]
    score = min(100, 38 + len(rows) * 6 + int(min(total_qty, 500) / 12))
    if blockers:
        score = max(0, score - 10)
    title = f"Проверить унификацию повторяющегося семейства: {key}"
    why = (
        f"В спецификации найдено {len(rows)} близких позиций, суммарно {total_qty:g} "
        "ед.; это типичный источник экономии у Claude: унификация типоразмеров, модульная закупка или заводская комплектация."
    )
    return OptimizationOpportunity(
        candidate_id=candidate_id,
        type=item_type,
        lens="repeatable_spec_family",
        title=title,
        why=why,
        spec_items=sample_items,
        related_pages=pages,
        related_sheets=sheets,
        evidence=[f"семейство: {key}; позиций: {len(rows)}; количество: {total_qty:g}"],
        blockers=blockers,
        confidence="high" if len(rows) >= 5 and total_qty >= 50 and not blockers else "medium",
        opportunity_score=score,
    )


def scan_optimization_opportunities(
    md_text: str,
    *,
    section: str = "",
    vendor_list_text: str = "",
    findings_data: Any = None,
    max_candidates: int = 24,
) -> list[OptimizationOpportunity]:
    rows = _extract_spec_rows(md_text)
    blockers = _extract_finding_blockers(findings_data)
    finding_candidates = _extract_finding_candidates(findings_data)

    candidates: list[OptimizationOpportunity] = []
    next_id = 1

    scored_rows = []
    for row in rows:
        if _looks_like_room_area(row):
            continue
        row_blockers = _row_blockers(row, blockers)
        score = _row_score(row, section, vendor_list_text, row_blockers)
        if score < 48:
            continue
        scored_rows.append((score, row, row_blockers))

    row_candidates: list[OptimizationOpportunity] = []
    for _, row, row_blockers in sorted(scored_rows, key=lambda x: x[0], reverse=True)[:14]:
        row_candidates.append(
            _make_row_candidate(
                candidate_id=f"OPT-PRE-{next_id:03d}",
                row=row,
                section=section,
                vendor_list_text=vendor_list_text,
                blockers=row_blockers,
            )
        )
        next_id += 1

    families: dict[str, list[_SpecRow]] = defaultdict(list)
    for row in rows:
        if _looks_like_room_area(row):
            continue
        key = _family_key(row)
        if len(key) >= 8:
            families[key].append(row)

    family_candidates: list[OptimizationOpportunity] = []
    for key, family_rows in families.items():
        if len(family_rows) < 4:
            continue
        total_qty = sum(row.qty for row in family_rows)
        if total_qty < 20:
            continue
        family_blockers: list[str] = []
        for row in family_rows[:4]:
            for blocker in _row_blockers(row, blockers):
                if blocker not in family_blockers:
                    family_blockers.append(blocker)
        family_candidates.append(
            _make_family_candidate(
                candidate_id=f"OPT-PRE-{next_id + len(family_candidates):03d}",
                key=key,
                rows=family_rows,
                blockers=family_blockers[:4],
            )
        )

    family_candidates.sort(key=lambda item: item.opportunity_score, reverse=True)
    for candidate in family_candidates[:10]:
        candidate.candidate_id = f"OPT-PRE-{next_id:03d}"
        candidates.append(candidate)
        next_id += 1

    # Keep the prompt balanced. Otherwise a long specification with many
    # high-quantity rows hides repeated families, while family unification is
    # one of the strongest optimization patterns in the Claude baselines.
    candidates = (
        row_candidates[:8]
        + candidates[:8]
        + finding_candidates[:8]
        + row_candidates[8:]
        + candidates[8:]
        + finding_candidates[8:]
    )

    deduped: list[OptimizationOpportunity] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = _norm(candidate.lens + " " + candidate.title)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)

    for idx, candidate in enumerate(deduped[:max_candidates], start=1):
        candidate.candidate_id = f"OPT-PRE-{idx:03d}"
    return deduped[:max_candidates]


def _format_candidate(candidate: OptimizationOpportunity, idx: int) -> list[str]:
    pages = ", ".join(str(p) for p in candidate.related_pages) or "?"
    sheets = ", ".join(candidate.related_sheets) or "?"
    lines = [
        f"{idx}. [{candidate.type}, {candidate.confidence}, score={candidate.opportunity_score}] {candidate.title}",
        f"   - Привязка: PDF стр. {pages}; лист {sheets}",
        f"   - Почему проверить: {candidate.why}",
    ]
    if candidate.spec_items:
        lines.append("   - spec_items: " + "; ".join(candidate.spec_items[:6]))
    if candidate.evidence:
        lines.append("   - Сигналы: " + "; ".join(_truncate(e, 160) for e in candidate.evidence[:3]))
    if candidate.blockers:
        lines.append(
            "   - Ограничение: есть связанные критические/экономические замечания "
            + ", ".join(candidate.blockers)
            + "; не предлагай дешёвый аналог без нормативной проверки."
        )
    return lines


def build_optimization_prescan_section_from_text(
    md_text: str,
    *,
    section: str = "",
    vendor_list_text: str = "",
    findings_data: Any = None,
    max_candidates: int = 16,
) -> str:
    candidates = scan_optimization_opportunities(
        md_text,
        section=section,
        vendor_list_text=vendor_list_text,
        findings_data=findings_data,
        max_candidates=max_candidates,
    )
    if not candidates:
        return ""

    unblocked = [candidate for candidate in candidates if not candidate.blockers]
    cheaper_candidates = [
        candidate for candidate in candidates
        if candidate.type == "cheaper_analog"
    ]
    cheaper_unblocked = [
        candidate for candidate in unblocked
        if candidate.type == "cheaper_analog"
    ]
    min_expected = min(12, len(candidates))
    min_cheaper = min(6, len(cheaper_candidates))

    lines = [
        "## Автопрескан оптимизаций из MD (обязательно проверить)",
        "",
        "Это НЕ готовые OPT-пункты и НЕ замена инженерному анализу. Это короткий список мест, где по локальной статистике Claude чаще всего находит сильные оптимизации. Проверь каждый кандидат по MD, 03_findings.json и вендор-листу: подтверждённые включи в `optimization.json`, неподтверждённые отбрось.",
        "",
        "Критично для Codex: не своди оптимизацию только к обязательным исправлениям из 03_findings. Кандидаты `cheaper_analog` без blockers нужно рассматривать как самостоятельные экономические предложения. Отсутствие прайс-листа НЕ является причиной пропустить пункт: укажи `savings_pct: 0`, а в `savings_basis` — `экспертная оценка` или `не определено`.",
        "",
        "Если в списке ниже есть премиальный/импортный/индивидуальный материал, крупная площадь/количество или поставщик без жёсткого требования заказчика, проверь более массовый/локальный/унифицированный аналог. Не объединяй разные спецификационные позиции в одно общее обязательное исправление, если это разные источники экономии.",
        "",
        (
            f"Контроль полноты для этого документа: найдено {len(candidates)} кандидатов, "
            f"из них без blockers {len(unblocked)}, cheaper_analog без blockers {len(cheaper_unblocked)}. "
            f"Если MD подтверждает позиции, итоговый `optimization.json` обычно должен содержать около "
            f"{min_expected} пунктов и до {min_cheaper} cheaper_analog. Blockers не являются разрешением "
            "выкинуть кандидата: они означают, что дешёвый аналог нужно заменить на обязательное исправление, "
            "faster_install/simpler_design/lifecycle или пометить `требует проверки`. Если у тебя получается 3-5 пунктов, "
            "это почти наверняка неполный анализ: вернись к списку ниже и продолжи проверку крупных позиций."
        ),
        "",
    ]
    for idx, candidate in enumerate(candidates[:max_candidates], start=1):
        lines.extend(_format_candidate(candidate, idx))
    lines.extend([
        "",
        "Правило: после проверки этого списка продолжи самостоятельный поиск по спецификациям. Не увеличивай количество OPT-пунктов искусственно; лучше меньше, но с конкретными `spec_items`, страницей, листом и честным `savings_basis`.",
    ])
    return "\n".join(lines)


def build_optimization_prescan_section(
    md_file_path: str | Path,
    *,
    section: str = "",
    vendor_list_text: str = "",
    findings_path: str | Path | None = None,
    max_candidates: int = 16,
) -> str:
    path = Path(md_file_path)
    if not path.exists():
        return ""
    try:
        md_text = path.read_text(encoding="utf-8")
    except Exception:
        return ""

    findings_data: Any = None
    if findings_path:
        fpath = Path(findings_path)
        if fpath.exists():
            try:
                findings_data = json.loads(fpath.read_text(encoding="utf-8"))
            except Exception:
                findings_data = None

    return build_optimization_prescan_section_from_text(
        md_text,
        section=section,
        vendor_list_text=vendor_list_text,
        findings_data=findings_data,
        max_candidates=max_candidates,
    )
