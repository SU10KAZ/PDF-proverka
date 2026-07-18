"""Codex targeted add-on passes for classic findings_merge.

These prompts are deliberately inline-only: the backend reads the project
artifacts and sends focused snippets to Codex JSON mode. Codex does not need
filesystem access for these add-on passes.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.app.pipeline.stages.prepare.prompt_builder import (
    _get_md_file_path,
    _version_output_dir,
)
from backend.app.services.common.results_md import (
    is_results_md_name,
    is_results_md_text,
)


@dataclass(frozen=True)
class CodexTargetedPass:
    stage: str
    output_filename: str
    messages: list[dict[str, str]]


def _canonical_finding_key(item: dict[str, Any]) -> str:
    text = item.get("problem") or item.get("finding") or item.get("description") or ""
    text = str(text).lower().replace("ё", "е")
    return re.sub(r"\s+", " ", text).strip()


TARGETED_KEYWORDS = (
    "ведомость материалов",
    "кладоч",
    "перемыч",
    "масса",
    "итого",
    "площад",
    "объем",
    "объём",
    "rei",
    "ei ",
    "огнестой",
    "лифтов",
    "простен",
    "уголок",
    "отверст",
    "проем",
    "проём",
    "sb-",
    "cb-",
    "св-",
    "ch-",
    "сн-",
    "pr-",
    "пр-",
)

DOCNORM_KEYWORDS = (
    "ведомость основных комплектов",
    "ведомость ссылочных документов",
    "общие указания",
    "заказчик",
    "адрес",
    "шифр",
    "13ab",
    "13ав",
    "рд",
    "гост",
    "сп ",
    "фз",
    "фэ",
    "123",
    "28013",
    "58766",
    "53299",
    "53306",
    "53310",
    "53301",
    "актуаль",
    "ссылоч",
    "норматив",
    "редакц",
)

_NORM_CODE_RE = re.compile(
    r"\b(?:ГОСТ(?:\s+Р)?|СП|СНиП|СанПиН)\s*"
    r"\d+(?:[._]\d+)*(?:[-–]\d{2,4})?\b",
    re.IGNORECASE,
)
_MARK_TOKEN_RE = re.compile(
    r"(?<![\w])(?:[A-ZА-ЯЁ]{1,4})[- ]?\d+(?:\.\d+)*(?![\w])",
    re.IGNORECASE,
)
_MARK_ENTITY_WORDS = (
    "марка",
    "двер",
    "проем",
    "проём",
    "светиль",
    "сантех",
    "унитаз",
    "умываль",
    "смесител",
    "мебел",
    "решет",
    "решёт",
    "покрыт",
    "плинтус",
    "профил",
    "люк",
    "отделк",
)

EOM_KEYWORDS = (
    "qf",
    "автомат",
    "ва-",
    "c10",
    "c16",
    "c25",
    "c32",
    "iкз",
    "iкз(1)",
    "икз",
    "кз",
    "in",
    "iр",
    "ip",
    "расчетный ток",
    "расчётный ток",
    "ппг",
    "frhf",
    "нг(а)",
    "линия",
    "уэрв",
    "ивэпр",
    "кровля",
    "лотк",
    "окл",
    "огнезащит",
    "короб",
    "вру",
    "рп",
    "шсоэ",
    "шсоуэ",
)

SS_KEYWORDS = (
    "сп 134",
    "134.13330",
    "51241",
    "скуд",
    "сов",
    "оспд",
    "cat5e",
    "utp",
    "u/utp",
    "90 м",
    "rs-485",
    "120 ом",
    "акб",
    "а·ч",
    "ач",
    "ибп",
    "токопотреб",
    "коэффициент старения",
    "вызыв",
    "контроллер",
    "домофон",
    "турникет",
    "точк",
    "доступ",
    "калит",
    "стойк",
    "800",
    "850",
    "1 м",
    "не менее",
)

KM_KEYWORDS = (
    "сп 10",
    "сп10",
    "сп 16",
    "16.13330",
    "стальные конструкции",
    "болт",
    "гайк",
    "класс прочности",
    "8.8",
    "10.9",
    "гост 7798",
    "гост 5915",
    "металлопрокат",
    "масса",
    "с245",
    "ст3сп",
    "лжд",
    "кол.",
    "масса общая",
    "анк",
    "м10",
    "м16",
    "8509",
    "19903",
    "28,363",
    "30,351",
    "18,147",
    "12,204",
    "см3сп",
)


def _brief_project_label(project_info: dict[str, Any], project_id: str) -> str:
    section = project_info.get("section") or project_info.get("discipline") or ""
    name = project_info.get("name") or project_info.get("project_id") or project_id
    return f"{section}/{name}".strip("/")


def _read_existing_findings(project_id: str, *, max_chars: int = 50000) -> str:
    path = _version_output_dir(project_id) / "03_findings.json"
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:max_chars]
    except OSError:
        return '{"findings":[]}'


def _keyword_context(
    md_path: Path,
    keywords: tuple[str, ...],
    *,
    window: int = 6,
    max_chars: int = 70000,
) -> str:
    lines = md_path.read_text(encoding="utf-8", errors="replace").splitlines()
    selected: set[int] = set()
    for idx, line in enumerate(lines):
        low = line.lower()
        if any(keyword in low for keyword in keywords):
            selected.update(range(max(0, idx - window), min(len(lines), idx + window + 1)))
    if not selected:
        selected = set(range(min(len(lines), 300)))

    chunks: list[tuple[int, int]] = []
    for idx in sorted(selected):
        if not chunks or idx > chunks[-1][1] + 1:
            chunks.append((idx, idx))
        else:
            chunks[-1] = (chunks[-1][0], idx)

    parts: list[str] = []
    total = 0
    for start, end in chunks:
        block_lines = [f"[lines {start + 1}-{end + 1}]"]
        block_lines.extend(f"{line_no + 1}: {lines[line_no]}" for line_no in range(start, end + 1))
        block = "\n".join(block_lines)
        if total + len(block) > max_chars and parts:
            parts.append(f"[truncated: targeted context limited to {max_chars} chars]")
            break
        parts.append(block)
        total += len(block)
    return "\n\n".join(parts)


def _norm_signature(value: str) -> str:
    """Сопоставить OCR-варианты `21.501.2011` и `21.501-2011`."""
    normalized = value.upper().replace("Ё", "Е")
    return re.sub(r"[^A-ZА-Я0-9]", "", normalized)


@lru_cache(maxsize=1)
def _norm_status_rows() -> tuple[tuple[str, dict[str, Any]], ...]:
    """Загрузить локально проверенные статусы норм для документного аудита.

    `norms_db.json` содержит ручные/внешне проверенные overrides, которых может
    ещё не быть в автоматически построенном status_index. При недоступном файле
    targeted pass продолжает работать без подсказок и ничего не выдумывает.
    """
    repo_root = Path(__file__).resolve().parents[5]
    path = repo_root / "norms" / "norms_db.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ()
    rows = payload.get("norms") if isinstance(payload, dict) else None
    if not isinstance(rows, dict):
        return ()
    return tuple(
        (str(code), data)
        for code, data in rows.items()
        if isinstance(data, dict)
    )


def _norm_status_hints(context: str) -> str:
    """Дать Codex статусы именно тех нормативов, которые видны в документе."""
    cited: list[str] = []
    for match in _NORM_CODE_RE.finditer(context):
        code = re.sub(r"\s+", " ", match.group(0)).strip()
        if code not in cited:
            cited.append(code)

    hints: list[str] = []
    rows = _norm_status_rows()
    for cited_code in cited:
        signature = _norm_signature(cited_code)
        matches = [
            (code, data)
            for code, data in rows
            if _norm_signature(code).startswith(signature)
        ]
        if not matches:
            continue
        code, data = min(matches, key=lambda row: len(_norm_signature(row[0])))
        status = data.get("status") or data.get("edition_status") or "unknown"
        replacement = data.get("replacement_doc") or data.get("current_version") or ""
        verified = data.get("last_verified") or data.get("last_checked") or ""
        note = str(data.get("notes") or "").replace("\n", " ")[:500]
        hints.append(
            f"- cited={cited_code}; catalog={code}; status={status}; "
            f"replacement={replacement or '-'}; verified={verified or '-'}; note={note or '-'}"
        )
    return "\n".join(hints) if hints else "- Статусных подсказок для найденных обозначений нет."


def _mark_system_context(md_path: Path, *, max_chars: int = 100000) -> str:
    """Собрать первичные строки для глобальной проверки системы обозначений.

    Производные `Summary/Description/Entities` обычно отбрасываются. Исключение
    — строки с D/Д/СП из image blocks: без них теряется графическая форма марки
    на плане. Они явно помечаются как derived и должны подтверждаться первичной
    строкой спецификации/легенды. Page/block refs сохраняются для каждой строки.
    """
    lines = md_path.read_text(encoding="utf-8", errors="replace").splitlines()
    page: int | None = None
    block_id: str | None = None
    selected: list[str] = []
    total = 0
    for idx, line in enumerate(lines):
        page_match = re.match(r"^##\s+(?:Page|СТРАНИЦА)\s+(\d+)", line, re.IGNORECASE)
        if page_match:
            page = int(page_match.group(1))
        block_match = re.match(r"^###\s+BLOCK\s+#[^:]+:\s*(\S+)", line, re.IGNORECASE)
        if block_match:
            block_id = block_match.group(1)

        stripped = line.strip()
        low = stripped.lower()
        is_derived = low.startswith(("**summary:**", "**description:**", "**entities:**"))
        if low.startswith("**verification:**"):
            continue
        has_mark = bool(_MARK_TOKEN_RE.search(stripped))
        is_abbreviation = "аббревиат" in low or ("марка" in low and has_mark)
        is_primary_table_row = stripped.startswith("|") and has_mark and any(
            word in low for word in _MARK_ENTITY_WORDS
        )
        # D/Д и частые кириллические префиксы нужны даже в строках таблицы без
        # слов классификации: класс изделия читается из остальных колонок.
        has_confusable_or_prefix = bool(
            re.search(r"(?<![\w])(?:D|Д|СП)[- ]?\d", stripped, re.IGNORECASE)
        )
        if is_derived and not has_confusable_or_prefix:
            continue
        if not (is_abbreviation or is_primary_table_row or (stripped.startswith("|") and has_confusable_or_prefix)):
            if not (is_derived and has_confusable_or_prefix):
                continue

        compact = stripped[:3500]
        rendered = (
            f"[line {idx + 1}; page {page if page is not None else '?'}; "
            f"block {block_id or '?'}; evidence_kind "
            f"{'derived_image_description' if is_derived else 'primary_text'}] {compact}"
        )
        if total + len(rendered) > max_chars:
            selected.append(f"[truncated: mark-system context limited to {max_chars} chars]")
            break
        selected.append(rendered)
        total += len(rendered)
    return "\n".join(selected)


def _md_is_results_format(md_path: Path) -> bool:
    """Новый ли это формат портала (*_results.md)?

    Детект по имени файла и по тексту через единый парсер results_md;
    старый Chandra-формат («## СТРАНИЦА N») не матчится никогда.
    """
    if is_results_md_name(md_path.name):
        return True
    try:
        return is_results_md_text(md_path.read_text(encoding="utf-8", errors="replace"))
    except OSError:
        return False


def _json_user(
    project_info: dict[str, Any],
    project_id: str,
    title: str,
    context: str,
    *,
    results_md: bool = False,
) -> str:
    existing = _read_existing_findings(project_id)
    # Заголовок страницы в MD-контексте: новый формат портала — «## Page N»
    # (номер страницы PDF), старый Chandra — «## СТРАНИЦА N». Для старого
    # формата подстановка воспроизводит прежний текст промпта байт-в-байт.
    page_header = "## Page N" if results_md else "## СТРАНИЦА N"
    return f"""
Проект: {_brief_project_label(project_info, project_id)}

## Уже найденные Codex findings
{existing}

## {title} MD context with line numbers
{context}

Верни JSON:
{{
  "findings": [
    {{
      "id": "TGT-001",
      "severity": "КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ",
      "category": "documentation|normative_refs|fire_safety|coordination|dimensions|other",
      "problem": "краткая суть",
      "description": "доказательство с конкретными числами/строками MD",
      "page": "номер страницы PDF (число) по ближайшему заголовку «{page_header}» выше цитируемых строк, иначе null",
      "norm": "",
      "norm_quote": null,
      "solution": "что исправить",
      "risk": "риск",
      "evidence": [
        {{"type": "text", "block_id": null, "page": "номер страницы PDF (число) или null", "md_lines": "строки N-M"}}
      ]
    }}
  ]
}}
""".strip()


def _ar_messages(
    project_info: dict[str, Any],
    project_id: str,
    md_path: Path,
    *,
    results_md: bool = False,
) -> list[dict[str, str]]:
    context = _keyword_context(md_path, TARGETED_KEYWORDS)
    system = """
Ты выполняешь дополнительный targeted audit для раздела АР: кладочные планы, ведомости материалов,
перемычки, отверстия/проёмы и специальные противопожарные элементы.

Не используй Claude baseline. Ниже только MD-контекст и уже найденные Codex findings.

Обязательные проверки:
1. Сравни одинаковые/типовые этажи по ведомостям материалов кладочных стен. Если одна и та же марка
   стены/перегородки имеет разные объёмы/площади между этажами с заявленной одинаковой геометрией,
   выдай отдельное замечание.
2. Сверь поэтажные ведомости со сводной ведомостью.
3. В спецификациях перемычек пересчитай `Итого × Масса ед.` и найди нулевые массы физических изделий.
4. Сверь марки/позиции перемычек между схемами, планами и спецификациями.
5. Найди повторное использование одной марки отверстия/проёма для разных размеров/назначений.
6. Проверь общие указания со специальным REI/EI, усилением, обрамлением уголками, простенками
   у лифтового холла: выделена ли конструкция в спецификациях/марках/ведомостях.

Верни только новые/недублирующие или уточняющие замечания. Строго один JSON object без Markdown.
""".strip()
    return [{"role": "system", "content": system}, {"role": "user", "content": _json_user(project_info, project_id, "AR targeted", context, results_md=results_md)}]


def _eom_messages(
    project_info: dict[str, Any],
    project_id: str,
    md_path: Path,
    *,
    results_md: bool = False,
) -> list[dict[str, str]]:
    context = _keyword_context(md_path, EOM_KEYWORDS)
    system = """
Ты выполняешь targeted audit для ЭОМ/ЭМ: защитные аппараты, токи КЗ, расчетные токи,
кабельные линии СПЗ/ПЭСПЗ/ОКЛ и согласованность обозначений линий.

Не используй Claude baseline. Ниже только MD-контекст и уже найденные Codex findings.

Обязательные проверки:
1. Для каждой линии с автоматом C-характеристики и указанным Iкз в конце линии пересчитай кратность:
   `Iкз_А / In`. Если Iкз задан в кА, умножь на 1000. Для C-характеристики зона мгновенного
   расцепления 5-10 In; при кратности <= 10 гарантированное мгновенное отключение не подтверждено.
   Не объединяй разные группы QF/щитов/кратностей. Примеры отдельных групп: `QF5.2`,
   `QF3.29...QF4.14`, `QF4.11/QF4.12`, `QF5.1.1`.
2. Сверь расчетный ток нагрузки `Iр` с номиналом автомата.
3. Если таблица условных обозначений требует один номинал автомата, а на схеме указан другой,
   выдай отдельное замечание.
4. Для линий СПЗ/ПЭСПЗ/ОКЛ и огнезащитных коробов проверь предел огнестойкости, габариты,
   отметки прокладки и однозначность участка применения.
5. Сверь обозначения одной отходящей линии между планом, видом шкафа, спецификацией и схемой.

Верни только новые/недублирующие или уточняющие замечания. Строго один JSON object без Markdown.
""".strip()
    return [{"role": "system", "content": system}, {"role": "user", "content": _json_user(project_info, project_id, "EOM/EM targeted", context, results_md=results_md)}]


def _ss_messages(
    project_info: dict[str, Any],
    project_id: str,
    md_path: Path,
    *,
    results_md: bool = False,
) -> list[dict[str, str]]:
    context = _keyword_context(md_path, SS_KEYWORDS)
    system = """
Ты выполняешь targeted audit для слаботочных систем: СКУД/СОВ/ОСПД/домофония/СКС.

Не используй Claude baseline. Ниже только MD-контекст и уже найденные Codex findings.

Обязательные проверки:
1. Нормативная база: устаревший СП 134.13330.2012, отсутствие ГОСТ Р 51241-2008 для СКУД,
   дубли и конфликтующие редакции.
2. АКБ/ИБП: пересчитай емкость по формуле `I суммарный × t × коэффициент`.
3. СКС/Ethernet: для U/UTP Cat5e проверь подтверждение ограничения 90 м.
4. RS-485: если не показаны терминальные резисторы 120 Ом на концах линии, выдай отдельный
   standalone finding именно про оконечивание/терминирование RS-485.
5. Монтажные размеры и собственные примечания: для калиток, стоек, вызывных панелей и
   считывателей сравни числовые размеры на схеме с примечаниями на том же блоке/листе. Если
   схема показывает `800-850 мм`, а примечание требует `не менее 1 м`, верни отдельный finding
   именно про это внутреннее противоречие.
6. СКУД/домофония: проверь контроллеры, вызывные панели, электрозамки, разблокировку при пожаре
   и резервное питание.

Каждый независимый дефект должен быть отдельным finding: нормативная ссылка, АКБ, Cat5e/90м,
RS-485, монтажный размер калитки и разблокировка при пожаре не должны сливаться в один общий пункт.

Верни только новые/недублирующие или уточняющие замечания. Строго один JSON object без Markdown.
""".strip()
    return [{"role": "system", "content": system}, {"role": "user", "content": _json_user(project_info, project_id, "SS targeted", context, results_md=results_md)}]


def _km_messages(
    project_info: dict[str, Any],
    project_id: str,
    md_path: Path,
    *,
    results_md: bool = False,
) -> list[dict[str, str]]:
    context = _keyword_context(md_path, KM_KEYWORDS)
    system = """
Ты выполняешь targeted audit для КМ: нормы стальных конструкций, болты/гайки,
спецификации металлопроката, массы и сортаменты.

Не используй Claude baseline. Ниже только MD-контекст и уже найденные Codex findings.

Обязательные проверки:
1. Нормативные ссылки: СП 10.13330 не является нормой по стальным конструкциям; для КМ ожидается
   СП 16.13330. Если в общих указаниях перепутан номер СП, выдай критическое замечание.
2. Болты/гайки: класс прочности гайки должен соответствовать классу болта. Гайка класса 4 для
   болтов 8.8 является опасной нестыковкой.
3. Сводная спецификация металлопроката: пересчитай суммы по маркам стали и сравни с итоговой массой.
   Обязательно выпиши исходные строки и формулы, например:
   `С245 18.147 т + Ст3сп 12.204 т = 30.351 т`, но в таблице указан итог `28.363 т`;
   либо более точную найденную формулу по колонкам таблицы.
4. Проверь, что уголки/профили отнесены к правильному сортаменту ГОСТ, а не к листовому прокату.
5. Групповые спецификации: физические изделия не должны иметь нулевые `Кол.` и `Масса общая`;
   `Кол. × Масса ед.` должно равняться общей массе.
6. Анкеры должны иметь производителя/ТУ/ETA/сертификат, несущую способность и требования к основанию.

Верни только новые/недублирующие или уточняющие замечания. Строго один JSON object без Markdown.
""".strip()
    return [{"role": "system", "content": system}, {"role": "user", "content": _json_user(project_info, project_id, "KM targeted", context, results_md=results_md)}]


def _docnorm_messages(
    project_info: dict[str, Any],
    project_id: str,
    md_path: Path,
    *,
    results_md: bool = False,
) -> list[dict[str, str]]:
    context = _keyword_context(md_path, DOCNORM_KEYWORDS, window=5, max_chars=60000)
    status_hints = _norm_status_hints(context)
    system = """
Ты выполняешь ОБЯЗАТЕЛЬНЫЙ ИСЧЕРПЫВАЮЩИЙ аудит нормативной части РД.
Это глобальная проверка всего документа, а не локального фрагмента.

Не используй Claude baseline. Ниже только MD-контекст и уже найденные Codex findings.
Блок LOCAL VERIFIED NORM STATUS считается авторитетной локальной проверкой статуса.

Обязательные проверки:
1. Перебери КАЖДУЮ строку ведомости ссылочных документов. Нельзя ограничиваться первыми
   найденными устаревшими СП или общим замечанием «есть неактуальные редакции».
2. Для КАЖДОЙ реально процитированной нормы со статусом cancelled/obsolete/replaced в
   LOCAL VERIFIED NORM STATUS верни замечание, если точное обозначение ещё не названо в
   существующем finding. Отменённый стандарт нельзя потерять из-за наличия общего finding
   про другие устаревшие редакции.
3. Проверь тематическую применимость каждой строки к указанной дисциплине. Например, для
   интерьеров стандарты исключительно на изготовление конструкционного бетона или заполнителей
   не являются профильными, если в комплекте нет соответствующего проектного решения.
   Перечисли точные обозначения и названия нерелевантных строк. Стандарты на готовые стальные
   профили/уголки, крепёж и другие изделия могут быть нужны в интерьерных узлах: не объявляй их
   нерелевантными только потому, что их применение не попало в выбранное текстовое окно.
   Не придумывай отсутствующие нормы.
4. Отдельно проверь дубли, конфликтующие редакции, неверное название и явные ошибки обозначения.
5. В каждом finding процитируй точную строку MD и обозначение нормы. Если данных недостаточно,
   не публикуй замечание.

Перед ответом внутренне составь receipt: число строк в ведомости и исход для каждой строки.
Верни только новые/недублирующие доказанные замечания. Строго один JSON object без Markdown.
""".strip()
    audit_context = (
        "## LOCAL VERIFIED NORM STATUS\n"
        f"{status_hints}\n\n"
        "## DOCUMENT NORMATIVE CONTEXT\n"
        f"{context}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": _json_user(project_info, project_id, "Doc/norm exhaustive", audit_context, results_md=results_md)}]


def _mark_system_messages(
    project_info: dict[str, Any],
    project_id: str,
    md_path: Path,
    *,
    results_md: bool = False,
) -> list[dict[str, str]]:
    context = _mark_system_context(md_path)
    system = """
Ты выполняешь ОБЯЗАТЕЛЬНЫЙ глобальный аудит системы марок и обозначений по всему комплекту РД.
Нельзя проверять марки только внутри одного блока или листа.

Ниже даны первичные строки таблиц/легенд и явно помеченные derived_image_description
для графических планов с page, block и line refs. Существующие findings нужны только
для дедупликации.

Обязательные проверки:
1. Канонизируй визуально похожие кириллические и латинские буквы и найди одну серию изделий,
   записанную разными алфавитами на планах, в легендах и спецификациях (например D... против Д...).
   Разные числовые суффиксы сами по себе не являются ошибкой.
2. Построй отображение `префикс марки -> класс сущности`. Если один префикс объявлен для одного
   класса, но фактически используется для другого (например светильник и сантехническое изделие),
   это самостоятельная системная коллизия.
3. Для публикации нужны минимум две конкретные строки из разных таблиц/контекстов, причём
   минимум одна должна иметь evidence_kind=primary_text. Перечисли обе формы/класса и
   несколько точных примеров марок.
4. Не публикуй предположения, одиночные OCR-опечатки без повторения и различия только в дефисе.

Каждую независимую системную коллизию верни отдельным finding. Строго один JSON object без Markdown.
""".strip()
    return [
        {"role": "system", "content": system},
        {
            "role": "user",
            "content": _json_user(
                project_info,
                project_id,
                "Global mark-system audit",
                context,
                results_md=results_md,
            ),
        },
    ]


def build_targeted_findings_passes(project_info: dict[str, Any], project_id: str) -> list[CodexTargetedPass]:
    md_path = Path(_get_md_file_path(project_info, project_id))
    if not md_path.is_file():
        return []

    # Формат MD определяем один раз на все пассы: от него зависит подсказка
    # заголовка страницы («## Page N» vs «## СТРАНИЦА N») в JSON-инструкции.
    results_md = _md_is_results_format(md_path)

    section = str(project_info.get("section") or project_info.get("discipline") or "").upper()
    passes: list[CodexTargetedPass] = []
    discipline_builders = {
        "AR": ("alia_ar_masonry_audit", _ar_messages),
        "EOM": ("alia_eom_protection_audit", _eom_messages),
        "SS": ("alia_ss_lowcurrent_audit", _ss_messages),
        "KM": ("alia_km_steel_audit", _km_messages),
    }
    discipline = discipline_builders.get(section)
    if discipline:
        stage, builder = discipline
        passes.append(
            CodexTargetedPass(
                stage=stage,
                output_filename=f"03_findings_targeted_{stage}.json",
                messages=builder(project_info, project_id, md_path, results_md=results_md),
            )
        )

    passes.append(
        CodexTargetedPass(
            stage="alia_docnorm_audit",
            output_filename="03_findings_targeted_alia_docnorm_audit.json",
            messages=_docnorm_messages(project_info, project_id, md_path, results_md=results_md),
        )
    )
    passes.append(
        CodexTargetedPass(
            stage="alia_mark_system_audit",
            output_filename="03_findings_targeted_alia_mark_system_audit.json",
            messages=_mark_system_messages(project_info, project_id, md_path, results_md=results_md),
        )
    )
    return passes


def _coerce_page_int(value: Any) -> int | None:
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.strip().isdigit():
        page = int(value.strip())
        return page if page > 0 else None
    return None


def _ensure_text_evidence_refs(item: dict[str, Any]) -> None:
    """Синтезировать текстовые refs для Верификатора (детерминированного критика).

    Критик собирает refs ТОЛЬКО из evidence[].block_id / related_block_ids /
    source_block_ids / evidence_text_refs; targeted-схема (block_id=null, md_lines)
    давала пустые refs → безусловный no_evidence → корректор понижал КАЖДОЕ
    targeted-замечание в «ПРОВЕРИТЬ ПО СМЕЖНЫМ». Строим `page_<N>_text` из
    evidence[].page / page замечания (такой ref проходит block_exists через
    endswith("_text")). Без страницы refs не синтезируем — замечание честно
    уйдёт в no_evidence.
    """
    if (
        item.get("evidence_text_refs")
        or item.get("related_block_ids")
        or item.get("source_block_ids")
    ):
        return
    pages: list[int] = []
    for ev in item.get("evidence") or []:
        if isinstance(ev, dict):
            if ev.get("block_id"):
                return  # нормальный ref уже есть — критик его увидит
            page = _coerce_page_int(ev.get("page"))
            if page is not None:
                pages.append(page)
    page_top = _coerce_page_int(item.get("page"))
    if page_top is not None:
        pages.append(page_top)
    if pages:
        item["evidence_text_refs"] = [
            f"page_{page}_text" for page in dict.fromkeys(pages)
        ]


def _observe_pdf_symbol_evidence(
    findings: list[dict[str, Any]],
    output_dir: str | Path | None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Run the shared PDF-vector observer after targeted union (fail-soft)."""
    from backend.app.core import config as _config

    if (
        output_dir is None
        or not getattr(
            _config, "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED", False
        )
    ):
        return findings, None
    try:
        from backend.app.pipeline.stages.block_analysis.finding_evidence_gate import (
            observe_symbol_token_evidence,
        )
        from backend.app.pipeline.stages.block_grounding.block_source_router import (
            vector_text_block_index,
        )

        output_path = Path(output_dir)
        vector_sources = vector_text_block_index(output_path) or {}
        graph: dict[str, Any] = {}
        try:
            graph = json.loads(
                (output_path / "document_graph.json").read_text(encoding="utf-8")
            )
        except (OSError, UnicodeError, json.JSONDecodeError):
            graph = {}
        return observe_symbol_token_evidence(
            findings,
            vector_sources=vector_sources,
            document_graph=graph,
            enabled=True,
        )
    except Exception:
        return findings, {
            "schema_version": 1,
            "enabled": True,
            "observe_only": True,
            "candidates": len(findings),
            "vector_sources": 0,
            "annotated_findings": 0,
            "observation_counts": {},
            "errors": 1,
        }


def combine_findings_with_targeted(
    production: dict[str, Any],
    targeted_payloads: list[tuple[str, dict[str, Any]]],
    *,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    production_findings = production.get("findings") if isinstance(production, dict) else []
    if not isinstance(production_findings, list):
        production_findings = []

    combined = dict(production) if isinstance(production, dict) else {"findings": []}
    findings = [item for item in production_findings if isinstance(item, dict)]
    seen_keys = {_canonical_finding_key(item) for item in findings}
    seen_keys.discard("")
    # next_idx от МАКСИМАЛЬНОГО существующего номера, не от len(): база от LLM может
    # иметь пропуски в нумерации (F-001, F-003 при len=2) — len()+1 давал дубль F-ID,
    # а expert_review/decisions/фронтенд ключуются на F-ID.
    max_idx = 0
    for item in findings:
        m = re.match(r"^F-(\d+)$", str(item.get("id") or "").strip())
        if m:
            max_idx = max(max_idx, int(m.group(1)))
    next_idx = max(max_idx, len(findings)) + 1
    targeted_added = 0
    targeted_stages: list[str] = []

    for stage, payload in targeted_payloads:
        targeted_findings = payload.get("findings") if isinstance(payload, dict) else []
        if not isinstance(targeted_findings, list):
            continue
        added_for_stage = 0
        for item in targeted_findings:
            if not isinstance(item, dict):
                continue
            normalized = dict(item)
            key = _canonical_finding_key(normalized)
            if key and key in seen_keys:
                continue
            normalized["id"] = f"F-{next_idx:03d}"
            normalized.setdefault("source_stage", stage)
            if not normalized.get("problem") and normalized.get("finding"):
                normalized["problem"] = normalized.get("finding")
            _ensure_text_evidence_refs(normalized)
            findings.append(normalized)
            if key:
                seen_keys.add(key)
            next_idx += 1
            targeted_added += 1
            added_for_stage += 1
        if added_for_stage:
            targeted_stages.append(stage)

    findings, observer_report = _observe_pdf_symbol_evidence(findings, output_dir)
    combined["findings"] = findings
    meta = combined.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        combined["meta"] = meta
    meta["total_findings"] = len(findings)
    meta["codex_targeted_added"] = targeted_added
    meta["codex_targeted_stages"] = targeted_stages
    meta["codex_targeted_enabled"] = bool(targeted_payloads)
    if observer_report is not None:
        meta["finding_evidence_observer"] = observer_report
    return combined


def _split_finding_text(value: Any) -> tuple[str, str]:
    text = str(value or "").strip()
    parts = re.split(r"\n\s*\nРекомендация:\s*", text, maxsplit=1, flags=re.IGNORECASE)
    problem = parts[0].strip()
    solution = parts[1].strip() if len(parts) > 1 else ""
    return problem, solution


def _dimension_receipt(item: dict[str, Any]) -> str:
    """Вернуть детерминированный расчёт для простой размерной цепочки."""
    finding = str(item.get("finding") or "")
    if not re.search(r"сумм|цепоч", finding, re.IGNORECASE):
        return ""
    raw = str(item.get("value_found") or "")
    values = [int(token) for token in re.findall(r"\d{2,5}", raw)]
    if len(values) < 3:
        return ""
    parts, stated = values[:-1], values[-1]
    calculated = sum(parts)
    expression = " + ".join(str(value) for value in parts)
    return f"Расчётная проверка: {expression} = {calculated} мм; указанный общий размер — {stated} мм."


def enforce_stage01_atomicity(
    production: dict[str, Any],
    stage01_path: Path,
) -> dict[str, Any]:
    """Не позволить merge объединить разные доказанные дефекты одного блока.

    Cross-block повторы по-прежнему можно объединять: это нормальная дедупликация
    одного дефекта, найденного в двух представлениях. Но два non-recommendational
    G-кандидата из ОДНОГО блока уже прошли evidence gate как отдельные сущности.
    Их объединение теряет affected entity (например, разные помещения/двери),
    поэтому итоговый finding детерминированно раскладывается обратно.
    """
    if not isinstance(production, dict):
        return production
    try:
        stage01 = json.loads(stage01_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return production
    blocks = stage01.get("block_analyses") if isinstance(stage01, dict) else None
    if not isinstance(blocks, list):
        return production

    candidates: dict[str, dict[str, Any]] = {}
    for block in blocks:
        if not isinstance(block, dict):
            continue
        for item in block.get("findings") or []:
            if not isinstance(item, dict) or not str(item.get("id") or "").startswith("G-"):
                continue
            candidate = dict(item)
            candidate["_block_id"] = block.get("block_id")
            candidate["_page"] = block.get("page")
            candidates[str(item["id"])] = candidate

    findings = production.get("findings")
    if not isinstance(findings, list):
        return production
    max_idx = 0
    for item in findings:
        if not isinstance(item, dict):
            continue
        match = re.match(r"^F-(\d+)$", str(item.get("id") or ""))
        if match:
            max_idx = max(max_idx, int(match.group(1)))

    result: list[dict[str, Any]] = []
    split_groups = 0
    restored = 0
    for final in findings:
        if not isinstance(final, dict):
            continue
        source_ids = [
            str(source_id)
            for source_id in final.get("source_finding_ids") or []
            if str(source_id).startswith("G-") and str(source_id) in candidates
        ]
        by_block: dict[str, list[dict[str, Any]]] = {}
        for source_id in source_ids:
            candidate = candidates[source_id]
            block_id = str(candidate.get("_block_id") or "")
            by_block.setdefault(block_id, []).append(candidate)
        atomic_group = None
        if len(by_block) == 1:
            only_group = next(iter(by_block.values()))
            if len(only_group) > 1 and all(
                str(item.get("severity") or "").upper() != "РЕКОМЕНДАТЕЛЬНОЕ"
                for item in only_group
            ):
                atomic_group = only_group
        if not atomic_group:
            result.append(final)
            continue

        split_groups += 1
        for index, candidate in enumerate(atomic_group):
            problem, solution = _split_finding_text(candidate.get("finding"))
            receipt = _dimension_receipt(candidate)
            description = problem
            if receipt:
                description = f"{description} {receipt}".strip()
            normalized = dict(final)
            if index:
                max_idx += 1
                normalized["id"] = f"F-{max_idx:03d}"
            normalized.update(
                {
                    "severity": candidate.get("severity") or final.get("severity"),
                    "category": candidate.get("category") or final.get("category"),
                    "page": candidate.get("_page") or final.get("page"),
                    "problem": problem,
                    "description": description,
                    "solution": solution or final.get("solution"),
                    "norm": candidate.get("norm") or final.get("norm"),
                    "norm_quote": candidate.get("norm_quote"),
                    "source_finding_ids": [candidate.get("id")],
                    "source_block_ids": [candidate.get("_block_id")],
                    "related_block_ids": [candidate.get("_block_id")],
                    "evidence": [
                        {
                            "type": "image",
                            "block_id": candidate.get("_block_id"),
                            "page": candidate.get("_page"),
                        }
                    ],
                    "highlight_regions": candidate.get("highlight_regions") or [],
                    "source_stage": "stage01_atomicity_guard",
                    "atomicity_guard": {
                        "split_from": final.get("id"),
                        "reason": "distinct_nonrecommendational_candidates_same_block",
                    },
                }
            )
            result.append(normalized)
            restored += 1

    if not split_groups:
        return production
    combined = dict(production)
    combined["findings"] = result
    meta = dict(combined.get("meta") or {})
    meta["total_findings"] = len(result)
    meta["stage01_atomicity_guard"] = {
        "split_groups": split_groups,
        "restored_findings": restored,
    }
    combined["meta"] = meta
    return combined


def json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)
