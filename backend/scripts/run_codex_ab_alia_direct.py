#!/usr/bin/env python3
"""Run direct Codex A/B for reviewed Alia classic-audit documents.

This runner deliberately avoids mutating project ``03_analysis/latest``:
it copies baseline artifacts into ``comparison/classic_codex_ab`` and asks
Codex exec to write fresh ``02_text_analysis.json`` and ``03_findings.json``
only inside the comparison run directory.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.services.llm.codex_runner import run_codex_exec, run_codex_json_messages
from backend.scripts.compare_classic_findings_outputs import compare
from backend.scripts.run_codex_ab_reviewed_candidates import (
    Candidate,
    accepted_subsets,
    copytree_clean,
    load_findings,
    safe_name,
    write_json,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = REPO_ROOT / "comparison" / "classic_codex_ab" / "alia_reviewed_candidates" / "candidates.json"
OUT_ROOT = REPO_ROOT / "comparison" / "classic_codex_ab" / "alia_direct_runs"
PROFILE_AGENTIC = "direct-agentic"
PROFILE_PRODUCTION_JSON = "production-json"
PROFILE_PRODUCTION_JSON_TARGETED = "production-json-targeted"
PROFILES = (PROFILE_AGENTIC, PROFILE_PRODUCTION_JSON, PROFILE_PRODUCTION_JSON_TARGETED)


@dataclass(frozen=True)
class AliaManifestItem:
    accepted: int
    rejected: int
    findings: int | None
    version_dir: Path


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def load_manifest(path: Path, *, min_accepted: int) -> list[AliaManifestItem]:
    data = json.loads(path.read_text(encoding="utf-8"))
    items: list[AliaManifestItem] = []
    for raw in data:
        if not isinstance(raw, dict):
            continue
        accepted = int(raw.get("accepted") or 0)
        if accepted < min_accepted:
            continue
        version_dir = Path(str(raw.get("version_dir") or "")).resolve()
        if not version_dir.is_dir():
            continue
        items.append(
            AliaManifestItem(
                accepted=accepted,
                rejected=int(raw.get("rejected") or 0),
                findings=raw.get("findings"),
                version_dir=version_dir,
            )
        )
    return items


def filter_manifest_items(
    items: list[AliaManifestItem],
    *,
    discipline: str | None,
    document: str | None,
    version: str | None,
) -> list[AliaManifestItem]:
    if not discipline and not document and not version:
        return items
    result: list[AliaManifestItem] = []
    for item in items:
        candidate = candidate_from_version_dir(item.version_dir)
        if discipline and candidate.discipline.lower() != discipline.lower():
            continue
        if document and document.lower() not in candidate.document.lower():
            continue
        if version and candidate.version.lower() != version.lower():
            continue
        result.append(item)
    return result


def candidate_from_version_dir(version_dir: Path) -> Candidate:
    parts = version_dir.parts
    marker = parts.index("projects_v2")
    object_slug = parts[marker + 2]
    discipline = parts[marker + 4]
    document = parts[marker + 6]
    version = parts[marker + 8]
    return Candidate(
        object_id="214_Alia_ASTERUS",
        object_slug=object_slug,
        discipline=discipline,
        document=document,
        version=version,
    )


def find_input_md(version_dir: Path) -> Path:
    candidates = [
        version_dir / "02_work" / "document.md",
        *sorted((version_dir / "01_input").glob("*_document.md")),
        *sorted((version_dir / "01_input").glob("*.md")),
    ]
    for path in candidates:
        if path.is_file():
            return path
    raise FileNotFoundError(f"document markdown not found under {version_dir}")


def find_project_info(version_dir: Path) -> Path:
    candidates = [
        version_dir / "01_input" / "project_info.json",
        version_dir / "02_work" / "project_info.json",
    ]
    for path in candidates:
        if path.is_file():
            return path
    raise FileNotFoundError(f"project_info.json not found under {version_dir}")


def copy_inputs(candidate: Candidate, input_dir: Path) -> dict[str, str]:
    version_dir = candidate.version_dir
    latest_dir = candidate.latest_dir
    input_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "project_info": find_project_info(version_dir),
        "document_md": find_input_md(version_dir),
        "baseline_01_text_analysis": latest_dir / "02_text_analysis.json",
        "baseline_02_blocks_analysis": latest_dir / "01_blocks_analysis.json",
        "baseline_document_graph": latest_dir / "document_graph.json",
        "baseline_03_findings": latest_dir / "03_findings.json",
        "expert_review": candidate.review_path,
    }
    for name, path in paths.items():
        if not path.is_file():
            raise FileNotFoundError(path)
        target = input_dir / {
            "project_info": "project_info.json",
            "document_md": "document.md",
            "baseline_01_text_analysis": "baseline_02_text_analysis.json",
            "baseline_02_blocks_analysis": "01_blocks_analysis.json",
            "baseline_document_graph": "document_graph.json",
            "baseline_03_findings": "baseline_03_findings.json",
            "expert_review": "expert_review.json",
        }[name]
        shutil.copy2(path, target)

    return {name: str(path) for name, path in paths.items()}


@contextmanager
def temporary_env(values: dict[str, str]):
    previous: dict[str, str | None] = {}
    for key, value in values.items():
        previous[key] = os.environ.get(key)
        os.environ[key] = value
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def prepare_production_json_layout(input_dir: Path, output_dir: Path, version_dir: Path) -> dict[str, str]:
    """Build a minimal v2-like layout for production prompt_builder reads."""
    if version_dir.exists():
        shutil.rmtree(version_dir)
    version_dir.mkdir(parents=True, exist_ok=True)
    (version_dir / "01_input").mkdir(parents=True, exist_ok=True)
    (version_dir / "02_work").mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy2(input_dir / "project_info.json", version_dir / "01_input" / "project_info.json")
    shutil.copy2(input_dir / "document.md", version_dir / "02_work" / "document.md")
    shutil.copy2(input_dir / "01_blocks_analysis.json", output_dir / "01_blocks_analysis.json")
    shutil.copy2(input_dir / "document_graph.json", output_dir / "document_graph.json")

    return {
        "production_output_dir": str(output_dir),
        "production_version_dir": str(version_dir),
    }


def stable_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def brief_project_label(candidate: Candidate) -> str:
    return f"{candidate.object_slug}/{candidate.discipline}/{candidate.document}/{candidate.version}"


def build_text_prompt(candidate: Candidate, input_dir: Path, output_path: Path) -> str:
    return f"""
Ты выполняешь Stage 01 `text_analysis` классического аудита проектной документации.

Проект: {brief_project_label(candidate)}
Рабочие файлы:
- project_info: {input_dir / "project_info.json"}
- markdown документа: {input_dir / "document.md"}
- baseline Claude 01 только для понимания ожидаемой структуры, не копировать слепо: {input_dir / "baseline_02_text_analysis.json"}

Задача:
1. Прочитай markdown и project_info.
2. Найди только замечания, которые можно обосновать текстом/таблицами markdown.
3. Не добавляй замечания, основанные только на отсутствии информации в томе, если предмет явно может быть передан в смежный раздел.
4. Для Alia особенно учитывай границы тома: архитектурные кладочные планы не обязаны показывать монолитные стены КЖ, ПБ-мероприятия, ОВ/ЭОМ-системы, если есть явные ссылки на смежные разделы.
5. Запиши JSON в файл: {output_path}

Формат JSON:
{{
  "stage": "02_text_analysis",
  "project_id": "{candidate.document}",
  "text_source": "md",
  "findings": [
    {{
      "id": "T-001",
      "severity": "КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ",
      "category": "documentation|normative_refs|fire_safety|accessibility|layout|dimensions|coordination|other",
      "sheet": "лист/страница/раздел",
      "page": null,
      "problem": "краткое название проблемы",
      "description": "доказательство по markdown",
      "norm": "норма или пустая строка",
      "solution": "что исправить",
      "risk": "риск",
      "evidence": [{{"type": "text", "block_id": "...", "page": null}}]
    }}
  ],
  "summary": {{"total_findings": 0}}
}}

Верни в stdout только короткий статус; основной результат должен быть записан в файл.
""".strip()


def build_merge_prompt(candidate: Candidate, input_dir: Path, codex_dir: Path, output_path: Path) -> str:
    return f"""
Ты выполняешь Stage 03 `findings_merge` классического аудита.

Проект: {brief_project_label(candidate)}
Источники:
- project_info: {input_dir / "project_info.json"}
- markdown документа: {input_dir / "document.md"}
- Codex Stage 01: {codex_dir / "02_text_analysis.json"}
- Stage 02 blocks/GPT baseline: {input_dir / "01_blocks_analysis.json"}
- document_graph: {input_dir / "document_graph.json"}
- baseline Claude 03 только для понимания структуры JSON, не копировать слепо: {input_dir / "baseline_03_findings.json"}

Задача:
1. Свести текстовые замечания Codex и готовые замечания из Stage 02 blocks.
2. Удалить дубли, объединить близкие темы, сохранить конкретные доказательства.
3. Не завышать severity. Если замечание требует смежного раздела, ставь `ПРОВЕРИТЬ ПО СМЕЖНЫМ`.
4. Не создавай замечание только из-за отсутствия в текущем томе того, что нормально находится в КЖ/ПБ/ОВ/ЭОМ.
5. Запиши итоговый JSON в файл: {output_path}

Формат JSON:
{{
  "meta": {{
    "project_id": "{candidate.document}",
    "audit_completed": "{datetime.now(UTC).isoformat()}",
    "total_findings": 0,
    "by_severity": {{}},
    "notes": "Codex direct A/B findings_merge"
  }},
  "findings": [
    {{
      "id": "F-001",
      "severity": "КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ",
      "category": "documentation|normative_refs|fire_safety|accessibility|layout|dimensions|coordination|other",
      "sheet": "лист/страница/раздел",
      "page": null,
      "problem": "краткое название проблемы",
      "description": "подробное доказательство",
      "norm": "норма или пустая строка",
      "norm_quote": null,
      "solution": "что исправить",
      "risk": "риск",
      "source_block_ids": [],
      "related_block_ids": [],
      "evidence": []
    }}
  ]
}}

Верни в stdout только короткий статус; основной результат должен быть записан в файл.
""".strip()


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


def build_targeted_md_context(md_path: Path, *, window: int = 6, max_chars: int = 70000) -> str:
    """Extract line-numbered MD snippets for AR masonry/lintel targeted checks."""
    lines = md_path.read_text(encoding="utf-8", errors="replace").splitlines()
    selected: set[int] = set()
    for idx, line in enumerate(lines):
        low = line.lower()
        if any(keyword in low for keyword in TARGETED_KEYWORDS):
            start = max(0, idx - window)
            end = min(len(lines), idx + window + 1)
            selected.update(range(start, end))

    if not selected:
        selected = set(range(min(len(lines), 400)))

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
            parts.append(f"[truncated: targeted MD context limited to {max_chars} chars]")
            break
        parts.append(block)
        total += len(block)
    return "\n\n".join(parts)


def build_targeted_messages(candidate: Candidate, input_dir: Path, codex_dir: Path) -> list[dict[str, str]]:
    md_context = build_targeted_md_context(input_dir / "document.md")
    existing_findings = (codex_dir / "03_findings.json").read_text(encoding="utf-8", errors="replace")
    system = """
Ты выполняешь дополнительный targeted audit для раздела АР: кладочные планы, ведомости материалов,
перемычки, отверстия/проёмы и специальные противопожарные элементы.

Это не baseline и не подсказка от Claude. Ниже дан только отфильтрованный MD-контекст и уже найденные
Codex findings, чтобы не плодить точные дубли.

Обязательные проверки:
1. Сравни одинаковые/типовые этажи по ведомостям материалов кладочных стен. Если одна и та же марка
   стены/перегородки имеет разные объёмы/площади между этажами с заявленной одинаковой геометрией,
   выдай ОТДЕЛЬНОЕ замечание. Не заменяй его замечанием про сводную ведомость.
2. Сверь поэтажные ведомости со сводной ведомостью. Это отдельный класс замечаний.
3. В спецификациях перемычек пересчитай `Итого × Масса ед.` и найди нулевые массы физических изделий.
4. Сверь марки/позиции перемычек между схемами, планами и спецификациями.
5. Найди повторное использование одной марки отверстия/проёма для разных размеров/назначений.
6. Найди общие указания со специальным REI/EI, усилением, обрамлением уголками, простенками у лифтового
   холла. Проверь, выделена ли такая конструкция в спецификациях/марках/ведомостях с количеством.
   Если указание есть, но нет трассировки в спецификации, выдай ОТДЕЛЬНОЕ эксплуатационное замечание.

Не добавляй замечания только из-за отсутствия данных, которые явно относятся к КЖ/ПБ/ОВ/ЭОМ.
Если defect отличается по факту, оставь отдельным finding даже при похожей зоне документа.
Верни строго один JSON object без Markdown.
""".strip()
    user = f"""
Проект: {brief_project_label(candidate)}

## Уже найденные Codex findings
{existing_findings[:60000]}

## Targeted MD context with line numbers
{md_context}

Верни JSON:
{{
  "findings": [
    {{
      "id": "AT-001",
      "severity": "КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ",
      "category": "documentation|fire_safety|coordination|dimensions|other",
      "problem": "краткая суть",
      "description": "доказательство с конкретными числами/строками MD",
      "norm": "",
      "norm_quote": null,
      "solution": "что исправить",
      "risk": "риск",
      "evidence": [
        {{"type": "text", "block_id": null, "page": null, "md_lines": "строки N-M"}}
      ]
    }}
  ]
}}
""".strip()
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def build_docnorm_md_context(md_path: Path, *, window: int = 5, max_chars: int = 60000) -> str:
    lines = md_path.read_text(encoding="utf-8", errors="replace").splitlines()
    selected: set[int] = set()
    for idx, line in enumerate(lines):
        low = line.lower()
        if any(keyword in low for keyword in DOCNORM_KEYWORDS):
            start = max(0, idx - window)
            end = min(len(lines), idx + window + 1)
            selected.update(range(start, end))
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
            parts.append(f"[truncated: doc/norm context limited to {max_chars} chars]")
            break
        parts.append(block)
        total += len(block)
    return "\n\n".join(parts)


def build_docnorm_messages(candidate: Candidate, input_dir: Path, codex_dir: Path) -> list[dict[str, str]]:
    md_context = build_docnorm_md_context(input_dir / "document.md")
    existing_findings = (codex_dir / "03_findings.json").read_text(encoding="utf-8", errors="replace")
    system = """
Ты выполняешь дополнительный targeted audit для документной части АР/РД:
титульные данные, шифры комплектов, ведомость основных комплектов, ведомость ссылочных документов,
общие указания и нормативные ссылки.

Не используй Claude baseline. Ниже только MD-контекст и уже найденные Codex findings, чтобы не плодить дубли.

Обязательные проверки:
1. Шифры рабочих чертежей: проверь смешение латиницы и кириллицы внутри одного шифра
   (например AB/R латиницей и РД/АР кириллицей), непоследовательные обозначения и OCR-ошибки.
2. Титульные данные: заказчик, адрес, объект, корпус/блок должны быть согласованы между титулом,
   ведомостью комплектов и общими указаниями.
3. Ведомость ссылочных документов: найди дубли, одновременное указание старого и заменившего ГОСТ,
   неверное название нормы, отсутствующую дату/редакцию у ГОСТ/СП, ошибочное ФЗ/ФЭ.
4. Общие указания: проверь нормативные ссылки на ГОСТ Р 53299/53306/53310/53301 и подобные индексы,
   если дата/редакция не указана или актуальность требует проверки.
5. Не создавай замечание только потому, что не знаешь актуальный статус нормы. Но если в самом MD есть
   дубли, конфликт старый+новый стандарт, отсутствие даты редакции или очевидная ошибка записи — выдай finding.

Верни только новые/недублирующие замечания. Строго один JSON object без Markdown.
""".strip()
    user = f"""
Проект: {brief_project_label(candidate)}

## Уже найденные Codex findings
{existing_findings[:50000]}

## Doc/norm MD context with line numbers
{md_context}

Верни JSON:
{{
  "findings": [
    {{
      "id": "DN-001",
      "severity": "КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ",
      "category": "documentation|normative_refs|coordination|other",
      "problem": "краткая суть",
      "description": "доказательство с конкретными строками MD",
      "norm": "",
      "norm_quote": null,
      "solution": "что исправить",
      "risk": "риск",
      "evidence": [
        {{"type": "text", "block_id": null, "page": null, "md_lines": "строки N-M"}}
      ]
    }}
  ]
}}
""".strip()
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def build_keyword_context(
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
            start = max(0, idx - window)
            end = min(len(lines), idx + window + 1)
            selected.update(range(start, end))
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
            parts.append(f"[truncated: context limited to {max_chars} chars]")
            break
        parts.append(block)
        total += len(block)
    return "\n\n".join(parts)


def build_eom_messages(candidate: Candidate, input_dir: Path, codex_dir: Path) -> list[dict[str, str]]:
    md_context = build_keyword_context(input_dir / "document.md", EOM_KEYWORDS)
    existing_findings = (codex_dir / "03_findings.json").read_text(encoding="utf-8", errors="replace")
    system = """
Ты выполняешь targeted audit для ЭОМ/ЭМ: защитные аппараты, токи КЗ, расчетные токи,
кабельные линии СПЗ/ПЭСПЗ/ОКЛ и согласованность обозначений линий.

Не используй Claude baseline. Ниже только MD-контекст и уже найденные Codex findings.

Обязательные проверки:
1. Для каждой линии с автоматом C-характеристики и указанным Iкз в конце линии пересчитай кратность:
   `Iкз_А / In`. Если Iкз задан в кА, умножь на 1000. Для C-характеристики зона мгновенного
   расцепления 5-10 In; при кратности <= 10 гарантированное мгновенное отключение не подтверждено.
   Сгруппируй только действительно однотипные линии. НЕ объединяй в один finding разные группы
   с разными QF, разными щитами/листами или разными кратностями, если их можно выписать отдельно.
   Примеры отдельных групп: `QF5.2`, `QF3.29...QF4.14`, `QF4.11/QF4.12`, `QF5.1.1`.
   Если в уже найденных Codex findings есть широкий пункт, который смешал несколько таких групп,
   всё равно верни отдельные уточняющие findings по каждой группе с расчетом `0.089 кА * 1000 / 16 А = 5.6 In`.
2. Сверь расчетный ток нагрузки `Iр` с номиналом автомата. Если In < Iр, это критическое замечание.
3. Если таблица условных обозначений требует один номинал автомата, а на схеме указан другой,
   выдай отдельное замечание.
4. Для линий СПЗ/ПЭСПЗ/ОКЛ и огнезащитных коробов проверь наличие предела огнестойкости, габаритов,
   отметки прокладки и однозначного участка применения.
5. Сверь обозначения одной отходящей линии между планом, видом шкафа, спецификацией и схемой.

Верни только новые/недублирующие замечания. Строго один JSON object без Markdown.
""".strip()
    user = f"""
Проект: {brief_project_label(candidate)}

## Уже найденные Codex findings
{existing_findings[:50000]}

## EOM/EM MD context with line numbers
{md_context}

Верни JSON с `findings[]`, id `EOM-001...`, severity/category/problem/description/norm/solution/risk/evidence.
""".strip()
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def build_ss_messages(candidate: Candidate, input_dir: Path, codex_dir: Path) -> list[dict[str, str]]:
    md_context = build_keyword_context(input_dir / "document.md", SS_KEYWORDS)
    existing_findings = (codex_dir / "03_findings.json").read_text(encoding="utf-8", errors="replace")
    system = """
Ты выполняешь targeted audit для слаботочных систем: СКУД/СОВ/ОСПД/домофония/СКС.

Не используй Claude baseline. Ниже только MD-контекст и уже найденные Codex findings.

Обязательные проверки:
1. Нормативная база: проверь устаревший СП 134.13330.2012, отсутствие ГОСТ Р 51241-2008
   для СКУД, дубли и конфликтующие редакции.
2. АКБ/ИБП: пересчитай емкость по формуле `I суммарный × t × коэффициент`.
   Если результат таблицы не совпадает или время работы в заголовке противоречит расчету,
   выдай отдельное замечание.
3. СКС/Ethernet: если применяются U/UTP Cat5e для удаленных IP-устройств, а кабельные журналы
   и расчет длин отсутствуют, проверь риск превышения 90 м горизонтальной линии.
4. RS-485: проверь наличие терминальных резисторов 120 Ом на концах линии. Если уже найденный
   Codex finding упоминает RS-485 вместе с другими темами (калитка, бирки, марки кабеля и т.п.),
   верни отдельный standalone finding только про оконечивание/терминирование RS-485.
5. СКУД/домофония: проверь увязку контроллеров, вызывных панелей, электрозамков, разблокировки
   при пожаре и резервного питания.
6. Монтажные размеры и собственные примечания: для схем калиток, стоек, вызывных панелей и
   считывателей сравни числовые размеры на схеме с примечаниями на том же блоке/листе. Если
   схема показывает `800-850 мм`, а примечание требует `не менее 1 м`, верни отдельный finding
   именно про это внутреннее противоречие. Не заменяй его замечанием про термин "счетчик" или
   высоты установки.

Каждый независимый дефект должен быть отдельным finding: нормативная ссылка, АКБ, Cat5e/90м,
RS-485, монтажный размер калитки и разблокировка при пожаре не должны сливаться в один общий пункт.

Верни только новые/недублирующие замечания. Строго один JSON object без Markdown.
""".strip()
    user = f"""
Проект: {brief_project_label(candidate)}

## Уже найденные Codex findings
{existing_findings[:50000]}

## SS MD context with line numbers
{md_context}

Верни JSON с `findings[]`, id `SS-001...`, severity/category/problem/description/norm/solution/risk/evidence.
""".strip()
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def build_km_messages(candidate: Candidate, input_dir: Path, codex_dir: Path) -> list[dict[str, str]]:
    md_context = build_keyword_context(input_dir / "document.md", KM_KEYWORDS)
    existing_findings = (codex_dir / "03_findings.json").read_text(encoding="utf-8", errors="replace")
    system = """
Ты выполняешь targeted audit для КМ: нормы стальных конструкций, болты/гайки,
спецификации металлопроката, массы и сортаменты.

Не используй Claude baseline. Ниже только MD-контекст и уже найденные Codex findings.

Обязательные проверки:
1. Нормативные ссылки: СП 10.13330 не является нормой по стальным конструкциям; для КМ ожидается
   СП 16.13330. Если в общих указаниях перепутан номер СП, выдай критическое замечание.
2. Болты/гайки: класс прочности гайки должен соответствовать классу болта. Гайка класса 4 для болтов 8.8
   является опасной нестыковкой.
3. Сводная спецификация металлопроката: пересчитай суммы по маркам стали и сравни с итоговой массой.
   Обязательно выпиши исходные строки и формулу. Пример требуемого уровня конкретики:
   `С245 18.147 т + Ст3сп 12.204 т = 30.351 т`, но в таблице указан итог `28.363 т`;
   расхождение `30.351 - 28.363 = 1.988 т`. Если видишь похожую нестыковку, finding должен
   содержать именно эти числа и арифметику, а не общее "итоги не сходятся".
4. Проверь, что уголки/профили отнесены к правильному сортаменту ГОСТ, а не к листовому прокату.
5. Групповые спецификации: физические изделия не должны иметь нулевые `Кол.` и `Масса общая`;
   `Кол. × Масса ед.` должно равняться общей массе. Для каждой найденной ошибки покажи формулу.
6. Анкеры должны иметь производителя/ТУ/ETA/сертификат, несущую способность и требования к основанию.
7. Если уже найденный Codex finding говорит об арифметике слишком широко, верни отдельный
   уточняющий finding с точной таблицей, строками и формулой. Не считай это дублем.

Верни только новые/недублирующие замечания. Строго один JSON object без Markdown.
""".strip()
    user = f"""
Проект: {brief_project_label(candidate)}

## Уже найденные Codex findings
{existing_findings[:50000]}

## KM MD context with line numbers
{md_context}

Верни JSON с `findings[]`, id `KM-001...`, severity/category/problem/description/norm/solution/risk/evidence.
""".strip()
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def discipline_targeted_messages(
    candidate: Candidate,
    input_dir: Path,
    codex_dir: Path,
) -> tuple[str, list[dict[str, str]] | None]:
    discipline = candidate.discipline.upper()
    if discipline == "AR":
        return "alia_ar_masonry_audit", build_targeted_messages(candidate, input_dir, codex_dir)
    if discipline == "EOM":
        return "alia_eom_protection_audit", build_eom_messages(candidate, input_dir, codex_dir)
    if discipline == "SS":
        return "alia_ss_lowcurrent_audit", build_ss_messages(candidate, input_dir, codex_dir)
    if discipline == "KM":
        return "alia_km_steel_audit", build_km_messages(candidate, input_dir, codex_dir)
    return "", None


def combine_findings_with_targeted(production_path: Path, targeted_paths: list[Path], combined_path: Path) -> int:
    production = json.loads(production_path.read_text(encoding="utf-8"))
    production_findings = production.get("findings") if isinstance(production, dict) else []
    if not isinstance(production_findings, list):
        production_findings = []

    combined = dict(production) if isinstance(production, dict) else {"findings": []}
    findings = [item for item in production_findings if isinstance(item, dict)]
    next_idx = len(findings) + 1
    targeted_added = 0
    for targeted_path in targeted_paths:
        targeted = json.loads(targeted_path.read_text(encoding="utf-8"))
        targeted_findings = targeted.get("findings") if isinstance(targeted, dict) else []
        if not isinstance(targeted_findings, list):
            continue
        source_stage = targeted_path.stem
        for item in targeted_findings:
            if not isinstance(item, dict):
                continue
            normalized = dict(item)
            normalized["id"] = f"F-{next_idx:03d}"
            normalized.setdefault("source_stage", source_stage)
            if not normalized.get("problem") and normalized.get("finding"):
                normalized["problem"] = normalized.get("finding")
            findings.append(normalized)
            targeted_added += 1
            next_idx += 1

    combined["findings"] = findings
    meta = combined.get("meta")
    if isinstance(meta, dict):
        meta["total_findings"] = len(findings)
        meta["targeted_alia_added"] = targeted_added
    write_json(combined_path, combined)
    return len(findings)


def validate_findings(path: Path) -> int:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    findings = data.get("findings")
    if not isinstance(findings, list):
        raise ValueError(f"{path} missing findings[]")
    return len(findings)


def validate_text_analysis(path: Path) -> int:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    findings = data.get("text_findings")
    if findings is None:
        findings = data.get("findings")
    if not isinstance(findings, list):
        raise ValueError(f"{path} missing text_findings[] or findings[]")
    return len(findings)


def compare_reports(accepted_path: Path, candidate_path: Path, out_path: Path, *, threshold: float) -> dict[str, Any]:
    report = compare(accepted_path, candidate_path, threshold=threshold)
    write_json(out_path, report)
    return report


def source_class(item: dict[str, Any]) -> str:
    evidence = item.get("evidence")
    types: set[str] = set()
    if isinstance(evidence, list):
        for ev in evidence:
            if isinstance(ev, dict):
                ev_type = str(ev.get("type") or ev.get("source_type") or "").lower()
                if ev_type:
                    types.add(ev_type)
    if not types and (item.get("source_block_ids") or item.get("related_block_ids")):
        types.add("block_or_ref")
    if not types:
        return "unknown"
    has_text = "text" in types
    has_image = bool(types & {"image", "graphic", "graphics", "vision", "table_image"})
    if has_text and has_image:
        return "mixed_text_image"
    if has_image:
        return "image_only"
    if has_text:
        return "text_only"
    return "+".join(sorted(types))


def source_split(path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in load_findings(path):
        cls = source_class(item)
        counts[cls] = counts.get(cls, 0) + 1
    return counts


async def run_one(
    candidate: Candidate,
    item_dir: Path,
    *,
    model: str,
    timeout_sec: int,
    threshold: float,
    profile: str,
    dry_run: bool,
) -> dict[str, Any]:
    baseline_dir = item_dir / "baseline_latest"
    input_dir = item_dir / "input"
    codex_dir = item_dir / "codex_direct"
    codex_dir.mkdir(parents=True, exist_ok=True)

    copytree_clean(candidate.latest_dir, baseline_dir)
    copied_inputs = copy_inputs(candidate, input_dir)
    subsets = accepted_subsets(candidate, item_dir)

    baseline_hashes = {
        rel: stable_hash(candidate.latest_dir / rel)
        for rel in ("02_text_analysis.json", "01_blocks_analysis.json", "03_findings.json", "document_graph.json")
        if (candidate.latest_dir / rel).is_file()
    }

    if dry_run:
        result = {
            "status": "dry_run",
            "mode": profile,
            "candidate": candidate.__dict__,
            "out_dir": str(item_dir),
            "copied_inputs": copied_inputs,
            "baseline_findings": len(load_findings(candidate.latest_dir / "03_findings.json")),
            "baseline_source_split": source_split(candidate.latest_dir / "03_findings.json"),
            "baseline_hashes": baseline_hashes,
            **subsets,
        }
        write_json(item_dir / "candidate_summary.json", result)
        return result

    os.environ.setdefault("AUDIT_CODEX_SANDBOX", "danger-full-access")
    os.environ.setdefault("AUDIT_CODEX_JSON_SANDBOX", "danger-full-access")
    os.environ["AUDIT_CODEX_MODEL"] = model

    usage: list[dict[str, Any]] = []

    if profile in {PROFILE_PRODUCTION_JSON, PROFILE_PRODUCTION_JSON_TARGETED}:
        import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder

        codex_dir = item_dir / "codex_production_json"
        sandbox_version_dir = item_dir / "production_json_version"
        layout = prepare_production_json_layout(input_dir, codex_dir, sandbox_version_dir)
        text_out = codex_dir / "02_text_analysis.json"
        merge_out = codex_dir / "03_findings.json"
        project_info = json.loads((input_dir / "project_info.json").read_text(encoding="utf-8"))

        with temporary_env({
            "AUDIT_OUTPUT_DIR": str(codex_dir),
            "AUDIT_VERSION_DIR": str(sandbox_version_dir),
            "AUDIT_CODEX_MODEL": model,
            "AUDIT_CODEX_JSON_SANDBOX": os.environ.get("AUDIT_CODEX_JSON_SANDBOX", "danger-full-access"),
        }):
            text_messages = prompt_builder.build_text_analysis_messages(project_info, candidate.document)
            text_result = await run_codex_json_messages(
                text_messages,
                timeout=timeout_sec,
                stage="text_analysis",
                project_id=candidate.document,
                model=f"codex/{model}",
            )
            usage.append({
                "stage": "text_analysis",
                "transport": "codex_json_messages",
                "exit_code": 0 if not text_result.is_error else 1,
                "duration_ms": text_result.duration_ms,
                "is_error": text_result.is_error,
                "error_message": text_result.error_message,
                "output_tail": (text_result.text or "")[-4000:],
                **layout,
            })
            if text_result.is_error or text_result.json_data is None:
                raise RuntimeError(f"Codex production-json text_analysis failed: {text_result.error_message}")
            write_json(text_out, text_result.json_data)
            text_count = validate_text_analysis(text_out)

            merge_messages = prompt_builder.build_findings_merge_messages(project_info, candidate.document)
            merge_result = await run_codex_json_messages(
                merge_messages,
                timeout=timeout_sec,
                stage="findings_merge",
                project_id=candidate.document,
                model=f"codex/{model}",
            )
            usage.append({
                "stage": "findings_merge",
                "transport": "codex_json_messages",
                "exit_code": 0 if not merge_result.is_error else 1,
                "duration_ms": merge_result.duration_ms,
                "is_error": merge_result.is_error,
                "error_message": merge_result.error_message,
                "output_tail": (merge_result.text or "")[-4000:],
            })
            if merge_result.is_error or merge_result.json_data is None:
                raise RuntimeError(f"Codex production-json findings_merge failed: {merge_result.error_message}")
            write_json(merge_out, merge_result.json_data)
            codex_count = validate_findings(merge_out)

            if profile == PROFILE_PRODUCTION_JSON_TARGETED:
                targeted_paths: list[Path] = []
                discipline_stage, discipline_messages = discipline_targeted_messages(candidate, input_dir, codex_dir)
                if discipline_messages:
                    targeted_result = await run_codex_json_messages(
                        discipline_messages,
                        timeout=timeout_sec,
                        stage=discipline_stage,
                        project_id=candidate.document,
                        model=f"codex/{model}",
                    )
                    usage.append({
                        "stage": discipline_stage,
                        "transport": "codex_json_messages",
                        "exit_code": 0 if not targeted_result.is_error else 1,
                        "duration_ms": targeted_result.duration_ms,
                        "is_error": targeted_result.is_error,
                        "error_message": targeted_result.error_message,
                        "output_tail": (targeted_result.text or "")[-4000:],
                    })
                    if targeted_result.is_error or targeted_result.json_data is None:
                        raise RuntimeError(f"Codex targeted audit failed: {targeted_result.error_message}")
                    targeted_out = codex_dir / f"targeted_{discipline_stage}_findings.json"
                    write_json(targeted_out, targeted_result.json_data)
                    targeted_paths.append(targeted_out)

                docnorm_result = await run_codex_json_messages(
                    build_docnorm_messages(candidate, input_dir, codex_dir),
                    timeout=timeout_sec,
                    stage="alia_docnorm_audit",
                    project_id=candidate.document,
                    model=f"codex/{model}",
                )
                usage.append({
                    "stage": "alia_docnorm_audit",
                    "transport": "codex_json_messages",
                    "exit_code": 0 if not docnorm_result.is_error else 1,
                    "duration_ms": docnorm_result.duration_ms,
                    "is_error": docnorm_result.is_error,
                    "error_message": docnorm_result.error_message,
                    "output_tail": (docnorm_result.text or "")[-4000:],
                })
                if docnorm_result.is_error or docnorm_result.json_data is None:
                    raise RuntimeError(f"Codex doc/norm audit failed: {docnorm_result.error_message}")
                docnorm_out = codex_dir / "targeted_alia_docnorm_findings.json"
                write_json(docnorm_out, docnorm_result.json_data)
                targeted_paths.append(docnorm_out)

                combined_out = codex_dir / "03_findings_targeted_union.json"
                codex_count = combine_findings_with_targeted(merge_out, targeted_paths, combined_out)
                merge_out = combined_out
    else:
        text_out = codex_dir / "02_text_analysis.json"
        merge_out = codex_dir / "03_findings.json"

        text_exit, text_output, text_cli = await run_codex_exec(
            build_text_prompt(candidate, input_dir, text_out),
            timeout=timeout_sec,
            stage="text_analysis",
            project_id=candidate.document,
            model=f"codex/{model}",
        )
        usage.append({
            "stage": "text_analysis",
            "transport": "codex_exec_agentic",
            "exit_code": text_exit,
            "duration_ms": text_cli.duration_ms,
            "is_error": text_cli.is_error,
            "output_tail": text_output[-4000:],
        })
        if text_exit != 0 or not text_out.is_file():
            raise RuntimeError(f"Codex text_analysis failed: exit={text_exit}")
        text_count = validate_findings(text_out)

        merge_exit, merge_output, merge_cli = await run_codex_exec(
            build_merge_prompt(candidate, input_dir, codex_dir, merge_out),
            timeout=timeout_sec,
            stage="findings_merge",
            project_id=candidate.document,
            model=f"codex/{model}",
        )
        usage.append({
            "stage": "findings_merge",
            "transport": "codex_exec_agentic",
            "exit_code": merge_exit,
            "duration_ms": merge_cli.duration_ms,
            "is_error": merge_cli.is_error,
            "output_tail": merge_output[-4000:],
        })
        if merge_exit != 0 or not merge_out.is_file():
            raise RuntimeError(f"Codex findings_merge failed: exit={merge_exit}")
        codex_count = validate_findings(merge_out)
    write_json(codex_dir / "direct_stage_usage.json", usage)

    strict_all = compare_reports(
        Path(subsets["accepted_all_path"]),
        merge_out,
        item_dir / "comparison_accepted_all_strict.json",
        threshold=threshold,
    )
    loose_all = compare_reports(
        Path(subsets["accepted_all_path"]),
        merge_out,
        item_dir / "comparison_accepted_all_loose.json",
        threshold=0.22,
    )
    strict_no_image = compare_reports(
        Path(subsets["accepted_no_image_path"]),
        merge_out,
        item_dir / "comparison_accepted_no_image_strict.json",
        threshold=threshold,
    )
    loose_no_image = compare_reports(
        Path(subsets["accepted_no_image_path"]),
        merge_out,
        item_dir / "comparison_accepted_no_image_loose.json",
        threshold=0.22,
    )

    current_hashes = {
        rel: stable_hash(candidate.latest_dir / rel)
        for rel in baseline_hashes
        if (candidate.latest_dir / rel).is_file()
    }

    result = {
        "status": "done",
        "mode": profile,
        "candidate": candidate.__dict__,
        "out_dir": str(item_dir),
        "model": model,
        "baseline_findings": len(load_findings(candidate.latest_dir / "03_findings.json")),
        "baseline_source_split": source_split(candidate.latest_dir / "03_findings.json"),
        "codex_text_findings": text_count,
        "codex_findings": codex_count,
        "codex_source_split": source_split(merge_out),
        "project_latest_hashes_unchanged": current_hashes == baseline_hashes,
        "baseline_hashes": baseline_hashes,
        "current_hashes": current_hashes,
        **subsets,
        "strict_accepted_all_matched": strict_all["matched"],
        "strict_accepted_all_recall": strict_all["candidate_recall_vs_baseline"],
        "strict_accepted_all_unique_candidates": strict_all.get("unique_candidate_matches"),
        "strict_accepted_all_reused": strict_all.get("candidate_reused_matches"),
        "loose_accepted_all_matched": loose_all["matched"],
        "loose_accepted_all_recall": loose_all["candidate_recall_vs_baseline"],
        "loose_accepted_all_unique_candidates": loose_all.get("unique_candidate_matches"),
        "loose_accepted_all_reused": loose_all.get("candidate_reused_matches"),
        "strict_accepted_no_image_matched": strict_no_image["matched"],
        "strict_accepted_no_image_recall": strict_no_image["candidate_recall_vs_baseline"],
        "strict_accepted_no_image_unique_candidates": strict_no_image.get("unique_candidate_matches"),
        "strict_accepted_no_image_reused": strict_no_image.get("candidate_reused_matches"),
        "loose_accepted_no_image_matched": loose_no_image["matched"],
        "loose_accepted_no_image_recall": loose_no_image["candidate_recall_vs_baseline"],
        "loose_accepted_no_image_unique_candidates": loose_no_image.get("unique_candidate_matches"),
        "loose_accepted_no_image_reused": loose_no_image.get("candidate_reused_matches"),
    }
    write_json(item_dir / "candidate_summary.json", result)
    return result


async def amain() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--max-candidates", type=int, default=2)
    parser.add_argument("--min-accepted", type=int, default=7)
    parser.add_argument("--timeout-sec", type=int, default=1800)
    parser.add_argument("--threshold", type=float, default=0.38)
    parser.add_argument("--model", default=os.environ.get("AUDIT_CODEX_MODEL", "gpt-5.4"))
    parser.add_argument("--profile", choices=PROFILES, default=PROFILE_AGENTIC)
    parser.add_argument("--discipline", help="Optional discipline filter, e.g. AR")
    parser.add_argument("--document", help="Optional substring filter for document name")
    parser.add_argument("--version", help="Optional version filter, e.g. v001")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    manifest_items = load_manifest(args.manifest, min_accepted=args.min_accepted)
    manifest_items = filter_manifest_items(
        manifest_items,
        discipline=args.discipline,
        document=args.document,
        version=args.version,
    )
    selected = manifest_items[: max(0, args.max_candidates)]
    run_prefix = "dry_run" if args.dry_run else "run"
    run_dir = OUT_ROOT / f"{run_prefix}_{utc_stamp()}"
    run_dir.mkdir(parents=True, exist_ok=True)

    summaries: list[dict[str, Any]] = []
    for index, item in enumerate(selected, start=1):
        candidate = candidate_from_version_dir(item.version_dir)
        item_dir = run_dir / safe_name(candidate, index)
        item_dir.mkdir(parents=True, exist_ok=True)
        print(f"[candidate] {index}/{len(selected)} {brief_project_label(candidate)}", flush=True)
        try:
            summary = await run_one(
                candidate,
                item_dir,
                model=args.model,
                timeout_sec=args.timeout_sec,
                threshold=args.threshold,
                profile=args.profile,
                dry_run=args.dry_run,
            )
        except Exception as exc:
            summary = {
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
                "candidate": candidate.__dict__,
                "out_dir": str(item_dir),
            }
            write_json(item_dir / "candidate_summary.json", summary)
            print(f"[error] {candidate.document}: {summary['error']}", flush=True)
        summaries.append(summary)
        write_json(run_dir / "summary.json", {"run_dir": str(run_dir), "items": summaries})

    compact = [
        {
            "document": item.get("candidate", {}).get("document"),
            "version": item.get("candidate", {}).get("version"),
            "status": item.get("status"),
            "accepted": item.get("accepted_present_in_baseline"),
            "accepted_no_image": item.get("accepted_no_image_evidence"),
            "codex_findings": item.get("codex_findings"),
            "strict_match": item.get("strict_accepted_all_matched"),
            "strict_recall": item.get("strict_accepted_all_recall"),
            "loose_match": item.get("loose_accepted_all_matched"),
            "loose_recall": item.get("loose_accepted_all_recall"),
            "latest_unchanged": item.get("project_latest_hashes_unchanged"),
            "error": item.get("error"),
        }
        for item in summaries
    ]
    print(json.dumps({"run_dir": str(run_dir), "items": compact}, ensure_ascii=False, indent=2))
    return 0 if all(item.get("status") in {"done", "dry_run"} for item in summaries) else 1


def main() -> int:
    return asyncio.run(amain())


if __name__ == "__main__":
    raise SystemExit(main())
