"""Deterministic MD pre-scan for Stage 01 text analysis.

The scanner is intentionally conservative: it only flags repeatable text
patterns that are common blind spots for LLM-only text analysis, and it never
tries to replace engineering judgement. Its output is used in two ways:

* prompt hints before the LLM run;
* optional post-processing of ``01_text_analysis.json`` to add missed
  high-confidence text findings and backfill text-block evidence.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


SEVERITIES = {
    "КРИТИЧЕСКОЕ",
    "ЭКОНОМИЧЕСКОЕ",
    "ЭКСПЛУАТАЦИОННОЕ",
    "РЕКОМЕНДАТЕЛЬНОЕ",
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
}


@dataclass(frozen=True)
class MdBlock:
    block_id: str
    block_type: str
    text: str


@dataclass(frozen=True)
class MdPage:
    page: int
    text: str
    blocks: tuple[MdBlock, ...] = ()

    @property
    def text_block_ids(self) -> list[str]:
        return [b.block_id for b in self.blocks if b.block_type == "TEXT"]


@dataclass
class PrescanFinding:
    key: str
    severity: str
    category: str
    source: str
    finding: str
    norm: str = ""
    norm_quote: str | None = None
    related_block_ids: list[str] = field(default_factory=list)
    keywords: tuple[str, ...] = ()

    def to_text_finding(self, finding_id: str) -> dict[str, Any]:
        severity = self.severity if self.severity in SEVERITIES else "РЕКОМЕНДАТЕЛЬНОЕ"
        return {
            "id": finding_id,
            "severity": severity,
            "category": self.category,
            "source": self.source,
            "finding": self.finding,
            "norm": self.norm,
            "norm_quote": self.norm_quote,
            "related_block_ids": sorted(set(self.related_block_ids)),
        }


def _norm_text(value: Any) -> str:
    text = str(value or "").lower().replace("ё", "е")
    text = text.replace("×", "x")
    text = re.sub(r"(?<=\d)\s*[xх]\s*(?=\d)", " x ", text)
    text = re.sub(r"[^0-9a-zа-я]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _split_pages(md_text: str) -> list[MdPage]:
    headers = list(re.finditer(r"(?m)^##\s+СТРАНИЦА\s+(\d+)\s*$", md_text))
    if not headers:
        return [MdPage(page=0, text=md_text, blocks=_split_blocks(md_text))]

    pages: list[MdPage] = []
    for idx, header in enumerate(headers):
        start = header.end()
        end = headers[idx + 1].start() if idx + 1 < len(headers) else len(md_text)
        body = md_text[start:end]
        page_no = int(header.group(1))
        pages.append(MdPage(page=page_no, text=body, blocks=_split_blocks(body)))
    return pages


def _split_blocks(page_text: str) -> tuple[MdBlock, ...]:
    headers = list(
        re.finditer(r"(?m)^###\s+BLOCK\s+\[(TEXT|IMAGE)\]:\s+([A-Z0-9-]+)\s*$", page_text)
    )
    blocks: list[MdBlock] = []
    for idx, header in enumerate(headers):
        start = header.end()
        end = headers[idx + 1].start() if idx + 1 < len(headers) else len(page_text)
        blocks.append(
            MdBlock(
                block_id=header.group(2),
                block_type=header.group(1),
                text=page_text[start:end],
            )
        )
    return tuple(blocks)


def _first_text_block(page: MdPage, match_text: str = "") -> list[str]:
    if match_text:
        needle = _norm_text(match_text)
        for block in page.blocks:
            if block.block_type != "TEXT":
                continue
            if needle and needle[:80] in _norm_text(block.text):
                return [block.block_id]
    return page.text_block_ids[:3]


def _find_pages(pages: Iterable[MdPage], pattern: str, flags: int = re.I | re.S) -> list[tuple[MdPage, re.Match[str]]]:
    result: list[tuple[MdPage, re.Match[str]]] = []
    rx = re.compile(pattern, flags)
    for page in pages:
        match = rx.search(page.text)
        if match:
            result.append((page, match))
    return result


def _append_unique(items: list[PrescanFinding], item: PrescanFinding) -> None:
    if any(existing.key == item.key for existing in items):
        return
    items.append(item)


def scan_md_text(md_text: str) -> list[PrescanFinding]:
    pages = _split_pages(md_text)
    full_norm = _norm_text(md_text)
    findings: list[PrescanFinding] = []

    # Incorrect SP shorthands like 60.1330 instead of 60.13330 / 7.13130.
    sp_typo_pages = _find_pages(pages, r"СП\s+\d{1,3}\.1330\.\d{4}")
    if sp_typo_pages:
        page, match = sp_typo_pages[0]
        refs = sorted(set(re.findall(r"СП\s+\d{1,3}\.1330\.\d{4}", md_text, flags=re.I)))
        _append_unique(
            findings,
            PrescanFinding(
                key="sp_1330_typo",
                severity="РЕКОМЕНДАТЕЛЬНОЕ",
                category="normative_refs",
                source=f"MD стр. {page.page} / перечень применяемых норм",
                finding=(
                    "В шифрах сводов правил встречается индекс «1330» вместо корректного "
                    "«13330»/«13130»: " + ", ".join(refs[:8]) + ". Нужно проверить и "
                    "исправить обозначения нормативных документов."
                ),
                norm="ГОСТ Р 21.101-2020, требования к ссылкам на НТД",
                related_block_ids=_first_text_block(page, match.group(0)),
                keywords=("1330", "сп"),
            ),
        )

    if "сп 18 1330 2022" in full_norm:
        page = next((p for p in pages if "сп 18 1330 2022" in _norm_text(p.text)), pages[0])
        _append_unique(
            findings,
            PrescanFinding(
                key="sp_118_missing_digit",
                severity="РЕКОМЕНДАТЕЛЬНОЕ",
                category="normative_refs",
                source=f"MD стр. {page.page} / перечень применяемых норм",
                finding=(
                    "Указан шифр «СП 18.1330.2022» для общественных зданий. Для этой нормы "
                    "ожидается СП 118.13330.2022; в текущем виде пропущена цифра и потерян "
                    "знак в индексе."
                ),
                norm="СП 118.13330.2022",
                related_block_ids=_first_text_block(page, "СП 18.1330.2022"),
                keywords=("сп 18 1330 2022", "118 13330"),
            ),
        )

    for page, match in _find_pages(pages, r"ППБ-01-2003"):
        _append_unique(
            findings,
            PrescanFinding(
                key="ppb_01_2003_outdated",
                severity="РЕКОМЕНДАТЕЛЬНОЕ",
                category="normative_refs",
                source=f"MD стр. {page.page} / указания по монтажу",
                finding=(
                    "Есть ссылка на ППБ-01-2003. Документ отменён; ссылку нужно заменить "
                    "на действующие правила противопожарного режима или убрать, если "
                    "требование уже покрыто СП."
                ),
                norm="Постановление Правительства РФ от 16.09.2020 №1479",
                related_block_ids=_first_text_block(page, match.group(0)),
                keywords=("ппб 01 2003",),
            ),
        )
        break

    for page, match in _find_pages(pages, r"ГОСТ\s+2822-75"):
        _append_unique(
            findings,
            PrescanFinding(
                key="gost_2822_75",
                severity="КРИТИЧЕСКОЕ",
                category="piping",
                source=f"MD стр. {page.page} / трубопроводы",
                finding=(
                    "В тексте указан ГОСТ 2822-75 для стальных водогазопроводных труб. "
                    "Это выглядит как ошибочная ссылка: для таких труб применяется ГОСТ 3262-75, "
                    "который также встречается в спецификации."
                ),
                norm="ГОСТ 3262-75; ГОСТ Р 21.101-2020",
                related_block_ids=_first_text_block(page, match.group(0)),
                keywords=("гост 2822 75", "гост 3262 75"),
            ),
        )
        break

    for page, match in _find_pages(
        pages,
        r"(?:гидравлическ\w*\s+испытан\w*|испытан\w*\s+давлен\w*)[\s\S]{0,220}20\s*кПа",
    ):
        _append_unique(
            findings,
            PrescanFinding(
                key="hydraulic_test_20_kpa",
                severity="КРИТИЧЕСКОЕ",
                category="piping",
                source=f"MD стр. {page.page} / указания по монтажу",
                finding=(
                    "В указаниях по гидравлическим испытаниям задан минимум «20 кПа». "
                    "Для систем отопления это на порядки ниже типового минимального "
                    "испытательного давления и требует исправления."
                ),
                norm="СП 73.13330.2016, требования к испытанию систем отопления",
                related_block_ids=_first_text_block(page, match.group(0)),
                keywords=("20 кпа", "гидравлическ", "испытан"),
            ),
        )
        break

    for page, match in _find_pages(pages, r"\bclosets\b"):
        _append_unique(
            findings,
            PrescanFinding(
                key="closets_temperature_label",
                severity="РЕКОМЕНДАТЕЛЬНОЕ",
                category="heating",
                source=f"MD стр. {page.page} / расчётные параметры внутреннего воздуха",
                finding=(
                    "В перечне внутренних температур встречается фраза «жилая комната closets». "
                    "Это похоже на OCR/редакционную ошибку и может искажать назначение помещения "
                    "и принятую температуру."
                ),
                norm="ГОСТ 30494-2011",
                related_block_ids=_first_text_block(page, match.group(0)),
                keywords=("closets",),
            ),
        )
        break

    if "установленная мощность электродвигателей" in full_norm and re.search(r"КЭВ-\s*\d+", md_text, re.I):
        page = next(
            (p for p in pages if "установленная мощность электродвигателей" in _norm_text(p.text)),
            pages[0],
        )
        _append_unique(
            findings,
            PrescanFinding(
                key="blank_installed_motor_power",
                severity="ПРОВЕРИТЬ ПО СМЕЖНЫМ",
                category="documentation",
                source=f"MD стр. {page.page} / основные показатели марки ОВ",
                finding=(
                    "В таблице основных показателей есть графа «Установленная мощность "
                    "электродвигателей», но рядом в документе присутствуют тепловые завесы "
                    "с электродвигателями. Нужно проверить, не оставлена ли графа пустой "
                    "или ошибочно вынесенной в смежный раздел."
                ),
                norm="ГОСТ 21.602-2016",
                related_block_ids=_first_text_block(page, "Установленная мощность электродвигателей"),
                keywords=("установленная мощность электродвигателей", "кэв"),
            ),
        )

    if "сп 50 13330" not in full_norm and "гост 30494" not in full_norm:
        has_indoor_air = "расчетные параметры внутреннего воздуха" in full_norm
        has_heat_load = "расход теплоты" in full_norm or "теплов" in full_norm
        if has_indoor_air and has_heat_load:
            page = next((p for p in pages if "расчетные параметры внутреннего воздуха" in _norm_text(p.text)), pages[0])
            _append_unique(
                findings,
                PrescanFinding(
                    key="missing_microclimate_norm_refs",
                    severity="РЕКОМЕНДАТЕЛЬНОЕ",
                    category="normative_refs",
                    source=f"MD стр. {page.page} / перечень применяемых норм",
                    finding=(
                        "В документе есть расчётные параметры внутреннего воздуха и тепловые "
                        "нагрузки, но в перечне норм не найдены СП 50.13330 и ГОСТ 30494. "
                        "Нужно проверить полноту нормативной базы для теплотехники и микроклимата."
                    ),
                    norm="СП 50.13330.2012; ГОСТ 30494-2011",
                    related_block_ids=_first_text_block(page, "Расчетные параметры внутреннего воздуха"),
                    keywords=("сп 50 13330", "гост 30494", "микроклимат"),
                ),
            )

    bvr_rows = []
    for line in md_text.splitlines():
        if "BVR-R DN15" not in line:
            continue
        if "|" not in line:
            continue
        code_match = re.search(r"\b0[0-9A-ZА-Я]{3,}\b", line)
        if code_match:
            bvr_rows.append((line.strip(), code_match.group(0)))
    bvr_codes = sorted({code for _, code in bvr_rows})
    if len(bvr_codes) >= 2:
        page = next(
            (p for p in pages if "BVR-R DN15" in p.text and p.text_block_ids),
            next((p for p in pages if "BVR-R DN15" in p.text), pages[0]),
        )
        _append_unique(
            findings,
            PrescanFinding(
                key="bvr_r_dn15_duplicate_codes",
                severity="ЭКОНОМИЧЕСКОЕ",
                category="documentation",
                source=f"MD стр. {page.page} / спецификация арматуры",
                finding=(
                    "В спецификации несколько позиций с маркой BVR-R DN15 и разными кодами "
                    f"продукции ({', '.join(bvr_codes[:6])}). Нужно развести наименования "
                    "и исполнения, чтобы исключить ошибку закупки."
                ),
                norm="ГОСТ Р 21.1101-2013 / требования однозначности спецификации",
                related_block_ids=_first_text_block(page, "BVR-R DN15"),
                keywords=("bvr r dn15", "код продукции"),
            ),
        )

    if re.search(r"\bУ\d(?:\.\d)?\b", md_text) and re.search(r"теплов\w*\s+завес|КЭВ-\s*\d+", md_text, re.I):
        page = next(
            (
                p
                for p in pages
                if re.search(r"\bУ\d(?:\.\d)?\b", p.text)
                and re.search(r"теплов\w*\s+завес|КЭВ-\s*\d+", p.text, re.I)
            ),
            pages[0],
        )
        _append_unique(
            findings,
            PrescanFinding(
                key="air_curtain_u_designation",
                severity="РЕКОМЕНДАТЕЛЬНОЕ",
                category="documentation",
                source=f"MD стр. {page.page} / характеристика систем",
                finding=(
                    "Воздушно-тепловые завесы обозначены индексами вида «У1.1», «У2.1» и т.п. "
                    "Нужно проверить, не должен ли применяться более однозначный индекс "
                    "«ВТЗ»/«ЗВТ», чтобы не смешивать завесы с установками."
                ),
                norm="ГОСТ 21.602-2016",
                related_block_ids=_first_text_block(page, "тепловые завес"),
                keywords=("тепловые завесы", "втз", "у1"),
            ),
        )

    if "закупается арендатором" in full_norm and re.search(r"КЭВ-\s*\d+", md_text, re.I):
        page = next((p for p in pages if "закупается арендатором" in _norm_text(p.text)), pages[0])
        _append_unique(
            findings,
            PrescanFinding(
                key="tenant_supply_air_curtains_ambiguous",
                severity="РЕКОМЕНДАТЕЛЬНОЕ",
                category="documentation",
                source=f"MD стр. {page.page} / спецификация тепловых завес",
                finding=(
                    "В спецификации тепловых завес есть примечание «Закупается арендатором». "
                    "Нужно проверить, однозначно ли отделены завесы МОП от завес аренды и "
                    "не попадут ли позиции в неправильную зону ответственности закупки."
                ),
                norm="ГОСТ Р 21.1101-2013 / требования однозначности ведомостей",
                related_block_ids=_first_text_block(page, "Закупается арендатором"),
                keywords=("закупается арендатором", "кэв"),
            ),
        )

    pexa_sleeve = _find_pages(
        pages,
        r"(?:108\s*(?:x|×|х|\\times)\s*2[,.]2[\s\S]{0,120}сшитого полиэтилена|сшитого полиэтилена[\s\S]{0,220}108\s*(?:x|×|х|\\times)\s*2[,.]2)",
    )
    if pexa_sleeve:
        page, match = pexa_sleeve[0]
        _append_unique(
            findings,
            PrescanFinding(
                key="pexa_sleeve_108x22",
                severity="ЭКСПЛУАТАЦИОННОЕ",
                category="piping",
                source=f"MD стр. {page.page} / таблица гильз",
                finding=(
                    "В таблице гильз для труб из сшитого полиэтилена указан диаметр "
                    "«108×2,2». Это не согласуется с типовыми PE-Xa трубами малых диаметров "
                    "и требует проверки по спецификации."
                ),
                norm="ГОСТ 21.602-2016",
                related_block_ids=_first_text_block(page, match.group(0)),
                keywords=("108x2 2", "сшитого полиэтилена", "гильз"),
            ),
        )

    insulation_mentions = re.search(r"толщин\w*\s+13\s*мм|толщин\w*\s+25\s*мм", md_text, re.I)
    if insulation_mentions and "расчет" not in _norm_text(md_text[max(0, insulation_mentions.start() - 400): insulation_mentions.end() + 400]):
        page = next((p for p in pages if insulation_mentions.group(0) in p.text), pages[0])
        _append_unique(
            findings,
            PrescanFinding(
                key="insulation_thickness_without_calc_ref",
                severity="РЕКОМЕНДАТЕЛЬНОЕ",
                category="insulation",
                source=f"MD стр. {page.page} / трубопроводы, изоляция",
                finding=(
                    "Толщина теплоизоляции трубопроводов задана числом, но рядом не найдена "
                    "ссылка на расчёт по СП 61.13330. Нужно проверить расчётное обоснование "
                    "толщины изоляции."
                ),
                norm="СП 61.13330.2012",
                related_block_ids=_first_text_block(page, insulation_mentions.group(0)),
                keywords=("толщина", "изоляц", "сп 61"),
            ),
        )

    if "тип системы" in full_norm and "т11 3" in full_norm and "аренд" in full_norm:
        page = next((p for p in pages if "тип системы" in _norm_text(p.text) and "т11 3" in _norm_text(p.text)), pages[0])
        _append_unique(
            findings,
            PrescanFinding(
                key="system_type_legend_mismatch",
                severity="РЕКОМЕНДАТЕЛЬНОЕ",
                category="documentation",
                source=f"MD стр. {page.page} / условные обозначения систем",
                finding=(
                    "В условных обозначениях встречаются индексы Т11.3/Т21.3 для аренды, "
                    "а легенда «Тип системы» расшифровывает индекс 3 как теплоснабжение. "
                    "Нужно проверить согласованность кодировки систем."
                ),
                norm="ГОСТ 21.602-2016",
                related_block_ids=_first_text_block(page, "Тип системы"),
                keywords=("тип системы", "т11 3", "аренд"),
            ),
        )

    return findings


def scan_md_file(md_file_path: str | Path) -> list[PrescanFinding]:
    path = Path(md_file_path)
    if not path.exists():
        return []
    try:
        return scan_md_text(path.read_text(encoding="utf-8"))
    except OSError:
        return []


def build_prescan_prompt_section(md_file_path: str | Path, *, max_items: int = 14) -> str:
    findings = scan_md_file(md_file_path)
    if not findings:
        return ""
    lines = [
        "## Deterministic MD Pre-scan: обязательные точки перепроверки",
        "",
        "Ниже перечислены высокоуверенные текстовые паттерны, найденные до LLM. "
        "Не копируй их слепо: проверь контекст в MD, но если подтверждается — включи "
        "как отдельные `text_findings[]` с `related_block_ids`.",
        "",
    ]
    for item in findings[:max_items]:
        blocks = ", ".join(item.related_block_ids) if item.related_block_ids else "нет block_id"
        lines.append(
            f"- `{item.key}` [{item.severity}] {item.source}; block_ids: {blocks}; "
            f"проверить: {item.finding}"
        )
    return "\n".join(lines)


def _existing_text_blob(findings: Iterable[dict[str, Any]]) -> str:
    parts: list[str] = []
    for finding in findings:
        parts.append(str(finding.get("finding") or ""))
        parts.append(str(finding.get("source") or ""))
        parts.append(str(finding.get("category") or ""))
    return _norm_text(" ".join(parts))


def _is_covered(candidate: PrescanFinding, findings: Iterable[dict[str, Any]]) -> bool:
    blob = _existing_text_blob(findings)
    if not blob:
        return False
    if candidate.key == "sp_118_missing_digit" and "сп 18 1330 2022" in blob:
        return True
    keyword_hits = 0
    for keyword in candidate.keywords:
        key = _norm_text(keyword)
        if key and key in blob:
            keyword_hits += 1
    if candidate.keywords and keyword_hits >= min(2, len(candidate.keywords)):
        return True
    candidate_text = _norm_text(candidate.finding)
    candidate_tokens = set(candidate_text.split())
    if not candidate_tokens:
        return False
    for finding in findings:
        existing = _norm_text(finding.get("finding") or "")
        existing_tokens = set(existing.split())
        if not existing_tokens:
            continue
        overlap = len(candidate_tokens & existing_tokens) / max(1, len(candidate_tokens))
        if overlap >= 0.62:
            return True
    return False


def _next_text_id(findings: Iterable[dict[str, Any]]) -> int:
    max_id = 0
    for finding in findings:
        match = re.match(r"^T-(\d{3})$", str(finding.get("id") or ""))
        if match:
            max_id = max(max_id, int(match.group(1)))
    return max_id + 1


def _pages_by_number(md_text: str) -> dict[int, MdPage]:
    return {page.page: page for page in _split_pages(md_text)}


def _source_pages(source: str) -> list[int]:
    pages = []
    for match in re.finditer(r"(?:MD\s*)?стр(?:\.|аница)?\s*(\d+)", source or "", flags=re.I):
        pages.append(int(match.group(1)))
    return pages


def _backfill_related_blocks(
    findings: list[dict[str, Any]],
    prescan: list[PrescanFinding],
    md_text: str,
) -> int:
    pages = _pages_by_number(md_text)
    changed = 0
    for finding in findings:
        existing = finding.get("related_block_ids")
        if isinstance(existing, list) and existing:
            continue
        blocks: list[str] = []
        finding_blob = _norm_text(finding.get("finding") or "")
        for candidate in prescan:
            if candidate.related_block_ids and _is_covered(candidate, [finding]):
                blocks.extend(candidate.related_block_ids)
        if not blocks:
            for page_no in _source_pages(str(finding.get("source") or "")):
                page = pages.get(page_no)
                if page:
                    blocks.extend(page.text_block_ids[:3])
        if blocks:
            finding["related_block_ids"] = sorted(set(blocks))
            changed += 1
    return changed


def augment_text_analysis_file(
    output_path: str | Path,
    md_file_path: str | Path,
    *,
    write_prescan_report: bool = True,
) -> dict[str, Any]:
    """Backfill and append deterministic Stage 01 text findings.

    Returns a small summary. Failures are deliberately left to the caller so the
    stage runner can decide whether to warn or ignore.
    """
    output = Path(output_path)
    md_path = Path(md_file_path)
    data = json.loads(output.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("text_findings"), list):
        return {"changed": False, "added": 0, "backfilled": 0, "prescan_total": 0}
    md_text = md_path.read_text(encoding="utf-8")
    prescan = scan_md_text(md_text)
    findings: list[dict[str, Any]] = data["text_findings"]

    backfilled = _backfill_related_blocks(findings, prescan, md_text)

    added = 0
    next_id = _next_text_id(findings)
    for candidate in prescan:
        if _is_covered(candidate, findings):
            continue
        findings.append(candidate.to_text_finding(f"T-{next_id:03d}"))
        next_id += 1
        added += 1

    if added or backfilled:
        output.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    if write_prescan_report:
        report_path = output.parent / "01_text_prescan.json"
        report = {
            "md_file": str(md_path),
            "prescan_total": len(prescan),
            "added": added,
            "backfilled": backfilled,
            "candidates": [
                {
                    "key": item.key,
                    "severity": item.severity,
                    "category": item.category,
                    "source": item.source,
                    "finding": item.finding,
                    "related_block_ids": item.related_block_ids,
                }
                for item in prescan
            ],
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "changed": bool(added or backfilled),
        "added": added,
        "backfilled": backfilled,
        "prescan_total": len(prescan),
    }
