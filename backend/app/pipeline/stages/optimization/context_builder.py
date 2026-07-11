from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME,
    TEXT_ANALYSIS_FILENAME,
    resolve_existing,
)


_TEXT_STOPWORDS = {
    'план', 'разрез', 'лист', 'узел', 'схема', 'вид', 'фрагмент', 'этаж', 'стр',
    'проект', 'общий', 'данные', 'экспликация', 'помещение', 'зона'
}

_REPEATABILITY_KEYWORDS = (
    'кроншт', 'креп', 'опор', 'рама', 'лоток', 'решет', 'решётк', 'клапан',
    'шкаф', 'щит', 'насос', 'вентил', 'анкер', 'кабель', 'воздуховод', 'труба'
)

_MOUNTING_KEYWORDS = (
    'монтаж', 'креп', 'опор', 'кроншт', 'рама', 'свар', 'болт', 'анкер', 'узел',
    'лоток', 'подвес', 'хомут'
)

_CRITICAL_FINDING_SEVERITIES = {'КРИТИЧЕСКОЕ', 'ЭКОНОМИЧЕСКОЕ'}


@dataclass
class BuildArtifactResult:
    path: Path
    items_count: int = 0


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None


def _clean_text(value: Any) -> str:
    return ' '.join(str(value or '').split())


def _truncate(value: str, limit: int = 220) -> str:
    value = _clean_text(value)
    return value if len(value) <= limit else value[: limit - 3] + '...'


def _normalize_signal_text(value: str) -> str:
    value = _clean_text(value).lower().replace('ё', 'е')
    value = re.sub(r'[^0-9a-zа-я+/.-]+', ' ', value)
    value = re.sub(r'\s+', ' ', value).strip()
    return value


def _looks_meaningful_signal(value: str) -> bool:
    text = _normalize_signal_text(value)
    if len(text) < 4:
        return False
    if text in _TEXT_STOPWORDS:
        return False
    has_letter = bool(re.search(r'[a-zа-я]', text))
    return has_letter


def _extract_signal_terms(block: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for raw in block.get('key_values_read') or []:
        text = _clean_text(raw)
        if _looks_meaningful_signal(text):
            values.append(text)
    summary = _clean_text(block.get('summary'))
    if summary and _looks_meaningful_signal(summary):
        values.append(summary)
    return values


def _finding_blockers(findings_data: Any) -> tuple[dict[tuple[int, str], list[dict[str, Any]]], list[dict[str, Any]]]:
    items = []
    if isinstance(findings_data, dict):
        items = findings_data.get('findings') or findings_data.get('items') or []
    elif isinstance(findings_data, list):
        items = findings_data
    blockers_by_loc: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    normalized_items: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        severity = _clean_text(item.get('severity')).upper()
        page = item.get('page') or 0
        sheet = _clean_text(item.get('sheet'))
        row = {
            'id': _clean_text(item.get('id')),
            'severity': severity,
            'category': _clean_text(item.get('category')),
            'page': page,
            'sheet': sheet,
            'problem': _truncate(item.get('problem') or item.get('description') or ''),
        }
        normalized_items.append(row)
        if severity in _CRITICAL_FINDING_SEVERITIES and page:
            blockers_by_loc[(int(page), sheet)].append(row)
    return blockers_by_loc, normalized_items


def _build_repeatability_summary(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    evidence: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for block in blocks:
        for signal in _extract_signal_terms(block):
            normalized = _normalize_signal_text(signal)
            if not normalized:
                continue
            counts[normalized] += 1
            evidence[normalized].append({
                'block_id': block.get('block_id'),
                'page': block.get('page'),
                'sheet': block.get('sheet'),
                'text': _truncate(signal, 140),
            })
    summary: list[dict[str, Any]] = []
    for key, count in counts.most_common(40):
        if count < 2:
            continue
        refs = evidence[key][:5]
        summary.append({
            'signal': refs[0]['text'],
            'normalized_signal': key,
            'count': count,
            'evidence_refs': refs,
        })
    return summary


def build_optimization_context(
    *,
    project_id: str,
    output_dir: Path,
    project_info: Optional[dict[str, Any]] = None,
    vendor_list_text: str = '',
) -> BuildArtifactResult:
    output_dir = Path(output_dir)
    text_data = _load_json(resolve_existing(output_dir, TEXT_ANALYSIS_FILENAME)) or {}
    blocks_data = _load_json(resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME)) or {}
    findings_data = _load_json(output_dir / '03_findings.json') or {}

    block_rows = []
    raw_blocks = blocks_data.get('block_analyses') or [] if isinstance(blocks_data, dict) else []
    blockers_by_loc, findings_rows = _finding_blockers(findings_data)

    for block in raw_blocks:
        if not isinstance(block, dict):
            continue
        page = int(block.get('page') or 0)
        sheet = _clean_text(block.get('sheet'))
        blockers = blockers_by_loc.get((page, sheet), [])
        block_rows.append({
            'block_id': _clean_text(block.get('block_id')),
            'page': page,
            'sheet': sheet,
            'sheet_type': _clean_text(block.get('sheet_type')),
            'label': _truncate(block.get('label') or ''),
            'summary': _truncate(block.get('summary') or ''),
            'key_values_read': [_truncate(v, 120) for v in (block.get('key_values_read') or [])[:20]],
            'evidence_text_refs': block.get('evidence_text_refs') or [],
            'findings_count': len(block.get('findings') or []),
            'critical_blockers': blockers[:4],
        })

    project_params = text_data.get('project_params') if isinstance(text_data, dict) else {}
    if not isinstance(project_params, dict):
        project_params = {}

    context = {
        'meta': {
            'project_id': project_id,
            'generated_at': datetime.now().isoformat(),
            'stage': 'optimization_context',
            'source_files': {
                'text_analysis': TEXT_ANALYSIS_FILENAME,
                'blocks_analysis': BLOCKS_ANALYSIS_FILENAME,
                'findings': '03_findings.json',
            },
            'block_rows_total': len(block_rows),
            'findings_total': len(findings_rows),
        },
        'project': {
            'section': _clean_text((project_info or {}).get('section') or project_params.get('section_code')),
            'project_name': _clean_text((project_info or {}).get('project_name')),
            'md_file': _clean_text((project_info or {}).get('md_file')),
        },
        'project_params': project_params,
        'vendor_context': {
            'raw_vendor_list': vendor_list_text,
        },
        'findings_summary': {
            'critical_or_economic': [f for f in findings_rows if f['severity'] in _CRITICAL_FINDING_SEVERITIES][:50],
            'all_findings_count': len(findings_rows),
        },
        'repeatability_signals': _build_repeatability_summary(raw_blocks),
        'block_inventory': block_rows,
    }

    path = output_dir / 'optimization_context.json'
    path.write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding='utf-8')
    return BuildArtifactResult(path=path, items_count=len(block_rows))



def _opportunity_score(*, repeatability: int, has_vendor_context: bool, has_blockers: bool, lens: str, evidence_count: int) -> int:
    score = 35
    score += min(25, repeatability * 5)
    score += min(15, evidence_count * 3)
    if has_vendor_context:
        score += 10
    if lens == 'mounting_unification':
        score += 8
    if has_blockers:
        score -= 18
    return max(0, min(100, score))



def _candidate_blockers(page: int, sheet: str, findings_summary: dict[str, Any]) -> list[str]:
    blockers = []
    for item in findings_summary.get('critical_or_economic') or []:
        if int(item.get('page') or 0) == int(page or 0) and _clean_text(item.get('sheet')) == _clean_text(sheet):
            label = item.get('id') or 'finding'
            severity = item.get('severity') or ''
            blockers.append(f'{label}:{severity}')
    return blockers



def build_optimization_candidates(*, project_id: str, output_dir: Path) -> BuildArtifactResult:
    output_dir = Path(output_dir)
    context = _load_json(output_dir / 'optimization_context.json') or {}
    if not isinstance(context, dict):
        context = {}
    block_inventory = context.get('block_inventory') or []
    findings_summary = context.get('findings_summary') or {}
    vendor_raw = _clean_text(((context.get('vendor_context') or {}).get('raw_vendor_list')))
    vendor_available = bool(vendor_raw and 'ограничений заказчика по вендорам нет' not in vendor_raw.lower())
    project_params = context.get('project_params') or {}

    candidates: list[dict[str, Any]] = []
    next_id = 1

    # Lens 1: analog/vendor opportunities from key equipment and vendor-sensitive blocks.
    key_equipment = project_params.get('key_equipment') or []
    if isinstance(key_equipment, list):
        for equipment in key_equipment[:10]:
            text = _truncate(equipment, 180)
            if not text:
                continue
            candidates.append({
                'candidate_id': f'OPT-CAND-{next_id:03d}',
                'lens': 'analog_vendor',
                'type': 'cheaper_analog',
                'title': f'Проверить допустимый аналог для: {text}',
                'hypothesis': 'Есть шанс подобрать допустимый аналог или более удачную закупочную конфигурацию без потери функций.',
                'evidence': [{'source': 'project_params.key_equipment', 'text': text}],
                'related_pages': [],
                'related_sheets': [],
                'confidence': 'medium',
                'blockers': [],
                'opportunity_score': _opportunity_score(repeatability=1, has_vendor_context=vendor_available, has_blockers=False, lens='analog_vendor', evidence_count=1),
            })
            next_id += 1

    for block in block_inventory[:80]:
        page = int(block.get('page') or 0)
        sheet = _clean_text(block.get('sheet'))
        summary_text = ' '.join([_clean_text(block.get('summary')), ' '.join(block.get('key_values_read') or [])]).lower()
        blockers = _candidate_blockers(page, sheet, findings_summary)
        evidence = []
        for text in (block.get('key_values_read') or [])[:4]:
            if _looks_meaningful_signal(text):
                evidence.append({'source': 'block.key_values_read', 'text': _truncate(text, 160), 'page': page, 'sheet': sheet, 'block_id': block.get('block_id')})
        if vendor_available and evidence and any(ch.isdigit() for ch in summary_text):
            candidates.append({
                'candidate_id': f'OPT-CAND-{next_id:03d}',
                'lens': 'analog_vendor',
                'type': 'cheaper_analog',
                'title': f'Проверить альтернативу для блока {block.get("block_id")}',
                'hypothesis': 'В блоке есть конкретные параметры или оборудование, что делает проверку допустимого аналога осмысленной.',
                'evidence': evidence,
                'related_pages': [page] if page else [],
                'related_sheets': [sheet] if sheet else [],
                'confidence': 'medium' if not blockers else 'low',
                'blockers': blockers,
                'opportunity_score': _opportunity_score(repeatability=1, has_vendor_context=True, has_blockers=bool(blockers), lens='analog_vendor', evidence_count=len(evidence)),
            })
            next_id += 1

    # Lens 2: mounting/unification from repeatability signals and mounting keywords.
    repeatability_signals = context.get('repeatability_signals') or []
    for signal in repeatability_signals[:20]:
        text = _clean_text(signal.get('signal'))
        normalized = _clean_text(signal.get('normalized_signal')).lower()
        count = int(signal.get('count') or 0)
        if count < 2:
            continue
        if not any(keyword in normalized for keyword in _REPEATABILITY_KEYWORDS):
            continue
        refs = signal.get('evidence_refs') or []
        page = int((refs[0] or {}).get('page') or 0) if refs else 0
        sheet = _clean_text((refs[0] or {}).get('sheet')) if refs else ''
        blockers = _candidate_blockers(page, sheet, findings_summary)
        lens_type = 'faster_install' if any(keyword in normalized for keyword in _MOUNTING_KEYWORDS) else 'simpler_design'
        candidates.append({
            'candidate_id': f'OPT-CAND-{next_id:03d}',
            'lens': 'mounting_unification',
            'type': lens_type,
            'title': f'Проверить унификацию / упрощение для: {text}',
            'hypothesis': 'Сигнал повторяемости указывает на возможную унификацию узла, крепления или монтажного решения.',
            'evidence': refs,
            'related_pages': sorted({int(r.get('page') or 0) for r in refs if r.get('page')})[:6],
            'related_sheets': sorted({_clean_text(r.get('sheet')) for r in refs if _clean_text(r.get('sheet'))})[:6],
            'confidence': 'high' if count >= 3 and not blockers else 'medium',
            'blockers': blockers,
            'opportunity_score': _opportunity_score(repeatability=count, has_vendor_context=False, has_blockers=bool(blockers), lens='mounting_unification', evidence_count=len(refs)),
        })
        next_id += 1

    candidates.sort(key=lambda item: item.get('opportunity_score', 0), reverse=True)
    payload = {
        'meta': {
            'project_id': project_id,
            'generated_at': datetime.now().isoformat(),
            'stage': 'optimization_candidates',
            'total_candidates': len(candidates),
            'by_lens': dict(Counter(item['lens'] for item in candidates)),
        },
        'candidates': candidates,
    }
    path = output_dir / 'optimization_candidates.json'
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return BuildArtifactResult(path=path, items_count=len(candidates))
