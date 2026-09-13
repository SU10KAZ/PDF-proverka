"""Read-only adapter over LineLedger/TextSection and paragraph materialization.

Ambiguous content is quarantined before matching, not interpreted as TEXT.
"""
from collections import Counter, defaultdict
import re
from experiments.text_alignment_v2.units import from_materialization
from experiments.text_safe_coverage.sections import materialize
from experiments.text_comparison_v1.common import digest, read, file_hash

NON_TEXT = re.compile(r'таблиц|спецификац|экспликац|условн\w*\s+обознач|легенд|схем[аы]|чертеж|план\s+\w*этаж', re.I)
BAD_LINE = re.compile(r'<\s*/?(?:table|tr|td|th)\b|\||\.{3,}|\b(?:инн|кпп|огрн)\b', re.I)
PREDICATE = re.compile(r'предусмотр|предусматрива|примен|принят|составля|составит|обеспеч|установ|выполн|долж|требу|допуска|запрещ|измен|равн|проект|работа|потребност|нагрузк|расход|мощност|площад|количеств|режим|модел', re.I)
SPLIT = re.compile(r'(?<=[.!?])\s+(?=[А-ЯЁA-Z])')
MAX_LOCAL_CHARS = 3000


def purity_reasons(unit, block_types, block_headers):
    text = unit['text']
    reasons = []
    if unit.get('source_route') != 'TEXT':
        reasons.append('NON_TEXT_ROUTE')
    if BAD_LINE.search(text):
        reasons.append('STRUCTURED_OR_SERVICE_CONTENT')
    titles = [s['title'] for s in unit.get('section_context', [])]
    titles += [unit.get('nearest_heading') or '']
    if any(NON_TEXT.search(t) for t in titles):
        reasons.append('NON_NARRATIVE_CONTEXT')
    for ref in unit['source_refs']:
        key = (ref['page'], ref['block_id'])
        types = block_types.get(key, set())
        if types != {'text'}:
            reasons.append('UNVERIFIED_TEXT_BLOCK')
        if block_headers.get(key, False):
            reasons.append('MIXED_TABLE_LEGEND_BLOCK')
    if len(re.findall(r'[а-яёa-z]{2,}', text, re.I)) < 4 or not PREDICATE.search(text):
        reasons.append('NO_NARRATIVE_ASSERTION')
    if len(text) > MAX_LOCAL_CHARS:
        reasons.append('LOCAL_BUDGET_EXCEEDED')
    if re.search(r'�|\\(?:frac|cdot|times)|\$', text):
        reasons.append('OCR_OR_FORMULA_AMBIGUITY')
    return sorted(set(reasons))


def build(document):
    for name in ('work_md', 'blocks', 'pdf'):
        r = document['artifacts'][name]
        if file_hash(r['path']) != r['sha256']:
            raise ValueError('Source receipt mismatch: ' + name)
    mat = materialize(document)
    baseline = from_materialization(document, mat)
    raw = read(document['artifacts']['blocks']['path'])
    block_types = defaultdict(set)
    for b in raw.get('blocks', []):
        block_types[(int(b['page_index']) + 1, str(b['block_id']))].add(b.get('block_type', '').lower())
    # Whole-block quarantine is deliberately conservative: many OCR blocks are
    # page-sized and cannot safely isolate untagged table/legend content.
    block_headers = defaultdict(bool)
    for section in mat['sections']:
        if NON_TEXT.search(section['section_title']):
            for ref in section['source_refs']:
                block_headers[(ref['page'], ref['block_id'])] = True
    units, quarantined = [], []
    for parent in baseline['units']:
        reasons = purity_reasons(parent, block_types, block_headers)
        if reasons:
            quarantined.append(dict(unit_id=parent['unit_id'], reasons=reasons,
                                    source_refs=parent['source_refs']))
            continue
        # Split at sentence boundaries only; decimal points and semicolon lists
        # remain intact. Quotes are exact substrings of the ledger paragraph.
        start = 0
        for end in [m.start() for m in SPLIT.finditer(parent['text'])] + [len(parent['text'])]:
            text = parent['text'][start:end].strip()
            offset = parent['text'].find(text, start)
            start = end
            if not text:
                continue
            unit = {**parent, 'text': text,
                    'unit_id': 'pcu_' + digest([parent['unit_id'], offset, text])[:24],
                    'sentence_span': [offset, offset + len(text)],
                    'text_purity_basis': ['FOUNDATION_TEXT_ROUTE', 'RAW_TEXT_BLOCK',
                                          'NO_STRUCTURED_OR_NON_NARRATIVE_CONTEXT', 'NARRATIVE_PREDICATE'],
                    'source_receipts': {k: {x: document['artifacts'][k][x] for x in ('path', 'sha256')}
                                        for k in ('work_md', 'blocks', 'pdf')}}
            if len(re.findall(r'[а-яёa-z]{2,}', text, re.I)) >= 4 and PREDICATE.search(text):
                units.append(unit)
    return dict(units=units, quarantined=quarantined,
                quality=dict(input_units=len(baseline['units']), accepted_sentences=len(units),
                             quarantined_units=len(quarantined),
                             quarantine_reasons=dict(Counter(r for q in quarantined for r in q['reasons'])),
                             foundation_exclusions=baseline['quality']['routes_excluded'],
                             pages=mat['quality']['pages']))
