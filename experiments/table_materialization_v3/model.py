"""Source-only table evidence. Every predicate runs; no first-rule-wins chain."""
from collections import Counter, defaultdict
from dataclasses import dataclass, fields
import hashlib
import re
import unicodedata
from functools import lru_cache

from experiments.semantic_foundation_v3 import frozen_v1 as v1

VERSION = 'table-materialization-v3.0.0'
ORDINAL = re.compile(r'^(\d+)[.)]?$')
CHAIN = re.compile(r'^(\d+(?:\.\d+)+)[.;)]?\s+\D')
NUMBER = re.compile(r'^[~≈<>+−-]?\d+(?:[.,]\d+)?(?:\s|$|[%°/xх×])')
ROLES = {
    'position': r'^(?:поз(?:иция)?\.?|№\s*(?:п/?п|п\\п|счетчика.*)|номер)$',
    'label': r'^(?:наименование\b.*|описание|обозначение|name|description)$',
    'unit': r'^(?:ед\.?\s*(?:изм.*|измерения)|единица измерения|unit)$',
    'quantity': r'^(?:кол(?:ичество|[-.]?во)?\.?|quantity)$',
    'model': r'^(?:тип\b.*|марка\b.*|типоразмер|артикул|модуль)$',
    'manufacturer': r'^(?:завод.?изготовитель|поставщик|производитель)$',
    'mass': r'^масса\b.*',
    'notes': r'^(?:примечани[ея]|notes)$',
    'cable': r'^(?:маркировка кабеля|кабель(?:, провод)?|cable)$',
    'direction': r'^(?:направление|начало|конец)$',
    'time': r'^(?:время|time)$',
    'frequency': r'^\d+\s*(?:к?гц)\b.*',
    'total': r'^полное.*',
    'pressure': r'^(?:pressure\b.*|давление(?:,.*)?)$',
    'flow': r'^(?:(?:free head|actual flow)\b.*|расход(?:,.*)?)$',
    'speed': r'^(?:motor speed\b.*|обороты(?:,.*)?)$',
    'appearance': r'^внешний вид$',
    'dimensions': r'^(?:внутренни[ей]\s+(?:размер.*|диаметр.*)|внешний диаметр.*)$',
    'power': r'^(?:потребляемый ток.*|пуст.*|u_n.*)$',
    'building': r'^(?:корпус|этаж|электроприемник|номер пом\.)$',
    'count_room': r'^(?:комнатность|количество ниш.*)$',
    'document': r'^(?:нормативная документация|код\b.*)$',
    'marking': r'^маркировка\b.*$',
    'loop': r'^№\s*шлейфа$',
    'floor': r'^№\s*этажа$',
    'apartment': r'^№\s*квартиры$',
    'serial': r'^заводской номер\b.*$',
}
ROLES = {k: re.compile(v, re.I) for k, v in ROLES.items()}
CELL_ROLE = re.compile('|'.join(f'(?P<{name}>{pattern.pattern})' for name, pattern in ROLES.items()), re.I)
RULES = {
    'EDGE_ORDINAL_FLOW': 'JOIN',
    'EDGE_KEY_FLOW': 'JOIN',
    'EDGE_TYPED_RECORD_FLOW': 'JOIN',
    'SPECIFICATION_ITEM_FLOW': 'JOIN',
    'EXPLICIT_CONTAINER_FLOW': 'JOIN',
    'SEMANTIC_CONTRACT_CHANGE': 'SPLIT',
    'EXPLICIT_TITLE_CHANGE': 'SPLIT',
    'PROVEN_CONTAINER_CHANGE': 'SPLIT',
    'MEANINGFUL_NARRATIVE': 'SPLIT',
    'STRICT_NUMBERING_RESTART': 'SPLIT',
}


def norm(value):
    # Foundation's search normalization removes punctuation: unsuitable for ordinals,
    # decimal data, sentence completion, or hierarchical keys.
    return ' '.join(unicodedata.normalize('NFC', value).replace('**', '').replace('__', '')
                    .casefold().replace('ё', 'е').split())


def parse_cells(line):
    """Remove exactly the two outer delimiters; retain empty and escaped-pipe cells."""
    content = line.strip()
    if not (content.startswith('|') and content.endswith('|')):
        raise ValueError('Not a complete source table row')
    inside = content[1:-1]
    return [c.strip() for c in (re.split(r'(?<!\\)\|', inside) if r'\|' in inside else inside.split('|'))]


def key(prefix, value):
    return prefix + '_' + hashlib.sha256(v1.canonical_bytes(value)).hexdigest()[:24]


def ordinal(cell):
    match = ORDINAL.fullmatch(cell)
    return int(match[1]) if match else None


def chain(cell):
    match = CHAIN.match(cell)
    return tuple(int(v) for v in match[1].split('.')) if match else None


def successor(left, right):
    return bool(left and right and ((len(left) == len(right) and left[:-1] == right[:-1]
                 and right[-1] == left[-1] + 1) or (right[:-1] == left and right[-1] == 1)))


def strict_run(values):
    """Two real numeric records and exclusively +1 increments. Constants cannot reset."""
    return len(values) >= 2 and all(b == a + 1 for a, b in zip(values, values[1:]))


def shape(cells):
    return tuple('EMPTY' if not c else 'NUMBER' if NUMBER.match(c) else 'TEXT' for c in cells)


@lru_cache(maxsize=8192)
def cell_role(cell):
    match = CELL_ROLE.fullmatch(cell)
    return match.lastgroup if match else ''


def column_roles(cells):
    return tuple(cell_role(c) for c in cells)


def semantic_header(cells):
    roles = column_roles(cells)
    # Two independently named columns, not a Markdown separator or letter count.
    return roles if len(set(roles) - {''}) >= 2 else ()


def table_type(roles):
    roles = set(roles)
    observed = {kind for kind, present in {
        'SPECIFICATION': {'label', 'quantity', 'unit'} <= roles,
        'CABLE_JOURNAL': {'cable', 'direction'} <= roles,
        'NOISE_CHARACTERISTICS': 'frequency' in roles,
        'FAN_PERFORMANCE': {'pressure', 'flow'} <= roles,
        'AUTOMATION_MODULES': {'label', 'model', 'quantity'} <= roles and 'unit' not in roles,
    }.items() if present}
    return next(iter(observed)) if len(observed) == 1 else None


def trim_empty(cells):
    end = len(cells)
    while end and not cells[end - 1]:
        end -= 1
    return cells[:end]


@dataclass(frozen=True)
class TableBoundaryEvidence:
    left_anchor: int
    right_anchor: int
    join_evidence: tuple
    split_evidence: tuple
    neutral_evidence: tuple
    conflict: bool
    decision: str
    basis: str
    candidate_join: tuple
    candidate_split: tuple
    witnesses: dict
    rule_strata: dict

    @classmethod
    def collect(cls, left, right, join=(), split=(), neutral=(), witnesses=None, promoted=(), rule_strata=None):
        candidates_j, candidates_s = set(join), set(split)
        promoted = set(promoted)
        rule_strata = rule_strata or {}
        eligible = {r for r in candidates_j | candidates_s if r in promoted or
                    r + '@' + rule_strata.get(r, '') in promoted}
        j, s = candidates_j & eligible, candidates_s & eligible
        neutral = set(neutral) | {'UNPROMOTED:' + x for x in (candidates_j | candidates_s) - eligible}
        # Calibration may withhold certainty, never erase contradictory source evidence.
        # A conflict veto does not promote either unsupported rule to a PROVEN answer.
        conflict = bool(candidates_j and candidates_s)
        if conflict:
            j, s = candidates_j, candidates_s
        decision = 'REVIEW' if conflict or not (j or s) else ('SAME' if j else 'NEW')
        basis = 'CONFLICT' if conflict else ('NO_EVIDENCE' if not (j or s) else 'PROVEN')
        return cls(left, right, tuple(sorted(j)), tuple(sorted(s)), tuple(sorted(neutral)), conflict,
                   decision, basis, tuple(sorted(candidates_j)), tuple(sorted(candidates_s)), witnesses or {}, rule_strata)

    def artifact(self):
        # Serialization is immediate; avoid recursively copying retained source witnesses.
        return {'kind': 'TABLE', **{f.name: getattr(self, f.name) for f in fields(self)}}


def segments(result, raw_lines):
    """Read cells once; ownership remains exclusively in the original ledger."""
    ledger, sem = result['ledger'], result['semantics']
    col, blocks = ledger['columns'], ledger['blocks']
    out = []
    captions = {int(i): c for i, c in sem['captions'].items()}
    for ref, unit in enumerate(sem['units']):
        if unit['kind'] != 'TABLE_SEGMENT':
            continue
        rows = []
        for i in range(unit['first_line'], unit['last_line'] + 1):
            if col['owner'][i] == ref and col['kind'][i] == 'TABLE_ROW':
                surface = tuple(c.replace('**', '').replace('__', '').strip()
                                for c in parse_cells(raw_lines[col['markdown_line'][i] - 1]))
                cells = tuple(norm(c) for c in surface)
                rows.append({'line': i, 'cells': cells, 'header': semantic_header(cells),
                             'surface': surface})
        headers = [r for r in rows if r['header']]
        header = headers[0] if headers else None
        # Column numbering is header evidence only following an independently recognized schema.
        def numbering(r):
            return bool(header and r['line'] > header['line'] and
                        r['cells'] == tuple(str(n) for n in range(1, len(r['cells']) + 1)))
        data = [r for r in rows if any(r['cells']) and not r['header'] and not numbering(r)]
        context = unit['section_context']
        section = sem['units'][context] if context is not None else None
        heading = section.get('heading_line') if section else None
        container = col['normalized_text'][heading] if heading is not None else None
        title_lines = sorted(set(unit['caption_lines']) | {i for i, c in captions.items()
                            if c['table_start'] == unit['first_line'] and c['decision'] == 'CAPTION'})
        title = ' '.join(col['normalized_text'][i] for i in title_lines) or None
        if title:
            title = re.sub(r'^(?:продолжение|окончание)\s+', '', title)
        block = blocks[col['block_ref'][unit['first_line']]]
        stamp = block.get('stamp') or {}
        stable_stamp = {k: norm(stamp[k]) for k in ('code', 'name', 'object') if stamp.get(k)}
        schema = header['header'] if header else ()
        kind = table_type(schema)
        labels = {r['cells'][0] for r in data if r['cells']}
        if {'серия', 'типоразмер', 'вес'} <= labels:
            kind = 'EQUIPMENT_CHARACTERISTICS'
        out.append({'ref': ref, 'unit': unit, 'rows': rows, 'data': data, 'header': header,
                    'schema': schema, 'type': kind, 'container': container,
                    'title': title, 'stamp': stable_stamp, 'page': block['page'],
                    'numbering_lines': [r['line'] for r in rows if numbering(r)]})
    return out


def boundary(left, right, result, promoted=(), repeated_notes=()):
    col = result['ledger']['columns']
    a, b = left['unit']['last_line'], right['unit']['first_line']
    notes = [i for i in range(a + 1, b) if i in repeated_notes]
    gap = [i for i in range(a + 1, b) if col['kind'][i] not in {'FURNITURE', 'CAPTION'} and i not in repeated_notes]
    furniture = [i for i in range(a + 1, b) if col['kind'][i] == 'FURNITURE']
    edge = not gap and right['page'] in {left['page'], left['page'] + 1}
    same_width = left['unit']['first_row']['width'] == right['unit']['first_row']['width']
    same_schema = bool(left['schema'] and left['schema'] == right['schema'])
    ld, rd = left['data'], right['data']
    lc, rc = (ld[-1]['cells'] if ld else ()), (rd[0]['cells'] if rd else ())
    edge_width = bool(lc and rc and len(trim_empty(lc)) == len(trim_empty(rc)))
    compatible = same_schema or same_width or edge_width
    same_title = bool(left['title'] and left['title'] == right['title'])
    same_container = bool(left['container'] and left['container'] == right['container'])
    same_stamp = bool(left['stamp'].get('code') and left['stamp'] == right['stamp'])
    same_spec = left['type'] == right['type'] == 'SPECIFICATION'
    repeated = bool(left['rows'] and right['rows'] and left['rows'][0]['cells'] == right['rows'][0]['cells'])
    neutral, join, split = set(), set(), set()
    facts = {'EDGE_ADJACENCY': edge, 'EQUAL_WIDTH': same_width, 'COMPATIBLE_EDGE_WIDTH': edge_width,
             'SAME_COLUMN_CONTRACT': same_schema, 'REPEATED_FIRST_ROW': repeated,
             'SAME_TITLE': same_title, 'SAME_CONTAINER': same_container, 'SAME_TITLE_BLOCK': same_stamp,
             'SAME_PAGE': left['page'] == right['page'], 'FURNITURE_ONLY_GAP': bool(furniture) and not gap}
    neutral.update(k for k, value in facts.items() if value)
    neutral.add('RIGHT_FIRST_ROW_' + right['unit']['first_row']['kind'])
    witnesses = {'gap': gap, 'furniture': furniture, 'repeated_notes': notes}
    if notes:
        neutral.add('REPEATED_TABLE_NOTE')

    def numbers(data):
        return [(r['line'], ordinal(r['cells'][0])) for r in data if r['cells'] and ordinal(r['cells'][0]) is not None]
    ln, rn = numbers(ld), numbers(rd)
    # Full observed sequences must progress. Do not turn arbitrary numeric endpoints into numbering.
    lrun, rrun = strict_run([n for _, n in ln]), strict_run([n for _, n in rn])
    numbering_flow = bool(ln and rn and (lrun or rrun) and (len(ln) == 1 or lrun)
                          and (len(rn) == 1 or rrun) and rn[0][1] == ln[-1][1] + 1)
    if numbering_flow:
        neutral.add('STRICT_ORDINAL_PROGRESSION')
        witnesses['ordinal'] = [ln[-1][0], rn[0][0]]
    if edge and compatible and numbering_flow:
        join.add('EDGE_ORDINAL_FLOW')
    reset = bool(lrun and rrun and ln[-1][1] > 1 and rn[0][1] == 1)
    if reset:
        neutral.add('NUMBERING_RESTART_IN_SPECIFICATION' if same_spec else 'STRICT_RESET_OBSERVED')
        witnesses['reset'] = [i for i, _ in ln + rn]
        if not same_spec:
            split.add('STRICT_NUMBERING_RESTART')

    lchain = chain(lc[0]) if lc else None
    rchain = chain(rc[0]) if rc else None
    key_flow = successor(lchain, rchain)
    if not key_flow and lc and rc and not rc[0]:
        ltext = next((c for c in reversed(ld[-1]['surface']) if c), '')
        rtext = next((c for c in rd[0]['surface'] if c), '')
        # Unfinished sentence plus lower-case continuation in a preserved value column.
        key_flow = bool(ltext and rtext and rtext[0].islower() and not re.search(r'[.!?;:]$', ltext))
    if key_flow:
        neutral.add('KEY_OR_CELL_CONTINUATION')
        witnesses['key_flow'] = [ld[-1]['line'], rd[0]['line']]
    if edge and compatible and key_flow:
        join.add('EDGE_KEY_FLOW')

    key_column = left['schema'].index('label') if same_spec else 0
    def typed_records(data):
        return [r for r in data if len(r['cells']) > key_column and r['cells'][key_column] and
                ordinal(r['cells'][key_column]) is None and
                any(NUMBER.match(c) for n, c in enumerate(r['cells']) if n != key_column)]
    ltyped, rtyped = typed_records(ld), typed_records(rd)
    if ltyped and rtyped:
        # Specifications use label + model; restarting group-local item numbers is harmless.
        def record_key(r):
            return (r['cells'][key_column], r['cells'][left['schema'].index('model')]
                    if same_spec and 'model' in left['schema'] and len(r['cells']) > left['schema'].index('model') else '')
        lkeys, rkeys = {record_key(r) for r in ltyped}, {record_key(r) for r in rtyped}
        lshapes = {shape(r['cells']) for r in ltyped}
        rshapes = {shape(r['cells']) for r in rtyped}
        typed_flow = bool(lshapes & rshapes and not (lkeys & rkeys))
        if typed_flow:
            neutral.add('DISJOINT_KEYS_WITH_TYPED_DATA_CONTRACT')
            witnesses['typed_records'] = [ltyped[-1]['line'], rtyped[0]['line']]
            if edge and compatible and (same_schema or same_container):
                join.add('EDGE_TYPED_RECORD_FLOW')

    if same_spec and same_schema and edge:
        # Row-level item evidence uses quantity AND unit, independent of equipment group/ordinal.
        qi, ui = left['schema'].index('quantity'), left['schema'].index('unit')
        def item(rows):
            return next((r for r in rows if len(r['cells']) > max(qi, ui) and
                         NUMBER.match(r['cells'][qi]) and r['cells'][ui]), None)
        li, ri = item(ld), item(rd)
        if li and ri:
            join.add('SPECIFICATION_ITEM_FLOW')
            witnesses['items'] = [li['line'], ri['line']]
    forward_numbering = bool(lrun and rrun and rn[0][1] > ln[-1][1])
    if same_title and same_schema and (numbering_flow or key_flow or forward_numbering) and (same_container or same_stamp):
        # Explicit identity permits non-adjacent source pages, still with row evidence.
        join.add('EXPLICIT_CONTAINER_FLOW')

    if left['title'] and right['title'] and left['title'] != right['title']:
        split.add('EXPLICIT_TITLE_CHANGE')
    if left['container'] and right['container'] and left['container'] != right['container']:
        split.add('PROVEN_CONTAINER_CHANGE')
    if gap:
        split.add('MEANINGFUL_NARRATIVE')
    different_types = bool(left['type'] and right['type'] and left['type'] != right['type'])
    different_schema = bool(right['schema'] and ((left['schema'] and left['schema'] != right['schema']) or
                            (not left['schema'] and not same_width)))
    if different_types or different_schema:
        split.add('SEMANTIC_CONTRACT_CHANGE')
        witnesses['schema'] = [left['header']['line'] if left['header'] else left['unit']['first_line'],
                               right['header']['line'] if right['header'] else right['unit']['first_line']]
    location = ('SAME_PAGE' if left['page'] == right['page'] else
                'CROSS_PAGE' if right['page'] == left['page'] + 1 else 'NONADJACENT')
    strata = {r: location + ('_HEADERED' if right['header'] else '_HEADERLESS') if r in join
              else location for r in join | split}
    return TableBoundaryEvidence.collect(a, b, join, split, neutral, witnesses, promoted, strata)


def assemble(result, raw_lines, promoted):
    segs = segments(result, raw_lines)
    col, blocks = result['ledger']['columns'], result['ledger']['blocks']
    note_pages, note_candidates = defaultdict(set), []
    for i, value in enumerate(col['normalized_text']):
        if col['kind'][i] in {'TABLE_ROW', 'TABLE_SEPARATOR'}:
            continue
        if value in {'примечание', 'примечания'} or (i and col['normalized_text'][i - 1] in {'примечание', 'примечания'}):
            note_candidates.append(i)
            note_pages[value].add(blocks[col['block_ref'][i]]['page'])
    repeated_notes = {i for i in note_candidates if len(note_pages[col['normalized_text'][i]]) > 1}
    boundaries = [boundary(a, b, result, promoted, repeated_notes).artifact() for a, b in zip(segs, segs[1:])]
    groups, relations = [], []
    for n, seg in enumerate(segs):
        if not n or boundaries[n - 1]['decision'] != 'SAME':
            groups.append([])
        groups[-1].append(seg)
    ledger, identities, segment_to_table = result['ledger'], [], {}
    occurrences = Counter()
    for group in groups:
        first = group[0]
        semantic = {'document_version': ledger['document_version'], 'container': first['container'],
                    'title': first['title'], 'type': first['type'], 'schema': first['schema'] or
                    ['UNRESOLVED', first['unit']['first_row']['width']]}
        signature = key('structure', semantic)
        occurrence = occurrences[signature]
        occurrences[signature] += 1
        table_key = key('table_v3', {**semantic, 'local_semantic_occurrence': occurrence})
        row_keys, row_ids, ordinals, roles = [], [], [], []
        row_occurrences = Counter()
        for seg in group:
            segment_to_table[seg['ref']] = len(identities)
            for r in seg['rows']:
                fingerprint = hashlib.sha256('\x1f'.join(r['cells']).encode()).hexdigest()[:24]
                occurrence = row_occurrences[fingerprint]
                row_occurrences[fingerprint] += 1
                row_keys.append(fingerprint + (':' + str(occurrence) if occurrence else ''))
                row_ids.append(r['line'])
                is_first_data = (r is seg['rows'][0] and seg['unit']['first_row']['kind'] == 'DATA_LIKE')
                role = ('DATA' if is_first_data else 'SCHEMA' if r['header'] else
                        'COLUMN_NUMBERS' if r['line'] in seg['numbering_lines'] else 'DATA')
                roles.append(role)
                ordinals.append(ordinal(r['cells'][0]) if r['cells'] and role == 'DATA' else None)
        identities.append({'table_key': table_key, 'semantic_structure': semantic,
                           'segments': [s['ref'] for s in group], 'pages': sorted({s['page'] for s in group}),
                           'rows': {'line': row_ids, 'content_key': row_keys, 'explicit_number': ordinals, 'role': roles}})
    for n, edge in enumerate(boundaries):
        if edge['decision'] == 'REVIEW':
            relations.append({'left_table': segment_to_table[segs[n]['ref']],
                              'right_table': segment_to_table[segs[n + 1]['ref']], 'boundary_ref': n})
    return {'schema': 'logical-tables.v3', 'producer_version': VERSION,
            'document_version': ledger['document_version'], 'tables': identities,
            'boundaries': boundaries, 'candidate_relations': relations,
            'row_contract': 'Row identity = (table_key, content_key). Cells are lossless source slices: '
                            'ledger.sources.work_md[ledger.columns.markdown_line[line]-1], parse_cells. '
                            'Pages and block provenance resolve through ledger.columns.block_ref. '
                            'Every content row retained; DATA_LIKE first rows remain DATA; separators remain in ledger.'}


def row_values(table, index, ledger, raw_lines):
    """Public lossless row materialization; no cell strings duplicated in semantic storage."""
    line = table['rows']['line'][index]
    cells = parse_cells(raw_lines[ledger['columns']['markdown_line'][line] - 1])
    block = ledger['blocks'][ledger['columns']['block_ref'][line]]
    return {'row_key': table['table_key'] + '/' + table['rows']['content_key'][index],
            'sequence': index, 'explicit_number': table['rows']['explicit_number'][index],
            'cells': cells, 'key_label_cells': [c for c in cells if c.strip()],
            'page': block['page'], 'line_refs': [line]}
