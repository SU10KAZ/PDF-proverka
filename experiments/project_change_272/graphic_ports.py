"""Explicit diagram port declarations -> circuit states -> one ProjectChange.

Native text must lie inside a pinned GRAPHIC region. A matching complete port
function and mark establishes a local declaration identity, not geometric
connectivity. Supply/return ports combine only with an explicit pair declaration
in each source. No ink delta is interpreted as added or removed equipment.
"""
from collections import defaultdict, Counter
from decimal import Decimal
from pathlib import Path
import re

import fitz

from experiments.project_change_text_v1.contract import validate
from experiments.text_comparison_v1.common import digest

from .inventory import ROOT, read, sha, immutable, now
from .policy import admitted_pairs

PORT = re.compile(r'^(Подающий|Обратный)\s+трубопровод\s+(.+?)\s+([ТTХX]\d+)\s*$', re.I)
PROPERTY = re.compile(r'^([QGТTРP][\w.]*)\s*=\s*([-+]?\d+(?:[.,]\d+)?)\s*(.+?)\s*$')
SIZE = re.compile(r'^(Ду|DN)\s*=\s*(\d+(?:[.,]\d+)?)\s*[xх×]\s*(\d+(?:[.,]\d+)?)\s*$', re.I)
PAIR = re.compile(r'([ТTХX]\d+)\s*/\s*([ТTХX]\d+)\s*[-–]\s*подающий\s*/\s*обратный\s+трубопровод\s+([^\n]+)', re.I)


def code(value):
    return value.upper().translate(str.maketrans('ТХ', 'TX'))


def norm(value):
    return re.sub(r'\s+', ' ', value.casefold().replace('ё', 'е')).strip()


def parent_function(value):
    # Supply "to the system" and return "from the system" share a parent;
    # every remaining service, room, building and function qualifier stays.
    return re.sub(r'^(?:в систему|из системы)\s+', '', value).rstrip(' ,.')


def parse_port(text):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    start = next((i for i, line in enumerate(lines) if PROPERTY.match(line)), None)
    if start is None:
        return None
    header = ' '.join(lines[:start])
    match = PORT.fullmatch(header)
    if not match:
        return None
    values = {}
    for line in lines[start:]:
        size = SIZE.fullmatch(line)
        if size:
            if 'DECLARED_PIPE_SIZE' in values:
                return None
            values['DECLARED_PIPE_SIZE'] = dict(
                value='x'.join(format(Decimal(x.replace(',', '.')).normalize(), 'f') for x in size.groups()[1:]),
                unit=None, quote=line)
            continue
        p = PROPERTY.fullmatch(line)
        if not p:
            continue
        label, number, unit = p.groups()
        label = code(label)
        unit = norm(unit).replace('°с', '°c').replace('³', '3')
        # Unknown/multivalued expressions cannot be scalar state evidence.
        if unit not in {'гкал/ч', 'м3/ч', '°c', 'м.вод.ст.', 'м.вод.ст', 'кпа', 'па', 'квт'}:
            continue
        if label in values:
            return None
        values[label] = dict(value=format(Decimal(number.replace(',', '.')).normalize(), 'f'),
                             unit=unit, quote=line)
    if len(values) < 2:
        return None
    return dict(role='supply' if match[1].casefold() == 'подающий' else 'return',
                function=norm(match[2]), mark=code(match[3]), header=header, values=values)


def extract(document, embargo):
    from .compound_states import observations
    from experiments.project_change_text_v1.engine import evidence as text_evidence
    pdf = fitz.open(document['artifacts']['pdf']['path'])
    geometry = read(document['artifacts']['blocks']['path'])['blocks']
    pages = defaultdict(list)
    for block in geometry:
        if block.get('block_type') == 'image' and block['page_index'] + 1 not in embargo:
            pages[block['page_index'] + 1].append(block)
    ports = []
    declarations = []
    for pageno, regions in pages.items():
        page = pdf[pageno - 1]
        for block in page.get_text('blocks'):
            if block[6] != 0:
                continue
            box = fitz.Rect(block[:4])
            containing = []
            for region in regions:
                c = region['coords_norm']
                rb = fitz.Rect(c[0]*page.rect.width, c[1]*page.rect.height, c[2]*page.rect.width, c[3]*page.rect.height)
                if (box & rb).get_area() >= box.get_area() * .98:
                    containing.append(region)
            if len(containing) != 1:
                continue
            text = block[4].strip()
            locator = dict(kind='native_graphic_annotation', page=pageno,
                           bbox_pdf_points=list(box), region_block_id=containing[0]['block_id'])
            ev = dict(evidence_id='ev_' + digest([document['document_version'], locator, text])[:24],
                route='GRAPHIC', document_version=document['document_version'], document_code=document['document_code'],
                quote=text, source_refs=[], source_receipts={'pdf': {k: document['artifacts']['pdf'][k] for k in ['path', 'sha256']}},
                local_unit_id=containing[0]['block_id'], section_titles=[], locator=locator, text_purity_basis=[])
            for match in PAIR.finditer(text):
                declarations.append(dict(supply=code(match[1]), return_=code(match[2]), quote=match[0], evidence=ev))
            port = parse_port(text)
            if port:
                ports.append(dict(**port, evidence=ev))
    pdf.close()
    # Pair legends can be separate TEXT blocks beside the diagram. Keep that
    # route and exact Markdown lineage; never relabel the legend as GRAPHIC.
    declared_pairs = {(d['supply'], d['return_']) for d in declarations}
    for observed in observations(document):
        unit = observed['unit']
        for match in PAIR.finditer(unit['text']):
            pair = (code(match[1]), code(match[2]))
            if pair not in declared_pairs:
                declarations.append(dict(supply=pair[0], return_=pair[1], quote=match[0], evidence=text_evidence(unit)))
    return dict(ports=ports, pair_declarations=declarations, graphic_pages=len(pages))


def compare(scope, old, new):
    indexes = {side: defaultdict(list) for side in ['old', 'new']}
    for side, observed in [('old', old), ('new', new)]:
        for p in observed['ports']:
            indexes[side][(p['role'], p['function'], p['mark'])].append(p)
    matched = {}
    outcomes = Counter()
    for key in sorted(indexes['old'].keys() & indexes['new'].keys()):
        aa, bb = indexes['old'][key], indexes['new'][key]
        # Unique declared port per document; repeated marks/functions need a
        # broader layout/scope certificate and cannot silently be consolidated.
        if len(aa) != 1 or len(bb) != 1:
            outcomes['NONUNIQUE_PORT_DECLARATION'] += 1
            continue
        matched[key] = (aa[0], bb[0])
    circuits = {}
    for side, observed in [('old', old), ('new', new)]:
        pairs = defaultdict(list)
        for declaration in observed['pair_declarations']:
            pairs[(declaration['supply'], declaration['return_'])].append(declaration)
        circuits[side] = pairs
    events = []
    for pair in sorted(circuits['old'].keys() & circuits['new'].keys()):
        if len(circuits['old'][pair]) != 1 or len(circuits['new'][pair]) != 1:
            outcomes['NONUNIQUE_CIRCUIT_DECLARATION'] += 1
            continue
        supply = [k for k in matched if k[0] == 'supply' and k[2] == pair[0]]
        returning = [k for k in matched if k[0] == 'return' and k[2] == pair[1]]
        if (len(supply) != 1 or len(returning) != 1 or
                parent_function(supply[0][1]) != parent_function(returning[0][1])):
            outcomes['CIRCUIT_FUNCTION_NOT_CORROBORATED'] += 1
            continue
        evidence = {s: [circuits[s][pair][0]['evidence']] for s in ['old', 'new']}
        facts = []
        states = defaultdict(list)
        issues = []
        for key in supply + returning:
            a, b = matched[key]
            for side, port in [('old', a), ('new', b)]:
                evidence[side].append(port['evidence'])
                states[side].append(port['evidence']['quote'])
            if a['values'].keys() != b['values'].keys():
                issues.append('INCOMPLETE_CORRESPONDING_PORT_PROPERTIES')
            for name in sorted(a['values'].keys() & b['values'].keys()):
                x, y = a['values'][name], b['values'][name]
                if x['unit'] != y['unit']:
                    issues.append('UNVERIFIED_UNIT_CHANGE')
                    continue
                if x['value'] == y['value']:
                    continue
                facts.append(dict(fact_id='fact_' + digest([scope, key, name, x, y])[:24],
                    property=key[0] + ':' + name, old=x, new=y,
                    evidence_old=[a['evidence']['evidence_id']], evidence_new=[b['evidence']['evidence_id']]))
        if not facts:
            outcomes['CIRCUIT_STATE_UNCHANGED'] += 1
            continue
        function = supply[0][1]
        event_key = digest([scope, 'explicit-diagram-circuit', pair, function, sorted(f['fact_id'] for f in facts)])
        ent = dict(entity_id='graphic_subject_' + digest([scope, pair, function])[:24],
            system=function, equipment_class=None, mark='/'.join(pair), equipment_model_old=None, equipment_model_new=None,
            room=None, floor=None, engineering_function=function, semantic_subject=function,
            scope_key=scope, identity_basis=['Unique explicit port marks AND full identical function',
                'Explicit supply/return pair declaration in both PDFs', 'Native annotation contained in pinned GRAPHIC region'],
            resolution='AMBIGUOUS' if issues else 'EXPLICIT')
        change = dict(project_change_id='pc_' + digest(event_key)[:24], event_key=event_key, comparison_scope=scope,
            change_type='ENGINEERING_SOLUTION_CHANGED', engineering_subject=ent,
            old_state='\n'.join(states['old']), new_state='\n'.join(states['new']),
            short_summary_ru='Изменены расчетные параметры контура ' + '/'.join(pair) + ' (' + function + ').',
            importance='MATERIAL', scope='SYSTEM', status='REVIEW' if issues else 'PROVEN', confidence='LOW' if issues else 'HIGH',
            evidence_old=evidence['old'], evidence_new=evidence['new'], supporting_fact_changes=facts,
            decision_reasons=['One explicitly paired diagram circuit owns all changed operating properties. No topology, added equipment, or removed equipment inferred.'],
            review_reasons=sorted(set(issues)), routes=sorted({e['route'] for evs in evidence.values() for e in evs}), conflicts=[])
        validate(change, text_only=False)
        events.append(change)
        outcomes['CIRCUIT_STATE_CHANGED'] += 1
    return dict(project_changes=events, outcomes=dict(outcomes))


def run(name='07_graphic_declared_circuits'):
    admitted_pairs('DEV')
    base = ROOT / 'sources/DEV'
    out = ROOT / 'cycles' / name
    immutable(out / 'MANIFEST.json', dict(started_at=now(), partition='DEV', split_sha256=sha(ROOT/'SPLIT.json'),
        code_sha256=sha(__file__), change='Explicit diagram circuits, native annotated states and declared port pairing'))
    events = []
    for pair in read(base / 'PAIRS.json'):
        sources = {s: extract(pair[s], pair['embargo_pages'][s]) for s in ['old', 'new']}
        result = compare(pair['pair_key'], sources['old'], sources['new'])
        immutable(out / 'pairs' / (str(pair['index']) + '.json'), dict(sources=sources, result=result))
        events += [dict(pair_index=pair['index'], change=c) for c in result['project_changes']]
        print(pair['index'], {s: len(sources[s]['ports']) for s in sources}, result['outcomes'], flush=True)
    immutable(out / 'RESULTS.json', dict(project_changes=events, adjudication='NOT_ADJUDICATED'))


if __name__ == '__main__':
    import sys
    run(sys.argv[1] if len(sys.argv)>1 else '07_graphic_declared_circuits')
