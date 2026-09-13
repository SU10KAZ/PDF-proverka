"""Read-only TEXT and frozen Table V3 adapters, with PDF layout purity veto."""
from collections import Counter
from functools import lru_cache
from pathlib import Path
import json
import re

import fitz
from experiments.text_comparison_v1.common import read, write, digest, file_hash
from experiments.project_change_text_v1.engine import evidence as text_evidence, titles
from experiments.table_materialization_v3.model import row_values
from experiments.table_project_change_v1.source import evidence as cell_evidence
from experiments.table_project_change_v1.source import role
from experiments.text_old_scope_recovery_v1.retrieval import tokens
from .core import features, make_subject, norm, rank

AUDITS = Path('/home/coder/auditmanager/corpus-audits')
ROOT = AUDITS / '20260913_engineering_subject_resolver_v1'
TEXT = AUDITS / '20260913_text_old_scope_recovery_v1'
BASE = AUDITS / '20260913_project_change_text_v1'
TABLE = AUDITS / '20260913_table_project_change_v1'
V3 = AUDITS / '20260913_table_materialization_v3/candidate_controls_optimized_run1'
TABLE_LIMIT = 12


@lru_cache(maxsize=32)
def pdf(path): return fitz.open(path)


@lru_cache(maxsize=512)
def grids(path, page_number):
    page = pdf(path)[page_number - 1]
    result = []
    for table in page.find_tables().tables:
        box = fitz.Rect(table.bbox)
        # Exclude drawing stamps and marginal title blocks by geometry only.
        # A sheet frame has one giant prose cell and small page-number/stamp
        # cells. Only substantive parallel content above the stamp is a table.
        substantive = any(sum(len((v or '').strip()) >= 4 and
                              cell is not None and cell[1] < .8 * page.rect.height
                              for v, cell in zip(values, row.cells)) >= 2
                          for values, row in zip(table.extract(), table.rows))
        if (substantive and table.col_count >= 2 and table.row_count >= 1 and
                box.width >= .4 * page.rect.width and box.height >= .06 * page.rect.height and
                box.y0 < .8 * page.rect.height): result.append(list(box))
    return result


@lru_cache(maxsize=32)
def block_map(path):
    return {b['block_id']: b for b in read(path)['blocks']}


def purity(unit):
    receipts = unit['source_receipts']; issues = []; checks = []
    for ref in unit['source_refs']:
        b = block_map(receipts['blocks']['path'])[ref['block_id']]
        page = pdf(receipts['pdf']['path'])[ref['page'] - 1]
        if b.get('block_type', '').lower() != 'text': issues.append('NON_TEXT_BLOCK')
        coords = b.get('coords_norm')
        if not coords:
            issues.append('SOURCE_GEOMETRY_UNPROVEN'); continue
        rect = fitz.Rect(coords[0] * page.rect.width, coords[1] * page.rect.height,
                         coords[2] * page.rect.width, coords[3] * page.rect.height)
        boxes = grids(receipts['pdf']['path'], ref['page'])
        fraction = max(((rect & fitz.Rect(g)).get_area() / max(1, rect.get_area()) for g in boxes), default=0)
        query_words = set(re.findall(r'[а-яa-z]{4,}', norm(unit['text'])))
        grid_text = ' '.join(page.get_textbox(fitz.Rect(g)) for g in boxes)
        grid_words = set(re.findall(r'[а-яa-z]{4,}', norm(grid_text)))
        lexical_overlap = len(query_words & grid_words) / max(1,len(query_words))
        checks.append(dict(page=ref['page'], block_id=ref['block_id'], grid_overlap=round(fraction, 4),
                           lexical_overlap=round(lexical_overlap,4), table_boxes=boxes))
        if fraction >= .10 and lexical_overlap >= .65: issues.append('TABLE_GRID_DOMINATES_TEXT_BLOCK')
        elif fraction >= .10 and lexical_overlap >= .35: issues.append('MIXED_TABLE_TEXT_BLOCK')
    return ('PROVEN' if not issues else 'TABLE' if 'TABLE_GRID_DOMINATES_TEXT_BLOCK' in issues else 'UNPROVEN',
            dict(issues=sorted(set(issues)), checks=checks))


def text_subject(unit, side, pair, context=None):
    pure, audit = purity(unit)
    clue = features(unit['text'], ' / '.join(titles(unit)), pair[side].get('discipline', ''))
    return make_subject(source_type='TEXT', side=side.upper(), version=unit['document_version'],
        scope=pair['pair_key'], text=unit['text'], evidence=[text_evidence(unit)], clues=clue,
        context=context, purity=pure, local_id=unit['unit_id'], unit=unit, purity_audit=audit)


def table_pool(document, scope, side):
    directory = V3 / document['document_version']
    tp, lp = directory / 'tables.json', directory / 'ledger.json'
    tables, ledger = read(tp), read(lp)
    assert tables['schema'] == 'logical-tables.v3'
    assert ledger['document_version'] == document['document_version']
    for name in ('pdf', 'blocks', 'work_md'):
        assert file_hash(ledger['sources'][name]['path']) == document['artifacts'][name]['sha256']
    raw = Path(ledger['sources']['work_md']['path']).read_text().splitlines()
    receipts = {k: dict(path=str(p), sha256=file_hash(p)) for k, p in [('tables', tp), ('ledger', lp)]}
    pool = []; coverage = Counter(); administrative = re.compile(r'ранее разработан|суть изменени|ссылка по внесенным', re.I)
    for table in tables['tables']:
        rows = [row_values(table, i, ledger, raw) for i in range(len(table['rows']['line']))]
        if any(administrative.search(' '.join(r['cells'])) for r in rows[:2]):
            coverage['change_log_rows_excluded'] += len(rows); continue
        header_rows = []
        for r in rows[:3]:
            # Headers may span several rows, but do not inherit them across V3 components.
            if any(re.fullmatch(r'[-+]?\d+[.,]?\d*', norm(c)) for c in r['cells'] if c): break
            header_rows.append(r)
        header_lines = [r['line_refs'][0] for r in header_rows]
        title = table['semantic_structure'].get('title') or ''
        container = table['semantic_structure'].get('container') or ''
        model_table = bool(rows and any(re.search(r'тип,?\s*марка|модель',norm(c)) for c in rows[0]['cells']))
        for i, row in enumerate(rows):
            coverage['rows_seen'] += 1
            cells = row['cells']; text = ' | '.join(cells)
            if not (2 <= len(cells) <= 24) or len(text) > 1300:
                coverage['unsupported_shape'] += 1; continue
            has_number = any(re.fullmatch(r'[-+]?\d+(?:[.,]\d+)?(?:\s*(?:м³/ч|м3/ч|кВт|Гкал/ч|л/с|шт\.?))?', norm(c), re.I) for c in cells)
            has_model = bool((model_table or re.search(r'насос|теплообменник|вентилятор', text, re.I)) and re.search(r'\d', text))
            if not (has_number or has_model) or not any(re.search('[а-яА-Яa-zA-Z]' if model_table else '[а-яА-Я]', c) for c in cells[:2]):
                coverage['no_subject_and_state'] += 1; continue
            line = row['line_refs'][0]
            assert ledger['columns']['kind'][line] == 'TABLE_ROW'
            assert ledger['columns']['owner'][line] in table['segments']
            evs = [cell_evidence(ledger, table, row, c, header_lines, receipts) for c in range(len(cells)) if cells[c].strip()]
            h_evs = [cell_evidence(ledger, table, h, c, header_lines, receipts) for h in header_rows for c in range(len(h['cells'])) if h['cells'][c].strip()]
            clues = features(text, title, document.get('discipline', ''))
            headers = [' | '.join(r['cells']) for r in header_rows]
            # A row label is a function/scope clue, never a certified identity.
            label = next((norm(c) for c in cells[:2] if re.search('[а-яА-Я]', c)), '')
            clues['engineering_function'] = [label] if label else []
            context = [dict(kind='table_header', text=h) for h in headers]
            for neighbor in rows[max(0, i-2):i] + rows[i+1:i+2]:
                context.append(dict(kind='neighbor_row', text=' | '.join(neighbor['cells'])[:900],
                    evidence=[cell_evidence(ledger, table, neighbor, c, header_lines, receipts)
                              for c in range(len(neighbor['cells'])) if neighbor['cells'][c].strip()]))
            if container: context.append(dict(kind='local_heading', text=container))
            s = make_subject(source_type='TABLE', side=side.upper(), version=document['document_version'],
                scope=scope, text=text, evidence=evs, clues=clues, context=context, local_id=row['row_key'],
                cells=cells, headers=[r['cells'] for r in header_rows], header_evidence=h_evs,
                table_key=table['table_key'], row_key=row['row_key'], page=row['page'],
                table_boundary_complete=False, purity_audit={'issues': [], 'basis': 'Frozen V3 TABLE_ROW ownership'},
                project_scope='source-only/' + scope)
            pool.append(s)
    return pool, dict(coverage)


def packet(candidate_id, query, candidates, neighbors=None):
    p = dict(schema='engineering-subject-packet.v1', candidate_id=candidate_id,
        source_type=query['source_type'], new=query, old_candidates=candidates,
        new_neighbors=neighbors or [], full_scope_coverage_proven=False)
    p['packet_hash'] = digest(p)
    return p


def prepare(root=ROOT):
    if (root / 'INPUT_MANIFEST.json').exists(): raise ValueError('Inputs already frozen')
    root.mkdir(parents=True, exist_ok=True)
    pairs = {p['pair_key']: p for p in read(BASE / 'FROZEN_PROJECT_INPUTS.json')['pairs']}
    recovered = read(TEXT / 'audited/project.json')
    reviews = {c['project_change_id']: c for c in recovered['project_changes'] if c['status'] == 'REVIEW'}
    inventory = read(TEXT / 'reports/PRIORITY_INVENTORY.json'); packets = []; documents = {}; stats = []
    source_files = set([TEXT / 'audited/project.json', TEXT / 'reports/PRIORITY_INVENTORY.json', BASE / 'FROZEN_PROJECT_INPUTS.json', TABLE / 'SOURCE_ONLY_PAIRS.json'])
    for entry in inventory:
        cid = entry['project_change_id']
        if entry['priority'] != 'HIGH' or cid not in reviews: continue
        old_packet_path = TEXT / 'packets' / (cid + '.json'); source_files.add(old_packet_path)
        previous = read(old_packet_path); pair = pairs[entry['comparison_scope']]
        for side in ('old', 'new'):
            version = pair[side]['document_version']
            if version not in documents:
                path = TEXT / 'documents' / version / 'scope_pool.json'; source_files.add(path)
                documents[version] = read(path)['units']
            source_files.update(Path(a['path']) for n,a in pair[side]['artifacts'].items() if n in ['pdf','blocks','work_md'])
        nv = pair['new']['document_version']; ov = pair['old']['document_version']
        narrative_path = BASE / 'run1/documents' / nv / 'narrative.json'; source_files.add(narrative_path)
        nu = next(u for u in read(narrative_path)['units'] if u['unit_id'] == previous['new']['unit_id'])
        # Context is subject to the same purity test; impure neighbors cannot leak back in.
        contexts = []
        for c in previous['new']['context']:
            u = next((u for u in documents[nv] if u['unit_id'] == c['unit_id']), None)
            if u and purity(u)[0] == 'PROVEN': contexts.append(dict(kind=c['position'], text=c['text'], evidence=[text_evidence(u)]))
        q = text_subject(nu, 'new', pair, contexts)
        # Search the entire prior eligible narrative pool with the shared representation.
        pool = [text_subject(u, 'old', pair) for u in documents[ov]]
        ranked = rank(q, pool)
        p = packet(cid, q, ranked); packets.append(p)
        stats.append(dict(candidate_id=cid, source_type='TEXT', old_pool=len(pool),
                          old_pure=sum(s['purity']=='PROVEN' for s in pool), candidates=len(ranked), purity=q['purity']))
    table_pairs = read(TABLE / 'SOURCE_ONLY_PAIRS.json')
    table_inventory = []
    for pair in table_pairs:
        scope = 'table/' + digest([pair['project_scope'], pair['code_key'], pair['old']['document_version'], pair['new']['document_version']])[:24]
        pools = {}; coverage = {}
        for side in ('old', 'new'):
            pools[side], coverage[side] = table_pool(pair[side], scope, side)
            source_files.update(Path(pair[side]['artifacts'][n]['path']) for n in ['pdf','blocks','work_md'])
            source_files.update(V3 / pair[side]['document_version'] / n for n in ['tables.json','ledger.json'])
        ranked = [(q, rank(q, pools['old'])) for q in pools['new']]
        # Half retrieval-enriched, half hash sample; explicitly not an accuracy benchmark.
        best = sorted(ranked, key=lambda x: (-x[1][0]['score'] if x[1] else 0, x[0]['subject_id']))[:TABLE_LIMIT//2]
        used = {q['subject_id'] for q,_ in best}
        tail = sorted([x for x in ranked if x[0]['subject_id'] not in used], key=lambda x:digest(['cohort-v1',x[0]['subject_id']]))[:TABLE_LIMIT-len(best)]
        selected = best + tail
        # Complete small equipment schedules provide restructuring opportunities
        # that a row sample can miss. Header roles, not project IDs, select them.
        selected_ids={q['subject_id'] for q,_ in selected}
        schedule_tables={q['table_key'] for q in pools['new']
                         if {'position','label','count'} <= {role(c) for h in q['headers'] for c in h}}
        schedule_tables={t for t in schedule_tables if sum(q['table_key']==t for q in pools['new'])<=12}
        selected += [x for x in ranked if x[0]['table_key'] in schedule_tables and x[0]['subject_id'] not in selected_ids]
        table_inventory.append(dict(comparison_scope=scope, document=pair['old']['document_code'],
                                    old_subjects=len(pools['old']), new_subjects=len(pools['new']), evaluated=len(selected), coverage=coverage))
        write(root / 'pools' / (scope.split('/')[-1]+'.json'), pools)
        for q, rows in selected:
            neighbors = [s for s in pools['new'] if s['table_key']==q['table_key'] and s['subject_id']!=q['subject_id']]
            qt=set(tokens(q['text']))
            neighbors=sorted(neighbors,key=lambda s:(-len(qt & set(tokens(s['text']))) / max(1,len(qt | set(tokens(s['text'])))),s['subject_id']))[:2]
            cid = 'table_' + digest([scope, q['subject_id']])[:24]
            packets.append(packet(cid,q,rows,neighbors))
            stats.append(dict(candidate_id=cid, source_type='TABLE', old_pool=len(pools['old']),
                              old_pure=len(pools['old']), candidates=len(rows), purity=q['purity']))
    for p in packets: write(root / 'packets' / (p['candidate_id']+'.json'),p)
    protected = {}
    for folder in ['project_change_text_v1','table_project_change_v1','table_materialization_v3','text_old_scope_recovery_v1']:
        for p in Path('experiments',folder).glob('*.py'): protected[str(p.resolve())] = file_hash(p)
    write(root/'INPUT_MANIFEST.json', dict(schema='engineering-subject-inputs.v1',
        sources={str(p):file_hash(p) for p in sorted(source_files)}, protected_code=protected,
        baseline_proven_ids=[c['project_change_id'] for c in recovered['project_changes'] if c['status']=='PROVEN'],
        packets={p['candidate_id']:p['packet_hash'] for p in packets},
        cohort_policy='All unresolved HIGH TEXT after audited recovery; TABLE per pair: up to 6 retrieval-enriched + 6 hash-sampled NEW subjects, plus all rows of small schedules with explicit position/label/count headers',
        old_scope_absence_never_proven=True))
    write(root/'RETRIEVAL.json',stats); write(root/'TABLE_INVENTORY.json',table_inventory)
    print(dict(packets=len(packets), by_source=dict(Counter(p['source_type'] for p in packets)), purity=dict(Counter(p['new']['purity'] for p in packets))))


if __name__ == '__main__': prepare()
