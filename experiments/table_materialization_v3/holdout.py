"""Post-freeze, source-only holdout selection and blind static packet. No V3 inference."""
from collections import Counter, defaultdict
from pathlib import Path
import argparse
import hashlib
import html
import json
import re

from experiments.semantic_foundation_v3 import frozen_v1 as v1
from experiments.semantic_foundation_v3.dev_packet import page_hashes
from experiments.semantic_foundation_v3.ledger import LineLedger
from experiments.semantic_foundation_v3.materialize import materialize_document as foundation
from experiments.semantic_foundation_v3.run import read, receipt
from .prepare import ROOT, FOUNDATION, FINAL, FINAL_SHA, immutable

QUOTAS = {k: 3 for k in ('ordinal_progression', 'distinct_explicit_titles', 'repeated_header_ambiguity',
                         'headerless_surface', 'same_page_tables', 'engineering_specification',
                         'equipment_passport', 'numbering_restart', 'cross_page_tables')}


def check_freeze():
    manifest = read(ROOT / 'reports/TABLE_V3_CANDIDATE_MANIFEST.json')
    assert manifest['frozen_before_holdout_selection']
    assert v1.file_sha(FINAL) == FINAL_SHA
    assert all(v1.file_sha(Path(r['path'])) == r['sha256'] for r in manifest['code'])
    return manifest


def family(code):
    return re.sub(r'\s*v\d+\s*$', '', code.replace('_', ' ').casefold()).strip()


def exclusions():
    old = read(FOUNDATION / 'source_exclusion.json')
    docs = read(FOUNDATION / 'dev_documents.json')
    dev_hashes, dev_pages = set(), set()
    for d in docs:
        for r in list(d['artifacts'].values()) + d['source_refs'].get('original_sources', []):
            dev_hashes.add(r['sha256'])
        dev_pages.update(page_hashes(Path(d['artifacts']['work_md']['path']).read_text()).values())
    return {'versions': set(old['eval_versions']) | {d['document_version'] for d in docs},
            'families': {family(x) for x in old['eval_codes']} | {family(d['document_code']) for d in docs},
            'source_hashes': set(old['eval_source_hashes']) | dev_hashes,
            'page_hashes': set(old['eval_page_hashes']) | dev_pages,
            'dev_versions': {d['document_version'] for d in docs}, 'old_versions': set(old['eval_versions']),
            'dev_page_hashes': dev_pages, 'old_page_hashes': set(old['eval_page_hashes'])}


def plain(cell):
    return cell.replace('**', '').replace('__', '').strip()


def strict_numbers(rows):
    values = []
    for cells in rows:
        # Column enumeration is structural metadata, never a record reset.
        if cells == [str(n) for n in range(1, len(cells) + 1)]:
            continue
        match = re.fullmatch(r'(\d+)[.)]?', plain(cells[0])) if cells else None
        if match:
            values.append(int(match[1]))
    return values if len(values) >= 2 and all(b == a + 1 for a, b in zip(values, values[1:])) else []


def surfaces(document):
    """Independent selector features, derived from frozen Foundation/source only."""
    result = foundation(document)
    ledger, _ = LineLedger.read(document)
    col, sem = result['ledger']['columns'], result['semantics']
    units = [u for u in sem['units'] if u['kind'] == 'TABLE_SEGMENT' and u['first_content_line'] is not None]
    records = []
    for u in units:
        ids = [i for i in range(u['first_line'], u['last_line'] + 1) if col['kind'][i] == 'TABLE_ROW']
        rows = [[plain(c) for c in ledger.lines[i].text.strip()[1:-1].split('|')] for i in ids]
        first = '\n'.join(ledger.lines[i].text for i in ids[:5]).casefold()
        schema_header = bool(re.search(r'наименование|маркировка|номер|описание|позиц|время|pressure|гц', first))
        specification = bool(re.search(r'позиц|поз\.', first) and 'наименование' in first and
                             re.search(r'колич|кол\.', first) and re.search(r'измер|ед\.', first))
        page = ledger.lines[u['first_line']].page
        page_text = '\n'.join(ledger.clean[i] for i in ledger.page_lines[page]).casefold()
        passport = ('технические данные' in page_text and ('установка' in page_text or 'типоразмер' in page_text)) or \
                   ('автоматика' in page_text and ('вентилятор' in page_text or 'рабочий ток' in page_text))
        titles = [col['normalized_text'][i] for i in u['caption_lines']]
        # Explicit headings adjacent to a table are source titles for selection only.
        context = u.get('section_context')
        if context is not None:
            h = sem['units'][context].get('heading_line')
            if h is not None:
                titles.append(col['normalized_text'][h])
        records.append({'unit': u, 'rows': rows, 'ids': ids, 'numbers': strict_numbers(rows), 'page': page,
                        'header': schema_header, 'specification': specification, 'passport': passport, 'titles': titles})
    out = []
    for left, right in zip(records, records[1:]):
        lu, ru = left['unit'], right['unit']
        a, b = lu['last_content_line'], ru['first_content_line']
        lp, rp = left['page'], right['page']
        if rp not in {lp, lp + 1}:
            continue
        gap = [i for i in range(lu['last_line'] + 1, ru['first_line']) if col['kind'][i] not in {'FURNITURE', 'CAPTION'}]
        repeated = [v1.normalize(c) for c in left['rows'][0]] == [v1.normalize(c) for c in right['rows'][0]]
        ln, rn = left['numbers'], right['numbers']
        flow = bool(ln and rn and rn[0] == ln[-1] + 1)
        reset = bool(ln and rn and ln[-1] > 1 and rn[0] == 1)
        tags = []
        if lp != rp and flow and not gap:
            tags.append('ordinal_progression')
        if left['titles'] and right['titles'] and left['titles'] != right['titles']:
            tags.append('distinct_explicit_titles')
        if lp != rp and repeated and not flow:
            tags.append('repeated_header_ambiguity')
        if lp != rp and not right['header'] and ru['first_row']['kind'] in {'DATA_LIKE', 'NONE', 'UNKNOWN'}:
            tags.append('headerless_surface')
        if lp == rp:
            tags.append('same_page_tables')
        if left['specification'] and right['specification']:
            tags.append('engineering_specification')
        if left['passport'] or right['passport']:
            tags.append('equipment_passport')
        if reset:
            tags.append('numbering_restart')
        if lp != rp:
            tags.append('cross_page_tables')
        if not tags:
            continue
        anchors = [ledger.anchor(a, 'LAST'), ledger.anchor(b, 'FIRST')]
        identity = {'namespace': 'FRESH_TABLE_V3_HOLDOUT', 'document_version': document['document_version'], 'anchors': anchors}
        out.append({**identity, 'case_id': 'tv3holdout_' + v1.digest(identity)[:24], 'tags': tags,
                    'page_pair': [lp, rp], 'source_segment_spans': [[lu['first_line'], lu['last_line']], [ru['first_line'], ru['last_line']]],
                    'selector_evidence': {'strict_left_numbers': ln, 'strict_right_numbers': rn,
                                          'gap_line_refs': gap, 'left_titles': left['titles'], 'right_titles': right['titles']}})
    return out


def scan():
    check_freeze()
    exclusion = exclusions()
    pool = read(FOUNDATION / 'source_pool_index.json')
    rejected, candidates, eligible_docs = Counter(), [], []
    for entry in sorted(pool, key=lambda r: r['document']['document_version']):
        d = entry['document']
        if d['document_version'] in exclusion['versions'] or family(d['document_code']) in exclusion['families']:
            rejected['dev_or_eval_document_family'] += 1
            continue
        if '/272_' in entry['original']:
            rejected['prior_eval_project'] += 1
            continue
        hashes = {r['sha256'] for r in list(d['artifacts'].values()) + d['source_refs'].get('original_sources', [])}
        if hashes & exclusion['source_hashes'] or set(entry['page_hashes'].values()) & exclusion['page_hashes']:
            rejected['source_or_page_content_overlap'] += 1
            continue
        pdf = Path(d['source_refs']['source_pdf'])
        if not pdf.is_file() or not any(c['kind'] == 'TABLE' for c in entry['cases']):
            rejected['no_pdf_or_table_surface'] += 1
            continue
        if any(not Path(d['artifacts'][k]['path']).is_file() or v1.file_sha(Path(d['artifacts'][k]['path'])) != d['artifacts'][k]['sha256'] for k in ('work_md', 'blocks')):
            rejected['source_changed_or_unavailable'] += 1
            continue
        fresh_pages = page_hashes(Path(d['artifacts']['work_md']['path']).read_text())
        if set(fresh_pages.values()) & exclusion['page_hashes']:
            rejected['reverified_page_content_overlap'] += 1
            continue
        rows = surfaces(d)
        if rows:
            eligible_docs.append(d)
            for row in rows:
                row['page_content_hashes'] = [fresh_pages[p] for p in row['page_pair']]
            candidates.extend(rows)
    path = ROOT / 'holdout_private/structural_pool.json'
    immutable(path, {'candidate_freeze': receipt(ROOT / 'reports/TABLE_V3_CANDIDATE_MANIFEST.json'),
                     'documents': eligible_docs, 'candidates': candidates, 'rejected': dict(rejected),
                     'source_pool': receipt(FOUNDATION / 'source_pool_index.json'), 'quotas': QUOTAS,
                     'predictions_used': False, 'answers_used': False})
    print({'documents': len(eligible_docs), 'candidates': len(candidates),
           'strata': dict(Counter(t for c in candidates for t in c['tags'])), 'rejected': dict(rejected)})


def select():
    check_freeze()
    data = read(ROOT / 'holdout_private/structural_pool.json')
    available = data['candidates']
    excluded = exclusions()
    documents = {d['document_version']: d for d in data['documents']}
    pdf_receipts, rejected_pdf, pdf_documents = {}, set(), {}
    selected, used_pairs, used_ids, doc_counts = [], set(), set(), Counter()
    # Rarest source stratum first; deterministic hash order with document diversity.
    order = sorted(QUOTAS, key=lambda tag: (sum(tag in c['tags'] for c in available), tag))
    for tag in order:
        while sum(c['selection_stratum'] == tag for c in selected) < QUOTAS[tag]:
            options = [c for c in available if tag in c['tags'] and c['case_id'] not in used_ids and
                       tuple(c['page_content_hashes']) not in used_pairs and doc_counts[c['document_version']] < 3 and
                       c['document_version'] not in rejected_pdf]
            if not options:
                raise ValueError(f'Insufficient independent source candidates for {tag}; do not relax silently')
            chosen = min(options, key=lambda c: (doc_counts[c['document_version']], c['case_id']))
            version = chosen['document_version']
            if version not in pdf_receipts:
                pdf_receipts[version] = receipt(documents[version]['source_refs']['source_pdf'])
            pdf = pdf_receipts[version]
            if pdf['sha256'] in excluded['source_hashes'] or (pdf['sha256'] in pdf_documents and pdf_documents[pdf['sha256']] != version):
                rejected_pdf.add(version)
                continue
            pdf_documents[pdf['sha256']] = version
            selected.append({**chosen, 'selection_stratum': tag})
            used_ids.add(chosen['case_id'])
            used_pairs.add(tuple(chosen['page_content_hashes']))
            doc_counts[chosen['document_version']] += 1
    selected.sort(key=lambda c: c['case_id'])
    docs = [d for d in data['documents'] if d['document_version'] in doc_counts]
    for d in docs:
        d['artifacts']['pdf'] = pdf_receipts[d['document_version']]
    immutable(ROOT / 'holdout_private/selected_sources.json', {'documents': docs, 'cases': selected,
               'pdf_overlap_rejections': sorted(rejected_pdf)})
    print({'cases': len(selected), 'documents': len(docs), 'assigned_strata': dict(Counter(c['selection_stratum'] for c in selected))})


def main():
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['scan', 'select'])
    a = p.parse_args()
    {'scan': scan, 'select': select}[a.action]()


if __name__ == '__main__':
    main()
