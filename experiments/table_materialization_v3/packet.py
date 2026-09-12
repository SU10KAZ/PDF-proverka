"""Build a blind source packet from the already frozen structural selection."""
from collections import Counter
from pathlib import Path
import shutil
import zipfile

import fitz

from experiments.semantic_foundation_v3 import frozen_v1 as v1
from experiments.semantic_foundation_v3.dev_packet import page_hashes
from experiments.semantic_foundation_v3.ledger import LineLedger
from experiments.semantic_foundation_v3.run import read, receipt
from .prepare import ROOT, FOUNDATION, immutable
from .holdout import check_freeze, exclusions, family, QUOTAS


def main():
    manifest = check_freeze()
    selected = read(ROOT / 'holdout_private/selected_sources.json')
    documents = {d['document_version']: d for d in selected['documents']}
    exclusion = exclusions()
    out = ROOT / 'fresh_holdout'
    if out.exists():
        raise ValueError('Holdout packets are immutable; refusing overwrite')
    out.mkdir()
    payload, evidence, actual_pages, actual_hashes = [], [], set(), set()
    for c in selected['cases']:
        d = documents[c['document_version']]
        for r in list(d['artifacts'].values()) + d['source_refs'].get('original_sources', []):
            assert v1.file_sha(Path(r['path'])) == r['sha256']
            actual_hashes.add(r['sha256'])
        hashes = page_hashes(Path(d['artifacts']['work_md']['path']).read_text())
        actual_pages.update(hashes.values())
        ledger, _ = LineLedger.read(d)
        directory = out / c['case_id']
        directory.mkdir()
        original = fitz.open(d['artifacts']['pdf']['path'])
        focused = fitz.open()
        for page in sorted(set(c['page_pair'])):
            assert 0 < page <= len(original)
            focused.insert_pdf(original, from_page=page - 1, to_page=page - 1)
        pdf = directory / 'source_pages.pdf'
        focused.save(pdf, garbage=4, deflate=True, no_new_id=True)
        focused.close()
        panels = []
        for anchor, span in zip(c['anchors'], c['source_segment_spans']):
            assert ledger.anchor(anchor['line_id'], anchor['edge']) == anchor
            page = anchor['page']
            image = directory / f'page_{page}.png'
            if not image.exists():
                original[page - 1].get_pixmap(matrix=fitz.Matrix(1.5, 1.5), alpha=False).save(image)
            panels.append({'page': page, 'anchor_text': ledger.lines[anchor['line_id']].text,
                           'context': '\n'.join(ledger.lines[i].text for i in range(span[0], span[1] + 1)),
                           'image': str(image.relative_to(out)), 'anchor': anchor})
        original.close()
        payload.append({'case_id': c['case_id'], 'document_code': d['document_code'],
                        'document_version': c['document_version'], 'panels': panels,
                        'pdf': str(pdf.relative_to(out)), 'question': 'Это одна таблица или разные?',
                        'allowed_choices': ['SAME', 'NEW', 'UNSURE']})
        evidence.append({'case_id': c['case_id'], 'source_pdf': d['artifacts']['pdf'],
                         'extracted_pdf': receipt(pdf), 'source_pages': c['page_pair'],
                         'images': [receipt(p) for p in sorted(directory.glob('*.png'))]})
    assert not actual_pages & exclusion['page_hashes']
    assert not actual_hashes & exclusion['source_hashes']
    assert not set(documents) & exclusion['versions']
    assert not {family(d['document_code']) for d in documents.values()} & exclusion['families']
    packet = {'schema': 'fresh-table-v3-blind-packet.v1', 'namespace': 'FRESH_TABLE_V3_HOLDOUT', 'cases': payload}
    packet_receipt = immutable(out / 'cases.json', packet)
    packet_sha = packet_receipt['sha256']
    (out / 'packet.js').write_bytes(b'window.HOLDOUT_PACKET=' + v1.canonical_bytes({**packet, 'packet_sha256': packet_sha}) + b';\n')
    shutil.copyfile(Path(__file__).with_name('holdout_ui.html'), out / 'index.html')
    (out / 'README.txt').write_text('Откройте index.html в браузере. Разметьте 27 случаев и экспортируйте JSON ответов.\n'
                                   'Полные исходные страницы и текст фрагментов находятся внутри пакета.\n'
                                   'Для начала работы не требуется сервер. Ответов в пакете нет.\n')
    human_bytes = (out / 'cases.json').read_text()
    forbidden = ['prediction', 'foundation_edge', 'candidate_join', 'selection_stratum', 'control_expectation', 'final_answer']
    assert not any('"' + key + '"' in human_bytes for key in forbidden)
    assert all('answer' not in c for c in packet['cases'])
    selection = {'schema': 'fresh-table-holdout-selection.v1', 'candidate_manifest': receipt(ROOT / 'reports/TABLE_V3_CANDIDATE_MANIFEST.json'),
                 'candidate_commit': manifest['commit'], 'cases': selected['cases'], 'documents': selected['documents'],
                 'case_count': len(payload), 'document_count': len(documents), 'quotas': QUOTAS,
                 'assigned_strata': dict(Counter(c['selection_stratum'] for c in selected['cases'])),
                 'structural_strata_definition': 'Source proxies only; ordinal progression and distinct explicit titles represent clear-continuation/new-table prototypes. They are not human answers.',
                 'selection_method': 'Rarest structural stratum first; deterministic source-anchor hash order, lowest document usage first; maximum three cases per document; unique page-content pairs.',
                 'selection_pool': receipt(ROOT / 'holdout_private/structural_pool.json'),
                 'human_packet': packet_receipt, 'evidence': evidence, 'predictions_used': False, 'answers_used': False}
    immutable(ROOT / 'reports/FRESH_TABLE_HOLDOUT_SELECTION.json', selection)
    independence = {'schema': 'fresh-table-holdout-independence.v1', 'pass': True,
                    'candidate_frozen_before_selection': True, 'candidate_manifest_sha256': selection['candidate_manifest']['sha256'],
                    'dev_documents_excluded': len(exclusion['dev_versions']), 'prior_eval_documents_excluded': len(exclusion['old_versions']),
                    'source_document_family_overlap': 0, 'document_version_overlap': 0, 'source_hash_overlap': 0,
                    'dev_holdout_case_overlap': 0, 'dev_holdout_page_content_overlap': 0, 'prior_eval_page_content_overlap': 0,
                    'holdout_duplicate_cases': 0, 'holdout_duplicate_page_pairs': 0,
                    'checks': 'Entire selected documents, all page-content hashes, normalized document families, source MD/raw blocks/original exports/PDF hashes; exact line hashes and PDF page range checked.',
                    'proof_inputs': [receipt(FOUNDATION / 'source_exclusion.json'), receipt(FOUNDATION / 'dev_documents.json'),
                                     receipt(ROOT / 'TABLE_V3_DEV_TRUTH.json'), receipt(ROOT / 'holdout_private/selected_sources.json')],
                    'excluded_eval_project': '272_*', 'table_v3_inference_on_holdout': 0, 'human_answers_generated': 0,
                    'human_ui_predictions': 0, 'namespace': packet['namespace'], 'packet_sha256': packet_sha,
                    'source_pool_note': 'Previously inventoried unannotated sources; none of the selected documents were used for Table V3 rule design. All selection inspected after the candidate freeze.'}
    immutable(ROOT / 'reports/FRESH_TABLE_HOLDOUT_INDEPENDENCE.json', independence)
    files = [receipt(p) for p in sorted(out.rglob('*')) if p.is_file()]
    immutable(ROOT / 'reports/FRESH_TABLE_HOLDOUT_PACKET_FREEZE.json', {'packet_sha256': packet_sha, 'files': files,
               'answers': 0, 'predictions': 0, 'case_count': len(payload), 'frozen': True})
    for p in out.rglob('*'):
        if p.is_file():
            p.chmod(0o444)
    archive = ROOT / 'FRESH_TABLE_V3_HOLDOUT_BLIND.zip'
    with zipfile.ZipFile(archive, 'x', compression=zipfile.ZIP_DEFLATED) as z:
        for path in sorted(out.rglob('*')):
            if path.is_file():
                info = zipfile.ZipInfo(str(path.relative_to(ROOT)), date_time=(2026, 9, 13, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o100444 << 16
                z.writestr(info, path.read_bytes())
    archive.chmod(0o444)
    print({'packet': packet_receipt, 'archive': receipt(archive), 'cases': len(payload), 'documents': len(documents)})


if __name__ == '__main__':
    main()
