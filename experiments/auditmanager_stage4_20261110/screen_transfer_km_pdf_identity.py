"""Hash current KM source PDFs without reading engineering text or answers."""
from collections import defaultdict
import json
from pathlib import Path
from .materialize_trial_inputs import sha

HERE = Path(__file__).resolve().parent


def main():
    inventory_path = HERE/'transfer_source_inventory.json'
    inventory = json.loads(inventory_path.read_text())
    rows = []
    for item in inventory['rows']:
        if item['storage_discipline'] != 'KM' or not item['valid_version_selector']:
            continue
        version = Path(item['path'])/'versions'/item['current_version']
        prepared = version/'02_work/document.pdf'
        candidates = sorted((version/'01_input').glob('*.pdf'))
        # Prefer the exact prepared source used by the pipeline, recording whether
        # it also matches an input PDF. No fallback that silently changes versions.
        if not prepared.is_file():
            rows.append({'document': item['path'], 'object_id': item['object_id'],
                         'version': item['current_version'], 'status': 'prepared_pdf_missing'})
            continue
        digest = sha(prepared)
        matching = [str(p) for p in candidates if p.stat().st_size == prepared.stat().st_size and sha(p) == digest]
        rows.append({'document': item['path'], 'object_id': item['object_id'],
            'version': item['current_version'], 'status': 'hashed_not_engineering_reviewed',
            'prepared_pdf': str(prepared), 'sha256': digest, 'bytes': prepared.stat().st_size,
            'matching_input_pdfs': matching, 'prior_exposure_verified': False,
            'reserve_eligible': None})
    groups = defaultdict(list)
    for row in rows:
        if 'sha256' in row:
            groups[row['sha256']].append(row)
    duplicates = [{'sha256': digest, 'documents': [r['document'] for r in group],
                   'object_ids': sorted({r['object_id'] for r in group})}
                  for digest, group in groups.items() if len(group) > 1]
    counts = {}
    for obj in sorted({r['object_id'] for r in rows}):
        subset = [r for r in rows if r['object_id'] == obj]
        counts[obj] = {'documents': len(subset), 'prepared': sum('sha256' in r for r in subset),
                      'distinct_pdf_hashes': len({r['sha256'] for r in subset if 'sha256' in r}),
                      'input_identity_confirmed': sum(bool(r.get('matching_input_pdfs')) for r in subset)}
    report = {'status': 'KM_pdf_identity_screen_not_reserve_assignment',
        'inventory_path': str(inventory_path), 'inventory_sha256': sha(inventory_path),
        'counts_by_object': counts, 'rows': rows, 'duplicate_groups': duplicates,
        'source_content_handling': 'PDF bytes streamed through SHA256; no rendering, OCR or text extraction.',
        'engineer_answers_read': False, 'model_calls': 0,
        'limits': ['Hash equality proves exact PDF identity; inequality does not prove unseen content.',
                   'Construction identity and prior exposure require separate verification.',
                   'Known object groups 214, 297, 314 remain DEV, including unseen sibling documents.',
                   'One additional construction object cannot alone support a disjoint calibration/reserve split across six disciplines.',
                   'No reserve selected and original eighteen-package requirement unchanged.'],
        'production_acceptance': False}
    output = HERE/'transfer_KM_pdf_identity_screen.json'
    with output.open('x') as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
    print(json.dumps({'counts': counts, 'duplicate_groups': len(duplicates), 'model_calls': 0}))


if __name__ == '__main__':
    main()
