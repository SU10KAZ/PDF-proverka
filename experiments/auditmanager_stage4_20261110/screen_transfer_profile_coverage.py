"""Metadata upper bounds only: no reserve source content or audit answers opened."""
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).parent
PROFILES = ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM')


def main():
    inventory_path = HERE / 'transfer_source_inventory.json'
    overlap_path = HERE / 'transfer_source_overlap.json'
    inventory = json.loads(inventory_path.read_text())
    overlap = json.loads(overlap_path.read_text())
    assert overlap['inventory_sha256'] == hashlib.sha256(inventory_path.read_bytes()).hexdigest()
    known = set(overlap['known_object_ids_by_hash_or_name_hint'])
    counts = defaultdict(Counter)
    rows = []
    for item in inventory['rows']:
        if item['object_id'] in known:
            continue
        profile = 'AK' if item['explicit_AK_filename_marker'] else item['storage_discipline']
        if profile not in PROFILES:
            continue
        prepared = item['prepared_md_present'] and item['prepared_pdf_present']
        if prepared:
            counts[item['object_id']][profile] += 1
        rows.append({'object_id': item['object_id'], 'document_directory': item['document_directory'],
                     'path': item['path'], 'profile_hint': profile,
                     'prepared_md_and_pdf': prepared, 'prior_exposure': 'not_checked',
                     'reserve_eligible': None})
    coverage = {obj: {p: count[p] for p in PROFILES} for obj, count in sorted(counts.items())}
    report = {
        'status': 'metadata_coverage_upper_bound_not_reserve_assignment',
        'inputs': {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in (inventory_path, overlap_path)},
        'excluded_known_object_groups': sorted(known),
        'candidate_counts_by_object_and_profile_hint': coverage,
        'candidate_object_groups_per_profile': {p: [obj for obj, count in coverage.items() if count[p]] for p in PROFILES},
        'rows': rows,
        'interpretation': [
            'Large document counts do not imply independent construction objects.',
            'These are upper bounds: prior exposure, true discipline, PDF duplicates and foreign documents remain unchecked.',
            'Only group 256 has prepared candidates for every requested discipline in this inventory after known-group exclusion.',
            'Group 213 currently has prepared AR/EOM candidates; its AK input PDF exists in a legacy bundle but has no prepared document.pdf. No content was opened.',
            'A full six-discipline calibration/reserve split disjoint by construction object is not demonstrated by this inventory.',
            'Do not silently weaken the original eighteen-package requirement or treat a one-object result as cross-object transfer evidence.',
        ],
        'next_steps': [
            'Audit prior exposure and source PDF identities before reserve assignment.',
            'Locate additional independent construction objects or document the missing transfer evidence explicitly.',
            'Keep six known-project regressions running; this source gap does not block local method evaluation.',
        ],
        'source_content_read': False, 'engineer_answers_read': False,
        'selected_calibration': [], 'selected_reserve': [], 'production_acceptance': False,
    }
    # Prevent prose/count drift when inventory changes.
    assert [obj for obj, count in coverage.items() if all(count[p] for p in PROFILES)] == ['256']
    output = HERE / 'transfer_profile_coverage_screen.json'
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(coverage, ensure_ascii=False))


if __name__ == '__main__':
    main()
