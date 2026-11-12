"""Build a hash-bound progress inventory; reviewed never means accepted."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RUN = Path('/tmp/auditmanager_publication_remaining84_20261112_run')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build():
    paths = sorted(ROOT.glob('publication_full_fields_F*_assessment.json'))
    paths += sorted(ROOT.glob('publication_F*_context1_assessment.json'))
    rows = []
    seen = set()
    for path in paths:
        assessment = json.loads(path.read_text())
        fid = assessment['finding_id']
        if fid in seen:
            raise ValueError(f'Duplicate selected assessment: {fid}')
        seen.add(fid)
        binding = assessment.get('validation', assessment)
        projection_path = Path(binding['projection_path'])
        if sha(projection_path) != binding['projection_sha256']:
            raise ValueError(f'Changed projection: {fid}')
        projection = json.loads(projection_path.read_text())
        rows.append({'finding_id': fid, 'assessment_path': str(path),
                     'assessment_sha256': sha(path), 'status': assessment['status'],
                     'projection_path': str(projection_path),
                     'projection_sha256': sha(projection_path),
                     'part_counts_not_finding_counts': {
                         key: len(projection[key]) for key in
                         ('technical_findings', 'questions', 'pending', 'withdrawn', 'history_only')},
                     'engineer_accepted': False, 'production_accepted': False})
    state = json.loads((RUN / 'status.json').read_text())
    expected_ids = {f'F-{i:03}' for i in range(1, 86)}
    unexpected = seen - expected_ids
    if unexpected:
        raise ValueError(f'Unexpected reviewed finding IDs: {sorted(unexpected)}')
    coverage_complete = seen == expected_ids
    return {'status': ('complete_review_inventory_not_acceptance' if coverage_complete
                       else 'partial_review_inventory_not_acceptance'),
            'review_coverage_complete': coverage_complete,
            'stage4_complete': False,
            'planned_findings': 85, 'reviewed_distinct_findings': len(rows),
            'unreviewed_finding_ids': [f'F-{i:03}' for i in range(1, 86)
                                       if f'F-{i:03}' not in seen],
            'batch_successful_responses': len([a for a in state['attempts'] if a.get('success')]),
            'batch_status_snapshot': state['status'],
            'process_liveness_not_inferred_from_status_file': True,
            'selected_followups': ['F-003', 'F-008'],
            'reused_full_field_pilot': 'F-006',
            'rows': sorted(rows, key=lambda row: row['finding_id']),
            'limitations': ['Source review may identify failure; reviewed is not passed.',
                           'Part counts are not independent defects or engineer workload.',
                           'Original benchmark scores unchanged; no full comparison acceptance.',
                           'Coordinator-selected context followups do not prove autonomous retrieval.'],
            'production_acceptance': False}


if __name__ == '__main__':
    result = build()
    destination = ROOT / 'publication_review_snapshot.json'
    destination.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k not in ('rows', 'unreviewed_finding_ids')},
                     ensure_ascii=False, indent=2))
