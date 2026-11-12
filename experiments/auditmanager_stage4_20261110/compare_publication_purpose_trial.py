"""Inventory matched-source DEV trials; counts never establish quality improvement."""
import json
from pathlib import Path

from .evaluate_publication_reconciliation import evaluate
from .run_primary_text_runtime_probe import sha, write
from .run_runtime_probe_recovery import verify_packet

ROOT = Path('/tmp/auditmanager_publication_purpose_controls_20261112')
OUTPUT = ROOT.with_name(ROOT.name + '_run')


def compare():
    preparation = json.loads((ROOT / 'preparation.json').read_text())
    state = json.loads((OUTPUT / 'status.json').read_text())
    rows = []
    for row in preparation['requests']:
        request = verify_packet(row)
        baseline_path = Path(row['baseline_request_path'])
        if sha(baseline_path) != row['baseline_request_sha256']:
            raise ValueError('Baseline request changed')
        baseline = json.loads(baseline_path.read_text())
        old = json.loads(baseline['messages'][-1]['content'])['packet']
        new = json.loads(request['messages'][-1]['content'])['packet']
        for key in ('finding_sha256', 'input_units', 'text_sources', 'attached_graphic_refs'):
            if old[key] != new[key]:
                raise ValueError(f'Nonmatched source or claim input: {row["finding_id"]} {key}')
        if baseline['image_hashes'] != request['image_hashes']:
            raise ValueError('Images differ')
        item = {'finding_id': row['finding_id'], 'matched_claims_and_sources': True,
                'baseline_request_sha256': sha(baseline_path),
                'candidate_request_sha256': row['sha256'], 'semantic_comparison': 'not_completed'}
        if not any(a.get('success') and a['stage'] == request['stage'] for a in state['attempts']):
            item['status'] = 'response_not_available'
        else:
            receipt = evaluate(ROOT, OUTPUT, row['finding_id'])
            item['status'] = receipt['status']
            item['validation'] = receipt
            fid = row['finding_id']
            base_output = (Path('/tmp/auditmanager_publication_F008_context1_20261112_run')
                           if fid == 'F-008' else
                           Path('/tmp/auditmanager_publication_remaining84_20261112_run/evaluations') /
                           f'publication_reconciliation_{fid[2:]}')
            bp = base_output / 'publication_projection.json'
            old_result = json.loads(bp.read_text())
            item['baseline_projection_sha256'] = sha(bp)
            item['baseline_part_counts'] = {k: len(old_result[k]) for k in
                ('technical_findings', 'questions', 'pending', 'withdrawn', 'history_only')}
            item['part_counts_are_not_defect_counts_or_quality_scores'] = True
        rows.append(item)
    return {'status': 'matched_source_DEV_comparison_in_progress', 'rows': rows,
            'quality_improvement_proven': False, 'production_acceptance': False,
            'scope': 'Prior purpose metadata and instructions changed together; not independent attribution of their effects.',
            'required_semantic_controls': {
                'F-004': 'Preserve M2/M4/M5 source conflicts; attach recommendation without extra question.',
                'F-010': 'Retain useful designation clarification; no unsupported promotion to technical.',
                'F-012': 'Preserve equipment conflict and application-unit clarification; no standalone correction recommendation.',
                'F-020': 'Retain actionable source-grounded question without asserting regulatory violation or exposing every internal limit.',
                'F-008': 'Preserve all three supported quantity conflicts despite original question routing.'}}


if __name__ == '__main__':
    result = compare()
    write(Path(__file__).resolve().parent / 'publication_purpose_comparison.json', result)
    print(json.dumps([{'finding_id': row['finding_id'], 'status': row['status']}
                      for row in result['rows']], ensure_ascii=False))
