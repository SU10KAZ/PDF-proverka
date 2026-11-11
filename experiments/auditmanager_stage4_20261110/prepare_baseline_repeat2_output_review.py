"""Prepare evaluator-only second-repeat queue; preserve unchecked quality dimensions."""
from collections import Counter
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
DESTINATION = Path('/tmp/auditmanager_comparison_evaluation_20261111/baseline_r2/output_review_queue.json')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    evaluator = HERE / 'evaluator_release_errata_v2.json'
    targets = {t['target_id']: t for t in json.loads(evaluator.read_text())['targets']}
    sources, rows = [], []
    for discipline in ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'):
        base = ROOT / f'{discipline.lower()}_r2_classic/classic_control'
        seal = json.loads((base / f'status/{discipline}.json').read_text())
        assert seal['status'] == 'completed_unadjudicated'
        manifest = json.loads((base / 'input_manifest.json').read_text())[0]
        final = Path(manifest['output_dir']) / '03_findings.json'
        assert sha(final) == seal['final_sha256']
        findings = json.loads(final.read_text())['findings']
        assert len({f['id'] for f in findings}) == len(findings)
        links = {f['id']: [] for f in findings}
        source = {'discipline': discipline, 'final_path': str(final), 'final_sha256': sha(final)}
        if discipline == 'EOM':
            # Explicitly unreviewed: do not misclassify these rows as absent/novel.
            source['coverage_status'] = 'pending_review'
        else:
            suffix = 'completed_coverage_ledger' if discipline in ('AR', 'KM', 'VK') else 'coverage_ledger'
            ledger = HERE / f'{discipline}_r2_{suffix}.json'
            review = json.loads(ledger.read_text())
            assert review['evaluator_sha256'] == sha(evaluator)
            assert review['findings_sha256'] == sha(final)
            assert all(r['coverage'] != 'unchecked' for r in review['targets'] if not any(r['target_id'] in h['target_ids'] and 'truth_scoring' in h['held_dimensions'] for h in json.loads(evaluator.read_text())['holds']))
            for target in review['targets']:
                for fid in target['finding_ids']:
                    links[fid].append({'target_id': target['target_id'], 'route': targets[target['target_id']]['route'], 'coverage': target['coverage']})
            source.update(ledger_path=str(ledger), ledger_sha256=sha(ledger), coverage_status='reviewed')
        sources.append(source)
        for finding in findings:
            fid = finding['id']
            triage = 'coverage_review_pending' if discipline == 'EOM' else ('mapped_requires_addition_review' if links[fid] else 'unmapped_not_yet_classified')
            rows.append({'discipline': discipline, 'repeat': 2, 'finding_id': fid,
                         'finding': finding, 'target_links': links[fid], 'triage': triage,
                         'claim_reviews': [], 'unsupported_additions': 'unchecked',
                         'novel_useful_content': 'unchecked', 'duplicate_burden': 'unchecked',
                         'profile_and_section': 'unchecked', 'engineer_reuse': 'unchecked',
                         'evidence': [], 'disposition': 'unchecked'})
    result = {'status': 'prepared_output_review_not_quality_verdict', 'repeat': 2,
              'evaluator_sha256': sha(evaluator), 'sources': sources, 'rows': rows,
              'counts': dict(Counter(r['triage'] for r in rows)),
              'discipline_counts': dict(Counter(r['discipline'] for r in rows)),
              'rules': ['Split compound rows into factual claims; assess remedies and consequences separately.',
                        'A matched historical target does not validate extra claims or prove engineer acceptance of the whole row.',
                        'No link means unclassified, not false or novel; EOM coverage is explicitly pending.',
                        'Reuse engineer decisions only for the same claim and source context.',
                        'Do not send this evaluator-only queue to independent discovery runs.',
                        'Multiple target links are not independent defects.'],
              'model_calls': 0, 'production_acceptance': False}
    DESTINATION.parent.mkdir(parents=True, exist_ok=True)
    with DESTINATION.open('x') as stream:
        stream.write(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    receipt = {k: v for k, v in result.items() if k != 'rows'}
    receipt.update(queue_path=str(DESTINATION), queue_sha256=sha(DESTINATION), findings=len(rows))
    (HERE / 'baseline_r2_output_review_queue_receipt.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'findings': len(rows), 'disciplines': result['discipline_counts'], 'counts': result['counts']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
