"""Bind completed output review to frozen target coverage; never accept a trial."""
import json
import copy
from pathlib import Path
from collections import Counter
from .score_atomic_review import score, sha
from .verify_output_review_history import verify

HERE = Path(__file__).resolve().parent
QUEUE = Path('/tmp/auditmanager_comparison_evaluation_20261111/baseline_r1/output_review_queue.json')


def build(queue_path=None, initial_path=None):
    queue_path = Path(queue_path or QUEUE)
    history = verify(queue_path, initial_path)
    queue = json.loads(queue_path.read_text())
    repeats = {row["repeat"] for row in queue["rows"]}
    assert len(repeats) == 1
    repeat = next(iter(repeats))
    evaluator = HERE / 'evaluator_release_errata_v2.json'
    assert queue['evaluator_sha256'] == sha(evaluator)
    assert all(r['disposition'] != 'unchecked' and r['claim_reviews'] for r in queue['rows'])
    frozen = json.loads(evaluator.read_text())
    section_holds = {target for hold in frozen['holds']
                     if 'report_section_accuracy' in hold['held_dimensions']
                     for target in hold['target_ids']}
    projects = []
    for source in queue['sources']:
        ledger_path = Path(source['ledger_path'])
        assert sha(ledger_path) == source['ledger_sha256']
        assert sha(source['final_path']) == source['final_sha256']
        ledger = json.loads(ledger_path.read_text())
        scoring_ledger = copy.deepcopy(ledger)
        adapters = []
        for target in scoring_ledger['targets']:
            if target['section_assessment'] == 'held_disputed_output_section_only':
                assert target['target_id'] in section_holds
                adapters.append({'target_id': target['target_id'],
                    'original': target['section_assessment'], 'scorer_input': 'unchecked',
                    'reason': 'Frozen evaluator excludes report_section_accuracy; original ledger remains unchanged.'})
                target['section_assessment'] = 'unchecked'
        result = score(evaluator, source['final_path'], scoring_ledger)
        rows = [r for r in queue['rows'] if r['discipline'] == source['discipline']]
        projects.append({
            'discipline': source['discipline'],
            'source': source,
            'coverage': result,
            'section_label_adapters': adapters,
            'reviewed_output_rows': len(rows),
            'review_dispositions': dict(Counter(r['disposition'] for r in rows)),
            'held_or_unchecked_targets': [r for r in ledger['targets'] if r['coverage'] == 'unchecked'],
            'limits': 'Coverage measures presence of a frozen claim, not correctness of every output addition. Full question coverage is not technical-defect recall.',
        })
    totals = {}
    for route in frozen['truth_denominators']:
        counts = Counter()
        for project in projects:
            counts.update(project['coverage']['routes'][route]['counts'])
        totals[route] = dict(counts)
    return {
        'status': 'first_baseline_review_complete_comparison_pending' if repeat == 1 else 'baseline_repeat_review_complete_comparison_pending',
        'repeat': repeat,
        'evaluator_sha256': sha(evaluator),
        'queue_path': str(queue_path), 'queue_sha256': sha(queue_path),
        'history_verification': history,
        'reviewed_findings': len(queue['rows']),
        'projects': projects,
        'coverage_counts_by_route': totals,
        'production_acceptance': False,
        'limits': [
            'One baseline repeat on six DEV-known projects; no candidate improvement measured here.',
            'Assessed rows can remain uncertain; assessment does not mean engineer acceptance.',
            'Held targets remain excluded under frozen evaluator rules.',
            'Absence of a negative claim is not evidence of successful filtering.',
            'Novel useful findings, duplicate burden and unsupported additions require semantic aggregation; do not count free-form verdict labels as defect totals.',
            'Questions, technical findings and negative controls have separate denominators.',
            'Engineer time, comparative retention, cost acceptance and new-project transfer are not established by this report.',
        ],
    }


if __name__ == '__main__':
    result = build()
    path = HERE / 'baseline_r1_review_summary.json'
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'reviewed_findings': result['reviewed_findings'],
        'projects': [{'discipline': p['discipline'], 'routes': p['coverage']['routes']}
                     for p in result['projects']], 'production_acceptance': False}, ensure_ascii=False))
