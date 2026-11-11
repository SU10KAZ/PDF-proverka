"""Collect both sealed pilot arms into an unchecked candidate review ledger."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re

from .materialize_trial_inputs import sha
from .observe_baseline_collection import process

HERE = Path(__file__).resolve().parent


def collect(root, output):
    state_path = root/'pilot_status.json'
    state = json.loads(state_path.read_text())
    if state.get('status') != 'completed_unadjudicated':
        raise ValueError('Both pilot arms must finish before comparative review')
    if process(state.get('pid'), 'run_merge_primary_text_pilot')['live'] is not False:
        raise ValueError('Pilot process has not exited')
    preparation_path = HERE/'merge_primary_text_pilot_preparation.json'
    preparation = json.loads(preparation_path.read_text())
    from backend.app.pipeline.stages.findings_merge.candidate_coverage import build_candidate_coverage
    from backend.app.pipeline.stages.findings_merge.candidate_decisions import reconcile_candidate_decisions
    source_ids = {re.search(r'\]:\s*(\S+)', row['heading'])[1] for row in preparation['text_spans']}
    responses = {}
    for arm in ('control', 'primary_text'):
        item = preparation['requests'][arm]
        path = Path(item['path'])
        if sha(path) != item['sha256']:
            raise ValueError('Request binding changed')
        attempts = [a for a in state['attempts'] if a['arm'] == arm]
        for attempt in attempts:
            if sha(Path(attempt['path'])) != attempt['sha256']:
                raise ValueError('Response binding changed')
        successful = [a for a in attempts if a['is_error'] is False and a['json_object'] is True]
        if len(successful) != 1:
            raise ValueError('Expected exactly one successful response per arm')
        response = json.loads(Path(successful[0]['path']).read_text())
        if not isinstance(response.get('json_data'), dict):
            raise ValueError('Missing response object')
        responses[arm] = (successful[0], response)
    request = json.loads(Path(preparation['requests']['control']['path']).read_text())
    content = request['messages'][1]['content']
    text, graphic = content.removeprefix('## 02_text_analysis.json:\n\n').split('\n\n## 01_blocks_analysis.json:\n\n', 1)
    text, graphic = json.loads(text), json.loads(graphic)
    rows, summaries = {}, {}
    for arm, (binding, response) in responses.items():
        final = response['json_data']
        coverage = build_candidate_coverage(text, graphic, final)
        # Only actually supplied primary blocks qualify as inspected references.
        # Prior detector references do not become primary evidence in the control.
        checked = reconcile_candidate_decisions(coverage, final.get('candidate_decisions'),
                                               source_ids if arm == 'primary_text' else set())
        rows[arm] = {'response': binding, 'coverage': coverage, 'decision_validation': checked,
            'findings': final.get('findings', []), 'engineering_assessment': 'unchecked'}
        raw = final.get('candidate_decisions', [])
        summaries[arm] = {'candidate_count': coverage['candidate_count'],
            'output_findings': len(final.get('findings', [])),
            'reported_decisions': dict(Counter(r.get('decision', 'missing') for r in raw if isinstance(r, dict))),
            'structurally_unresolved_decisions': checked['not_checked'],
            'unmatched_decisions': len(checked['unmatched_decisions']),
            'malformed_decisions': len(checked['malformed_decisions']),
            'semantic_acceptance': False,
            'usage': {k: response.get(k) for k in ('model','duration_ms','input_tokens','output_tokens','cached_tokens','cost_source')}}
    report = {'status':'paired_responses_sealed_review_unchecked',
        'state_path':str(state_path),'state_sha256':sha(state_path),
        'preparation_sha256':sha(preparation_path), 'summary':summaries, 'arms':rows,
        'review_requirements':['Review all retained/refuted/duplicate claims against attached TEXT.',
            'Check partial preservation and changes to compound source claims.',
            'Do not validate graphical topology from TEXT-only packets.',
            'Separate questions from technical defects and record unsupported additions.',
            'ID linkage and model-reported decisions are not semantic acceptance.'],
        'production_acceptance':False}
    with output.open('x') as f:
        json.dump(report,f,ensure_ascii=False,indent=2);f.write('\n')
    return summaries


if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(collect(a.root,a.output),ensure_ascii=False))
