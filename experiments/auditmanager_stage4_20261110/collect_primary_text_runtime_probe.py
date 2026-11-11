"""Seal a terminal runtime probe and inspect structural claim coverage, not truth."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re

from .run_primary_text_runtime_probe import sha
from .observe_baseline_collection import process


def collect(root, output):
    from backend.app.pipeline.stages.findings_merge.candidate_coverage import build_candidate_coverage
    from backend.app.pipeline.stages.findings_merge.candidate_decisions import reconcile_candidate_decisions
    from backend.app.pipeline.stages.findings_merge.subclaim_coverage import pending_subclaims
    state_path = root / 'status.json'
    state = json.loads(state_path.read_text())
    if state.get('status') != 'completed_unadjudicated':
        raise ValueError('Probe must finish successfully before review')
    if process(state.get('pid'), 'run_primary_text_runtime_probe')['live'] is not False:
        raise ValueError('Probe process has not exited')
    preparation = json.loads((root / 'preparation.json').read_text())
    started = json.loads((root / 'started.json').read_text())
    request = root / 'request.json'
    if sha(request) != preparation['request_sha256'] or sha(request) != started['request_sha256']:
        raise ValueError('Request seal differs')
    successes = []
    for attempt in state['attempts']:
        path = root / f"attempt_{attempt['attempt']}.json"
        if sha(path) != attempt['sha256']:
            raise ValueError('Response seal differs')
        if not attempt['is_error'] and attempt['json_object']:
            successes.append(path)
    if len(successes) != 1:
        raise ValueError('Expected one successful response')
    response = json.loads(successes[0].read_text())
    final = response['json_data']
    body = json.loads(request.read_text())['messages'][1]['content']
    text, rest = body.removeprefix('## 02_text_analysis.json:\n\n').split('\n\n## 01_blocks_analysis.json:\n\n', 1)
    blocks, rest = rest.split('\n\n## Primary source manifest\n\n', 1)
    manifest = json.loads(rest.split('\n\n## Authoritative primary TEXT blocks', 1)[0])
    known = {re.search(r'\]:\s*(\S+)', item['heading'])[1] for item in manifest['included_text_spans']}
    coverage = build_candidate_coverage(json.loads(text), json.loads(blocks), {'findings': final.get('findings', [])})
    decisions = final.get('candidate_decisions', [])
    checked = reconcile_candidate_decisions(coverage, decisions, known)
    obligations = pending_subclaims(final, known, coverage)
    addressed = {(r.get('origin'), r.get('pointer')): r for r in checked['binding']['decisions'] if isinstance(r, dict)}
    missing = [{'origin': c['origin'], 'pointer': c['pointer'], 'source_id': c['source_id']}
               for c in coverage['candidates'] if not isinstance(addressed.get((c['origin'], c['pointer']), {}).get('subclaims'), list)
               or not addressed.get((c['origin'], c['pointer']), {}).get('subclaims')]
    summary = {'candidate_count': coverage['candidate_count'], 'findings': len(final.get('findings', [])),
               'reported_decisions': dict(Counter(r.get('decision', 'missing') for r in decisions if isinstance(r, dict))),
               'candidates_without_subclaims': len(missing), 'candidates_with_pending_subclaims': len(obligations),
               'pending_subclaims': sum(map(len, obligations.values())), 'not_checked_decisions': checked['not_checked'],
               'usage': {k: response.get(k) for k in ('model', 'duration_ms', 'input_tokens', 'output_tokens', 'cached_tokens')}}
    report = {'status': 'sealed_structural_review_semantic_review_pending', 'summary': summary,
              'request_sha256': sha(request), 'response_path': str(successes[0]), 'response_sha256': sha(successes[0]),
              'status_sha256': sha(state_path), 'coverage': coverage, 'decision_validation': checked,
              'missing_subclaims': missing, 'pending_subclaims': [{'origin': k[0], 'pointer': k[1], 'claims': v} for k, v in obligations.items()],
              'findings': final.get('findings', []), 'production_acceptance': False,
              'limits': ['Known-case merge mechanism probe; no full-project or paired quality inference.',
                         'Subclaim receipts and valid source references do not certify factual correctness.',
                         'Missing subclaims require follow-up, not an assumption of complete coverage.']}
    with output.open('x') as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    print(json.dumps(collect(args.root, args.output), ensure_ascii=False))
