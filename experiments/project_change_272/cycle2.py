"""DEV-only experiment for descriptive quantity owners and grouping diagnostics."""
from collections import Counter
from pathlib import Path

from .inventory import ROOT, immutable, sha, now, read
from .policy import admitted_pairs
from .labeled_state import compare


def run(typed=False):
    admitted_pairs('DEV')
    base = ROOT / 'sources/DEV'
    out = ROOT / ('cycles/03_conditioned_property_states' if typed else 'cycles/02_labeled_quantity_owner')
    if out.exists():
        raise ValueError('Cycle exists')
    immutable(out / 'MANIFEST.json', dict(started_at=now(), partition='DEV',
        split_sha256=sha(ROOT / 'SPLIT.json'),
        root_failure='Engineering labels were discarded because scope required equipment/group ownership',
        change='Self-describing design-quantity owner with exact caption and header certificate',
        code={str(p): sha(p) for p in Path(__file__).parent.glob('*.py')}))
    all_changes = []
    stats = []
    for pair in read(base / 'PAIRS.json'):
        pools = {s: read(base / 'pools' / (pair[s]['document_version'] + '.json'))['pool'] for s in ['old', 'new']}
        result = compare(pools['old'], pools['new'], typed=typed)
        immutable(out / 'pairs' / (str(pair['index']) + '.json'), result)
        all_changes += [dict(pair_index=pair['index'], change=c) for c in result['project_changes']]
        row = dict(index=pair['index'], cases=len(result['cases']), outcomes=result['outcomes'],
                   changes=len(result['project_changes']),
                   statuses=dict(Counter(c['status'] for c in result['project_changes'])))
        stats.append(row)
        print(row, flush=True)
    immutable(out / 'RESULTS.json', dict(pairs=stats, project_changes=all_changes,
                                        adjudication='NOT_ADJUDICATED'))


if __name__ == '__main__':
    import sys
    run(typed='--typed' in sys.argv)
