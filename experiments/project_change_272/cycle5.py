from collections import Counter
from .inventory import ROOT, read, immutable, sha, now
from .policy import admitted_pairs
from .grouped_demand import compare


def run(name='05_grouped_demand'):
    admitted_pairs('DEV')
    base = ROOT / 'sources/DEV'
    out = ROOT / 'cycles' / name
    immutable(out / 'MANIFEST.json', dict(started_at=now(), split_sha256=sha(ROOT / 'SPLIT.json'),
        partition='DEV', code_sha256=sha(__file__), purpose='Explicit table row groups own demand events'))
    events = []
    summaries = []
    for pair in read(base / 'PAIRS.json'):
        pools = {s: read(base / 'pools' / (pair[s]['document_version'] + '.json'))['pool'] for s in ['old', 'new']}
        result = compare(pools['old'], pools['new'])
        immutable(out / 'pairs' / (str(pair['index']) + '.json'), result)
        events += [dict(pair_index=pair['index'], change=c) for c in result['project_changes']]
        summary = dict(index=pair['index'], outcomes=result['outcomes'],
                       events=len(result['project_changes']), statuses=dict(Counter(c['status'] for c in result['project_changes'])))
        summaries.append(summary)
        print(summary, flush=True)
    immutable(out / 'RESULTS.json', dict(pairs=summaries, project_changes=events, adjudication='NOT_ADJUDICATED'))


if __name__ == '__main__':
    import sys
    run(sys.argv[1] if len(sys.argv) > 1 else '05_grouped_demand')
