"""Verify the frozen evaluator release, not engineering truth or model quality."""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def read(path):
    return json.loads(path.read_text())


def main():
    release = read(HERE / 'benchmark_release_v1.json')
    for ref in release['files']:
        path = ROOT / ref['path']
        assert hashlib.sha256(path.read_bytes()).hexdigest() == ref['sha256'], path
    targets = read(HERE / 'consolidated/evaluation_targets.json')['targets']
    assert len({t['target_id'] for t in targets}) == len(targets)
    for route, ids in release['denominators'].items():
        assert set(ids) == {t['target_id'] for t in targets if t['route'] == route}
    records = read(HERE / 'consolidated/records.json')
    seen = []
    for path in sorted((HERE / 'decisions').glob('*.json')):
        seen.extend(read(path)['candidate_ids'])
    assert len(seen) == len(set(seen)) == len(records) == 291
    assert set(seen) == {r['candidate_id'] for r in records}
    ledger = read(HERE / 'consolidated/atom_ledger.json')
    assert ledger['summary']['explicit_scoring_reviews'] == 1193
    assert ledger['summary']['remaining_scoring_normalizations'] == 0
    for t in targets:
        for ctx in t['contexts']:
            path = ROOT / ctx['source']['path']
            assert hashlib.sha256(path.read_bytes()).hexdigest() == ctx['source']['sha256']
    covered = set(read(HERE / 'consolidated/evaluation_targets.json')['theme_index'])
    excluded = {r['theme'] for r in release['unscored_themes']}
    assert covered.isdisjoint(excluded)
    assert covered | excluded == {p.stem for p in (HERE / 'decisions').glob('*.json')}
    inputs = read(HERE / 'consolidated/input_inventory.json')
    for project in inputs['projects'].values():
        for ref in project['files']:
            with Path(ref['path']).open('rb') as stream:
                assert hashlib.file_digest(stream, 'sha256').hexdigest() == ref['sha256'], ref['path']
    print(json.dumps({'release': release['version'], 'sealed_files': len(release['files']),
                      'records': len(records), 'targets': len(targets), 'primary_files': 18,
                      'integrity_passed': True, 'runtime_acceptance': False}, ensure_ascii=False))


if __name__ == '__main__':
    main()
