"""Read-only inspection: no provider calls, imports of pipeline, or job resumption."""
import hashlib
import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
CONTROL = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')


def read(path):
    return json.loads(path.read_text())


def ref(path):
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def main():
    historical = read(ROOT / 'experiments/auditmanager_v4_transfer_20261105/implementation_sources.json')
    code = []
    for old in historical:
        path = ROOT / old['path']
        if not path.is_file():
            code.append({'path': old['path'], 'current_exists': False})
            continue
        digest = ref(path)['sha256']
        code.append({'path': old['path'], 'symbol': old.get('symbol'),
                     'current_sha256': digest, 'historical_sha256': old['sha256'],
                     'same_as_historical_snapshot': digest == old['sha256'],
                     'historical_claim_not_new_verification': old.get('claim')})
    runs = []
    for discipline in ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'):
        status_path = CONTROL / 'status' / (discipline + '.json')
        status = read(status_path)
        before_path = CONTROL / 'snapshots' / discipline / 'findings_merge.json'
        after_path = CONTROL / 'snapshots' / discipline / 'findings_verify.json'
        before = read(before_path)['findings']
        after = read(after_path)['findings']
        b = {f['id']: f for f in before}
        a = {f['id']: f for f in after}
        assert len(b) == len(before) and len(a) == len(after)
        changes = []
        for key in sorted(b.keys() & a.keys()):
            fields = sorted(k for k in b[key].keys() | a[key].keys() if b[key].get(k) != a[key].get(k))
            if fields:
                changes.append({'finding_id': key, 'changed_fields': fields,
                                'before': {k: b[key].get(k) for k in fields},
                                'after': {k: a[key].get(k) for k in fields}})
        runs.append({'discipline': discipline, 'saved_status': status.get('status'),
                     'sources': [ref(status_path), ref(before_path), ref(after_path)],
                     'usage_records': status.get('usage', []),
                     'block_progress': status.get('block_progress'),
                     'verifier_detail': status.get('stages', {}).get('findings_corrector', {}).get('detail'),
                     'before_count': len(before), 'after_count': len(after),
                     'removed_ids': sorted(b.keys() - a.keys()), 'added_ids': sorted(a.keys() - b.keys()),
                     'changed_findings': changes,
                     'saved_final_sha_matches_verify_snapshot': status.get('final_sha256') == ref(after_path)['sha256']})
    output = {'purpose': 'Stage 2 evidence: saved classic-control runs and code identity; not new model runs.',
              'current_checkout_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'code_identity': code, 'runs': runs,
              'limits': ['Saved completion status is not evidence of a currently live process.',
                         'A saved usage model name is a recorded attribution, not independent provider receipt verification.',
                         'Zero recorded cost is not proof of zero subscription or total economic cost.',
                         'ID preservation does not prove semantic preservation; inspect changed text and upstream candidate mapping.',
                         'Matching current code hash does not by itself prove which code executed in a historical run.',
                         'No inference that a removed candidate was useful or a retained candidate was correct without the frozen adjudication.']}
    (HERE / 'saved_runtime_inventory.json').write_text(json.dumps(output, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'code_files': len(code), 'changed_since_snapshot': sum(x.get('same_as_historical_snapshot') is False for x in code),
                      'runs': [{'discipline': r['discipline'], 'before': r['before_count'], 'after': r['after_count'],
                                'removed_ids': r['removed_ids'], 'changed_findings': len(r['changed_findings']),
                                'final_hash_match': r['saved_final_sha_matches_verify_snapshot']} for r in runs]}, ensure_ascii=False))


if __name__ == '__main__':
    main()
