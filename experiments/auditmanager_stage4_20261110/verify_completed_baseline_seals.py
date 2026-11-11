"""Verify completed transport/output receipts without disclosing finding semantics."""
import argparse
import hashlib
import json
from pathlib import Path


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def verify(root):
    trial_path = root / 'baseline_collection_trial.json'
    trial = json.loads(trial_path.read_text())
    reports = []
    for run in trial['runs']:
        work = Path(run['work']) / 'classic_control'
        status_path = work / 'status' / (run['discipline'] + '.json')
        if not status_path.is_file():
            continue
        state = json.loads(status_path.read_text())
        if state['status'] != 'completed_unadjudicated':
            continue
        usage = state['request_usage']
        assert usage['accounting_complete'] and not usage['issues']
        refs = usage['artifact_sha256']
        request_root = work / 'requests' / run['discipline']
        for name, expected in refs.items():
            path = (request_root / name).resolve()
            assert path.is_relative_to(request_root.resolve())
            assert sha(path) == expected, str(path)
        snapshot = work / 'snapshots' / run['discipline'] / 'findings_verify.json'
        manifest = json.loads((work / 'input_manifest.json').read_text())
        project, = [p for p in manifest if p['discipline'] == run['discipline']]
        final = Path(project['output_dir']) / '03_findings.json'
        assert sha(final) == state['final_sha256']
        # The harness writes snapshots with a trailing newline; compare JSON locally
        # without printing or inspecting finding semantics. Preserve both byte hashes.
        assert json.loads(snapshot.read_bytes()) == json.loads(final.read_bytes())
        reports.append({'discipline': run['discipline'], 'repeat': run['repeat'],
                        'status_path': str(status_path), 'status_sha256': sha(status_path),
                        'final_sha256': state['final_sha256'], 'snapshot_sha256': sha(snapshot), 'snapshot_JSON_matches_output': True,
                        'request_artifacts_verified': len(refs), 'requests': usage['request_count'],
                        'attempts': usage['attempt_count'], 'failed_attempts': usage['failed_attempt_count'],
                        'wall_seconds': state['finished'] - state['started'],
                        'semantic_results_read': False, 'quality_accepted': False})
    return {'trial_sha256': sha(trial_path), 'verified_runs': reports,
            'scope': 'Bytes and completion receipts only; no discovery quality or method acceptance inferred.'}


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    result = verify(args.root)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result, ensure_ascii=False))
