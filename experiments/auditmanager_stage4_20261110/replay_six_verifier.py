"""Preservation audit of saved findings through current deterministic verifier."""
import hashlib
import json
import tempfile
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase

ROOT = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')


def main():
    projects = json.loads((ROOT / 'input_manifest.json').read_text())
    results = []
    for project in projects:
        source = Path(project['output_dir'])
        original = json.loads((source / '03_findings.json').read_text())
        with tempfile.TemporaryDirectory() as temporary:
            out = Path(temporary)
            sources = []
            for name in ('03_findings.json','01_blocks_analysis.json','02_text_analysis.json','document_graph.json'):
                path = source / name
                if path.exists():
                    raw = path.read_bytes()
                    (out / name).write_bytes(raw)
                    sources.append({'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()})
            stats = _run_deterministic_phase(out, 'six-preservation-' + project['discipline'])
            after = json.loads((out / '03_findings.json').read_text())
            review = json.loads((out / '03_findings_review.json').read_text())
        before_by_id = {f['id']: f for f in original['findings']}
        after_by_id = {f['id']: f for f in after['findings']}
        if len(before_by_id) != len(original['findings']) or len(after_by_id) != len(after['findings']):
            raise ValueError('Duplicate finding IDs: ' + project['discipline'])
        changes = []
        for fid in sorted(before_by_id.keys() & after_by_id.keys()):
            old, new = before_by_id[fid], after_by_id[fid]
            fields = {k: {'before': old.get(k), 'after': new.get(k)}
                      for k in old.keys() | new.keys() if old.get(k) != new.get(k)}
            if fields:
                changes.append({'id': fid, 'fields': fields})
        row = {'discipline': project['discipline'], 'before': len(before_by_id), 'after': len(after_by_id),
               'removed': sorted(before_by_id.keys() - after_by_id.keys()),
               'added': sorted(after_by_id.keys() - before_by_id.keys()),
               'changes': changes, 'stats': stats, 'review': review, 'sources': sources}
        results.append(row)
        print(row['discipline'], row['before'], row['after'], 'changed', len(changes), 'removed', row['removed'])
    result = {'scope': 'Saved six-project findings through current deterministic verifier/corrector only',
              'projects': results, 'new_model_calls': 0,
              'boundary': 'Not independent discovery, not a full new audit, not semantic truth adjudication. Any changed fields require assessment before acceptance.'}
    Path(__file__).with_name('six_project_verifier_preservation.json').write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__':
    main()
