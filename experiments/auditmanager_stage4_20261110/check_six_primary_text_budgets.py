"""Read-only sizing against sealed repeat-1 analyses; no inference or quality claims."""
import json
from pathlib import Path
import hashlib

from backend.app.pipeline.stages.findings_merge.primary_text import build_primary_text_messages

BASE = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
SOURCES = Path('/tmp/auditmanager_corrected_source_inputs_20261111')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inspect():
    rows = []
    for discipline in ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'):
        paths = BASE / f'{discipline.lower()}_r1_classic' / 'classic_control/requests' / discipline
        matched = []
        for path in sorted(paths.glob('*/request.json')):
            request = json.loads(path.read_text())
            if request.get('options', {}).get('stage') == 'findings_merge':
                matched.append((path, request))
        if len(matched) != 1:
            rows.append({'discipline': discipline, 'status': 'request_not_unique', 'matches': len(matched)})
            continue
        path, request = matched[0]
        body = request['messages'][1]['content']
        text, blocks = body.removeprefix('## 02_text_analysis.json:\n\n').split('\n\n## 01_blocks_analysis.json:\n\n', 1)
        block_data, end = json.JSONDecoder().raw_decode(blocks)
        source = SOURCES / discipline / '02_work/document.md'
        historical_md = BASE / f'{discipline.lower()}_r1_classic' / f'classic_control/inputs/{discipline}/versions/v001/02_work/document.md'
        messages = build_primary_text_messages(request['messages'][0]['content'], text,
                      json.dumps(block_data, ensure_ascii=False), source.read_text(), 100_000_000)
        size = sum(len(m['content']) for m in messages)
        rows.append({'discipline': discipline, 'status': 'fits' if size <= 1_000_000 else 'requires_bounded_evidence_operations',
                     'message_characters': size, 'budget': 1_000_000, 'over_budget': max(0, size - 1_000_000),
                     'historical_md_matches_corrected_source': historical_md.is_file() and sha(historical_md) == sha(source),
                     'request': str(path), 'request_sha256': sha(path), 'md_sha256': sha(source),
                     'original_trailing_note_present': bool(blocks[end:].strip()),
                     'system_profile': 'sealed historical request; not the refrozen current candidate'})
    return {'status': 'six_project_input_sizing_only', 'rows': rows, 'model_calls': 0,
            'limitations': ['Uses sealed baseline repeat-1 detector outputs, not a new audit.',
                           'Historical merge template/profile retained for sizing only; final candidate preflight remains required.',
                           'No source or candidate was truncated; overflow is a planning requirement, not a failed model run.'],
            'production_acceptance': False}


if __name__ == '__main__':
    report = inspect()
    output = Path(__file__).with_name('six_primary_text_budget_check.json')
    with output.open('x') as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
    print(json.dumps([{k:r[k] for k in ('discipline','status','message_characters') if k in r} for r in report['rows']]))
