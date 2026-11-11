"""Rebuild source context followups from sealed parent responses; no model calls."""
import argparse
import json
from pathlib import Path
from .run_primary_text_runtime_probe import sha, write
from .run_runtime_probe_recovery import verify_packet


def prepare(previous_preparation, destination):
    from backend.app.pipeline.stages.prepare.recovery_context import build_context_followup
    from backend.app.pipeline.stages.prepare.candidate_recovery import validate_recovery_payload
    from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass
    destination.mkdir(exist_ok=False)
    (destination / 'requests').mkdir()
    rows = []
    for old in json.loads(previous_preparation.read_text())['requests']:
        parent = old['parent']
        request = verify_packet({'path': parent['request_path'], 'sha256': parent['request_sha256']})
        response_path = Path(parent['response_path'])
        if sha(response_path) != parent['response_sha256']:
            raise ValueError('Parent response seal changed')
        response = json.loads(response_path.read_text())['json_data']
        targeted = CodexTargetedPass(request['stage'], request['output_filename'], request['messages'], tuple(request['image_hashes']))
        validated = validate_recovery_payload(targeted, response)
        followup, receipt = build_context_followup(targeted, validated)
        if followup is None:
            raise ValueError(f'Followup no longer schedules: {receipt}')
        packet = {'stage': followup.stage, 'output_filename': followup.output_filename,
            'messages': followup.messages, 'image_hashes': {str(p): sha(Path(p)) for p in followup.image_paths},
            'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high', 'allowed_tools': ''}
        path = destination / 'requests' / (followup.stage + '.json')
        write(path, packet)
        size = sum(Path(p).stat().st_size for p in packet['image_hashes'])
        chars = sum(len(m['content']) for m in packet['messages'])
        budget = 24_000_000 if len(packet['image_hashes']) == 1 else 16_000_000
        rows.append({'path': str(path), 'sha256': sha(path), 'message_characters': chars,
            'images': len(packet['image_hashes']), 'image_bytes': size, 'image_budget': budget,
            'transport_size_ready': size <= budget and chars < 1_000_000,
            'parent': parent, 'context_retrieval': receipt})
    write(destination / 'preparation.json', {'history_supplied': False, 'production_acceptance': False,
        'supersedes_preparation': str(previous_preparation), 'supersedes_sha256': sha(previous_preparation), 'requests': rows})
    return [{k: row[k] for k in ('path', 'message_characters', 'images', 'image_bytes', 'transport_size_ready')} for row in rows]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('previous_preparation', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.previous_preparation, args.destination), ensure_ascii=False))
