"""Run sealed, size-ready source recovery packets via direct Astra, serially."""
import argparse
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import time

from .run_primary_text_runtime_probe import sha, write


def verify_packet(row):
    path = Path(row['path'])
    if sha(path) != row['sha256']:
        raise ValueError('Prepared request changed')
    packet = json.loads(path.read_text())
    if packet['model'] != 'codex/gpt-6-astra' or packet['allowed_tools'] != '':
        raise ValueError('Unexpected model or tool authorization')
    images = packet['image_hashes']
    for name, digest in images.items():
        if sha(Path(name)) != digest:
            raise ValueError('Source image changed')
    size = sum(Path(p).stat().st_size for p in images)
    budget = 24_000_000 if len(images) == 1 else 16_000_000
    if size > budget or sum(len(m['content']) for m in packet['messages']) >= 1_000_000:
        raise ValueError('Request exceeds local input budget')
    return packet


async def run(root, output):
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    preparation = root / 'preparation.json'
    digest = sha(preparation)
    prepared = json.loads(preparation.read_text())
    if prepared['history_supplied'] is not False:
        raise ValueError('History must not enter source recovery')
    ready = [r for r in prepared['requests'] if r['transport_size_ready']]
    held = [r for r in prepared['requests'] if not r['transport_size_ready']]
    for row in ready:
        verify_packet(row)
    output.mkdir(parents=True, exist_ok=False)
    state = {'status': 'running', 'pid': os.getpid(), 'started': time.time(),
             'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high', 'concurrency': 1,
             'preparation_sha256': digest, 'script_sha256': sha(Path(__file__)),
             'git_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
             'planned_ready': len(ready), 'held': held, 'attempts': [], 'completed_packets': 0,
             'production_acceptance': False}
    status = output / 'status.json'
    write(status, state)
    try:
        for row in ready:
            for attempt in range(1, 4):
                if (output / 'STOP_REQUESTED').exists():
                    state['status'] = 'stopped_before_dispatch'
                    return
                if sha(preparation) != digest:
                    raise ValueError('Preparation changed during collection')
                if substitute_codex_model('gpt-6-astra') != 'gpt-6-astra':
                    raise ValueError('Model substitution forbidden')
                packet = verify_packet(row)
                state.update(active_stage=packet['stage'], active_attempt=attempt)
                write(status, state)
                result = await run_codex_json_messages(packet['messages'], model=packet['model'],
                    reasoning_effort=packet['reasoning_effort'], allowed_tools='',
                    image_paths=list(packet['image_hashes']), timeout=1800,
                    stage=packet['stage'], project_id='AK-runtime-recovery')
                response = output / f"{packet['stage']}_attempt_{attempt}.json"
                write(response, asdict(result))
                success = not result.is_error and isinstance(result.json_data, dict)
                state['attempts'].append({'stage': packet['stage'], 'attempt': attempt,
                    'request_path': row['path'], 'request_sha256': row['sha256'],
                    'response_path': str(response), 'response_sha256': sha(response),
                    'success': success, 'duration_ms': result.duration_ms})
                if success:
                    state['completed_packets'] += 1
                write(status, state)
                if success:
                    break
                if attempt < 3:
                    await asyncio.sleep(10)
            else:
                raise RuntimeError('Packet attempts exhausted; inspect before further dispatch')
        state['status'] = 'ready_packets_completed_held_pending' if held else 'completed_unadjudicated'
    except Exception as exc:
        state.update(status='failed_requires_inspection', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        state['finished'] = time.time()
        write(status, state)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    asyncio.run(run(args.root.resolve(), args.output.resolve()))
