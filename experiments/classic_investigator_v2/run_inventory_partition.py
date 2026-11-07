"""Run a frozen recovery partition separately, retaining child-level provenance."""
import argparse
import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import time
from inventory import PROMPT, validate, registry
from inventory_partition import validate_partition
from source_contract import Corpus, read, write, sha
from investigate import configure
from quota_policy import assert_allowed
from subscription_retry import retry_subscription_call


def check_plan(inventory, prepared):
    plan = read(prepared)
    state = read(inventory / 'status.json')
    parent = read(inventory / 'plan.json')
    index = plan['parent_packet_index']
    if (sha(inventory / 'plan.json') != plan['parent_plan_sha256'] or
            state['source_hashes'] != plan['source_hashes'] or
            state['completed_packets'] != index or parent[index] != plan['parent_packet']):
        raise ValueError('Recovery no longer matches first unaccepted parent packet')
    if plan['prompt_sha256'] != hashlib.sha256(PROMPT.encode()).hexdigest():
        raise ValueError('Inventory prompt changed')
    digest = hashlib.sha256(json.dumps(plan['children'], sort_keys=True,
                                      ensure_ascii=False).encode()).hexdigest()
    if digest != plan['partition_sha256']:
        raise ValueError('Partition changed')
    validate_partition(plan['parent_packet'], plan['children'])
    return plan


async def run(work, disc, prepared, output, *, resume=False, timeout=600):
    if timeout <= 0:
        raise ValueError('Positive timeout required')
    inventory = work / 'inventory_v2' / disc
    # This is the same lock as the original inventory worker. A second worker
    # cannot issue duplicate requests even if its stale status says "running".
    with (inventory / '.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        plan = check_plan(inventory, prepared)
        if resume:
            state = read(output / 'status.json')
            if state['prepared_sha256'] != sha(prepared):
                raise ValueError('Resume partition changed')
            if state['status'] == 'completed_unverified':
                raise ValueError('Already completed')
        else:
            output.mkdir(parents=True, exist_ok=False)
            state = {'status': 'prepared', 'prepared_sha256': sha(prepared),
                     'started': time.time(), 'accepted': {}, 'discipline': disc,
                     'model': 'gpt-6-astra', 'parent_packet_index': plan['parent_packet_index']}
        corpus = Corpus(work / 'sources' / disc, output / 'images')
        for name, digest in plan['source_hashes'].items():
            if sha(corpus.source / name) != digest:
                raise ValueError('Frozen source changed')
        configure(output)
        from backend.app.services.llm import codex_runner
        results = []
        state.update(status='running', code_sha256=sha(__file__), request_timeout=timeout)
        state.pop('error', None)
        write(output / 'status.json', state)
        try:
            for index, child in enumerate(plan['children']):
                packet = child['packet']
                images, receipts = [], []
                for block in packet:
                    if block['navigation_only']:
                        receipt, attached = corpus.execute({'tool': 'image', 'id': block['id']})
                        images.extend(attached)
                        receipts.append(receipt)
                request = {'messages': [{'role': 'system', 'content': PROMPT},
                    {'role': 'user', 'content': json.dumps({'packet': packet,
                        'image_receipts': receipts}, ensure_ascii=False)}],
                    'images': images, 'image_hashes': {p: sha(p) for p in images}}
                prefix = state['accepted'].get(str(index))
                if prefix:
                    if read(output / f'{prefix}_request.json') != request:
                        raise ValueError('Accepted child request changed')
                    response = read(output / f'{prefix}_response.json')
                    if response.get('error'):
                        raise ValueError('Accepted child response failed')
                    data = response['json']
                    validate(data, packet)
                else:
                    for validation_attempt in range(3):
                        if (work / 'STOP_REQUESTED.json').exists():
                            raise RuntimeError('Stop requested')
                        prefix = f'{index:03d}_{time.time_ns()}'
                        write(output / f'{prefix}_request.json', request)
                        attempts = []
                        def record(attempt):
                            attempts.append(attempt)
                            write(output / f'{prefix}_attempts.json', attempts)
                        async def guarded(*args, **kwargs):
                            assert_allowed(work, kwargs['model'])
                            return await codex_runner.run_codex_json_messages(*args, **kwargs)
                        result = await retry_subscription_call(guarded, request['messages'],
                            model='gpt-6-astra', timeout=timeout, reasoning_effort='medium',
                            stage='inventory_partition', project_id=disc, image_paths=images,
                            allowed_tools='', stop_requested=lambda: (work / 'STOP_REQUESTED.json').exists(),
                            on_attempt=record)
                        write(output / f'{prefix}_response.json', {'json': result.json_data,
                            'text': result.text, 'error': result.error_message})
                        if result.is_error:
                            raise RuntimeError(result.error_message)
                        data = result.json_data
                        try:
                            validate(data, packet)
                        except (ValueError, TypeError, KeyError) as error:
                            write(output / f'{prefix}_validation.json', {'accepted': False, 'error': str(error)})
                            if validation_attempt == 2:
                                raise
                            await asyncio.sleep(10)
                            continue
                        write(output / f'{prefix}_validation.json', {'accepted': True})
                        state['accepted'][str(index)] = prefix
                        write(output / 'status.json', state)
                        break
                # Child identity remains explicit; no synthetic parent model response.
                results.append((f'P{plan["parent_packet_index"]:03d}:S{index:03d}', data))
                write(output / 'registry.json', registry(results))
                print(f'{disc} recovery {index + 1}/{len(plan["children"])}', flush=True)
            state['status'] = 'completed_unverified'
        except Exception as error:
            state.update(status='failed', error=f'{type(error).__name__}: {error}')
            raise
        finally:
            state['updated_at'] = time.time()
            write(output / 'status.json', state)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--work', type=Path, required=True)
    p.add_argument('--discipline', required=True, choices=['AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'])
    p.add_argument('--prepared', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--resume', action='store_true')
    p.add_argument('--timeout', type=int, default=600)
    a = p.parse_args()
    asyncio.run(run(a.work, a.discipline, a.prepared, a.output, resume=a.resume, timeout=a.timeout))
