"""Accept completed child observations with explicit provenance, never synthetic answers."""
import argparse
import fcntl
import json
from pathlib import Path
import time
from source_contract import read, write, sha
from inventory import PROMPT, validate, recover, registry
from inventory_partition import validate_partition


def child_results(prepared, output, parent, index):
    plan, state = read(prepared), read(output / 'status.json')
    if (state['status'] != 'completed_unverified' or state['prepared_sha256'] != sha(prepared)
            or plan['parent_packet'] != parent or plan['parent_packet_index'] != index):
        raise ValueError('Incomplete or changed recovery')
    validate_partition(parent, plan['children'])
    if set(state['accepted']) != {str(i) for i in range(len(plan['children']))}:
        raise ValueError('Missing or unexpected accepted children')
    results, receipts = [], {str(prepared): sha(prepared)}
    for i, child in enumerate(plan['children']):
        prefix = state['accepted'][str(i)]
        if not isinstance(prefix, str) or Path(prefix).name != prefix:
            raise ValueError('Invalid response identity')
        reqpath, respath = output / f'{prefix}_request.json', output / f'{prefix}_response.json'
        request, response = read(reqpath), read(respath)
        messages = request['messages']
        if len(messages) != 2 or messages[0] != {'role': 'system', 'content': PROMPT}:
            raise ValueError('Child prompt changed')
        delivered = json.loads(messages[1]['content'])
        if messages[1]['role'] != 'user' or delivered['packet'] != child['packet']:
            raise ValueError('Child source delivery changed')
        hashes = request['image_hashes']
        if set(request['images']) != set(hashes) or any(sha(p) != digest for p, digest in hashes.items()):
            raise ValueError('Child image changed')
        graphics = [b for b in child['packet'] if b['navigation_only']]
        if graphics and (not hashes or len(delivered['image_receipts']) != len(graphics)):
            raise ValueError('Graphic attachment missing')
        for block, receipt in zip(graphics, delivered['image_receipts']):
            if (receipt.get('id') != block['id'] or receipt.get('region') != [0, 0, 1, 1]
                    or receipt.get('image_sha256') not in hashes.values()):
                raise ValueError('Graphic receipt does not match delivered image')
        if response.get('error'):
            raise ValueError('Failed child response')
        validate(response['json'], child['packet'])
        receipts.update({str(reqpath): sha(reqpath), str(respath): sha(respath), **hashes})
        results.append((f'P{index:03d}:S{i:03d}', response['json']))
    return results, receipts


def load_accepted(record, parent, index, source_hashes, plan_hash):
    if any(sha(path) != digest for path, digest in record['receipts'].items()):
        raise ValueError('Accepted recovery artifact changed')
    plan = read(record['prepared'])
    if plan['source_hashes'] != source_hashes or plan['parent_plan_sha256'] != plan_hash:
        raise ValueError('Accepted recovery corpus changed')
    results, receipts = child_results(Path(record['prepared']), Path(record['output']), parent, index)
    if receipts != record['receipts']:
        raise ValueError('Accepted response selection changed')
    return results


def accept(inventory, prepared, output):
    with (inventory / '.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        from run_inventory_partition import check_plan
        plan = check_plan(inventory, prepared)
        batch = read(inventory / 'plan.json')
        state, previous = recover(inventory, batch, plan['source_hashes'])
        index = plan['parent_packet_index']
        results, receipts = child_results(prepared, output, batch[index], index)
        record = {'prepared': str(prepared.resolve()), 'output': str(output.resolve()),
                  'receipts': receipts, 'accepted_at': time.time(),
                  'meaning': 'Validated source observations; not engineering truth or completed operations'}
        # Save the old state before committing the acceptance. A stopped run is
        # resumed explicitly by the existing inventory worker, from the next parent.
        write(inventory / f'partition_accept_{time.time_ns()}_previous_status.json', state)
        state.setdefault('accepted_partitions', {})[str(index)] = record
        state.update(completed_packets=index + 1, status='failed',
                     error='Recovery accepted; awaiting explicit inventory resume')
        write(inventory / 'registry.json', registry(previous + results))
        write(inventory / 'status.json', state)
        return state


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--inventory', type=Path, required=True)
    p.add_argument('--prepared', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    print('Accepted parent packets:', accept(a.inventory, a.prepared, a.output)['completed_packets'])
