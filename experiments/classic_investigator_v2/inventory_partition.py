"""Lossless source-only recovery partition; never modifies an accepted inventory."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
from source_contract import read, write, sha


def partition(packet, max_chars=4000):
    if max_chars <= 0:
        raise ValueError('Positive text budget required')
    result = []
    for source_index, block in enumerate(packet):
        text = block['text']
        # Keep each graphic intact, including its navigation metadata. Split TEXT
        # at line boundaries when possible, without stripping even one character.
        start = 0
        while True:
            end = len(text) if block['navigation_only'] else min(start + max_chars, len(text))
            if end < len(text):
                newline = text.rfind('\n', start, end)
                if newline >= start:
                    end = newline + 1
            piece = copy.deepcopy(block)
            piece.update(text=text[start:end], offset=block['offset'] + start)
            result.append({'parent_source_index': source_index, 'packet': [piece]})
            start = end
            if start == len(text):
                break
    validate_partition(packet, result)
    return result


def validate_partition(parent, children):
    cursor = 0
    for index, source in enumerate(parent):
        pieces = []
        offset = source['offset']
        while cursor < len(children) and children[cursor]['parent_source_index'] == index:
            child = children[cursor]['packet']
            if len(child) != 1:
                raise ValueError('Ambiguous child packet')
            piece = child[0]
            expected = {**source, 'offset': offset, 'text': piece['text']}
            if piece != expected:
                raise ValueError('Source identity or offset changed')
            pieces.append(piece['text'])
            offset += len(piece['text'])
            cursor += 1
        if not pieces or ''.join(pieces) != source['text']:
            raise ValueError('Missing, reordered, or altered source content')
        if source['navigation_only'] and len(pieces) != 1:
            raise ValueError('Graphic must remain intact')
    if cursor != len(children):
        raise ValueError('Unexpected source fragment')


def prepare(inventory, packet_index, output, max_chars=4000):
    if output.exists():
        raise ValueError('Refusing to overwrite prepared recovery')
    plan = read(inventory / 'plan.json')
    state = read(inventory / 'status.json')
    if packet_index != state['completed_packets']:
        raise ValueError('Only first unaccepted packet may be prepared')
    packet = plan[packet_index]
    record = {'status': 'prepared_not_executed', 'parent_packet_index': packet_index,
              'parent_plan_sha256': sha(inventory / 'plan.json'),
              'source_hashes': state['source_hashes'],
              'prompt_sha256': state['prompt_sha256'],
              'max_text_chars': max_chars, 'parent_packet': packet,
              'children': partition(packet, max_chars),
              'limits': 'Exact source coverage only, not object recall. Table headers may be in '
                        'another fragment; unknown context must remain unknown. Cross-fragment '
                        'identity and operations require subsequent reconciliation. No model '
                        'request, response acceptance, or running-state mutation performed.'}
    record['partition_sha256'] = hashlib.sha256(json.dumps(record['children'], sort_keys=True,
                                                          ensure_ascii=False).encode()).hexdigest()
    write(output, record)
    return record


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--inventory', type=Path, required=True)
    p.add_argument('--packet', type=int, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--max-chars', type=int, default=4000)
    a = p.parse_args()
    record = prepare(a.inventory, a.packet, a.output, a.max_chars)
    print('Prepared source fragments:', len(record['children']))
