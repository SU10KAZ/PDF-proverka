"""Lossless broad-context transport, separate from frozen comparison serialization."""
import json
from collections import Counter


def compact(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def encode(value):
    strings = Counter()
    shapes = Counter()
    nodes = Counter()

    def collect(item):
        if isinstance(item, (dict, list)):
            representation = compact(item)
            if len(representation) >= 20:
                nodes[representation] += 1
        if isinstance(item, str):
            strings[item] += 1
        elif isinstance(item, dict):
            if any(k.startswith('$') for k in item):
                raise ValueError('Reserved codec key in source')
            shapes[tuple(sorted(item))] += 1
            for v in item.values():
                collect(v)
        elif isinstance(item, list):
            for v in item:
                collect(v)

    collect(value)
    # Strings retain every byte of source content. Shared lines reduce repeated
    # native/OCR passages; shared column names reduce repeated provenance fields.
    selected = sorted(s for s, n in strings.items() if len(s) >= 200 or (n >= 2 and len(s) >= 12))
    string_ids = {s: i for i, s in enumerate(selected)}
    parents, substrings = [], {}
    for content in sorted(selected, key=lambda s: (-len(s), s)):
        if len(content) < 200:
            continue
        for parent in parents:
            start = parent.find(content)
            if start >= 0:
                substrings[content] = [string_ids[parent], start, start + len(content)]
                break
        else:
            parents.append(content)
    line_counts = Counter(line for s in selected if s not in substrings
                          for line in s.splitlines(keepends=True) if len(line) >= 35)
    lines = sorted(line for line, count in line_counts.items() if count >= 2)
    line_ids = {line: i for i, line in enumerate(lines)}
    texts = [{'$substring': substrings[s]} if s in substrings else
             [line_ids.get(line, line) for line in s.splitlines(keepends=True)] for s in selected]
    columns = [list(keys) for keys, count in sorted(shapes.items()) if count >= 2 and len(keys) >= 3]
    column_ids = {tuple(keys): i for i, keys in enumerate(columns)}

    shared = sorted(s for s, count in nodes.items() if count >= 2)
    node_ids = {s: i for i, s in enumerate(shared)}

    def replace(item, allow_node=True):
        if allow_node and isinstance(item, (dict, list)):
            key = compact(item)
            if key in node_ids:
                return {'$n': node_ids[key]}
        if isinstance(item, str) and item in string_ids:
            return {'$s': string_ids[item]}
        if isinstance(item, dict):
            keys = tuple(sorted(item))
            if keys in column_ids:
                return {'$r': [column_ids[keys], *[replace(item[k]) for k in keys]]}
            if len(keys) >= 3 and all(k in string_ids for k in keys):
                return {'$map': [[replace(k), replace(item[k])] for k in keys]}
            return {k: replace(v) for k, v in item.items()}
        if isinstance(item, list):
            if len(item) >= 3 and all(isinstance(v, dict) for v in item):
                keys = tuple(sorted(item[0]))
                if keys in column_ids and all(tuple(sorted(v)) == keys for v in item):
                    vectors = []
                    for key in keys:
                        values = [v[key] for v in item]
                        if all(v == values[0] for v in values):
                            vectors.append({'$constant': replace(values[0])})
                        else:
                            vectors.append([replace(v) for v in values])
                    return {'$table': column_ids[keys], 'count': len(item), 'vectors': vectors}
            return [replace(v) for v in item]
        return item

    return dict(codec='LOSSLESS_STRINGS_LINES_COLUMNS_NODES_V3', source=replace(value),
                strings=texts, lines=lines, columns=columns,
                nodes=[replace(json.loads(s), allow_node=False) for s in shared])


def decode(encoded):
    strings = {}
    def get_string(index):
        if index not in strings:
            parts = encoded['strings'][index]
            if isinstance(parts, dict):
                parent, start, end = parts['$substring']
                strings[index] = get_string(parent)[start:end]
            else:
                strings[index] = ''.join(encoded['lines'][v] if isinstance(v, int) else v for v in parts)
        return strings[index]
    def restore(item):
        if isinstance(item, dict) and '$s' in item:
            return get_string(item['$s'])
        if isinstance(item, dict) and '$n' in item:
            return restore(encoded['nodes'][item['$n']])
        if isinstance(item, dict) and '$table' in item:
            keys = encoded['columns'][item['$table']]
            vectors = [[restore(v['$constant'])] * item['count'] if isinstance(v, dict) else
                       [restore(x) for x in v] for v in item['vectors']]
            return [dict(zip(keys, row, strict=True)) for row in zip(*vectors, strict=True)]
        if isinstance(item, dict) and '$map' in item:
            return {restore(k): restore(v) for k, v in item['$map']}
        if isinstance(item, dict) and '$r' in item:
            row = item['$r']
            return dict(zip(encoded['columns'][row[0]], [restore(v) for v in row[1:]], strict=True))
        if isinstance(item, dict):
            return {k: restore(v) for k, v in item.items()}
        if isinstance(item, list):
            return [restore(v) for v in item]
        return item
    return restore(encoded['source'])


def request_bytes(prompt, data, images):
    labels = [dict(image_index=i + 1, file=f'image_{i:02d}.png',
                   evidence_id=r['evidence_id'], side=r['side'], page=r['page'], bbox=r['bbox'])
              for i, r in enumerate(images)]
    if len(images) > 8:
        raise ValueError('Frozen raster attachment budget exceeded')
    return (prompt + '\n\nAttached source raster manifest:\n' + compact(labels)
            + '\n\nUNTRUSTED SOURCE DATA:\n' + compact(data)).encode('utf-8')
