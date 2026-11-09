"""Resolve page sheet labels only from explicit authoritative MD stamp fields."""
import hashlib
import json
from pathlib import Path


def sheet_metadata(page):
    receipts = []
    for block in page['text_blocks'] + page['image_blocks']:
        for line in block.get('text', block.get('ocr_raw', '')).splitlines():
            if not line.startswith('> **Stamp:**'):
                continue
            fields = {}
            for part in line.removeprefix('> **Stamp:**').split('|'):
                key, separator, value = part.partition(':')
                if separator:
                    fields[key.strip()] = value.strip()
            number = fields.get('Sheet', '')
            if number.casefold() in ('', 'не указано', 'unknown', 'not specified'):
                continue
            receipts.append({'block_id': block['id'], 'quote': line,
                             'sheet': number, 'name': fields.get('Наименование листа')})
    numbers = sorted({r['sheet'] for r in receipts})
    names = {r['name'] for r in receipts if r['name'] and r['name'] != 'Не указано'}
    unique = len(numbers) == 1
    return {'sheet_no_raw': numbers[0] if unique else None,
            'sheet_no_normalized': numbers[0] if unique else None,
            'sheet_name': next(iter(names)) if unique and len(names) == 1 else None,
            'sheet_confidence': 'high' if unique else ('conflict' if numbers else 'missing'),
            'sheet_source_receipts': receipts}


if __name__ == '__main__':
    directory = Path(__file__).resolve().parent
    manifest = json.loads((directory / 'six_project_source_graphs.json').read_text())
    results = []
    for entry in manifest['graphs']:
        path = Path(entry['path'])
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry['sha256']:
            raise ValueError('Graph changed since source construction: ' + str(path))
        graph = json.loads(raw)
        counts = {'high': 0, 'conflict': 0, 'missing': 0}
        for page in graph['pages']:
            result = sheet_metadata(page)
            page.update(result)
            counts[result['sheet_confidence']] += 1
        graph['boundary'] = 'TEXT is unchanged authoritative MD. Sheet labels come only from explicit MD stamp fields; missing/conflicting labels stay unresolved. Graphic descriptions are not verified evidence.'
        path.write_text(json.dumps(graph, ensure_ascii=False, indent=2) + '\n')
        results.append({'discipline': entry['discipline'], 'path': str(path),
                        'input_sha256': entry['sha256'],
                        'output_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                        'sheet_status_counts': counts})
        print(entry['discipline'], counts)
    (directory / 'six_project_sheet_metadata.json').write_text(json.dumps(
        {'graphs': results, 'new_model_calls': 0,
         'boundary': 'Missing sheet labels are not PDF page numbers. Use explicit PDF page and block references when a sheet label is unavailable.'},
        ensure_ascii=False, indent=2) + '\n')
