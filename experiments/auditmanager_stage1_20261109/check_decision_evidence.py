"""Audit direct and linked evidence integrity; never certify engineering truth."""
import hashlib
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def resolve(raw):
    p = Path(raw)
    return p if p.is_absolute() else ROOT / p


def main():
    checks, failures, visited = [], [], set()
    cache = {}

    def check(raw, expected, context):
        p = resolve(raw)
        key = (str(p), expected)
        if key not in cache:
            try:
                actual = hashlib.sha256(p.read_bytes()).hexdigest()
                cache[key] = actual == expected
                detail = 'ok' if actual == expected else 'hash_mismatch'
            except OSError as exc:
                cache[key] = False
                detail = type(exc).__name__
            checks.append({'path': str(p), 'expected_sha256': expected,
                           'status': detail, 'context': context})
            if not cache[key]:
                failures.append(checks[-1])
        return cache[key]

    def walk(obj, context):
        if isinstance(obj, list):
            for i, value in enumerate(obj):
                walk(value, f'{context}/{i}')
        elif isinstance(obj, dict):
            if isinstance(obj.get('path'), str) and isinstance(obj.get('sha256'), str):
                check(obj['path'], obj['sha256'], context)
            for name, value in obj.items():
                # Legacy records use image/source_md paired with *_sha256.
                if name.endswith('_sha256') and isinstance(value, str):
                    raw = obj.get(name[:-7])
                    if isinstance(raw, str):
                        check(raw, value, context + '/' + name)
                # Some historical image manifests map absolute paths to hashes.
                if name.startswith('/') and isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value):
                    check(name, value, context)
                walk(value, context + '/' + name)

    def inspect(path):
        path = path.resolve()
        if path in visited:
            return
        visited.add(path)
        obj = json.loads(path.read_text())
        walk(obj, str(path))
        source = obj.get('source_md') or obj.get('project_TEXT')
        if isinstance(source, dict):
            source = source.get('path')
        if isinstance(source, str):
            text = resolve(source).read_text()
            for field in ('evidence', 'TEXT_evidence'):
                for item in obj.get(field, []):
                    if isinstance(item, dict) and 'authoritative_TEXT' in item:
                        if text[item['start']:item['end']] != item['authoritative_TEXT']:
                            failures.append({'context': str(path), 'status': 'text_slice_mismatch',
                                             'block_id': item.get('block_id')})
            if 'exact_quote' in obj and 'quote_offset' in obj:
                start = obj['quote_offset']
                if text[start:start + len(obj['exact_quote'])] != obj['exact_quote']:
                    failures.append({'context': str(path), 'status': 'exact_quote_mismatch'})
        ref = obj.get('primary_adjudication')
        if ref and check(ref['path'], ref['sha256'], str(path)):
            inspect(resolve(ref['path']))

    for path in sorted((HERE / 'decisions').glob('*.json')):
        inspect(path)
    for path in sorted((HERE / 'followup').glob('*control_pair.json')):
        inspect(path)
    report = {
        'scope': 'All decision JSON, primary_adjudication links and followup control pairs; explicit path/hash pairs and legacy image maps; exact TEXT slices where source path is provided.',
        'limits': 'Does not verify unlabeled references, external URLs, engineering conclusions, or independent discovery. Passing does not freeze the benchmark.',
        'summary': {'documents_inspected': len(visited), 'unique_file_hash_checks': len(checks),
                    'failures': len(failures), 'engineering_acceptance': False},
        'failures': failures, 'checks': checks,
    }
    (HERE / 'consolidated/decision_evidence_integrity.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(report['summary'], ensure_ascii=False))
    if failures:
        print(json.dumps(failures, ensure_ascii=False, indent=2))
        raise SystemExit(1)


if __name__ == '__main__':
    main()
