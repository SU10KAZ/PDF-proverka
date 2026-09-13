"""Deterministic receipts and normalization; no route inference."""
import hashlib
import json
import re
import unicodedata
from pathlib import Path


def canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')) + '\n').encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical(value))


def read(path):
    return json.loads(Path(path).read_text())


def norm(value):
    return ' '.join(unicodedata.normalize('NFC', str(value)).casefold()
                    .replace('ё', 'е').replace('**', '').replace('__', '').split())


def code_key(value):
    # Syntax-only comparison, no inferred aliases or changed document scopes.
    return re.sub(r'[^\w]', '', norm(value)).replace('_', '')
