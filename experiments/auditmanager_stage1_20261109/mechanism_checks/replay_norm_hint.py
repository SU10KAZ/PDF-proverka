"""Offline replay of exact AST functions; no application imports or model calls."""
import ast
import hashlib
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / 'backend/app/pipeline/stages/prepare/codex_targeted_findings.py'
DB = ROOT / 'norms/norms_db.json'
NAMES = {'_norm_signature', '_norm_status_rows', '_norm_status_hints'}

def main():
    tree = ast.parse(SOURCE.read_text())
    nodes = [n for n in tree.body if
             (isinstance(n, ast.FunctionDef) and n.name in NAMES) or
             (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == '_NORM_CODE_RE' for t in n.targets))]
    assert len(nodes) == 4
    scope = dict(re=re, json=json, Path=Path, Any=Any, lru_cache=lru_cache, __file__=str(SOURCE))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), 'exec'), scope)
    context = '| СНиП 12-03-2001 | Безопасность труда в строительстве. Часть 1. Общие требования |'
    hint = scope['_norm_status_hints'](context)
    record = json.loads(DB.read_text())['norms']['СНиП 12-03-2001']
    result = {
        'scope': 'Exact extracted local functions; not full pipeline or historical model replay.',
        'source': str(SOURCE), 'source_sha256': hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        'database': str(DB), 'database_sha256': hashlib.sha256(DB.read_bytes()).hexdigest(),
        'input': context, 'database_record': record, 'actual_hint': hint,
        'observations': {
            'replaced_emitted': 'status=replaced' in hint,
            'replacement_missing': 'replacement=-;' in hint,
            'database_has_replaced_by': bool(record.get('replaced_by')),
            'date_labeled_verified': 'verified=' + record['last_checked'] in hint,
        },
        'limits': 'Reproduces serialization, not truth of norm status; no claim that historical input was identical.',
        'network_or_model_calls': False,
    }
    out = Path(__file__).with_name('norm_hint_replay.json')
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result['observations'], ensure_ascii=False))

if __name__ == '__main__':
    main()
