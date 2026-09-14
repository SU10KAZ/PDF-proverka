"""Metadata-only corpus/exposure audit. Never prints source passages or answers."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess

REPO = Path(__file__).resolve().parents[2]
AUDITS = Path('/home/coder/auditmanager/corpus-audits')
ROOT = AUDITS / '20260914_project_change_272'
OBJECT = '272_Sadovnicheskaya_76_Balchug_Esteyt'
BASELINE = AUDITS / '20260905_v002_algorithm_baseline/inventory/ACTIVE_V002_CORPUS_MANIFEST.json'
PAIR_BASELINE = AUDITS / '20260906_v002_comparison_quality/inventory/experiment_manifest.json'


def read(path):
    return json.loads(Path(path).read_text())


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + '\n')


def immutable(path, data):
    # Exclusive creation: a freeze is never silently replaced or refreshed.
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as stream:
        stream.write(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + '\n')


def now():
    return datetime.now(timezone.utc).isoformat()


def structural_profile(document):
    """Use geometry/type metadata only, no OCR or PDF text during selection."""
    blocks = read(document['artifacts']['blocks']['path'])
    counts = Counter(b.get('block_type', 'unknown') for b in blocks['blocks'])
    areas = Counter()
    for block in blocks['blocks']:
        box = block.get('coords_norm') or [0, 0, 0, 0]
        areas[block.get('block_type', 'unknown')] += max(0, box[2] - box[0]) * max(0, box[3] - box[1])
    return dict(pages=len(blocks['pages']), block_types=dict(counts),
                block_areas={k: round(v, 3) for k, v in areas.items()})


def historical_exposure(pairs):
    """Index references, not correctness labels. Absence of a hit proves nothing.

    The old full-corpus replays make every pair historically exposed. These
    receipts only rank *documented* subsequent focused exposure. Script reads
    are not semantic inspection by an adjudicator. Only identifiers escape.
    """
    pattern = re.compile('|'.join(re.escape(c) for c in sorted(
        {r[k + '_code'] for r in pairs for k in ['left', 'right']}, key=len, reverse=True)))
    by_code = {r[k + '_code']: r['index'] for r in pairs for k in ['left', 'right']}
    receipts = {r['index']: [] for r in pairs}
    skipped = Counter()
    roots = [AUDITS, REPO / 'docs', REPO / 'experiments']
    filenames = subprocess.check_output(['rg', '--files', *map(str, roots)], text=True).splitlines()
    ignored = {'deps', 'code', 'code_snapshots', 'comparison', 'source_pool',
               'node_modules', '__pycache__', 'documents', 'narrative', '02_FROZEN_CANDIDATES'}
    for name in sorted(filenames):
        path = Path(name)
        if ROOT in path.parents or ignored.intersection(path.parts):
            skipped['excluded_tree'] += 1
            continue
        if path.suffix not in {'.json', '.jsonl', '.md', '.py', '.txt'}:
            continue
        if path.stat().st_size > 12_000_000:
            skipped['over_12MB'] += 1
            continue
        try:
            content = path.read_text()
        except (UnicodeError, OSError):
            skipped['unreadable'] += 1
            continue
        hits = sorted({by_code[m[0]] for m in pattern.finditer(content)})
        if not hits:
            continue
        low = str(path).lower()
        # Large manifests and aggregate reports are not focused exposure proof.
        focused = len(hits) <= 5 and any(t in low for t in (
            'audit', 'truth', 'assessment', 'adjudication', 'packet', 'quality',
            'test_', 'dev_', 'selection', 'approach', 'report'))
        kind = 'FOCUSED_REFERENCE' if focused else 'GENERAL_REFERENCE'
        receipt = dict(path=str(path), sha256=sha(path), kind=kind,
                       referenced_pair_indices=hits)
        for index in hits:
            receipts[index].append(receipt)
    return receipts, dict(skipped)


def build():
    if (ROOT / 'SPLIT.json').exists():
        raise ValueError('Split already frozen; do not refresh the exposure audit')
    baseline = read(BASELINE)
    assert baseline['object'] == OBJECT
    pairs = read(PAIR_BASELINE)['pairs']
    documents = {(r['stage'], r['document_code']): r for r in baseline['documents']}
    exposure, skipped = historical_exposure(pairs)
    result = []
    used = set()
    for pair in pairs:
        row = dict(index=pair['index'], pair_id=pair['stable_pair_hash'][:24],
                   cipher_identity=pair['stable_pair_identity'], baseline_pair_id=pair['golden_pair_id'],
                   historical_split=pair['split'], historical_exposure='DEV_KNOWN',
                   historical_blind=False, exposure_receipts=exposure[pair['index']])
        for side, stage, prefix in [('old', 'stage_1', 'left'), ('new', 'stage_2', 'right')]:
            key = (stage, pair[prefix + '_code'])
            document = documents[key]
            assert document['physical_version_used'] == pair[prefix + '_physical_version']
            for name, artifact in document['artifacts'].items():
                if name in {'pdf', 'work_md', 'blocks'} and sha(artifact['path']) != artifact['sha256']:
                    raise ValueError('Baseline source drift: ' + artifact['path'])
            row[side] = dict(document_code=document['document_code'], stage=stage,
                             version_id=document['physical_version_used'],
                             membership_class=document['membership_class'],
                             artifacts={k: document['artifacts'][k] for k in ['pdf', 'work_md', 'blocks']},
                             structure=structural_profile(document))
            used.add(key)
        row['complete_document_pair'] = not pair['left_code'].startswith('Страница_')
        row['alias_of_index'] = 10 if pair['index'] == 23 else None
        row['scope_warning'] = ('FOREIGN_CIPHER_13AB_IN_BASELINE_REQUIRES_PROJECT_MEMBERSHIP_CHECK'
                                if pair['index'] == 1 else None)
        result.append(row)
    unpaired = [dict(stage=s, document_code=c, version_id=r['physical_version_used'],
                     artifacts={k: r['artifacts'][k] for k in ['pdf', 'work_md', 'blocks']},
                     status='NO_ESTABLISHED_OLD_NEW_PAIR_NOT_ADDITION_OR_REMOVAL')
                for (s, c), r in documents.items() if (s, c) not in used]
    inventory = dict(created_at=now(), object=OBJECT, old='stage_1', new='stage_2',
                     logical_baseline=baseline['logical_baseline'],
                     baseline=dict(path=str(BASELINE), sha256=sha(BASELINE)),
                     pair_baseline=dict(path=str(PAIR_BASELINE), sha256=sha(PAIR_BASELINE)),
                     documents=len(documents), pairs=result, unpaired=unpaired,
                     exposure_scan_exclusions=skipped,
                     leakage_limitation='All pairs previously replayed and used in research; focused receipts are a lower bound. Missing inspection logs prevent any claim of historical blindness. Reserve sets can only be prospective cipher isolation.',
                     semantic_source_inspection_this_audit=False)
    immutable(ROOT / 'INVENTORY.json', inventory)
    for row in result:
        counts = Counter(r['kind'] for r in row['exposure_receipts'])
        print(row['index'], row['old']['document_code'], counts,
              [(s, row[s]['structure']) for s in ['old', 'new']])


if __name__ == '__main__':
    build()
