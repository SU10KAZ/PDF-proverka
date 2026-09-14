"""Fail-closed source access and candidate-freeze receipts for research runs."""
import hashlib
from pathlib import Path

from .inventory import ROOT, REPO, OBJECT, read, sha, now, immutable


def verify_split(root=ROOT):
    digest = (root / 'SPLIT.sha256').read_text().strip()
    if sha(root / 'SPLIT.json') != digest:
        raise ValueError('Frozen split drift')
    split = read(root / 'SPLIT.json')
    if split['object'] != OBJECT or split['old'] != 'stage_1' or split['new'] != 'stage_2':
        raise ValueError('Only object 272 stage_1 -> stage_2 is authorized')
    for name, key in [('INVENTORY.json', 'inventory_sha256'), ('OVERLAP_AUDIT.json', 'overlap_audit_sha256')]:
        if sha(root / name) != split[key]:
            raise ValueError('Frozen inventory drift')
    return split


def verify_candidate(path, partition, root=ROOT):
    manifest = read(path)
    if manifest['split_sha256'] != sha(root / 'SPLIT.json'):
        raise ValueError('Candidate belongs to another split')
    if manifest['purpose'] != partition:
        raise ValueError('Wrong candidate-freeze purpose')
    for relative, digest in manifest['code'].items():
        if sha(REPO / relative) != digest:
            raise ValueError('Frozen candidate drift: ' + relative)
    return manifest


def admitted_pairs(partition='DEV', candidate=None, root=ROOT):
    split = verify_split(root)
    if partition not in split['allocation']:
        raise ValueError('Unknown partition')
    if partition != 'DEV':
        if candidate is None:
            raise PermissionError('Reserve sources require candidate freeze')
        verify_candidate(candidate, partition, root)
    inventory = {r['index']: r for r in read(root / 'INVENTORY.json')['pairs']}
    output = []
    for assignment in split['pairs']:
        if assignment['partition'] != partition:
            continue
        row = inventory[assignment['index']]
        pair = dict(index=row['index'], pair_key=row['pair_id'], project=OBJECT,
                    discipline=assignment['discipline'], partition=partition,
                    embargo_pages=assignment['embargo_pages'])
        for side, stage in [('old', 'stage_1'), ('new', 'stage_2')]:
            doc = row[side]
            if doc['stage'] != stage:
                raise ValueError('OLD/NEW direction changed')
            for artifact in doc['artifacts'].values():
                path = Path(artifact['path'])
                expected = REPO / 'projects_v2/objects' / OBJECT / 'comparison' / stage / 'documents' / doc['document_code'] / 'versions' / doc['version_id']
                if not path.resolve().is_relative_to(expected.resolve()):
                    raise ValueError('Foreign source or wrong physical version')
                if sha(path) != artifact['sha256']:
                    raise ValueError('Source drift')
            pair[side] = dict(**doc, document_version=hashlib.sha256(
                (OBJECT + '/' + stage + '/' + doc['document_code'] + '/' + doc['version_id']).encode()).hexdigest(),
                project=OBJECT, discipline=assignment['discipline'])
        output.append(pair)
    return output


def freeze(name, purpose, config, packages, root=ROOT):
    verify_split(root)
    if purpose not in {'VALIDATION', 'FINAL_HOLDOUT'}:
        raise ValueError('Invalid freeze purpose')
    files = [p for package in packages for p in (REPO / 'experiments' / package).glob('*')
             if p.suffix in {'.py', '.json'}]
    manifest = dict(frozen_at=now(), purpose=purpose, config=config,
                    split_sha256=sha(root / 'SPLIT.json'),
                    code={str(p.relative_to(REPO)): sha(p) for p in files},
                    source_policy='object 272 only; DEV tuning; reserve evidence remains historically exposed')
    path = root / 'candidates' / name / 'MANIFEST.json'
    immutable(path, manifest)
    for p in files:
        out = path.parent / 'code' / p.relative_to(REPO)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(p.read_bytes())
    return path
