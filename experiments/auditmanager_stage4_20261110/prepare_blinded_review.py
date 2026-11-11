"""Prepare a shuffled quality-review packet from sealed outputs, without answers.

This is not the separate counterbalanced workload experiment. Text style can still
reveal an author; hiding labels alone does not prove successful human blinding.
"""
import argparse
import hashlib
import json
from pathlib import Path
import random
import secrets

SCALAR_FIELDS = ('problem', 'description', 'finding', 'statement', 'solution', 'risk',
                 'consequence', 'severity', 'norm', 'norm_quote', 'report_section',
                 'question_basis', 'profile_reason', 'sheet', 'page')
SOURCE_FIELDS = ('source_block_ids', 'related_block_ids')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(bindings, public_dir, private_dir, *, seed=None):
    public_dir, private_dir = Path(public_dir).resolve(), Path(private_dir).resolve()
    if public_dir == private_dir or public_dir in private_dir.parents or private_dir in public_dir.parents:
        raise ValueError('Public and private packages must be separate sibling or unrelated directories')
    if public_dir.exists() or private_dir.exists():
        raise ValueError('Preserve prior packets; use new destinations')
    if len(bindings) < 2:
        raise ValueError('Need at least two compared outputs')
    if len({(b['discipline'], b['repeat']) for b in bindings}) != 1:
        raise ValueError('Do not mix disciplines or repeats')
    if len({b['arm'] for b in bindings}) != len(bindings):
        raise ValueError('Duplicate comparison arm')
    seed = seed if seed is not None else secrets.token_hex(32)
    rng = random.Random(seed)
    rows, mapping = [], []
    for binding in bindings:
        path = Path(binding['path'])
        if sha(path) != binding['sha256']:
            raise ValueError('Sealed output bytes changed')
        report = json.loads(path.read_text())
        seen = set()
        for finding in report['findings']:
            fid = finding['id']
            if not isinstance(fid, str) or not fid or fid in seen:
                raise ValueError('Missing or duplicate finding identity')
            seen.add(fid)
            visible = {}
            for name in SCALAR_FIELDS:
                if name in finding:
                    value = finding[name]
                    if value is not None and type(value) not in (str, int, float):
                        raise ValueError('Inspect unsupported visible-field shape: ' + name)
                    visible[name] = value
            if not any(visible.get(k) for k in ('problem', 'description', 'finding', 'statement')):
                raise ValueError('Cannot silently omit the finding claim')
            for name in SOURCE_FIELDS:
                if name in finding:
                    value = finding[name]
                    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                        raise ValueError('Invalid project source references')
                    visible[name] = value
            # No model/provider, original ID, origin, trace, suppression history,
            # proposed match, or engineer answer is copied to the public payload.
            review_id = 'R-' + secrets.token_hex(8)
            rows.append({'review_id': review_id, 'finding': visible,
                         'review': {'reviewer': None, 'truth': 'unchecked', 'usefulness': 'unchecked',
                                    'duplicate_of': None, 'source_evidence': [], 'reason': None,
                                    'corrected_claim': None}})
            mapping.append({'review_id': review_id, 'arm': binding['arm'], 'original_id': fid,
                            'output_sha256': binding['sha256'],
                            'omitted_fields': sorted(set(finding) - set(visible) - {'id'})})
    rng.shuffle(rows)
    discipline, repeat = next(iter({(b['discipline'], b['repeat']) for b in bindings}))
    public = {'schema_version': 1, 'purpose': 'independent_quality_review',
              'discipline': discipline, 'repeat': repeat, 'rows': rows,
              'instructions': ['Evaluate technical truth separately from usefulness and duplication.',
                  'Keep unsupported, out-of-profile and not-checked distinct from refuted.',
                  'Cite project sources; do not infer that an omitted finding was false.',
                  'Do not use time spent on this combined packet as per-method workload.',
                  'Source block IDs refer to a separately supplied project source index.'],
              'review_complete': False}
    public_dir.mkdir(parents=True)
    private_dir.mkdir(parents=True, mode=0o700)
    public_path = public_dir / 'review.json'
    public_path.write_text(json.dumps(public, ensure_ascii=False, indent=2) + '\n')
    private = {'bindings': bindings, 'shuffle_seed': seed, 'mapping': mapping,
               'public_sha256': sha(public_path), 'duplicate_rows_preserved': True,
               'answers_in_public': False, 'workload_measured': False,
               'limits': 'Label blinding only; unchanged finding wording can contain stylistic cues. Project source access and independent reviewer assignment are separate prerequisites.'}
    private_path = private_dir / 'mapping.json'
    private_path.write_text(json.dumps(private, ensure_ascii=False, indent=2) + '\n')
    private_path.chmod(0o600)
    return {'rows': len(rows), 'public_sha256': sha(public_path), 'workload_measured': False}


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--bindings', type=Path, required=True)
    p.add_argument('--public-dir', type=Path, required=True)
    p.add_argument('--private-dir', type=Path, required=True)
    a = p.parse_args()
    print(json.dumps(prepare(json.loads(a.bindings.read_text()), a.public_dir, a.private_dir)))
