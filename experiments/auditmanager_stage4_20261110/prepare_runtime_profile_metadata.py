"""Prepare separate, validated runtime metadata proposals; never rewrite trial inputs."""
from copy import deepcopy
import json
from pathlib import Path

from backend.app.core.config import DISCIPLINES_DIR
from backend.app.services.common.discipline_identity import discipline_id
from .score_atomic_review import sha
from .verify_output_review_history import encode


def prepare(manifest, destination, overrides=None):
    manifest, destination = Path(manifest), Path(destination)
    proposals = []
    records = json.loads(manifest.read_text())['projects']
    labels = [r['discipline'] for r in records]
    if len(set(labels)) != len(labels):
        raise ValueError('Duplicate evaluation discipline')
    if set(overrides or {}) - set(labels):
        raise ValueError('Unknown override label')
    # Validate the complete package before creating any output.
    for record in records:
        source = Path(record['path'])
        if sha(source) != record['sha256']:
            raise ValueError('Source metadata changed')
        metadata = json.loads(source.read_text())
        if metadata != record['metadata']:
            raise ValueError('Manifest metadata differs')
        label = record['discipline']
        requested = (overrides or {}).get(label, metadata.get('section'))
        identity = discipline_id(requested, source='explicit_experiment_profile')
        profile = Path(DISCIPLINES_DIR) / identity.profile_dir
        if not profile.is_dir() or not (profile / 'role.md').read_text().strip():
            raise ValueError('Missing runtime profile')
        proposed = deepcopy(metadata)
        proposed['section'] = identity.code
        proposals.append({'evaluation_discipline': label, 'runtime_profile': identity.code,
            'source_path': str(source), 'source_sha256': sha(source),
            'proposed_metadata': proposed,
            'profile_files': {str(p): sha(p) for p in sorted(profile.iterdir()) if p.is_file()}})
    result = {'status': 'runtime_metadata_proposal_not_runnable_trial',
        'manifest_path': str(manifest), 'manifest_sha256': sha(manifest),
        'registry_sha256': sha(Path(DISCIPLINES_DIR) / '_registry.json'),
        'projects': proposals, 'model_calls': 0, 'production_acceptance': False,
        'remaining': ['Build a separate immutable input package and freeze comparison.',
                      'Do not substitute these metadata into ongoing or sealed runs.',
                      'Validate the selected source revision profiles again at dispatch.']}
    with destination.open('xb') as stream:
        stream.write(encode(result))
    return result
