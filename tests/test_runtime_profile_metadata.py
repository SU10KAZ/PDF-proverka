import json
from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.auditmanager_stage4_20261110.prepare_runtime_profile_metadata import prepare
from experiments.auditmanager_stage4_20261110.score_atomic_review import sha
from backend.app.services.common.discipline_identity import UnknownDisciplineError


def test_unknown_AK_fails_before_any_output_and_explicit_SS_keeps_label(tmp_path):
    source = tmp_path / 'project_info.json'
    source.write_text(json.dumps({'section': 'AK', 'name': 'source-only'}))
    original = source.read_bytes()
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'projects': [{'discipline': 'AK',
        'path': str(source), 'sha256': sha(source), 'metadata': json.loads(original)}]}))
    output = tmp_path / 'proposal.json'
    with pytest.raises(UnknownDisciplineError):
        prepare(manifest, output)
    assert not output.exists()
    result = prepare(manifest, output, {'AK': 'SS'})
    row, = result['projects']
    assert row['evaluation_discipline'] == 'AK' and row['runtime_profile'] == 'SS'
    assert row['proposed_metadata']['section'] == 'SS'
    assert source.read_bytes() == original
    with pytest.raises(FileExistsError):
        prepare(manifest, output, {'AK': 'SS'})


def test_changed_source_metadata_is_rejected(tmp_path):
    source = tmp_path / 'p.json'
    source.write_text('{"section":"SS"}')
    manifest = tmp_path / 'm.json'
    manifest.write_text(json.dumps({'projects': [{'discipline': 'AK', 'path': str(source),
        'sha256': sha(source), 'metadata': {'section': 'SS'}}]}))
    source.write_text('{"section":"EOM"}')
    with pytest.raises(ValueError, match='Source metadata changed'):
        prepare(manifest, tmp_path / 'out.json')
