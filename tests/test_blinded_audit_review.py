import importlib.util
import json
from pathlib import Path
import pytest

spec = importlib.util.spec_from_file_location('blinded_review', Path(__file__).resolve().parents[1] /
    'experiments/auditmanager_stage4_20261110/prepare_blinded_review.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)


def inputs(tmp_path):
    result = []
    for arm in ('alpha_secret', 'beta_secret'):
        p = tmp_path / (arm + '.json')
        p.write_text(json.dumps({'findings': [{'id': 'F-secret', 'problem': 'Same technical claim',
            'source_block_ids': ['project-block'], 'model': 'model_secret',
            'engineer_answer': 'answer_secret', 'source_review_history': ['history_secret']}]}))
        result.append({'path': str(p), 'sha256': m.sha(p), 'arm': arm, 'discipline': 'AK', 'repeat': 1})
    return result


def test_blind_packet_preserves_duplicates_and_sources_but_hides_provenance(tmp_path):
    bindings = inputs(tmp_path)
    r = m.prepare(bindings, tmp_path/'public', tmp_path/'private', seed='test')
    text = (tmp_path/'public/review.json').read_text(); public = json.loads(text)
    private = json.loads((tmp_path/'private/mapping.json').read_text())
    assert r['rows'] == 2 and len({x['review_id'] for x in public['rows']}) == 2
    for secret in ('alpha_secret', 'beta_secret', 'model_secret', 'answer_secret', 'history_secret', 'F-secret'):
        assert secret not in text
    assert all(x['finding']['source_block_ids'] == ['project-block'] for x in public['rows'])
    assert {x['review_id'] for x in private['mapping']} == {x['review_id'] for x in public['rows']}
    assert private['public_sha256'] == m.sha(tmp_path/'public/review.json')
    assert not r['workload_measured'] and not public['review_complete']


def test_changed_sealed_bytes_and_nested_private_mapping_rejected(tmp_path):
    b = inputs(tmp_path)
    with pytest.raises(ValueError, match='separate'):
        m.prepare(b, tmp_path/'public', tmp_path/'public/private')
    Path(b[0]['path']).write_text('{}')
    with pytest.raises(ValueError, match='bytes changed'):
        m.prepare(b, tmp_path/'public', tmp_path/'private')
    assert not (tmp_path/'public').exists()


def test_mixed_repeats_rejected(tmp_path):
    b = inputs(tmp_path); b[1]['repeat'] = 2
    with pytest.raises(ValueError, match='mix'):
        m.prepare(b, tmp_path/'public', tmp_path/'private')
