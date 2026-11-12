from copy import deepcopy
import hashlib
import json
import pytest

from backend.app.pipeline.stages.findings_verify.graphic_observation_handoff import (
    SOURCE_ONLY_INSTRUCTIONS, SOURCE_ONLY_USER, bind_observation, checked_observations,
)


def observation(tmp_path):
    image = tmp_path / 'graphic.png'
    image.write_bytes(b'fixed graphic bytes')
    digest = hashlib.sha256(image.read_bytes()).hexdigest()
    request = tmp_path / 'request.json'
    request.write_text(json.dumps({'model': 'codex/gpt-6-astra', 'allowed_tools': '',
        'image_hashes': {str(image): digest}, 'messages': [
            {'role': 'system', 'content': SOURCE_ONLY_INSTRUCTIONS},
            {'role': 'user', 'content': SOURCE_ONLY_USER}]}))
    response = tmp_path / 'response.json'
    response.write_text(json.dumps({'is_error': False, 'json_data': {
        'sections': [{'system_literal': 'A', 'location_bbox_norm_xyxy': [0, 0, 1, 1]}],
        'legend_entries': [], 'limitations': ['Small lines uncertain']}}))
    return bind_observation(request, response, 'g'), {'block_id': 'g', 'sha256': digest, 'path': str(image)}


@pytest.mark.parametrize('change', ['image', 'response', 'candidate_request', 'embedded_reading', 'different_attachment'])
def test_stale_or_candidate_conditioned_observation_is_rejected(tmp_path, change):
    record, image = observation(tmp_path)
    from pathlib import Path
    if change == 'image':
        Path(image['path']).write_bytes(b'changed')
    elif change == 'response':
        path = Path(record['response_path'])
        data = json.loads(path.read_text()); data['json_data']['limitations'] = ['changed']
        path.write_text(json.dumps(data))
    elif change == 'candidate_request':
        path = Path(record['request_path'])
        data = json.loads(path.read_text()); data['messages'][1]['content'] += ' Expected answer: reject finding'
        path.write_text(json.dumps(data))
    elif change == 'embedded_reading':
        record['reading']['limitations'] = ['tampered']
    else:
        image['sha256'] = 'another version'
    with pytest.raises(ValueError):
        checked_observations([record], [image])


def test_final_check_receives_bound_observations_separate_from_text_and_verdicts(tmp_path, monkeypatch):
    from tests.test_source_review_admission import case
    from backend.app.pipeline.stages.findings_verify import source_review_final_check as final
    record, image = observation(tmp_path)
    doc, entries, md = case('retain', 'supported'); entry = entries[0]
    source = final.prepare_source_review_request(entry['queue'], doc['findings'][0], md)
    source.update(image_bindings=[image], image_paths=[image['path']])
    monkeypatch.setattr(final, 'prepare_source_review_request', lambda *a, **kw: deepcopy(source))
    entry['queue']['source_only_graphic_observations'] = [record]
    entry['decision']['reason'] = 'SECRET REVIEWER VERDICT'
    before = deepcopy(entry)
    request = final.prepare_final_check(entry, doc, md)['request']
    payload = json.loads(request['messages'][1]['content'])
    assert payload['independent_graphic_observations_untrusted'][0]['reading'] == record['reading']
    assert payload['independent_graphic_observations_untrusted'][0]['semantic_verified'] is False
    assert 'Small lines uncertain' not in json.dumps(payload['sources']['authoritative_text'])
    assert 'SECRET REVIEWER' not in json.dumps(request)
    assert entry == before
    image['sha256'] = 'changed image in final packet'
    source['image_bindings'] = [image]
    with pytest.raises(ValueError, match='not_supplied'):
        final.prepare_final_check(entry, doc, md)


def test_duplicate_observations_are_not_counted_as_independent_support(tmp_path):
    record, image = observation(tmp_path)
    with pytest.raises(ValueError, match='duplicate'):
        checked_observations([record, record], [image])
