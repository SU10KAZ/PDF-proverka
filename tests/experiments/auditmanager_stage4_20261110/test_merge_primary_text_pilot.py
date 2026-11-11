import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import pytest
from experiments.auditmanager_stage4_20261110.prepare_merge_primary_text_pilot import prepare


def sources(tmp_path):
    text = {'text_findings': [{'id': 'T-001', 'finding': 'Проверить длину'}]}
    graphic = {'block_analyses': [{'block_id': 'g1', 'findings': [],
        'deferred_findings': [{'id': 'G-001', 'required_context': ['Разрез']}]}]}
    request = {'messages': [{'role': 'system', 'content': 'Evaluate candidates'},
        {'role': 'user', 'content': '## 02_text_analysis.json:\n\n' + json.dumps(text) +
         '\n\n## 01_blocks_analysis.json:\n\n' + json.dumps(graphic)}],
        'options': {'allowed_tools': ''}, 'image_hashes': {}}
    path = tmp_path/'request.json'
    path.write_text(json.dumps(request))
    md = tmp_path/'document.md'
    md.write_text('# Project\n\n### BLOCK #1 [TEXT]: t1\nДлина 730 м.\n\n'
                  '### BLOCK #2 [IMAGE]: g1\nUntrusted graphic transcription\n\n'
                  '### BLOCK #3 [TEXT]: t2\nСпецификация: 700 м.\n')
    return path, md, text, graphic


def test_only_authoritative_text_changes_between_arms(tmp_path):
    request, md, text, graphic = sources(tmp_path)
    original = request.read_bytes(), md.read_bytes()
    report = prepare(request, md, tmp_path/'pilot')
    control = json.loads(Path(report['requests']['control']['path']).read_text())
    treated = json.loads(Path(report['requests']['primary_text']['path']).read_text())
    assert control['messages'][0] == treated['messages'][0]
    raw = control['messages'][1]['content']
    a, b = raw.removeprefix('## 02_text_analysis.json:\n\n').split('\n\n## 01_blocks_analysis.json:\n\n')
    assert json.loads(a) == text
    assert json.loads(b) == graphic  # Deferred work must survive compression.
    assert treated['messages'][1]['content'].startswith(raw)
    assert 'Untrusted graphic transcription' not in treated['messages'][1]['content']
    for span in report['text_spans']:
        assert md.read_text()[span['start']:span['end']] in treated['messages'][1]['content']
    assert len(report['text_spans']) == 2
    assert len(report['omitted_nontext_spans']) == 1
    assert original == (request.read_bytes(), md.read_bytes())


def test_over_budget_fails_before_creating_output(tmp_path):
    request, md, _, _ = sources(tmp_path)
    md.write_text('### BLOCK #1 [TEXT]: t1\n' + 'x'*1_000_000)
    with pytest.raises(AssertionError):
        prepare(request, md, tmp_path/'pilot')
    assert not (tmp_path/'pilot').exists()


def test_existing_destination_is_not_overwritten(tmp_path):
    request, md, _, _ = sources(tmp_path)
    out = tmp_path/'pilot'
    out.mkdir()
    sentinel = out/'control_request.json'
    sentinel.write_text('sealed')
    with pytest.raises(FileExistsError):
        prepare(request, md, out)
    assert sentinel.read_text() == 'sealed'
