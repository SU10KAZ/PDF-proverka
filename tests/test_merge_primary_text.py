import hashlib
import json

import pytest

from backend.app.pipeline.stages.findings_merge.primary_text import (
    PRIMARY_TEXT_MARKER, build_primary_text_messages, primary_text_section,
)

MD = ('preamble\n### BLOCK #1 [TEXT]: t1\n| Кабель | 4×2,5 |\n\n'
      '### BLOCK #2 [IMAGE]: g2\nUNINSPECTED_GRAPHIC_DESCRIPTION\n'
      '### BLOCK #3 [TEXT]: t3\nХвост документа\n')


def test_sources_verbatim_and_traceable_graphic_description_excluded():
    section = primary_text_section(MD)
    manifest = json.loads(section.split('## Primary source manifest\n\n')[1].split('\n\n' + PRIMARY_TEXT_MARKER)[0])
    assert len(manifest['included_text_spans']) == 2
    assert len(manifest['excluded_nontext_spans']) == 1
    assert manifest['md_utf8_sha256'] == hashlib.sha256(MD.encode()).hexdigest()
    assert 'UNINSPECTED_GRAPHIC_DESCRIPTION' not in section
    for span in manifest['included_text_spans']:
        source = MD[span['start']:span['end']]
        assert source in section
        assert span['sha256'] == hashlib.sha256(source.encode()).hexdigest()


def test_all_candidate_fields_preserved_and_overflow_refused():
    text = {'text_findings': [{'id': 'T-1', 'extra': ['не терять', True]}]}
    blocks = {'block_analyses': [], 'deferred': [{'id': 'D-1'}]}
    messages = build_primary_text_messages('system', json.dumps(text), json.dumps(blocks), MD, 10000)
    body = messages[1]['content']
    a, b = body.split('## 02_text_analysis.json:\n\n')[1].split('\n\n## 01_blocks_analysis.json:\n\n')
    assert json.loads(a) == text
    assert json.loads(b.split('\n\n## Primary source manifest')[0]) == blocks
    size = sum(len(m['content']) for m in messages)
    with pytest.raises(ValueError, match='no evidence was truncated'):
        build_primary_text_messages('system', json.dumps(text), json.dumps(blocks), MD, size - 1)
    with pytest.raises(ValueError, match='typed MD'):
        primary_text_section('unstructured source')


def test_runtime_opt_in_and_provider_source_contract(monkeypatch):
    from backend.app.pipeline.stages.prepare import prompt_builder as pb
    from backend.app.pipeline.stages.findings_merge.provider_transport import build_provider_prompt
    monkeypatch.setattr(pb, '_load_and_clean_template', lambda *a: 'Task')
    monkeypatch.setattr(pb, '_read_json_file', lambda *a: '{}')
    monkeypatch.setattr(pb, '_read_findings_merge_blocks', lambda *a, **k: '{}')
    monkeypatch.setattr(pb, 'get_stage_model', lambda *a: 'test')
    calls = []
    def read(info, project):
        calls.append((info, project))
        return MD
    monkeypatch.setattr(pb, '_read_md_file', read)
    monkeypatch.delenv('AUDIT_MERGE_PRIMARY_TEXT', raising=False)
    control = pb.build_findings_merge_messages({}, 'p')
    assert not calls
    assert PRIMARY_TEXT_MARKER not in control[1]['content']
    monkeypatch.setenv('AUDIT_MERGE_PRIMARY_TEXT', '1')
    candidate = pb.build_findings_merge_messages({'md_file': 'active.md'}, 'p')
    assert calls == [({'md_file': 'active.md'}, 'p')]
    provider = build_provider_prompt(candidate)['prompt']
    assert 'Primary TEXT from project MD — attached below' in provider
    assert 'Project MD file — NOT available' not in provider
    assert 'UNINSPECTED_GRAPHIC_DESCRIPTION' not in provider
    assert 'Хвост документа' in provider
    assert 'Project MD file — NOT available' in build_provider_prompt(control)['prompt']
