import json
from copy import deepcopy
import pytest
from PIL import Image
from backend.app.pipeline.stages.findings_verify.source_review_bundle import build_source_review_bundle, bind_source_review_bundle
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


def setup(tmp_path):
    sources=[]
    for version,value in [('v1',12),('v2',15)]:
        path=tmp_path/(version+'.md')
        path.write_text(f'## Page 1\n### BLOCK #1 [TEXT]: same\nLiteral value {value}\n### BLOCK #2 [GRAPHIC]: drawing\n> **Stamp:** Code: KM | Sheet: 3\n**Summary:** UNTRUSTED_SUMMARY\n')
        sources.append({'document_code':'KM','version':version,'md_path':str(path), 'engineer_answer':'SECRET'})
    bundle=build_source_review_bundle(sources)
    finding={'id':'F1','problem':'Check source applicability','description':'Different versions','solution':'Clarify',
        'report_section':'questions','source_evidence_review_status':'pending',
        'source_review_requests':[{'block_ids':list(bundle['source_origins'])}]}
    queue=bind_source_review_bundle(build_source_review_queue([finding],bundle['md']),bundle)
    for bid,origin in bundle['source_origins'].items():
        if origin['original_block_id']=='drawing':Image.new('RGB',(3,3)).save(tmp_path/f'block_{bid}.png')
    attach_graphic_receipts(queue,bundle['md'],tmp_path)
    return sources,bundle,finding,queue


def test_same_block_ids_in_distinct_versions_keep_literal_text_and_identity(tmp_path):
    sources,bundle,finding,queue=setup(tmp_path)
    assert len(bundle['source_origins'])==4
    texts=authoritative_text_blocks(bundle['md'])
    assert set(texts.values())=={'\nLiteral value 12\n','\nLiteral value 15\n'}
    request=prepare_source_review_request(queue,finding,bundle['md'])
    packet=json.loads(request['messages'][1]['content'])
    assert {d['version'] for d in packet['source_catalogue']}=={'v1','v2'}
    assert len(packet['source_origins'])==4 and len(request['image_paths'])==2
    assert 'SECRET' not in json.dumps(request['messages'])
    assert 'UNTRUSTED_SUMMARY' not in json.dumps(request['messages'])
    assert str(tmp_path) not in json.dumps(request['messages'])


@pytest.mark.parametrize('change',['source','catalogue','origin','unbound'])
def test_stale_or_forged_bundle_is_rejected_before_dispatch(tmp_path,change):
    sources,bundle,finding,queue=setup(tmp_path)
    if change=='source':
        path=tmp_path/'v1.md';path.write_text(path.read_text().replace('12','99'))
    elif change=='catalogue':queue['source_catalogue'][0]['version']='v3'
    elif change=='origin':next(iter(queue['source_origins'].values()))['original_block_id']='different'
    else:queue.pop('_bundle_source_bindings')
    with pytest.raises(ValueError,match='bundle_source_version_changed|unbound_source_catalogue'):
        prepare_source_review_request(queue,finding,bundle['md'])


def test_duplicate_document_version_and_ambiguous_original_blocks_fail(tmp_path):
    sources,bundle,finding,queue=setup(tmp_path)
    with pytest.raises(ValueError,match='duplicate_document_version'):
        build_source_review_bundle([sources[0],deepcopy(sources[0])])
    path=tmp_path/'v1.md';path.write_text(path.read_text()+'### BLOCK #3 [TEXT]: same\nAmbiguous\n')
    with pytest.raises(ValueError,match='missing_or_ambiguous_block'):
        build_source_review_bundle(sources)
