from copy import deepcopy
import json
import pytest
from tests.test_reviewed_grouping import case
from backend.app.pipeline.stages.findings_merge.group_proposal import prepare_group_proposal,validate_group_proposal


def fixture():
    doc,proposal,_,_=case();proposal.update(reason='One specific discrepancy',distinctions_to_preserve=['Addresses A and B remain separate'])
    return doc,{'groups':[proposal],'keep_separate_ids':['C']}


def test_whole_partition_and_no_history_in_prompt():
    doc,result=fixture();doc['findings'][0]['engineer_decision']='SECRET'
    assert validate_group_proposal(doc,result)==result
    assert 'SECRET' not in json.dumps(prepare_group_proposal(doc))

@pytest.mark.parametrize('bad',['lost','duplicated','invented','borrowed_source'])
def test_loss_duplicate_invented_and_unrelated_sources_rejected(bad):
    doc,result=fixture()
    if bad=='lost':result['keep_separate_ids']=[]
    if bad=='duplicated':result['keep_separate_ids'].append('A')
    if bad=='invented':result['keep_separate_ids']=['FAKE']
    if bad=='borrowed_source':
        doc['findings'][2]['evidence']=[{'block_id':'unrelated'}]
        result['groups'][0]['source_refs']=['unrelated']
    with pytest.raises(ValueError):validate_group_proposal(doc,result)


def test_oversized_report_never_silently_truncated():
    doc,_=fixture();before=deepcopy(doc)
    with pytest.raises(ValueError,match='finding_budget'):prepare_group_proposal(doc,max_findings=2)
    with pytest.raises(ValueError,match='character_budget'):prepare_group_proposal(doc,max_chars=10)
    assert doc==before
