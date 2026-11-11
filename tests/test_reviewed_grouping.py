from copy import deepcopy
import hashlib
import pytest
from backend.app.pipeline.stages.findings_merge.reviewed_grouping import apply_reviewed_group
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest


def case():
    md='### BLOCK #1 [TEXT]: b\nShared specification\n'
    members=[{'id':i,'severity':'medium','report_section':'technical_findings','problem':'Different '+i,
        'description':'Conditional source discrepancy '+i,'solution':'Check line '+i,'risk':'Conditional '+i,
        'norm':'Unverified '+i,'evidence':[{'block_id':'b','quote':'Shared specification'}],
        'source_finding_ids':['source-'+i]} for i in ('A','B')]
    doc={'findings':members+[{'id':'C','problem':'Other useful issue'}]}
    proposal={'member_ids':['A','B'],'shared_issue':'Shared document inconsistency',
        'common_corrective_action':'Reconcile individual lines','source_refs':['b']}
    decision={'proposal_sha256':evidence_digest(proposal),'members_sha256':evidence_digest(members),
        'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),'action':'group','reviewer':'test explicit review',
        'reason':'Specific common discrepancy checked','same_issue_verified':True,
        'common_corrective_action_verified':True,'individual_scopes_preserved':True}
    return doc,proposal,decision,md


def test_group_preserves_individual_claims_sources_and_unrelated_findings():
    doc,p,d,md=case();before=deepcopy(doc);out,r=apply_reviewed_group(doc,p,d,md)
    assert r['status']=='applied' and doc==before and out['findings'][1]==doc['findings'][2]
    g=out['findings'][0];assert g['grouping_original_findings']==doc['findings'][:2]
    for f in doc['findings'][:2]:
        for key in ('problem','description','solution','risk','norm'):assert f[key] in g['description']
        assert set(f['source_finding_ids'])<=set(g['source_finding_ids'])
    assert not r['semantic_certified']


@pytest.mark.parametrize('bad',['no_review','stale','different_section','different_severity','scope','unseen','overflow'])
def test_unapproved_or_lossy_group_preserves_original(bad):
    doc,p,d,md=case()
    if bad=='no_review':d={}
    if bad=='stale':doc['findings'][0]['description']='Changed after review'
    if bad=='different_section':doc['findings'][1]['report_section']='questions'
    if bad=='different_severity':doc['findings'][1]['severity']='critical'
    if bad=='scope':d['same_issue_verified']=False
    if bad=='unseen':p['source_refs']=['unseen'];d['proposal_sha256']=evidence_digest(p)
    if bad=='overflow':doc['findings'][0]['description']='x'*32768
    if bad in ('different_section','different_severity','overflow'):d['members_sha256']=evidence_digest(doc['findings'][:2])
    out,r=apply_reviewed_group(doc,p,d,md)
    assert out==doc and r['status']=='not_applied'
