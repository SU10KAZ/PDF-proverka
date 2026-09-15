"""Synthetic protection tests; no corpus judgements or tuned case IDs."""
from dataclasses import replace
from types import SimpleNamespace
import pytest
from .test_v4 import req
from .allocation_v6 import package
from .priority_v6 import audit_displacements, allocation_plan
from .requirements_v6 import build
from .common import fingerprint, AccessAudit


def request(qid, side='OLD', page=1, kind='TEXT_SECTION', role='STATE', roles=(), parents=(), **extra):
    r = req(qid, side, page, parents)
    return replace(r, evidence_role=role, required_type=kind, evidence_type=kind, scope_binding=qid,
        provenance=dict(dependency_of=list(parents), evidence_roles=list(roles), **extra))


def loader(r):
    return dict(native='local extract identity.\n' + 'extra context ' * 40, ocr='',
        raster=dict(sha256=str(r.page)+r.side, path='synthetic.png'), evidence_types=[r.required_type.value],
        scope_binding=r.scope_binding, bbox=[0,0,100,100], full_page=False,
        provenance=dict(region_id=r.scope_binding))


def log(body, qid):
    return next(r for r in body['delivery_decision_log'] if r['requirement_id'] == qid)


def test_p0_cannot_be_displaced_by_p2():
    b=package([request('p0', page=9, role='PRIMARY'), request('p2', page=1, kind='TABLE_COMPLETE')], loader, raster_budget=1)
    assert log(b,'p0')['raster_selected']
    assert not log(b,'p2')['raster_selected']


def test_p1_cannot_be_displaced_by_optional_continuation():
    b=package([request('p0'), request('p1',page=8,kind='GRAPHIC_REGION'),
        request('dep',page=2,role='CONTINUATION',parents=('p0',))], loader, raster_budget=2)
    assert log(b,'p1')['raster_selected'] and not log(b,'dep')['raster_selected']


def test_old_new_p0_reserved_slots():
    b=package([request('o',page=8),request('n',side='NEW',page=9),
               request('s',page=1,role='COUNTER')],loader,raster_budget=2)
    assert log(b,'o')['raster_selected'] and log(b,'n')['raster_selected']


def test_identity_is_protected_and_indexed():
    b=package([request('identity',roles=('SUBJECT_IDENTITY',))],loader)
    assert log(b,'identity')['protected']=='YES'
    assert b['identity_evidence_ids']==[log(b,'identity')['evidence_id']]


def identity_build(monkeypatch, kind='TEXT'):
    import experiments.project_change_f5_272.requirements_v6 as module
    monkeypatch.setattr(module,'analyze',lambda *args:dict(requirement='UNKNOWN',required_pages=[],notes=[]))
    regions=[dict(region_id='r'+str(i),page=i,source_type=kind,bbox_norm=[0,0,1,1]) for i in (2,4)]
    docs={s:SimpleNamespace(regions={r['region_id']:r for r in regions},text={r['page']:r for r in regions}) for s in ('old','new')}
    canonical={'a':dict(evidence_support=[dict(region_id=r['region_id'],accepted=True) for r in regions],subject_scope={},functional_role='local')}
    c=dict(candidate_id='c',subject='local',subject_confidence='SUBJECT_UNRESOLVED',old=['a'],new=[],scope=[],functional_key='roof')
    prepared=dict(pair={s:dict(document_code='A',document_version=s) for s in ('old','new')})
    return build(c,canonical,prepared,docs,[])[0]


def test_identity_only_preserves_all_relevant_text(monkeypatch):
    qs=identity_build(monkeypatch)
    assert {r.page for r in qs if r.side=='OLD'}=={2,4}
    assert all('SUBJECT_IDENTITY' in r.provenance['evidence_roles'] for r in qs if r.side=='OLD')
    b=package(qs,lambda r:loader(r) if r.scope_binding else None)
    assert all(log(b,r.requirement_id)['text_selected'] for r in qs if r.side=='OLD')


def test_multiple_roles_survive():
    roles=('SUBJECT_IDENTITY','STATE_SUPPORT','SCOPE_BINDING','COUNTER_EVIDENCE')
    b=package([request('multi',roles=roles)],loader)
    assert set(log(b,'multi')['role'])==set(roles)
    assert log(b,'multi')['selected']=='YES'


def test_unknown_continuation_does_not_displace_primary():
    b=package([request('primary',page=9),request('unknown',page=1,parents=('primary',),
        role='CONTINUATION',continuation_status='UNKNOWN')],loader,raster_budget=1)
    assert log(b,'primary')['raster_selected']
    assert not log(b,'unknown')['mandatory']
    assert log(b,'unknown')['raster_omitted_reason']=='NOT_DELIVERED_BUDGET_LIMIT'


def test_mandatory_graphic_not_displaced_by_text_support():
    b=package([request('p'),request('g',page=10,kind='GRAPHIC_REGION'),request('s',page=2)],loader,raster_budget=2)
    assert log(b,'g')['raster_selected'] and not log(b,'s')['raster_selected']


def test_mandatory_table_not_displaced_by_optional_graphic():
    b=package([request('p'),request('t',page=9,kind='TABLE_COMPLETE',role='MANDATORY'),
        request('g',page=2,kind='GRAPHIC_REGION')],loader,profile='REQUIREMENT',raster_budget=2)
    assert log(b,'t')['raster_selected'] and not log(b,'g')['raster_selected']


def test_decision_log_has_every_candidate_and_omission():
    b=package([request('p'),request('g',page=2,kind='GRAPHIC_REGION')],loader,raster_budget=1,text_budget=0)
    assert len(b['delivery_decision_log'])==2
    row=log(b,'g')
    assert row['selected']=='NO' and row['omitted_reason']=='NOT_DELIVERED_BUDGET_LIMIT'
    assert row['displaced_by']==[]  # P0 capacity, not an optional eviction.
    assert all(k in row for k in ('evidence_id','subject_id','side','type','role','priority','protected'))


def test_protected_displacement_automatic_fail():
    rows=[dict(evidence_id='p',protected='YES',displaced_by=['s']),dict(evidence_id='s',protected='NO',displaced_by=[])]
    assert audit_displacements(rows)['status']=='FAIL'


def test_graphic_supported_subject_survives_identity_view(monkeypatch):
    qs=identity_build(monkeypatch,'GRAPHIC')
    b=package(qs,lambda r:loader(r) if r.scope_binding else None)
    assert all(log(b,r.requirement_id)['raster_selected'] for r in qs if r.side=='OLD')


def test_stable_repeated_delivery_hash():
    qs=[request('a'),request('b',side='NEW')]
    a=package(qs,loader); b=package(qs,loader)
    assert a==b and a['package_hash']==fingerprint({k:v for k,v in a.items() if k!='package_hash'})


def test_no_truth_leakage(tmp_path):
    from experiments.project_change_272.inventory import ROOT
    guard=AccessAudit([],tmp_path)
    for name in ('sources/VALIDATION/PAIRS.json','sources/FINAL/PAIRS.json','source_truth.json','fresh_dev_sample_f5_boundary_diagnostic/SOURCE_AUDIT_12_PACKAGES.json'):
        with pytest.raises(PermissionError): guard.check('open',(str(ROOT/name),'r',0))
    with pytest.raises(PermissionError): guard.check('socket.connect',())


def test_proven_continuation_mandatory_but_after_direct_support():
    qs=[request('p'),request('dep',page=2,parents=('p',),role='CONTINUATION',continuation_status='REQUIRED',continuation_proof='hash'),request('g',page=9,kind='GRAPHIC_REGION')]
    b=package(qs,loader,raster_budget=2)
    assert log(b,'dep')['mandatory'] and log(b,'g')['raster_selected'] and not log(b,'dep')['raster_selected']


def test_text_reservation_survives_large_optional_text():
    qs=[request('p',page=5,roles=('SUBJECT_IDENTITY',)),request('n',side='NEW',page=6,roles=('SUBJECT_IDENTITY',)),request('optional',page=1)]
    b=package(qs,loader,text_budget=100,raster_budget=0)
    assert log(b,'p')['text_selected'] and log(b,'n')['text_selected']
    assert b['limits']['remaining_text_characters']>=0


def test_cyclic_dependency_rejected():
    with pytest.raises(ValueError,match='cyclic'):
        allocation_plan([request('a',parents=('b',)),request('b',parents=('a',))])


def test_mandatory_graphic_reserved_before_identity_text_rasters():
    qs=[request('primary')]+[request('id'+str(i),page=i,roles=('SUBJECT_IDENTITY',)) for i in range(2,10)]
    qs.append(request('graphic',page=50,kind='GRAPHIC_REGION'))
    b=package(qs,loader,raster_budget=2)
    assert log(b,'graphic')['raster_selected']
    assert all(log(b,'id'+str(i))['text_selected'] for i in range(2,10))
