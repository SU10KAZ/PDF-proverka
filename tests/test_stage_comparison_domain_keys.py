from dataclasses import replace
import json
import os

import pytest

from backend.app.services.stage_comparison import domain_keys as d
from backend.app.services.stage_comparison.production_artifacts import file_content_identity
from backend.app.services.stage_comparison.decision_registry import DecisionRegistry, RegistryConflict, legacy_alias_policy


def document(label='v1', digest='a'*64):
    return d.document_version('D', label, [{'kind':'pdf','sha256':digest,'size':10,'missing':False}])


def claim():
    return d.Claim(document().key, document('v2').key, 'owner', 'area', 'PARAMETER', 'MODIFIED', 'MATERIAL_CHANGE', d.typed('6.80',unit='m2'), d.typed('7.20',unit='m2'))


def test_canonical_types_and_unicode():
    assert d.signature({'x':'e\u0301'}) == d.signature({'x':'é'})
    assert len({d.signature(v) for v in ({}, {'x':None}, {'x':''}, {'x':0}, {'x':False})}) == 5
    with pytest.raises(ValueError): d.signature(float('nan'))
    with pytest.raises(ValueError): d.signature({'é':1, 'e\u0301':2})
    assert d.typed('A') != d.typed('a')
    assert d.typed(None) != d.typed(None,missing=True)


def test_version_boundary():
    assert len({document().key, document('v2').key, document(digest='b'*64).key}) == 3


def test_path_and_stat_are_private(tmp_path):
    a=tmp_path/'a'; b=tmp_path/'b'; a.write_bytes(b'abc'); b.write_bytes(b'abc')
    def key(path):
        i=file_content_identity(path)
        return d.document_version('D','v1',[{'kind':'pdf','sha256':i['sha256'],'size':i['size'],'missing':False}])
    before=key(a); assert before==key(b)
    os.utime(a,(100,100)); assert key(a)==before
    a.write_bytes(b'xyz'); os.utime(a,(100,100)); assert key(a)!=before


@pytest.mark.parametrize('field,value',[('facet','volume'),('before',d.typed('6.81')),('after',d.typed('7.21')),('left',document('v3').key),('right',document('v4').key)])
def test_change_dimensions(field,value):
    c=claim(); assert d.atomic_change(c,field=c.facet)!=d.atomic_change(replace(c,**{field:value}),field=c.facet)
    assert d.atomic_change(c,field='area')!=d.atomic_change(c,field='height')


@pytest.mark.parametrize('left,right',[([1],[2,3]),([1,2],[3])])
def test_cardinality_order_and_versions(left,right):
    a,b=document().key,document('v2').key
    def key(l,r): return d.sheet_relation(a,b,[d.sheet(a,x).key for x in l],[d.sheet(b,x).key for x in r],'MATCHED')
    assert key(left,right)==key(left[::-1],right[::-1])
    assert key(left,right)!=d.sheet_relation(b,a,[d.sheet(a,x).key for x in left],[d.sheet(b,x).key for x in right],'MATCHED')


def test_runtime_instance_is_separate():
    c=claim(); expected=d.atomic_change(c,field='area')
    runs=[d.run_instance(session_id=s,pair_id=p,run_id=r).key for s,p,r in [('s','p','r'),('s2','p','r'),('s','p2','r'),('s','p','r2')]]
    assert len(set(runs))==4
    for _ in runs: assert d.atomic_change(c,field='area')==expected


def test_unresolved_review_and_resolved_alias():
    c=claim(); a=d.text_unit(c.left,'row','MARKDOWN','x',1); b=d.text_unit(c.left,'row','MARKDOWN','x',2)
    assert a!=b
    assert d.atomic_review(c,text_unit_keys=[a.key],reason_family=['a','b'])==d.atomic_review(c,text_unit_keys=[a.key],reason_family=['b','a'])
    assert d.atomic_review(c,text_unit_keys=[a.key],reason_family=['a'])!=d.atomic_review(c,text_unit_keys=[b.key],reason_family=['a'])
    change=d.atomic_change(c,field='area')
    assert d.atomic_review(c,text_unit_keys=[],reason_family=[],resolved_change=change)==change
    with pytest.raises(ValueError): d.atomic_review(c,text_unit_keys=[],reason_family=[])


def unresolved_endpoint(*, doc=None, role='SUBJECT', scope='TABLE_LOCAL', sheet_number=1,
                        container='Schedule A', subject='3', fields=None):
    doc = doc or document()
    return d.unresolved_endpoint(
        doc.key,
        endpoint_role=role,
        scope_kind=scope,
        owner_kind='TABLE_ROW',
        source_kind='table_row',
        sheet_key=d.sheet(doc.key, sheet_number).key,
        container_label=container,
        subject_label=subject,
        field_schema=fields or ['No.', 'Name', 'Note'],
    )


def test_unresolved_endpoint_semantic_boundaries_and_normalization():
    expected = unresolved_endpoint()
    assert unresolved_endpoint(container='  SCHEDULE   A ', fields=['NO.', 'NAME', 'NOTE']) == expected
    assert unresolved_endpoint(subject='4') != expected
    assert unresolved_endpoint(container='Schedule B') != expected
    assert unresolved_endpoint(sheet_number=2) != expected
    assert unresolved_endpoint(doc=document('v2')) != expected
    assert unresolved_endpoint(role='CANDIDATE_SUBJECT') != expected
    assert unresolved_endpoint(fields=['No.', 'Note', 'Name']) != expected


def test_unresolved_endpoint_ignores_runtime_and_evidence_occurrence():
    # Runtime/evidence inputs are deliberately absent from the constructor.
    a = unresolved_endpoint()
    for _pair, _run, _session, _fragment, _bbox in (
        ('p1', 'r1', 's1', 'f1', [0, 0, 1, 1]),
        ('p2', 'r2', 's2', 'f9', [9, 9, 1, 1]),
    ):
        assert unresolved_endpoint() == a


def test_unresolved_endpoint_fails_closed_without_semantic_scope():
    doc = document()
    with pytest.raises(ValueError, match='table-local endpoint'):
        d.unresolved_endpoint(doc.key, endpoint_role='SUBJECT', scope_kind='TABLE_LOCAL',
            owner_kind='TABLE_ROW', source_kind='table_row', sheet_key=d.sheet(doc.key, 1).key,
            container_label=None, subject_label='3', field_schema=['Name'])
    with pytest.raises(ValueError, match='subject label'):
        d.unresolved_endpoint(doc.key, endpoint_role='SUBJECT', scope_kind='TABLE_LOCAL',
            owner_kind='TABLE_ROW', source_kind='table_row', sheet_key=d.sheet(doc.key, 1).key,
            container_label='Schedule', subject_label='', field_schema=['Name'])


@pytest.mark.parametrize('kind',sorted(d.QUESTION_CLASSES))
def test_question_class_and_order(kind):
    c=claim()
    assert d.question(c.left,c.right,kind,['a','b'],['c','d'])==d.question(c.left,c.right,kind,['b','a'],['d','c'])
    assert d.question(c.left,c.right,kind,['a'],[])!=d.question(c.left,c.right,kind,['b'],[])


def test_collision_fails_closed():
    registry=d.KeyRegistry(); key=document(); registry.add(key);registry.add(key)
    with pytest.raises(d.CollisionError): registry.add(d.Key(key.key,'f'*64,'different payload'))


def record():
    return dict(domain_key=document().key,domain_key_version='v1',target_kind='CHANGE',decision_scope='ATOMIC',decision='APPROVED',authority='HUMAN',author='engineer',based_on_input_content_signature='input',based_on_algorithm_signature='algorithm')


def test_registry_reuse_revalidation_supersession_and_lock(tmp_path):
    registry=DecisionRegistry(tmp_path/'registry.sqlite'); r=record()
    old=registry.append(r,expected_revision=0,current_input_signature='input')
    assert registry.resolve(r['domain_key'],'ATOMIC','input','algorithm')['decision']=='APPROVED'
    assert registry.resolve(r['domain_key'],'ATOMIC','changed','algorithm')['state']=='STALE'
    assert registry.resolve(r['domain_key'],'ATOMIC','input','new')['validation_state']=='REQUIRES_REVALIDATION'
    with pytest.raises(RegistryConflict): registry.append(r,expected_revision=0,current_input_signature='input')
    with pytest.raises(RegistryConflict): registry.append({**r,'authority':'AI','supersedes_decision_id':old['decision_id']},expected_revision=1,current_input_signature='input')
    locked=registry.append({**r,'state':'LOCKED','supersedes_decision_id':old['decision_id']},expected_revision=1,current_input_signature='input')
    with pytest.raises(RegistryConflict): registry.append({**r,'supersedes_decision_id':locked['decision_id']},expected_revision=2,current_input_signature='input')
    assert registry.history(r['domain_key'],'ATOMIC')[0]==old
    assert legacy_alias_policy({'source':'manual','reason':['user_corrected']})['authority'] is None
    with pytest.raises(ValueError): registry.append({**r,'decision':'PENDING_REVIEW'},expected_revision=2,current_input_signature='input')


def fixture_materialization(pair_id='p1', session_id='s1', run_id='r1', reverse=False):
    from backend.app.services.stage_comparison.domain_key_materialization import Materializer
    pair={'id':pair_id,'left':{'document_code':'L','version_id':'v1'},'right':{'document_code':'R','version_id':'v2'}}
    contents={side:[{'kind':'pdf','sha256':side*16,'size':10,'missing':False}] for side in ['left','right']}
    fragments=[{'id':'f1','pdf_page':1,'order':1,'source_kind':'table_row','source':'MARKDOWN','text':'Same text','bboxes':[]}, {'id':'f2','pdf_page':2,'order':2,'source_kind':'table_row','source':'MARKDOWN','text':'Same text','bboxes':[]}]
    if reverse:fragments.reverse()
    prep={'fragments':{'left':fragments,'right':[]}}
    m=Materializer(pair=pair,document_contents=contents,preparation=prep,session_id=session_id,pair_id=pair_id,run_id=run_id)
    rows=[dict(atom_id='a'+str(n),source='TEXT',subject_ref=None,project_entity_ref=pair_id+'_volatile',scope_ref=pair_id+'_group',facet_ref=None,dimension='UNKNOWN_DIMENSION',direction='REMOVED',outcome='REVIEW_REQUIRED',before_value='Same text',after_value=None,provenance={'locations':{'LEFT':[{'fragment_id':'f'+str(n),'page':n}]}}) for n in [1,2]]
    reviews=[{**r,'review_evidence_id':'review'+r['atom_id'],'reason_codes':['unknown']} for r in rows]
    if reverse:rows.reverse();reviews.reverse()
    output=m.apply({'text_atoms':{'atoms':rows},'unified_synthesis':{'changes':[],'review_items':reviews}})
    return m,output


def unresolved_fixture(pair_id='p1', session_id='s1', run_id='r1', reverse=False,
                       legacy_subject='text_scope_subject_runtime', group_suffix='a'):
    from backend.app.services.stage_comparison.domain_key_materialization import Materializer
    pair={'id':pair_id,'left':{'document_code':'L','version_id':'v1'},'right':{'document_code':'R','version_id':'v2'}}
    contents={side:[{'kind':'pdf','sha256':side*16,'size':10,'missing':False}] for side in ['left','right']}
    fragments={'left': [], 'right': []}
    target_ids={}
    for side in ('left','right'):
        block=f'block-{group_suffix}-{side}'
        group=f'group-{group_suffix}-{side}'
        prefix=side[0]
        values=['Project' if side == 'left' else 'Rebuilt', 'Other', 'Other', 'Other']
        rows=[
            {'id':f'{prefix}-title','pdf_page':1,'order':1,'source_kind':'paragraph','source':'MARKDOWN',
             'source_block_id':block,'text':'Building schedule','location_parts':['Building schedule'],'bboxes':[]},
            {'id':f'{prefix}-header','pdf_page':1,'order':2,'source_kind':'table_row','source':'MARKDOWN',
             'source_block_id':block,'source_group':group,'text':'No Name Note',
             'location_parts':['No','Name','Note'],'bboxes':[]},
        ]
        for number, value in enumerate(values, 3):
            fid=f'{prefix}-row-{number}'
            rows.append({'id':fid,'pdf_page':1,'order':number,'source_kind':'table_row','source':'MARKDOWN',
                'source_block_id':block,'source_group':group,'text':f'{number} Building {number} {value}',
                'location_parts':[str(number),f'Building {number}',value],'bboxes':[]})
        target_ids[side]=f'{prefix}-row-3'
        fragments[side]=list(reversed(rows)) if reverse else rows
    prep={'fragments':fragments}
    m=Materializer(pair=pair,document_contents=contents,preparation=prep,session_id=session_id,pair_id=pair_id,run_id=run_id)
    locations={side.upper():[{'fragment_id':target_ids[side],'page':1}] for side in ('left','right')}
    common=dict(subject_ref=legacy_subject,project_entity_ref=None,facet_ref='note',dimension='TYPE',
        direction='REPLACED',outcome='REVIEW_REQUIRED',before_value='Project',after_value='Rebuilt',
        provenance={'parser_rule':'owned_table_row_field','locations':locations,'source_fragment_ids':{
            side.upper():[target_ids[side]] for side in ('left','right')}})
    fact={'fact_id':'fact-1',**common}
    atom={'atom_id':'atom-1','source':'TEXT',**common}
    review={'review_evidence_id':'review-1','atom_id':'atom-1','source':'TEXT','reason_codes':['entity_unknown'],**common}
    relation={'relation_id':'relation-1','left_entity_ref':legacy_subject,'right_entity_ref':legacy_subject,'relation':'UNKNOWN'}
    question={'question_id':'question-1','category':'ENTITY','dependencies':[{'ref':'relation-1'}],'context':{}}
    artifacts={
        'sheet_relations':{'relations':[{'relation_id':'sheet-relation-1','left_pages':[1],'right_pages':[1],'relation_type':'MATCHED'}]},
        'text_fact_production':{'facts':[fact]},
        'text_atoms':{'atoms':[atom]},
        'entity_relations':{'relations':[relation]},
        'unified_synthesis':{'changes':[],'review_items':[review]},
        'review_questions':{'questions':[question]},
    }
    return m,m.apply(artifacts)


def test_actual_adapter_runtime_and_evidence_order_invariance():
    _,a=fixture_materialization()
    for kwargs in [dict(pair_id='p2'),dict(session_id='s2'),dict(run_id='r2'),dict(reverse=True)]:
        _,b=fixture_materialization(**kwargs)
        for name,field in [('text_atoms','atoms'),('unified_synthesis','review_items')]:
            assert sorted(r['domain_key'] for r in a[name][field])==sorted(r['domain_key'] for r in b[name][field])
            assert len({r['domain_key'] for r in b[name][field]})==2
    _,b=fixture_materialization(run_id='r2')
    assert a['text_atoms']['atoms'][0]['run_instance_key']!=b['text_atoms']['atoms'][0]['run_instance_key']


def test_blocking_unresolved_endpoint_class_is_fully_materialized_and_stable():
    materializer,a=unresolved_fixture()
    expected_rows=(
        ('sheet_relations','relations'), ('text_fact_production','facts'),
        ('text_atoms','atoms'), ('entity_relations','relations'),
        ('unified_synthesis','review_items'), ('review_questions','questions'),
    )
    assert all(row.get('domain_key') for artifact, field in expected_rows for row in a[artifact][field])
    for kwargs in (
        {'pair_id':'p2','legacy_subject':'text_scope_subject_other'},
        {'session_id':'s2'}, {'run_id':'r2'}, {'reverse':True},
        {'group_suffix':'runtime-other'},
    ):
        materializer,b=unresolved_fixture(**kwargs)
        for artifact,field in expected_rows:
            assert [row['domain_key'] for row in a[artifact][field]] == [row['domain_key'] for row in b[artifact][field]]
    payloads='\n'.join(materializer.registry.payloads.values())
    assert 'text_scope_subject_' not in payloads
    assert 'group-runtime' not in payloads
    assert '"pair_id"' not in payloads


def test_distinct_unresolved_rows_do_not_merge_and_relation_fails_ambiguous():
    m,artifacts=unresolved_fixture()
    second=dict(artifacts['text_fact_production']['facts'][0])
    second.pop('domain_key',None); second.pop('domain_key_version',None); second.pop('domain_key_sha256',None)
    second.pop('legacy_id',None); second.pop('run_instance_key',None); second.pop('occurrence_id',None)
    second['fact_id']='fact-2'
    second['provenance']=json.loads(json.dumps(second['provenance']))
    for field in ('locations','source_fragment_ids'):
        for side in ('LEFT','RIGHT'):
            values=second['provenance'][field][side]
            if field == 'locations':
                values[0]['fragment_id']=values[0]['fragment_id'].replace('row-3','row-4')
            else:
                values[0]=values[0].replace('row-3','row-4')
    clean={'text_fact_production':{'facts':[artifacts['text_fact_production']['facts'][0],second]}}
    # Recreate to avoid feeding already-materialized metadata back as input.
    base,_=unresolved_fixture()
    rows=base.apply(clean)['text_fact_production']['facts']
    assert len({row['domain_key'] for row in rows}) == 2
    with pytest.raises(ValueError, match='ambiguous unresolved endpoint'):
        base.apply({**clean,'entity_relations':{'relations':[{
            'relation_id':'relation-ambiguous','left_entity_ref':'text_scope_subject_runtime',
            'right_entity_ref':'text_scope_subject_runtime','relation':'UNKNOWN'}]}})


def test_legacy_reader_opt_in(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import production_store
    monkeypatch.setenv('COMPARISON_ROOT',str(tmp_path))
    _,artifacts=fixture_materialization();payload=artifacts['text_atoms']
    payload['stable_domain_keys']={'schema':'stable-domain-materialization.v1'}
    production_store.save_artifact('s','p','text_atoms',payload)
    assert 'domain_key' not in production_store.load_artifact('s','p','text_atoms')['atoms'][0]
    assert 'domain_key' in production_store.load_artifact('s','p','text_atoms',include_domain_keys=True)['atoms'][0]
    old={'atoms':[{'atom_id':'legacy','before_value':None}]}
    production_store.save_artifact('s','p','text_atoms',old)
    assert production_store.load_artifact('s','p','text_atoms')==old
