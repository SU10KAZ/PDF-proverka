import pytest
from backend.app.pipeline.stages.text_analysis.object_identity_candidates import build_object_identity_candidates


def obj(oid,identity,scope):return {'id':oid,'identity':identity,'scope':scope,'kind':'beam','source_refs':[oid+'-source']}


def test_equal_marks_preserve_distinct_scopes_and_never_merge():
    rows=[obj('p1:a','Балка Б1, исполнение А','floor 1'),obj('p2:a','Балка Б1, исполнение Б','floor 2')]
    r=build_object_identity_candidates(rows)
    assert len(r['pairs'])==1 and r['automatic_merges']==0
    assert r['pairs'][0]['left_scope']!=r['pairs'][0]['right_scope']
    assert r['pairs'][0]['identity_relation']=='not_checked'
    assert rows[0]['id']=='p1:a' and rows[1]['id']=='p2:a'


def test_budget_and_nonmatches_remain_explicitly_unchecked():
    rows=[obj(f'p{i}:a','Б1','same scope') for i in range(3)]+[obj('p4:b','Другой объект','scope')]
    r=build_object_identity_candidates(rows,max_pairs=1)
    assert len(r['pairs'])==1 and len(r['backlog'])==2
    assert r['objects_preserved']==4 and not r['identity_coverage_complete']


def test_duplicate_ids_do_not_overwrite_objects():
    with pytest.raises(ValueError,match='duplicate_object_id'):
        build_object_identity_candidates([obj('p1:a','Б1','one'),obj('p1:a','Б2','two')])
