from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry, OPERATIONS


def obj():
    return {'id':'o','identity':'cabinet A','scope':'building 1 level 2','kind':'cabinet','source_refs':['b']}


def test_read_block_does_not_close_missing_operations_or_inventory():
    r = reconcile_object_registry({'object_registry':[obj()], 'analyzed':True}, {'b','c'})
    assert r['unresolved_operations'] == len(OPERATIONS)
    assert r['unresolved_source_blocks'] == 2
    assert not r['coverage_complete']


def test_receipts_require_addressed_basis_and_never_certify_semantics():
    receipt={'object_id':'o','operation':'dimension_consistency','status':'checked','reason':'sum differs','source_refs':['b']}
    data={'object_registry':[obj()], 'object_operation_results':[receipt],
          'object_inventory_coverage':[{'block_id':'b','status':'inventoried','object_ids':['o'],'reason':'cabinet drawing'}]}
    r=reconcile_object_registry(data,{'b'})
    assert r['unresolved_operations']==len(OPERATIONS)-1 and r['unresolved_source_blocks']==0
    assert not r['semantic_verified']
    receipt['source_refs']=['missing']
    assert reconcile_object_registry(data,{'b'})['unresolved_operations']==len(OPERATIONS)
    receipt['status']='not_applicable'
    assert reconcile_object_registry(data,{'b'})['unresolved_operations']==len(OPERATIONS)


def test_ambiguous_identity_and_invented_object_links_stay_unresolved():
    data={'object_registry':[obj(),obj()], 'object_inventory_coverage':[
        {'block_id':'b','status':'inventoried','object_ids':['o'],'reason':'claimed'}]}
    r=reconcile_object_registry(data,{'b'})
    assert len(r['invalid_objects'])==2
    assert r['unresolved_source_blocks']==1


def test_block_cannot_be_declared_empty_when_registry_contains_an_object():
    data={'object_registry':[obj()], 'object_inventory_coverage':[
        {'block_id':'b','status':'no_objects','object_ids':[],'reason':'read and clean'}]}
    r=reconcile_object_registry(data,{'b'})
    assert r['unresolved_source_blocks']==1
    assert r['source_inventory'][0]['reason']=='inventory_conflicts_with_registered_objects'


def test_declared_unreadable_regions_cannot_claim_complete_source_inventory():
    receipt={'block_id':'b','status':'inventoried','object_ids':['o'],'reason':'Read views',
        'unreadable_regions':[{'region':'edge','description':'Callout clipped'}]}
    data={'object_registry':[obj()],'object_inventory_coverage':[receipt]}
    result=reconcile_object_registry(data,{'b'})
    assert result['unresolved_source_blocks']==1
    assert result['source_inventory'][0]['status']=='needs_data'
    assert result['source_inventory'][0]['reason']=='declared_unreadable_regions'
    assert receipt['status']=='inventoried'  # Preserve the model's contradictory claim.
    receipt['unreadable_regions']=[]
    assert reconcile_object_registry(data,{'b'})['unresolved_source_blocks']==0
    receipt['unreadable_regions']='unknown'
    result=reconcile_object_registry(data,{'b'})
    assert result['source_inventory'][0]['reason']=='invalid_unreadable_regions'
    assert result['unresolved_source_blocks']==1
