from backend.app.pipeline.stages.findings_verify.source_review_image_refs import repair_unknown_image_labels

MD='### BLOCK #1 [GRAPHIC]: a\n### BLOCK #2 [GRAPHIC]: b\n'


def test_unique_hash_repairs_unknown_label_but_never_existing_other_label():
    request={'image_bindings':[{'block_id':'a','sha256':'exact'}]}
    original={'claims':[{'evidence':[{'type':'image','block_id':'typo','image_sha256':'exact','bbox_norm_xyxy':[0,0,1,1],'observation':'unchanged'}]}]}
    fixed,receipt=repair_unknown_image_labels(original,request,MD)
    assert fixed['claims'][0]['evidence'][0]['block_id']=='a'
    assert original['claims'][0]['evidence'][0]['block_id']=='typo'
    assert fixed['claims'][0]['evidence'][0]['observation']=='unchanged' and len(receipt['repairs'])==1
    original['claims'][0]['evidence'][0]['block_id']='b'
    fixed,receipt=repair_unknown_image_labels(original,request,MD)
    assert fixed==original and not receipt['repairs']


def test_unknown_or_ambiguous_hash_does_not_acquire_source():
    for images in [[],[{'block_id':'a','sha256':'x'},{'block_id':'b','sha256':'x'}]]:
        original={'claims':[{'evidence':[{'type':'image','block_id':'unknown','image_sha256':'x'}]}]}
        fixed,receipt=repair_unknown_image_labels(original,{'image_bindings':images},MD)
        assert fixed==original and not receipt['repairs']
