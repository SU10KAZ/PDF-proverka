from copy import deepcopy
from backend.app.pipeline.stages.findings_verify.source_review_quote_spacing import repair_table_quote_spacing


def test_repairs_only_cell_padding_and_keeps_raw_claims():
    md='### BLOCK #1 [TEXT]: b\n| Name | Quantity | Note |\n| A |  | 28 |\n'
    response={'claims':[{'claim':'quantity remains unknown','status':'needs_data','reason':'other column',
                        'evidence':[{'type':'text','block_id':'b','quote':'| A | | 28 |'}]}]}
    original=deepcopy(response)
    repaired,receipt=repair_table_quote_spacing(response,md)
    assert response==original
    assert repaired['claims'][0]['evidence'][0]['quote']=='| A |  | 28 |'
    assert repaired['claims'][0]['status']=='needs_data'
    assert len(receipt['repairs'])==1 and not receipt['semantic_verified']


def test_does_not_repair_values_columns_duplicate_rows_or_internal_text():
    quotes=['| A | 28 |  |','| A | 29 |','| A |  |  | 28 |','A  28','| B C | | 28 |']
    md='### BLOCK #1 [TEXT]: b\n| A |  | 28 |\n| A |  | 28 |\n| B  C |  | 28 |\n'
    response={'claims':[{'evidence':[{'type':'text','block_id':'b','quote':q} for q in quotes+['| A | | 28 |']]}]}
    repaired,receipt=repair_table_quote_spacing(response,md)
    assert repaired==response and not receipt['repairs']
