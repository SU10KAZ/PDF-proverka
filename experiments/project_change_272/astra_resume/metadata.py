"""Research metadata only; raw receipts and accepted answers are immutable."""
from copy import deepcopy

MODEL = 'gpt-6-astra'
REASONING = 'xhigh'


def region_id(data):
    return data.get('frozen_region', {}).get('region_id')


def normalize_checkpoint(raw, receipt_by_call):
    result = deepcopy(raw)
    result.update(model=MODEL, reasoning=REASONING)
    for region in result['regions']:
        receipt = receipt_by_call[region['accepted_call_id']]
        assert receipt['model'] == MODEL
        assert receipt['reasoning']['effort'] == REASONING
        region.update(model=MODEL, reasoning=REASONING)
        region['model_visible_input'].update(model=MODEL, reasoning=REASONING)
        region['metadata_lineage'] = receipt['lineage']
    return result
