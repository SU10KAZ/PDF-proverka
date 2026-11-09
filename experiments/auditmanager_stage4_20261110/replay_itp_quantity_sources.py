"""Local source replay. No inference, PDF re-extraction or historical truth edits."""
import hashlib
import json
from decimal import Decimal
from pathlib import Path

from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


def replay(review):
    source = Path(review['source']['path'])
    raw = source.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != review['source']['sha256']:
        raise ValueError('Source version changed')
    blocks = authoritative_text_blocks(raw.decode('utf-8'))
    rows = []
    for expected in review['component_rows']:
        block = blocks[expected['block_id']]
        matches = [line for line in block.splitlines() if expected['code'] + '*' in line]
        if len(matches) != 1 or matches[0] != expected['source_row']:
            raise ValueError(f"Missing or ambiguous row: {expected['code']}")
        cells = [cell.strip() for cell in matches[0].strip('|').split('|')]
        unit, quantity = cells[5], cells[6]
        if unit != expected['unit'] or quantity != expected['quantity']:
            raise ValueError(f"Quantity/unit mismatch: {expected['code']}")
        rows.append({**expected, 'parsed_quantity': str(Decimal(quantity))})
    for item in review['requirement_source'] + review['scope_heading_source']:
        if item['source_row'] not in blocks[item['block_id']]:
            raise ValueError('Requirement or heading source mismatch')
    total = sum((Decimal(row['parsed_quantity']) for row in rows), Decimal(0))
    if str(total) != review['quantity_sum']:
        raise ValueError('Historical arithmetic mismatch')
    return {
        'kind': 'primary_MD_quantity_and_scope_replay',
        'source': {'path': str(source), 'sha256': digest},
        'verified_rows': rows,
        'derived_sum': str(total),
        'derived_sum_is_project_stated_total': False,
        'project_total_comparison_performed': False,
        'reason_no_total_comparison': 'No independently source-addressed total for this four-row set was established. Do not attach a fabricated 819 target to the specification block.',
        'scope_sources_verified': review['requirement_source'] + review['scope_heading_source'],
        'confirmed_violation_count': None,
        'unresolved': review['unresolved'],
        'boundary': 'Four quantities and their arithmetic are verified. Assignment to residential water supply, diameter interpretation and applicability remain unresolved. This is neither 819 confirmed violations nor proof of compliance.',
        'new_model_calls': 0,
        'independent_discovery': False,
    }


if __name__ == '__main__':
    directory = Path(__file__).resolve().parent
    review = json.loads((directory / 'ITP_requirement_local_review.json').read_text())
    output = directory / 'ITP_quantity_source_replay.json'
    output.write_text(json.dumps(replay(review), ensure_ascii=False, indent=2) + '\n')
    print(output)
