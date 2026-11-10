"""Refute a preserved historical claim using complete authoritative row sets."""
import hashlib
import json
import re
from pathlib import Path

from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks
from backend.app.pipeline.stages.text_analysis.quantity_checks import evaluate_quantity_checks


if __name__ == '__main__':
    here = Path(__file__).resolve().parent
    case_path = here.parent/'auditmanager_stage1_20261109/decisions/EOM_F040_parking_sum_refuted.json'
    case = json.loads(case_path.read_text())
    source = Path(case['source_md']['path'])
    assert hashlib.sha256(source.read_bytes()).hexdigest() == case['source_md']['sha256']
    blocks = authoritative_text_blocks(source.read_text())
    checks = []
    for evidence in case['evidence']:
        bid = evidence['block_id']
        rows = []
        for line in blocks[bid].splitlines():
            cells = [s.strip() for s in line.strip('|').split('|')]
            if len(cells) == 4 and re.fullmatch(r'15\d\d',cells[0]):
                value = re.fullmatch(r'(\d+,\d+) м²',cells[3])
                assert value, line
                rows.append({'id':cells[0], 'quote':line, 'value':value.group(1).replace(',','.')})
        assert len(rows) == 89 and {r['id'] for r in rows} == {str(i) for i in range(1501,1590)}
        total_quote = '| Итого: |  |  | 1636,5 м² |'
        assert total_quote in blocks[bid]
        def operand(value):
            return {'object':'Опубликованные строки экспликации 1501–1589','parameter':'площадь',
                    'unit':'м²','basis':'Площадь каждой строки один раз, включая групповые строки',
                    'regime':'проект','source_refs':[bid],'value':value}
        operation = {'id':'parking_'+bid,'kind':'sum',
                     'expected_term_ids':[r['id'] for r in rows],
                     'terms':[{**operand(r['value']), 'term_id':r['id']} for r in rows],
                     'target':operand('1636.5')}
        execution = evaluate_quantity_checks([operation],set(blocks))
        assert execution['operations'][0]['numerically_equal']
        omitted = {**operation, 'terms': operation['terms'][:-1]}
        omitted_result = evaluate_quantity_checks([omitted],set(blocks))['operations'][0]
        assert omitted_result['reason'] == 'term_inventory_mismatch'
        assert omitted_result['missing_term_ids'] == ['1589']
        checks.append({'block_id':bid,'rows':rows,'total_quote':total_quote,'execution':execution})
    result = {'finding_id':'F-040','candidate_id':'T-020','source':case['source_md'],
              'engineer_decision':case['engineer_decision'],'checks':checks,
              'decision':'refuted_by_complete_source_row_arithmetic',
              'rejected_claim':'1622.7 m² row sum and 13.8 m² deficit',
              'verified_sum_each_table':'1636.5',
              'lineage_policy':'Resolvable source links must not rehabilitate a source-refuted claim.',
              'preservation_replay_interpretation':'The deterministic 244/244 preservation result is compatibility evidence, not proof that every preserved finding is useful or true.',
              'boundary':'No claim about physical number of spaces, geometry, architectural model or supply. Runtime model semantic rejection remains untested.',
              'new_model_calls':0}
    (here/'EOM_F040_semantic_refutation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print('F-040 refuted: both 89-row source sums equal 1636.5 m²')
