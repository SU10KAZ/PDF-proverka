"""Source-bound arithmetic control; does not infer missing load or tolerance."""
import hashlib
import json
import re
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from backend.app.pipeline.stages.text_analysis.quantity_checks import evaluate_quantity_checks
from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


def main():
    here = Path(__file__).resolve().parent
    case_path = here.parent / 'auditmanager_stage1_20261109/decisions/EOM27_active_socket_circuits.json'
    case = json.loads(case_path.read_text())
    source = Path(case['source_md']['path'])
    if hashlib.sha256(source.read_bytes()).hexdigest() != case['source_md']['sha256']:
        raise ValueError('Source changed')
    bid = case['evidence'][0]['block_id']
    block = authoritative_text_blocks(source.read_text())[bid]
    lines = block.splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith('| РПЗ |'))
    end = next(i for i in range(start+1,len(lines)) if lines[i].startswith('| РП4 |'))
    target = [part.strip() for part in lines[start].strip('|').split('|')]
    rows = [[part.strip() for part in line.strip('|').split('|')]
            for line in lines[start+1:end] if re.match(r'^\| C23\d+ \|',line)]
    if {row[0] for row in rows} != {'C23'+str(i) for i in range(1,25)} or len(rows) != 24:
        raise ValueError('Unexpected source row inventory')
    operations = []
    for column, parameter in [(2,'Py'),(3,'Pp')]:
        def operand(raw):
            return {'object':'ВРУ-2 РП3: 24 указанные строки C231–C2324',
                    'parameter':parameter,'unit':'кВт','basis':'Сумма опубликованных строк таблицы',
                    'regime':'Режим строки РПЗ Pp, не пожарный Pp.лож',
                    'value':raw.replace(',','.'),'source_refs':[bid]}
        operations.append({'id':'RP3_'+parameter,'kind':'sum',
                           'terms':[operand(row[column]) for row in rows],
                           'target':operand(target[column])})
    result = evaluate_quantity_checks(operations,{bid})
    rounding = []
    for operation in result['operations']:
        stated = Decimal(operation['stated'])
        displayed = Decimal(operation['calculated']).quantize(
            Decimal(1).scaleb(stated.as_tuple().exponent),rounding=ROUND_HALF_UP)
        rounding.append({'id':operation['id'],'exact_sum':operation['calculated'],
                         'project_total':operation['stated'],'rounded_to_displayed_precision':str(displayed),
                         'compatible_with_ordinary_rounding':displayed == stated})
    assert all(x['compatible_with_ordinary_rounding'] for x in rounding)
    payload = {'case':'H-EOM-27','source':case['source_md'],'block_id':bid,
               'source_rows':rows,'source_total_row':target,'operations':operations,
               'execution':result,'rounding_review':rounding,
               'retain_separately':'Missing separate active rows C2326/C2327 remains the source-backed reconciliation finding.',
               'boundary':'Reported totals are reproducible from listed rows with ordinary rounding. No universal tolerance introduced. This does not establish correct scope or exclude aggregation, and does not justify adding 4 kW automatically.',
               'new_model_calls':0,'independent_discovery':False}
    (here/'EOM27_arithmetic_control.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(rounding,ensure_ascii=False))


if __name__ == '__main__':
    main()
