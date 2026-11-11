"""Local, column-preserving evidence trace; never infer a blank quantity cell."""
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks

MD=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/KM/versions/v001/02_work/document.md')
OUT=Path(__file__).parent/'KM_LK6_literal_quantity_trace.json'


def cells(line):return [c.strip() for c in line.split('|')[1:-1]]


def main():
    raw=MD.read_bytes();blocks=authoritative_text_blocks(raw.decode())
    bid='blk_b9096ac74f95416da66cc607a9828c3f';spec=blocks[bid]
    within=False;instances=[]
    for line in spec.splitlines():
        if not line.startswith('|'):continue
        row=cells(line)
        if len(row)!=6:continue
        if row[2]=='ЛК6':within=True;continue
        if within and re.fullmatch(r'ОГМ\d+',row[0]):
            assert row[3].isdigit()
            instances.append({'mark':row[0],'instances':int(row[3]),'source_block':bid,'source_row':line})
    assert len(instances)==8
    trace=[]
    for assembly in instances:
        matches=[(k,t) for k,t in blocks.items() if re.search(r'Ведомость элементов ограждения '+re.escape(assembly['mark'])+r'\b',t)]
        assert len(matches)==1,assembly
        block,text=matches[0]
        headers=[line for line in text.splitlines() if line.startswith('|') and 'Количество' in cells(line)]
        assert len(headers)==1
        header=cells(headers[0]);column=header.index('Количество')
        for kind in ('Анкер','Лист'):
            matching=[line for line in text.splitlines() if line.startswith('|') and kind in cells(line)]
            assert len(matching)==1,(assembly['mark'],kind)
            line=matching[0];values=cells(line);assert len(values)==len(header)
            literal=values[column]
            assert not literal or literal.isdigit()
            trace.append({**assembly,'kind':kind,'detail_block':block,'header_row':headers[0],
                'detail_row':line,'all_cells':values,'quantity_column_zero_based':column,
                'quantity_literal':literal,'quantity':int(literal) if literal else None,
                'subtotal':int(literal)*assembly['instances'] if literal else None})
    plates=sum(r['subtotal'] for r in trace if r['kind']=='Лист')
    known_anchors=sum(r['subtotal'] or 0 for r in trace if r['kind']=='Анкер')
    unknown=[r['mark'] for r in trace if r['kind']=='Анкер' and r['quantity'] is None]
    report={'md_path':str(MD),'md_sha256':hashlib.sha256(raw).hexdigest(),'rows':trace,
        'assembly_instances':sum(r['instances'] for r in instances),'literal_plate_count':plates,
        'known_anchor_subtotal':known_anchors,'unresolved_anchor_marks':unknown,
        'conditional_blank_plate_mass_kg':str(Decimal(plates)*Decimal('0.010')*Decimal('0.120')*Decimal('0.120')*Decimal('7850')),
        'mass_boundary':'Assumes identical solid rectangular blanks and density 7850 kg/m3. Does not verify hole deductions, procurement scope or total section C2.',
        'anchor_boundary':'Numbers in other columns are retained verbatim. Any inference from plate count or node geometry requires independent source proof; it is not a literal quantity.',
        'source_review_truth_certified':False,'global_F005_totals_verified':False}
    assert plates==57 and known_anchors==172 and set(unknown)=={'ОГМ21','ОГМ24'}
    OUT.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ['assembly_instances','literal_plate_count','known_anchor_subtotal','unresolved_anchor_marks','conditional_blank_plate_mass_kg']},ensure_ascii=False))


if __name__=='__main__':main()
