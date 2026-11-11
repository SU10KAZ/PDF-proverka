"""Independent TEXT-only arithmetic by sheet-scoped mark; no model conclusions."""
from collections import defaultdict
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks
from .check_km_lk6_literal_quantities import MD, cells


def main():
    raw=MD.read_bytes();blocks=authoritative_text_blocks(raw.decode());details=defaultdict(list)
    for bid,text in blocks.items():
        sheet=re.search(r'Sheet: (\d+)\s*(?:\||\n)',text)
        heading=re.search(r'Ведомость элементов ограждения\s+ОГМ\s*([0-9A-Za-zА-Яа-я]+)(?:\s*\((\d+)\s*шт\.?\))?',text)
        if not sheet or not heading:continue
        headers=[line for line in text.splitlines() if line.startswith('|') and 'Количество' in cells(line)]
        if len(headers)!=1:continue
        header=cells(headers[0]);col=header.index('Количество');quantities={};quotes={}
        for kind in ('Анкер','Лист'):
            rows=[line for line in text.splitlines() if line.startswith('|') and kind in cells(line)]
            if len(rows)!=1:continue
            row=cells(rows[0]);assert len(row)==len(header),(bid,kind)
            value=row[col]
            if value and not value.isdigit():raise ValueError((bid,value))
            quantities[kind]=int(value) if value else None;quotes[kind]=rows[0]
        details[(int(sheet[1]),heading[1])].append({'block_id':bid,'heading':heading[0],
            'heading_instances':int(heading[2]) if heading[2] else None,'quantity_header':headers[0],
            'quantities':quantities,'source_rows':quotes})
    groups=defaultdict(list);spec_id='blk_b9096ac74f95416da66cc607a9828c3f';group=None
    for line in blocks[spec_id].splitlines():
        if not line.startswith('|'):continue
        row=cells(line)
        if len(row)!=6:continue
        if row[2] in ('ЛК1.1','ЛК3.1','ЛК4.1','ЛК5','ЛК6'):group=row[2];continue
        mark=re.fullmatch(r'ОГМ(\d+)',row[0]);sheet=re.fullmatch(r'лист (\d+)',row[1])
        if group and mark and sheet:
            assert row[3].isdigit()
            groups[group].append({'mark':mark[1],'sheet':int(sheet[1]),'instances':int(row[3]),'instance_source':{'block_id':spec_id,'quote':line}})
    for name,sheet in [('С2',4),('С3',6)]:
        for (s,mark),matches in sorted(details.items()):
            if s!=sheet:continue
            assert len(matches)==1
            item=matches[0];assert item['heading_instances'] is not None,(name,mark)
            groups[name].append({'mark':mark,'sheet':sheet,'instances':item['heading_instances'],
                'instance_source':{'block_id':item['block_id'],'quote':item['heading']}})
    output=[]
    for name,assemblies in groups.items():
        trace=[];totals={kind:0 for kind in ('Анкер','Лист')};unknown=[]
        for assembly in assemblies:
            matches=details[(assembly['sheet'],assembly['mark'])]
            if len(matches)!=1:
                unknown.extend({'mark':assembly['mark'],'sheet':assembly['sheet'],'kind':kind,'reason':'missing_or_ambiguous_exact_sheet_mark'} for kind in totals)
                trace.append({**assembly,'detail':None})
                continue
            detail=matches[0];trace.append({**assembly,'detail':detail})
            for kind in totals:
                quantity=detail['quantities'].get(kind)
                if quantity is None:unknown.append({'mark':assembly['mark'],'sheet':assembly['sheet'],'kind':kind})
                else:totals[kind]+=assembly['instances']*quantity
        output.append({'group':name,'trace':trace,'known_subtotals':totals,'unresolved':unknown,
            'complete_for_listed_assemblies':not unknown,'physical_inventory_completeness_proven':False})
    assert len(output)==7
    anchors=sum(g['known_subtotals']['Анкер'] for g in output);plates=sum(g['known_subtotals']['Лист'] for g in output)
    report={'source_md_sha256':hashlib.sha256(raw).hexdigest(),'groups':output,
        'known_anchor_subtotal':anchors,'known_plate_subtotal':plates,
        'unresolved':[dict(group=g['group'],**u) for g in output for u in g['unresolved']],
        'conditional_known_plate_mass_kg':str(Decimal(plates)*Decimal('1.1304')),
        'scope':'Literal quantities by provided sheet and mark. Not a physical inventory, not proof of simultaneous applicability of P/R sheet versions.',
        'boundary':'Nominal mass assumes all counted plate rows represent 10x120x120 solid blanks and density 7850; check actual plate sizes below before using mass. Holes, procurement and cutting not certified.',
        'plate_dimensions':[r['detail']['source_rows'].get('Лист') for g in output for r in g['trace'] if r['detail'] is not None],
        'identity_boundary':'Literal Cyrillic З and digit 3 remain distinct labels; no OCR correction or raster reinterpretation. Section heading multiplicities are counted under their literal marks.',
        'normative_violation_verified':False,'full_acceptance':False}
    path=Path(__file__).with_name('KM_complete_literal_quantity_trace.json');path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'groups':[(g['group'],g['known_subtotals'],g['unresolved']) for g in output],
        'known_anchor_subtotal':anchors,'known_plate_subtotal':plates},ensure_ascii=False))


if __name__=='__main__':main()
