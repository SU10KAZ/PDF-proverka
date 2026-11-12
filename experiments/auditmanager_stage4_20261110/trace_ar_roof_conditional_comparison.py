"""Trace the known AR roof operation and retention, without relabelling the frozen trial."""
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from openpyxl import load_workbook

BASE = Path(__file__).resolve().parent

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    candidate_path = BASE/'ar_roof_handoff_v2_source_evidence/F-024/candidate.json'
    request_path = BASE/'ar_roof_handoff_v2_source_evidence/F-024/00_review/request.json'
    final_path = BASE/'ar_rope_current_release_evidence/release/03_findings.json'
    previous_path = Path('/tmp/auditmanager_ar_roof_reviewed_question_20261111/03_findings.json')
    excel_path = Path('/tmp/auditmanager_ar_rope_current_release_20261112/report.xlsx')
    md_path = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/AR/versions/v001/02_work/document.md')
    matrix_path = BASE/'stage4_original_control_matrix.json'
    candidate = json.loads(candidate_path.read_text())['entry']
    request = json.loads(request_path.read_text())['request']
    final = json.loads(final_path.read_text())['findings']
    previous = json.loads(previous_path.read_text())['findings']
    target = next(f for f in final if f['id'] == 'F-024')
    approved = candidate['decision']['approved_fields']
    assert all(target[k] == approved[k] for k in ('problem','description','solution'))
    assert target['report_section'] == 'questions'
    assert final[:len(previous)] == previous
    levels = ['20.840','20.875','20.870','20.910']
    heights = [int((Decimal('20.990') - Decimal(z)) * 1000 - 40) for z in levels]
    assert heights == [110,75,80,40]
    assert all(str(h) in approved['description'] for h in heights)
    assert '25–50' in approved['description']
    assert 'Если' in approved['description'] and 'не установлены' in approved['description']
    ids = ['blk_7542a26ddb2e4317bf9a1c3e1e239b62','blk_f3aec0b585f6435080a6caa87767ba7c','blk_602b484f088b4a029d3ba6e01df3f645']
    images = []
    for bid in ids:
        binding, = [i for i in request['image_bindings'] if i['block_id'] == bid]
        assert sha(Path(binding['path'])) == binding['sha256']
        images.append(binding)
    wb = load_workbook(excel_path, read_only=True)
    rows = list(wb['AR'].values)
    matches = [i for i,row in enumerate(rows) if 'F-024' in row]
    headings = [i for i,row in enumerate(rows) if 'Вопросы и уточнения' in row]
    assert len(matches) == 1 and headings and matches[0] > headings[0]
    row = rows[matches[0]]
    assert all(value in row for value in approved.values())
    wb.close()
    result = {
        'control':'H05-P03',
        'original_requirement':'АР: остаток75мм на угловом участке сопоставить с проектным диапазоном опор.',
        'operation_observed':True,
        'calculation':{'finish_m':'20.990','tile_mm':40,'lower_levels_m':levels,'conditional_heights_mm':heights,'project_range_mm':[25,50],'conditional_excess_mm':[60,25,30,0]},
        'source_inspection':'Three saved graphic blocks visually inspected against their exact bound hashes. Lower/finish plans share corner geometry and K-3; roof schedule explicitly gives 40 mm and 25–50 mm. Source stamps for sheets 5 and 7 identify AR6.2 sections 2,3; a nearby boundary label Section 1 alone does not establish different source applicability.',
        'conditions':['Lower levels must be the actual surface immediately below supports.','Finish 20.990 must apply at corresponding points.'],
        'physical_violation_proven':False,
        'approved_fields':approved,
        'retention':{'approved_fields_equal_later_final':True,'prior_findings_unchanged_in_later_release':len(previous),'excel_occurrences':len(matches),'separate_questions_section':True,'exact_user_fields_in_excel':True},
        'distinction':'The required conditional comparison is executed and retained. This is not proof of actual excessive support height, blind rediscovery, or full-audit acceptance.',
        'remaining_acceptance':['Demonstrate automatic operation and retention in a frozen full audit.','Do not treat unavailable level-to-layer mapping as either confirmed installation impossibility or refutation.'],
        'new_model_calls':0,'frozen_matrix_modified':False,'stage4_complete':False,'production_acceptance':False,
        'images':images,
        'files':{str(p):sha(p) for p in [candidate_path,request_path,final_path,previous_path,excel_path,md_path,matrix_path]},
    }
    out = BASE/'AR_roof_conditional_comparison_trace.json'
    out.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'operation_observed':True,'retention':result['retention'],'physical_violation_proven':False},ensure_ascii=False))

if __name__ == '__main__': main()
