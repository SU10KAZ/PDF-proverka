"""Archive a known source-bound release pair and exercise its actual Excel output."""
import hashlib
import json
from pathlib import Path
import shutil
from openpyxl import Workbook,load_workbook
from .run_km_citation_completion import ROOT,BASE
from .citation_completion import apply_candidate
from .run_km_final_check_pair import CANDIDATES
from backend.app.pipeline.stages.findings_verify.source_review_final_check import validate_final_check
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet,PROJ_COLUMNS


def main():
    original_entry=json.loads(CANDIDATES['version_b'].read_text())['entry']
    entry=json.loads((ROOT/'candidate.json').read_text())['entry']
    baseline=json.loads((BASE/'snapshots/KM/findings_verify.json').read_text())
    md=(BASE/'inputs/KM/versions/v001/02_work/document.md').read_text()
    original=next(p['finding_snapshot'] for p in entry['queue']['packets'] if p['finding_id']=='F-005')
    document={**baseline,'findings':[original if f['id']=='F-005' else f for f in baseline['findings']]}
    prior_check=json.loads(Path('/tmp/auditmanager_km_final_check_pair_20261111/version_b/receipt.json').read_text())['review']
    result=json.loads((ROOT/'result.json').read_text())
    assert apply_candidate(original_entry,document,md,prior_check,result['json_data'])==entry
    receipt=json.loads((ROOT/'receipt.json').read_text())
    assert validate_final_check(entry,document,md,receipt['review'])['status']=='release_candidate'
    after=json.loads((ROOT/'output_preview.json').read_text())
    assert len(after['findings'])==len(baseline['findings'])==26
    assert [f for f in after['findings'] if f['id']!='F-005']==[f for f in baseline['findings'] if f['id']!='F-005']
    revised=next(f for f in after['findings'] if f['id']=='F-005')
    prior=json.loads(Path('/tmp/auditmanager_km_f005_output_readjudication_20261111/local_output.json').read_text())
    # Citation completion must not invent a more convenient final proposition.
    for key in ('problem','description','solution','severity','risk','norm','norm_quote','report_section'):
        assert revised.get(key)==next(f for f in prior['findings'] if f['id']=='F-005').get(key),key
    out=Path(__file__).parent/'km_citation_completion_evidence';out.mkdir(exist_ok=True)
    wb=Workbook();build_project_sheet(wb,{'project_id':'KM','sheet_name':'KM','findings_json':after});wb.save(out/'report.xlsx');wb.close()
    wb=load_workbook(out/'report.xlsx');columns={k:i for i,(k,*_) in enumerate(PROJ_COLUMNS)}
    ids={f['id'] for f in after['findings']};rows=[r for r in wb['KM'].values if r[columns['finding_id']] in ids]
    assert len(rows)==26 and len({r[columns['finding_id']] for r in rows})==26
    row=next(r for r in rows if r[columns['finding_id']]=='F-005')
    assert revised['description'] in row[columns['description']] and revised['sheet']==row[columns['sheet']]
    wb.close()
    files={}
    for path in sorted(ROOT.rglob('*.json')):
        relative=path.relative_to(ROOT);target=out/relative;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
        files[str(relative)]=hashlib.sha256(target.read_bytes()).hexdigest()
    files['report.xlsx']=hashlib.sha256((out/'report.xlsx').read_bytes()).hexdigest()
    results=[result,json.loads((ROOT/'final_check/result.json').read_text())]
    summary={'status':'known_control_citation_completion_and_release_passed','original_rows':26,'final_rows':26,
        'other_classic_findings_unchanged':25,'original_target_snapshot_adapter':True,
        'target_visible_prose_unchanged_by_completion':True,'text_citation_additions':len(result['json_data']['additions']),
        'final_evidence_items':len(revised['evidence']),'final_pages':revised['page'],'final_sheet':revised['sheet'],
        'checks':{c['check_id']:c['status'] for c in receipt['review']['checks']},
        'negative_control':'km_final_check_pair_evidence/version_a/receipt.json',
        'same_source_pair':True,'model':'codex/gpt-6-astra','effort':'high','new_calls':2,
        'input_tokens':sum(r['input_tokens'] for r in results),'output_tokens':sum(r['output_tokens'] for r in results),
        'summed_model_duration_ms':sum(r['duration_ms'] for r in results),'monetary_cost':'unknown; subscription CLI reports 0, not evidence of free service',
        'known_development_control':True,'full_project_quality_acceptance':False,'production_changed':False,
        'unresolved':['Frozen evaluator profile differs from model technical/economic routing; this release pair does not resolve engineer routing.',
            'Known target snapshot adapter is not an autonomous whole-project run or validation of the other 25 findings.',
            'Citation completion is an experimental control helper, not yet connected to the general automatic release phase.',
            'Large evidence payload and additional two calls must be included in cost/time and engineer burden comparison.'],
        'files':files}
    (out/'assessment.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in summary.items() if k!='files'},ensure_ascii=False))

if __name__=='__main__':main()
