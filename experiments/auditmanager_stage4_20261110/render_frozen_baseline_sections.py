import sys,json,hashlib
from pathlib import Path
from collections import Counter
source=Path('/tmp/auditmanager_paired_comparison_sources_20261111/classic_astra_with_catalog');sys.path.insert(0,str(source))
from backend.app.pipeline.stages.report import generate_excel_report as renderer
from openpyxl import Workbook,load_workbook
h=Path('/home/coder/projects/PDF-proverka/experiments/auditmanager_stage4_20261110');summary=json.loads((h/'baseline_three_repeat_coverage_summary.json').read_text());outdir=Path('/tmp/auditmanager_comparison_evaluation_20261111/baseline_report_sections');outdir.mkdir(exist_ok=True)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
wb=Workbook();wb.remove(wb.active);rows=[]
for repeat in summary['repeats']:
 for project in repeat['projects']:
  src=project['source'];p=Path(src['final_path']);assert sha(p)==src['final_sha256'];report=json.loads(p.read_text());disc=project['discipline'];name=f'{disc}_r{repeat["repeat"]}'
  renderer.build_project_sheet(wb,{'project_id':name,'sheet_name':name,'findings_json':report,'project_info':{},'folder':str(outdir/'no_engineer_feedback')})
  ws=wb[name];headers=[c.row for col in ws.columns for c in col if c.value=='Вопросы и уточнения']
  rows.append({'discipline':disc,'repeat':repeat['repeat'],'findings_path':str(p),'findings_sha256':sha(p),'finding_count':len(report['findings']),'explicit_routes':dict(Counter(f.get('report_section','missing') for f in report['findings'])),'rendered_questions_header_rows':headers,'worksheet':name,'worksheet_rows':ws.max_row})
p=outdir/'baseline_18_reports.xlsx';assert not p.exists();wb.save(p);loaded=load_workbook(p,read_only=True);assert len(loaded.sheetnames)==18;loaded.close()
result={'status':'baseline_renderer_observed_no_trial_acceptance','renderer_path':renderer.__file__,'renderer_sha256':sha(renderer.__file__),'workbook_path':str(p),'workbook_sha256':sha(p),'runs':rows,'production_acceptance':False,'limits':['Rendered with frozen baseline code, no engineer feedback loaded and no finding content altered.','Harness originally stops at findings_verify; this is evaluator-side export after sealing.','Missing report_section does not classify a finding as technical or prove severity is correct.','Candidate renderer supports explicit questions but still needs its own sealed-output rendering test.','No coverage ledger was modified; report-section scoring remains a separate step.']}
p=h/'baseline_report_section_observation.json';assert not p.exists();p.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');print(json.dumps({'runs':len(rows),'findings':sum(r['finding_count'] for r in rows),'question_headers':sum(len(r['rendered_questions_header_rows']) for r in rows),'routes':dict(sum((Counter(r['explicit_routes']) for r in rows),Counter()))},ensure_ascii=False))
