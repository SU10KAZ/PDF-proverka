from openpyxl import Workbook
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS


def test_explicit_questions_have_separate_section_and_preserve_ids():
    findings=[{'id':'F-001','problem':'Need related detail','report_section':'questions'},
              {'id':'F-002','problem':'Confirmed conflict','report_section':'technical_findings'},
              {'id':'F-003','problem':'Legacy finding'}]
    wb=Workbook()
    build_project_sheet(wb,{'project_id':'test','sheet_name':'test','findings_json':{'findings':findings}})
    ws=wb['test']
    rows=list(ws.values)
    assert sum('Вопросы и уточнения' in row for row in rows)==1
    assert sum('Замечания' in row for row in rows)==1
    id_col=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id')
    ids=[row[id_col] for row in rows if row[id_col] in {'F-001','F-002','F-003'}]
    assert ids==['F-002','F-003','F-001']
    assert findings[0]['id']=='F-001'  # no input mutation


def test_legacy_report_does_not_gain_empty_question_section():
    wb=Workbook()
    build_project_sheet(wb,{'project_id':'test','sheet_name':'test','findings_json':{'findings':[{'id':'F-1','problem':'legacy'}]}})
    assert not any('Вопросы и уточнения' in row for row in wb['test'].values)
