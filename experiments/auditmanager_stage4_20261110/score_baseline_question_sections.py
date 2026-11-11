"""Measure missing question sections without changing frozen coverage ledgers."""
import json
from pathlib import Path
from collections import Counter
from openpyxl import load_workbook
from .score_atomic_review import sha
from .verify_output_review_history import encode

HERE = Path(__file__).resolve().parent


def build():
    summary_path=HERE/'baseline_three_repeat_coverage_summary.json'
    observation_path=HERE/'baseline_report_section_observation.json'
    evaluator_path=HERE/'evaluator_release_errata_v2.json'
    summary=json.loads(summary_path.read_text());observation=json.loads(observation_path.read_text());evaluator=json.loads(evaluator_path.read_text())
    assert sha(evaluator_path)==summary['evaluator_sha256']
    assert sha(observation['workbook_path'])==observation['workbook_sha256']
    assert sha(observation['renderer_path'])==observation['renderer_sha256']
    targets={t['target_id']:t for t in evaluator['targets']}
    holds={tid for hold in evaluator['holds'] if 'report_section_accuracy' in hold['held_dimensions'] for tid in hold['target_ids']}
    rendered={(r['repeat'],r['discipline']):r for r in observation['runs']}
    workbook=load_workbook(observation['workbook_path'],read_only=True,data_only=False)
    rows=[];counts={}
    for repeat in summary['repeats']:
        total=Counter()
        for project in repeat['projects']:
            src=project['source'];assert sha(src['ledger_path'])==src['ledger_sha256'];assert sha(src['final_path'])==src['final_sha256']
            ledger=json.loads(Path(src['ledger_path']).read_text());report=json.loads(Path(src['final_path']).read_text());render=rendered[(repeat['repeat'],project['discipline'])]
            assert render['findings_sha256']==src['final_sha256']
            assert not render['rendered_questions_header_rows']
            assert all(f.get('report_section') is None for f in report['findings'])
            sheet=workbook[render['worksheet']]
            assert not any(cell.value=='Вопросы и уточнения' for row in sheet for cell in row)
            for target in ledger['targets']:
                if targets[target['target_id']]['route']!='question':continue
                state=('held' if target['target_id'] in holds else 'not_fully_matched_not_section_scored' if target['coverage']!='full' else 'incorrect_missing_questions_section')
                total[state]+=1
                rows.append({'repeat':repeat['repeat'],'discipline':project['discipline'],'target_id':target['target_id'],'coverage':target['coverage'],'finding_ids':target['finding_ids'],'section_assessment':state,'worksheet':render['worksheet'],'ledger_path':src['ledger_path'],'ledger_sha256':src['ledger_sha256']})
        counts[str(repeat['repeat'])]=dict(total)
    workbook.close()
    return {'status':'baseline_question_section_assessment_only','source_summary_sha256':sha(summary_path),'observation_path':str(observation_path),'observation_sha256':sha(observation_path),'evaluator_sha256':sha(evaluator_path),'counts_by_repeat':counts,'targets':rows,'production_acceptance':False,'limits':['Only fully covered question targets without section holds are scored.','Counts are target occurrences across repeated audits, not independent defects or unique output rows.','No inferred technical classification from severity; technical routing is not scored here.','Coverage ledgers remain immutable; missing question section does not reduce detection coverage.','This is the frozen baseline renderer; candidate output routing requires separate verification.']}


if __name__=='__main__':
    result=build()
    with (HERE/'baseline_question_section_score.json').open('xb') as stream:stream.write(encode(result))
    print(json.dumps(result['counts_by_repeat'],ensure_ascii=False))
