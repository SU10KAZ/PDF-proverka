"""Known-control downstream preservation replay; no independent discovery claim."""
import copy
import hashlib
import json
import tempfile
from pathlib import Path

from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted


def main():
    here = Path(__file__).resolve().parent
    control_path = here / 'VK20_sensor_identity_control.json'
    control = json.loads(control_path.read_text())
    version = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/VK/versions/v002')
    source = version / '03_analysis/runs/control'
    md = (version / '02_work/document.md').read_text()
    for evidence in control['verified_TEXT']:
        assert evidence['quote'] in md
    refs = [e['block_id'] for e in control['verified_TEXT']]
    question = {**control['retained_question'], 'id': 'VK-CONTROL-Q20',
        'severity': 'ПРОВЕРИТЬ ПО СМЕЖНЫМ', 'category': 'sewerage',
        'description': control['retained_question']['problem'],
        'solution': control['retained_question']['recommendation'],
        'source_block_ids': refs, 'related_block_ids': refs,
        'evidence': [{'type': 'text', **e} for e in control['verified_TEXT']],
        'source_finding_ids': [], 'norm': '',
        'provenance': {'origin': 'manual_known_control_for_downstream_replay'}}
    original = json.loads((source / '03_findings.json').read_text())
    candidate = combine_findings_with_targeted(copy.deepcopy(original), [('known_control', {'findings': [question]})])
    outcomes = []
    for label, data in [('baseline', original), ('candidate', candidate)]:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            for name in ('01_blocks_analysis.json', 'document_graph.json'):
                (output / name).write_bytes((source / name).read_bytes())
            (output / '03_findings.json').write_text(json.dumps(data, ensure_ascii=False))
            stats = _run_deterministic_phase(output, 'VK-question-preservation')
            assert not stats['critic_error'] and not stats['corrector_error']
            findings = json.loads((output / '03_findings.json').read_text())['findings']
            outcomes.append({'label': label, 'stats': stats, 'findings': findings})
    baseline, after = outcomes
    assert after['findings'][:-1] == baseline['findings']
    retained = after['findings'][-1]
    assert retained['report_section'] == 'questions'
    assert retained['problem'] == question['problem']
    assert retained['solution'] == question['solution']
    wb = Workbook()
    build_project_sheet(wb, {'project_id': 'VK-control', 'sheet_name': 'VK',
                            'findings_json': {'findings': after['findings']}})
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'report.xlsx'
        wb.save(path)
        saved = load_workbook(path)
        rows = list(saved['VK'].values)
        col = next(i for i, (key, *_) in enumerate(PROJ_COLUMNS) if key == 'finding_id')
        header = next(i for i, row in enumerate(rows) if 'Вопросы и уточнения' in row)
        exported = [i for i, row in enumerate(rows) if row[col] == retained['id']]
        assert len(exported) == 1 and exported[0] > header
        saved.close()
    result = {'scope': 'Known manually supplied VK20 question through merge, real deterministic verifier/corrector and saved Excel',
        'original_count': len(original['findings']), 'candidate_count': len(after['findings']),
        'baseline_outputs_preserved': True, 'question_text_and_route_preserved': True,
        'question_exported_under_question_heading': True, 'retained_question': retained,
        'verifier_stats': [o['stats'] for o in outcomes],
        'control_sha256': hashlib.sha256(control_path.read_bytes()).hexdigest(),
        'new_model_calls': 0, 'independent_discovery': False,
        'boundary': 'Does not test model finding generation, semantic verification, absence-model pass, or complete six-project acceptance.'}
    (here / 'VK20_output_preservation_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k: result[k] for k in ('original_count','candidate_count','baseline_outputs_preserved','question_exported_under_question_heading')}))


if __name__ == '__main__':
    main()
