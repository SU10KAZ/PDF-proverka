"""Export sealed baseline outputs locally; never rewrite or reclassify findings."""
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS

ROOT = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
OUT = Path('/tmp/auditmanager_baseline_r1_export_check_20261111')

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    OUT.mkdir(exist_ok=True)
    records = []
    for discipline in ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'):
        work = ROOT / (discipline.lower() + '_r1_classic') / 'classic_control'
        state = json.loads((work / 'status' / (discipline + '.json')).read_text())
        assert state['status'] == 'completed_unadjudicated'
        manifest = json.loads((work / 'input_manifest.json').read_text())[0]
        source = Path(manifest['output_dir']) / '03_findings.json'
        before = sha(source)
        assert before == state['final_sha256']
        data = json.loads(source.read_text())
        snapshot = json.dumps(data, sort_keys=True)
        workbook = Workbook()
        workbook.remove(workbook.active)
        build_project_sheet(workbook, {'project_id': discipline, 'sheet_name': discipline,
                                      'findings_json': data})
        target = OUT / (discipline + '.xlsx')
        workbook.save(target)
        workbook.close()
        assert json.dumps(data, sort_keys=True) == snapshot
        assert sha(source) == before
        workbook = load_workbook(target, read_only=True)
        rows = list(workbook[discipline].values)
        column = next(i for i, (key, *_) in enumerate(PROJ_COLUMNS) if key == 'finding_id')
        expected = [f['id'] for f in data['findings']]
        actual = [row[column] for row in rows if row[column] in set(expected)]
        assert actual == expected
        assert not any('[Ошибка чтения замечания:' in str(v) for row in rows for v in row)
        headings = [i + 1 for i, row in enumerate(rows) if 'Вопросы и уточнения' in row]
        explicit = [f['id'] for f in data['findings'] if f.get('report_section') == 'questions']
        assert not explicit and not headings
        workbook.close()
        records.append({'discipline': discipline, 'source': str(source), 'source_sha256': before,
                        'xlsx': str(target), 'xlsx_sha256': sha(target), 'rows_preserved': len(actual),
                        'ids_and_order_preserved': True, 'source_unchanged': True,
                        'question_heading_rows': headings, 'explicit_question_ids': explicit})
    exporter = Path('backend/app/pipeline/stages/report/generate_excel_report.py')
    receipt = {'status': 'verified_current_exporter_on_sealed_baseline_outputs', 'runs': records,
               'exporter': str(exporter), 'exporter_sha256': sha(exporter),
               'model_calls': 0, 'production_acceptance': False,
               'limits': ['Current exporter replay, not a historical deployed UI test.',
                          'No findings were reclassified; all six outputs lack explicit section metadata.',
                          'Checks saved workbook cells, IDs and headings; no visual layout acceptance.',
                          'Preserved rows do not prove findings are technically valid.']}
    Path(__file__).with_name('baseline_r1_question_export_check.json').write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'rows_preserved': {r['discipline']: r['rows_preserved'] for r in records},
                      'separate_question_headings': 0}, ensure_ascii=False))

if __name__ == '__main__':
    main()
