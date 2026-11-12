"""Build reviewable Excel previews from sealed groups; no inference or release."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet
from .group_scope_visibility_candidate import expose_group_scope_notes
from .run_itp_pipe_operations import immutable

BASE = Path(__file__).resolve().parent
ROOT = Path('/tmp/auditmanager_group_scope_visibility_candidate_20261112')


def main():
    ROOT.mkdir(exist_ok=True)
    cases = []
    for name, discipline in [('AK_00','AK'),('EOM_01','EOM')]:
        source = BASE/'grouping_scope_pair_evidence'/name/'output_preview.json'
        original = json.loads(source.read_text())
        document = deepcopy(original)
        groups = [f for f in document['findings'] if f.get('grouping_original_findings')]
        assert len(groups) == 1
        group = groups[0]
        revised = expose_group_scope_notes(group)
        document['findings'] = [revised if f['id'] == group['id'] else f for f in document['findings']]
        assert [f for f in document['findings'] if f['id'] != group['id']] == [f for f in original['findings'] if f['id'] != group['id']]
        issues = group['grouping_provenance']['decision']['source_review']['inherited_member_issues']
        out = ROOT/name
        out.mkdir(exist_ok=True)
        immutable(out/'candidate_findings.json',document)
        wb = Workbook()
        build_project_sheet(wb, {'project_id': discipline,'sheet_name': discipline,'findings_json':document})
        excel = out/'candidate_report.xlsx'
        wb.save(excel);wb.close()
        wb = load_workbook(excel,read_only=True)
        rows = list(wb[discipline].values)
        row, = [row for row in rows if group['id'] in row]
        for issue in issues:
            assert any(isinstance(cell,str) and issue['issue'] in cell for cell in row)
        for member in group['grouping_original_findings']:
            for key in ('problem','description','solution'):
                if member.get(key):
                    assert any(isinstance(cell,str) and member[key] in cell for cell in row)
        wb.close()
        cases.append({'case':name,'members_preserved':len(group['grouping_original_findings']),
            'scope_notes_visible_in_excel':len(issues),
            'source':str(source),'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
            'output':str(excel),'output_sha256':hashlib.sha256(excel.read_bytes()).hexdigest()})
    result = {'status':'offline_visibility_replay_passed','cases':cases,'new_model_calls':0,
        'runtime_integrated':False,'automatic_grouping_enabled':False,'production_acceptance':False,
        'negative_control_confirmed':False,'stage4_control_closed':False,
        'boundary':'Presentation preserves recorded source-review limits. It does not establish member identity, validate the EOM common heading, resolve 3/З or ЭОМ/ЗОМ, or prove grouping reduces engineer workload.',
        'next_action':'Assess the EOM common heading independently of visible caveats; do not enable grouping until a genuine negative control establishes rejection of unsupported common scope.'}
    (BASE/'group_scope_visibility_candidate_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'status':result['status'],'cases':[{k:v for k,v in c.items() if k in ['case','members_preserved','scope_notes_visible_in_excel']} for c in cases]},ensure_ascii=False))

if __name__ == '__main__': main()
