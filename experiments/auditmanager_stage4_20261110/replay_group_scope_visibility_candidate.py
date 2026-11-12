"""Build reviewable Excel previews from sealed groups; no inference or release."""
from copy import deepcopy
import argparse
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet
from .group_scope_visibility_candidate import expose_group_scope_notes
from .run_itp_pipe_operations import immutable

BASE = Path(__file__).resolve().parent
ROOT = Path('/tmp/auditmanager_group_scope_visibility_candidate_20261112')


def main(runtime=False):
    root = ROOT.with_name(ROOT.name + "_runtime") if runtime else ROOT
    root.mkdir(exist_ok=True)
    cases = []
    for name, discipline in [('AK_00','AK'),('EOM_01','EOM')]:
        source = BASE/'grouping_scope_pair_evidence'/name/'output_preview.json'
        original = json.loads(source.read_text())
        document = deepcopy(original)
        groups = [f for f in document['findings'] if f.get('grouping_original_findings')]
        assert len(groups) == 1
        group = groups[0]
        if runtime:
            from backend.app.pipeline.stages.findings_merge.reviewed_grouping import apply_reviewed_group
            provenance = group['grouping_provenance']
            source_root = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs')
            matches = [p for p in (source_root/discipline/'versions').glob('*/02_work/document.md')
                       if hashlib.sha256(p.read_bytes()).hexdigest() == provenance['decision']['source_md_sha256']]
            md_path, = matches
            reconstructed = deepcopy(original)
            reconstructed['findings'] = [member for f in original['findings']
                for member in (group['grouping_original_findings'] if f['id'] == group['id'] else [f])]
            applied, receipt = apply_reviewed_group(reconstructed, provenance['proposal'], provenance['decision'], md_path.read_text())
            assert receipt['status'] == 'applied'
            revised = next(f for f in applied['findings'] if f['id'] == group['id'])
            assert revised == expose_group_scope_notes(group)
        else:
            revised = expose_group_scope_notes(group)
        document['findings'] = [revised if f['id'] == group['id'] else f for f in document['findings']]
        assert [f for f in document['findings'] if f['id'] != group['id']] == [f for f in original['findings'] if f['id'] != group['id']]
        issues = group['grouping_provenance']['decision']['source_review']['inherited_member_issues']
        out = root/name
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
    result = {'status':'runtime_visibility_replay_passed' if runtime else 'offline_visibility_replay_passed','cases':cases,'new_model_calls':0,
        'runtime_integrated':runtime,'automatic_grouping_enabled':False,'production_acceptance':False,
        'negative_control_confirmed':False,'stage4_control_closed':False,
        'boundary':'Presentation preserves recorded source-review limits. It does not establish member identity, validate the EOM common heading, resolve 3/З or ЭОМ/ЗОМ, or prove grouping reduces engineer workload.',
        'next_action':'Assess the EOM common heading independently of visible caveats; do not enable grouping until a genuine negative control establishes rejection of unsupported common scope.'}
    (BASE/('group_scope_visibility_runtime_replay.json' if runtime else 'group_scope_visibility_candidate_replay.json')).write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'status':result['status'],'cases':[{k:v for k,v in c.items() if k in ['case','members_preserved','scope_notes_visible_in_excel']} for c in cases]},ensure_ascii=False))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime', action='store_true')
    main(parser.parse_args().runtime)
