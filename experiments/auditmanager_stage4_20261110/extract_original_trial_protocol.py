"""Preserve original proposed acceptance criteria without executing report code."""
import ast
import hashlib
import json
from pathlib import Path

SOURCE=Path('experiments/auditmanager_v4_transfer_20261105/build_review.py')
OUT=Path(__file__).parent/'original_trial_protocol.json'


def main():
    raw=SOURCE.read_bytes();tree=ast.parse(raw)
    extracted={}
    for node in tree.body:
        if isinstance(node,ast.Assign):
            for target in node.targets:
                if isinstance(target,ast.Name) and target.id in ('trial','metrics'):
                    extracted[target.id]=ast.literal_eval(node.value)
    assert set(extracted)=={'trial','metrics'}
    report={'source':str(SOURCE),'source_sha256':hashlib.sha256(raw).hexdigest(),
        'status':'original_proposal_preserved_not_comparison_freeze',
        'trial_steps':[{'step':row[0],'requirement':row[1]} for row in extracted['trial']],
        'metrics':[{'name':row[0],'definition':row[1],'proposed_acceptance':row[2]} for row in extracted['metrics']],
        'scope_reminders':['Six historical projects are development regressions, not new blind projects.',
            'Original proposal specifies three repeats per arm and at least 18 new projects: six calibration and twelve reserved, distributed across all six disciplines.',
            'Budget, model topology, code and evaluator must be frozen before comparative runs; current local controls do not establish these metrics.',
            'Independent engineer judgments and measured review time cannot be replaced by model self-assessment.',
            'Documented evaluator errata remain holds until explicitly resolved; preserve their history.'],
        'comparison_completed':False}
    OUT.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'trial_steps':len(report['trial_steps']),'metrics':len(report['metrics']),'status':report['status']}))


if __name__=='__main__':main()
