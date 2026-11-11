"""New input package revision restores verified render-policy metadata only."""
import json
from pathlib import Path
import shutil
from .check_six_project_indexes import check
from .materialize_trial_inputs import sha

HERE=Path(__file__).parent
DEST=Path('/tmp/auditmanager_six_candidate_inputs_policy_v2_20261111')

def main():
    preflight=json.loads((HERE/'six_project_comparison_preflight.json').read_text())
    layout=json.loads((HERE/'comparative_trial_layout_draft.json').read_text())
    DEST.mkdir(exist_ok=False);rows=[]
    for project in preflight['projects']:
        disc=project['discipline'];old=layout['sources_observed_now_not_yet_trial_frozen'][disc];source=Path(old['root'])
        for name,digest in old['files'].items():assert sha(source/name)==digest
        checked=check(project);assert checked['status']=='passed'
        target=DEST/disc;shutil.copytree(source,target)
        name='03_analysis/runs/candidate/blocks_stage02_100/index.json';p=target/name
        before=json.loads(p.read_text());after=checked['candidate_index']
        assert before['blocks']==after['blocks'] and before['total_blocks']==after['total_blocks']
        p.write_text(json.dumps(after,ensure_ascii=False,indent=2)+'\n')
        changed=[n for n,h in old['files'].items() if sha(target/n)!=h];assert changed==[name]
        rows.append({'discipline':disc,'changed_files':changed,'index_before':old['files'][name],'index_after':sha(p),
                     'policy':{k:after[k] for k in ['profile','dpi','min_long_side','compact','skip_small']},
                     'all_text_pdf_and_image_bytes_unchanged':True})
    result={'status':'new_source_package_revision_policy_restored','root':str(DEST),'projects':rows,
            'prior_failed_run':'ak_r1_classic','prior_model_requests':0,'prior_run_artifacts_preserved':True,'model_calls':0}
    (HERE/'crop_policy_packaging_repair.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'projects':len(rows),'root':str(DEST),'only_index_policy_changed':True}))

if __name__=='__main__':main()
