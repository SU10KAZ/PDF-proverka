"""Final research deliverable checks and deterministic report/selection replay."""
import argparse
from pathlib import Path

from experiments.text_comparison_v1.common import read,write,file_hash
from .run import verify as verify_candidate
from .audit import select
from .finish import finish

REQUIRED=('GOAL.md','APPROACHES_COMPARED.md','TEXT_ALIGNMENT_ARCHITECTURE.md','LOCAL_UNIT_CONTRACT.md',
          'CANDIDATE_RETRIEVAL_REPORT.md','AI_ALIGNMENT_CONTRACT.md','ALIGNMENT_SCORECARD.md','TEXT_CHANGE_SCORECARD.md',
          'PROJECT_TEXT_ALIGNMENT_REPORT.md','PROJECT_TEXT_CHANGES.json','SECTION_GATED_VS_CONTENT_FIRST.md','QUALITY_AUDIT.md',
          'MODEL_CONTEXT_BUDGET.md','PERFORMANCE_REPORT.md','REPLAY_AUDIT.json','FRESH_TEXT_ALIGNMENT_HOLDOUT.json',
          'CANDIDATE_MANIFEST.json','NEXT_ACTION.md','CHECKPOINT.md')


def verify(root):
    root=Path(root);candidate=verify_candidate(root)
    for name in REQUIRED:
        p=root/'reports'/name
        if not p.is_file() or p.stat().st_size==0:raise ValueError('Missing deliverable '+name)
    source=read(root/'DEV_TEXT_ONLY.json')
    if file_hash(source['source'])!=candidate['dev_truth_sha256']:raise ValueError('Human truth changed')
    metrics=read(root/'reports/FINAL_METRICS.json')
    p=metrics['project']
    if p['old_units']+p['new_units']!=p['aligned_old_units']+p['aligned_new_units']+p['review_units']:raise ValueError('Unit accounting')
    if p['aligned_relations']!=sum(p['shapes'].get(k,0) for k in ('ONE_TO_ONE','ONE_TO_N','N_TO_ONE')):raise ValueError('Shape accounting')
    holdout=read(root/'reports/FRESH_TEXT_ALIGNMENT_HOLDOUT.json')
    if holdout['N']!=len(holdout['cases']) or holdout['annotations']!=0 or holdout['predictions_included']:raise ValueError('Blind packet invariant')
    if any(any(c[k] is not None for k in ('alignment_answer','change_answer','human_notes')) for c in holdout['cases']):raise ValueError('Packet annotated')
    if read(root/'fresh_holdout/selection.json')['selected_vs_excluded_page_overlap']!=0:raise ValueError('Source overlap')
    if not read(root/'reports/REPLAY_AUDIT.json')['pass'] or not read(root/'reports/HOLDOUT_REPLAY.json')['pass']:raise ValueError('Replay failed')
    if metrics['absolute_no_table_content_claim_valid'] or metrics['quality_audit']['engineering']['false']!=1:raise ValueError('Known safety failure hidden')
    # Reproduce generated report bytes and deterministic sample membership.
    watched=list((root/'reports').glob('*'))+[root/'quality_audit/selection.json']
    hashes={str(p.relative_to(root)):file_hash(p) for p in watched if p.is_file() and p.name not in {'FINAL_VALIDATION.json','DELIVERABLE_MANIFEST.json'}}
    select(root);finish(root)
    changes=[p for p,h in hashes.items() if file_hash(root/p)!=h]
    if changes:raise ValueError('Report or selection replay differs: '+repr(changes))
    result={'pass':True,'required_deliverables':len(REQUIRED),'report_and_selection_files_replayed_or_preserved':len(hashes),
            'changed_files':changes,'candidate_unchanged':True,'human_truth_unchanged':True,'holdout_unannotated':True,
            'unit_and_shape_accounting_pass':True,'known_candidate_failures_preserved':True,
            'note':'Validation of artifacts/replay, NOT acceptance of engineering quality. Agent audit decisions are cached evidence artifacts, not re-queried or represented as human labels.'}
    write(root/'reports/FINAL_VALIDATION.json',result)
    print(result,flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);verify(p.parse_args().root)
