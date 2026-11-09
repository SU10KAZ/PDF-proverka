"""Prepare a new Astra-only queue for rejected revisions; no inference here."""
import argparse
from pathlib import Path
from run_source_review import prepare, read, write, sha, HERE

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--work',type=Path,required=True)
    args=parser.parse_args()
    parent=Path('/home/coder/auditmanager/corpus-audits/20261109_stage1_source_review')
    failures_path=HERE/'adjudication/validation_failures.json'
    failures=read(failures_path);selected={x['candidate_id'] for x in failures}
    original=read(parent/'review/jobs.json');jobs=[]
    for j in original:
        candidates=[c for c in j['candidates'] if c['id'] in selected]
        if candidates:jobs.append({**j,'candidates':candidates})
    assert len(jobs)==22 and sum(len(j['candidates']) for j in jobs)==25
    prepare(args.work,Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation'))
    # Same complete sealed candidate bundles keep their original source hashes.
    for j in jobs:assert sha(args.work/'runs'/j['origin_run']/'status.json')==j['origin_sha256']
    write(args.work/'review/jobs.json',jobs)
    protocol=read(args.work/'protocol.json')
    protocol.update(jobs=len(jobs),historical_candidates=sum(len(j['candidates']) for j in jobs if j['phase']=='historical_control'),
                    classic_candidates=sum(len(j['candidates']) for j in jobs if j['phase']=='classic_usefulness'),
                    jobs_sha256=sha(args.work/'review/jobs.json'),
                    repair_of=str(parent),selection_reason='latest revision rejected but prior provisional result satisfied old completion gate',
                    selection_manifest={'path':str(failures_path),'sha256':sha(failures_path)},
                    prior_assessments_sent=False,engineer_answers_sent=False)
    write(args.work/'protocol.json',protocol)
    print('Repair prepared: 22 jobs / 25 candidates. No model calls.')

if __name__=='__main__':main()
