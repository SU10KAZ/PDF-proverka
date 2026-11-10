"""Persist completed source-review evidence without inventing process liveness."""
import hashlib
import json
from pathlib import Path
HERE=Path(__file__).parent

def collect():
    reports=[]
    for variant in ('source','detail_source'):
        root=Path(f'/tmp/auditmanager_eom_cable_{variant}_workflow_20261111')
        dest=HERE/'eom_source_review_evidence'/variant;dest.mkdir(parents=True,exist_ok=True)
        for name in ('manifest.json','findings.json','queue.json','detail_preparation.json'):
            if (root/name).exists():(dest/name).write_bytes((root/name).read_bytes())
        for fid in ('F-050','F-051'):
            source=root/fid;target=dest/fid;target.mkdir(exist_ok=True)
            row={'variant':variant,'finding_id':fid,'status':'result_pending','completed_calls':[],
                 'semantic_acceptance':False}
            for directory in sorted(source.glob('*_*')):
                if not directory.is_dir() or not (directory/'result.json').exists():continue
                output=target/directory.name;output.mkdir(exist_ok=True)
                for name in ('request.json','result.json','attempt.json'):
                    if (directory/name).exists():(output/name).write_bytes((directory/name).read_bytes())
                raw=(directory/'result.json').read_bytes();result=json.loads(raw)
                row['completed_calls'].append({'phase':directory.name,'is_error':result.get('is_error'),
                    'duration_ms':result.get('duration_ms'),'input_tokens':result.get('input_tokens'),
                    'output_tokens':result.get('output_tokens'),'result_sha256':hashlib.sha256(raw).hexdigest()})
            for p in source.glob('*.json'):
                if p.name=='binding.json' or p.name=='candidate.json' or p.name.endswith(('_validation.json','_followup.json')):
                    (target/p.name).write_bytes(p.read_bytes())
            if (source/'candidate.json').exists():
                candidate=json.loads((source/'candidate.json').read_text())
                row.update(status=candidate['status'],action=candidate['entry']['decision'].get('action'),
                    admission_receipts=candidate['admission_receipts'],candidate=str(target/'candidate.json'))
            failure=root/(fid+'_failure.json')
            if failure.exists():
                (target/'failure.json').write_bytes(failure.read_bytes());row.update(status='not_completed',failure=json.loads(failure.read_text()))
            row['completed_request_minutes_sum']=round(sum(c.get('duration_ms') or 0 for c in row['completed_calls'])/60000,3)
            reports.append(row)
    report={'scope':'Known EOM cable source-review comparison; original and detailed-graphic variants.','reports':reports,
            'boundary':'candidate_ready/applied denote structural workflow admission only. Original reviewer scope and every substantive conclusion still need independent checking. Pending result is not a liveness assertion.','full_acceptance_proven':False}
    (HERE/'EOM_source_workflow_progress.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps([{'variant':r['variant'],'finding_id':r['finding_id'],'status':r['status'],'calls':len(r['completed_calls']),'action':r.get('action')} for r in reports],ensure_ascii=False))
if __name__=='__main__':collect()
