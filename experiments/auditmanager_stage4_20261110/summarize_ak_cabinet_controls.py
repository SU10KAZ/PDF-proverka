"""Separate extraction, operation candidates, source review and final output."""
import hashlib
import json
from pathlib import Path

OUT=Path(__file__).parent
INVENTORY=Path('/tmp/auditmanager_ak_cabinet_inventory_control_20261110/AK')
OPERATIONS=Path('/tmp/auditmanager_ak_cabinet_operations_20261110')

def reference(path):
    return {'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}

def summarize():
    rows=[]
    manifest=json.loads((INVENTORY/'manifest.json').read_text())
    for i,packet in enumerate(manifest['packets']):
        row={'packet':i,'blocks':packet['block_ids'],'extraction_result':'not_saved',
             'operation_result':'not_saved','candidate_count':0,'final_output_verified':False,
             'baseline_address':'F-054' if i==0 else 'F-055' if i==2 else None}
        path=INVENTORY/'execution'/packet['stage']/'result.json'
        if path.exists():
            result=json.loads(path.read_text())
            row.update(extraction_result='error' if result.get('is_error') else 'received',extraction_evidence=reference(path))
            if isinstance(result.get('json_data'),dict):
                row['raw_object_records']=len(result['json_data'].get('object_registry',[]))
        for revision in ('graphic_context_v4','page_context_v3','local_claim_v2'):
            path=OPERATIONS/revision/f'packet_{i:03d}'/'validated.json'
            if not path.exists():continue
            data=json.loads(path.read_text())
            row.update(operation_result='received',operation_revision=revision,operation_evidence=reference(path),
                candidate_count=len(data['findings']),pending_candidates=len(data['operation_pending']),
                operation_statuses=[{'operation':r['operation'],'status':r['status']} for r in data['object_operation_results']],
                source_candidate_ids=[sid for f in data['findings'] for sid in f.get('source_finding_ids',[])])
            break
        review=OUT/f'AK_cabinet_{i:03d}_source_review.json'
        if review.exists():row['local_source_review']=reference(review)
        rows.append(row)
    return {'scope':'Original AK14 five-cabinet known-source control, not blind discovery or full project audit.',
        'rows':rows,'extraction_responses':sum(r['extraction_result']=='received' for r in rows),
        'operation_responses':sum(r['operation_result']=='received' for r in rows),
        'candidate_rows':sum(r['candidate_count'] for r in rows),
        'full_control_complete':False,'production_accepted':False,
        'remaining':['Confirm broad acceptance separately from the completed bounded cabinet control.',
            'Second-cabinet named label controls corrected; other raw extraction records remain unaccepted.',
            'Check same-page notes for each affected operation before final acceptance.',
            'Reconcile systemic topic with F054/F055 without losing additional cabinet addresses or duplicating findings.',
            'Verify final output; candidate count is not unique incremental defect count.']}

if __name__=='__main__':
    report=summarize();(OUT/'AK_five_cabinet_control_status.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k!='rows'},ensure_ascii=False))
