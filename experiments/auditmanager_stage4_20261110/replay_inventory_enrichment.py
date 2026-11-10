"""Locally append sealed source-only inventory to saved classic KM analysis."""
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.text_analysis.inventory_enrichment import merge_inventory_analysis
from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage


def replay():
    root=Path('/tmp/auditmanager_whole_project_inventory_20261110/KM')
    manifest=json.loads((root/'manifest.json').read_text())
    base_path=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/KM/versions/v001/03_analysis/runs/control/02_text_analysis.json')
    base=json.loads(base_path.read_text());packets=[];hashes={}
    for row in manifest['packets']:
        p=root/'execution'/row['stage']/'result.json'
        if not p.exists():break
        raw=p.read_bytes();result=json.loads(raw)
        if result['is_error']:raise ValueError('Failed inventory result')
        hashes[row['stage']]=hashlib.sha256(raw).hexdigest()
        packets.append({'packet_id':row['stage'],'block_ids':row['block_ids'],'payload':result['json_data']})
    known={bid for row in manifest['packets'] for bid in row['block_ids']}
    combined=merge_inventory_analysis(base,packets,known)
    n=len(base['text_findings'])
    assert combined['text_findings'][:n]==base['text_findings']
    for key in base.keys()-{'text_findings','object_registry','object_inventory_coverage','object_operation_results'}:
        assert combined[key]==base[key]
    recovered=[f for f in combined['text_findings'][n:] if f['inventory_origin']['packet_id']=='object_inventory_006']
    assert len(recovered)==1
    final=[{'source_finding_ids':[recovered[0]['id']]}]
    text_candidate_lineage(final,combined,known)
    assert final[0]['source_candidate_lineage'][0]['source_claim']==recovered[0]['finding']
    target=Path('/tmp/auditmanager_km_inventory_enrichment_replay_20261110.json')
    target.write_text(json.dumps(combined,ensure_ascii=False,indent=2))
    summary={'completed_inventory_packets':len(packets),'inventory_result_sha256':hashes,
        'baseline_sha256':hashlib.sha256(base_path.read_bytes()).hexdigest(),
        'classic_text_candidates_unchanged':n,'added_candidates':len(combined['text_findings'])-n,
        'registered_packet_local_objects':len(combined['object_registry']),
        'ua009_source_lineage_preserved':True,'output_path':str(target),
        'output_sha256':hashlib.sha256(target.read_bytes()).hexdigest(),
        'semantic_verified':False,'full_candidate_audit_executed':False,
        'scope':'Saved real source inventory merged into saved classic text analysis; no model calls or final report generation.'}
    Path(__file__).with_name('KM_inventory_enrichment_replay.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in summary.items() if k!='inventory_result_sha256'},ensure_ascii=False))


if __name__=='__main__':replay()
