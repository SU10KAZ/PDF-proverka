"""Replay saved inventory outputs locally; never invoke a provider."""
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.text_analysis.inventory_candidates import inventory_text_candidates
from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage


def replay(root):
    manifest=json.loads((root/'manifest.json').read_text())
    known={bid for row in manifest['packets'] for bid in row['block_ids']}
    packets=[]
    hashes={}
    for row in manifest['packets']:
        path=root/'execution'/row['stage']/'result.json'
        if not path.exists():
            break
        raw=path.read_bytes()
        result=json.loads(raw)
        if result['is_error']:
            raise ValueError('Failed inventory result')
        packets.append({'packet_id':row['stage'],'findings':result['json_data'].get('text_findings',[])})
        hashes[row['stage']]=hashlib.sha256(raw).hexdigest()
    adapted=inventory_text_candidates(packets,known)
    probe=[{'source_finding_ids':[row['id']]} for row in adapted['text_findings']]
    text_candidate_lineage(probe,adapted,known)
    for candidate,final in zip(adapted['text_findings'],probe,strict=True):
        receipt=final['source_candidate_lineage'][0]
        assert receipt['source_claim']==candidate['finding']
        assert receipt['status']=='requires_semantic_review'
    return {'completed_packets':len(packets),'result_sha256':hashes,
        'adapted':adapted,'lineage_probe':probe,
        'automatic_report_generation_verified':False,
        'limitation':'Local transport and lineage only; no semantic acceptance or final report generation.'}


if __name__=='__main__':
    result=replay(Path('/tmp/auditmanager_whole_project_inventory_20261110/KM'))
    path=Path(__file__).with_name('KM_inventory_candidate_transport_replay.json')
    path.write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps({'completed_packets':result['completed_packets'],
        'candidates':len(result['adapted']['text_findings']),
        'pending':len(result['adapted']['pending'])}))
