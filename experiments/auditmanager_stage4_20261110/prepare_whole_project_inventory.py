"""Prepare source-only whole-project object inventory; no model dispatch."""
import json
from pathlib import Path
from backend.app.pipeline.stages.prepare.object_inventory_passes import build_object_inventory_passes
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from experiments.auditmanager_stage4_20261110.replay_integrated_admission import BASE

ROOT=Path('/tmp/auditmanager_whole_project_inventory_20261110')

def main():
    reports=[]
    for p in json.loads((BASE/'input_manifest.json').read_text()):
        disc=p['discipline'];root=ROOT/disc;root.mkdir(parents=True,exist_ok=True)
        requests,receipt=build_object_inventory_passes(Path(p['version_dir'])/'02_work/document.md',Path(p['output_dir'])/'blocks_stage02_100',max_image_bytes=24000000)
        assert receipt['source_md_sha256']==p['source_md_sha256']
        for request in requests:
            payload={'messages':request.messages,'image_paths':[str(i) for i in request.image_paths]}
            target=root/(request.stage+'.json')
            if target.exists() and json.loads(target.read_text())!=payload:raise ValueError('Frozen packet changed')
            target.write_text(json.dumps(payload,ensure_ascii=False,indent=2))
        receipt.update(discipline=disc,requests=len(requests),request_digests=[evidence_digest(json.loads((root/(r.stage+'.json')).read_text())) for r in requests],
            prepared_dir=str(root),independent_new_discovery_measured=False,model_calls=0)
        (root/'manifest.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2));reports.append(receipt)
        print(disc,'blocks',receipt['blocks'],'requests',len(requests),'missing_graphics',len(receipt['unavailable_graphics']))
    Path(__file__).with_name('whole_project_inventory_preflight.json').write_text(json.dumps({'projects':reports,'model_calls':0,'input_coverage_not_semantic_coverage':True},ensure_ascii=False,indent=2)+'\n')

if __name__=='__main__':main()
