"""Known-source EOM cable composition control; generic inventory, no expected answers sent."""
import argparse
import asyncio
import json
import re
import hashlib
from pathlib import Path
from backend.app.pipeline.stages.prepare.object_inventory_passes import build_object_inventory_passes
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from experiments.auditmanager_stage4_20261110 import run_whole_project_inventory as runner

ROOT=Path('/tmp/auditmanager_eom_cable_inventory_control_20261110')
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002')
# Source selection is known-case evaluation, not blind project discovery.
BLOCKS={'blk_8fac348a820c41aeb2d941339987da14','blk_513e35c4f8a7432986e9a498d4573996','blk_67c8cc82d7e24f738797d9b75ce14e97','blk_1da24da5ad594599aa59defe9fde265b'}

def prepare():
    ROOT.mkdir(parents=True,exist_ok=True)
    md=(BASE/'02_work/document.md').read_text()
    headers=list(re.finditer(r'^### BLOCK[^\n]*?:\s*(blk_\w+)[^\n]*$',md,re.M))
    pages=list(re.finditer(r'^## Page[^\n]*$',md,re.M))
    chunks=[]
    for i,h in enumerate(headers):
        if h[1] not in BLOCKS:continue
        end=headers[i+1].start() if i+1<len(headers) else len(md)
        end=min([end]+[p.start() for p in pages if h.end()<p.start()<end])
        page=next(p for p in reversed(pages) if p.start()<h.start())
        chunks.append(page.group()+'\n'+md[h.start():end])
    assert len(chunks)==4
    subset=ROOT/'source_subset.md'; selected_md='\n'.join(chunks)
    if subset.exists():assert subset.read_text()==selected_md
    else:subset.write_text(selected_md)
    requests,manifest=build_object_inventory_passes(subset,BASE/'03_analysis/runs/control/blocks_stage02_100',max_chars=60000,max_images=2)
    manifest['full_source_md_sha256']=hashlib.sha256(md.encode()).hexdigest()
    manifest['source_selection']='Two complete original diagram GRAPHIC blocks and two unchanged authoritative specification TEXT blocks. No selected line identifiers, expected mismatches or historical findings.'
    selected=[(request,row) for request,row in zip(requests,manifest['packets'],strict=True) if BLOCKS.intersection(row['block_ids'])]
    assert len(selected)==1
    assert BLOCKS.issubset({bid for _,row in selected for bid in row['block_ids']})
    root=ROOT/'EOM';root.mkdir(parents=True,exist_ok=True)
    digests=[]
    for request,row in selected:
        packet={'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
        p=root/(request.stage+'.json')
        if p.exists():assert json.loads(p.read_text())==packet
        else:p.write_text(json.dumps(packet,ensure_ascii=False,indent=2))
        digests.append(evidence_digest(packet))
    manifest.update(packets=[row for _,row in selected],request_digests=digests,requests=len(selected),
        blocks=len({bid for _,row in selected for bid in row['block_ids']}),known_source_selection=True,
        whole_project_control=False,independent_discovery=False,model='codex/gpt-6-astra',reasoning_effort='high',automatic_retries=0)
    p=root/'manifest.json'
    if p.exists():assert json.loads(p.read_text())==manifest
    else:p.write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    Path(__file__).with_name('EOM_cable_inventory_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    return manifest

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    manifest=prepare();runner.ROOT=ROOT
    print('PREPARED',len(manifest['packets']),'packets',flush=True)
    if args.execute:asyncio.run(runner.execute('EOM',evidence_digest(manifest)))
