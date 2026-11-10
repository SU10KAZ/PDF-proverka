"""Known-source GVS branch and equipment identity control; original MD and source graphics only, no benchmark answers."""
import argparse
import asyncio
import hashlib
import json
import re
from pathlib import Path
from backend.app.pipeline.stages.prepare.object_inventory_passes import build_object_inventory_passes
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from experiments.auditmanager_stage4_20261110 import run_whole_project_inventory as runner

ROOT = Path('/tmp/auditmanager_itp_gvs_source_inventory_20261111')
BASE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/ITP/versions/v003')
BLOCKS = {'blk_fa4cca816905454ab134c787b1d1862a', 'blk_fed30df45d124919a8b7cc13253733b1', 'blk_7527066e645e47bb917a9fc17fb82ccc', 'blk_d4b7a506870a4c3dbe5a3d19eb26dfdc'}

def immutable(path, data):
    if path.exists(): assert json.loads(path.read_text()) == data
    else: path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')

def prepare():
    ROOT.mkdir(exist_ok=True)
    md = (BASE/'02_work/document.md').read_text()
    headers = list(re.finditer(r'^### BLOCK[^\n]*?:\s*(blk_\w+)[^\n]*$', md, re.M))
    pages = list(re.finditer(r'^## Page[^\n]*$', md, re.M))
    chunks = []
    for i,h in enumerate(headers):
        if h[1] not in BLOCKS: continue
        end = headers[i+1].start() if i+1<len(headers) else len(md)
        end = min([end]+[p.start() for p in pages if h.end()<p.start()<end])
        page = next(p for p in reversed(pages) if p.start()<h.start())
        chunks.append(page.group()+'\n'+md[h.start():end])
    assert len(chunks)==len(BLOCKS)
    subset = ROOT/'source_subset.md'; content='\n'.join(chunks)
    if subset.exists(): assert subset.read_text()==content
    else: subset.write_text(content)
    requests, manifest = build_object_inventory_passes(subset, BASE/'03_analysis/runs/control/blocks_stage02_100', max_chars=60000,max_images=4)
    root=ROOT/'ITP';root.mkdir(exist_ok=True)
    digests=[]
    for request in requests:
        wire={'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
        immutable(root/(request.stage+'.json'),wire);digests.append(evidence_digest(wire))
    assert BLOCKS == {bid for row in manifest['packets'] for bid in row['block_ids']}
    manifest.update(request_digests=digests,full_source_md_sha256=hashlib.sha256(md.encode()).hexdigest(),
        known_source_selection=True,independent_discovery=False,history_or_engineer_answers_supplied=False,
        model='codex/gpt-6-astra',reasoning_effort='high',automatic_retries=0)
    immutable(root/'manifest.json',manifest)
    immutable(Path(__file__).with_name('ITP_gvs_source_inventory_manifest.json'),manifest)
    runner.ROOT=ROOT;runner.preflight('ITP')
    print('PREPARED',len(requests),'packets',sum(len(m['content']) for r in requests for m in r.messages),'chars',flush=True)
    return manifest

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');args=p.parse_args()
    manifest=prepare()
    if args.execute: asyncio.run(runner.execute('ITP',evidence_digest(manifest)))
