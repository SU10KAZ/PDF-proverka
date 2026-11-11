"""Materialize private source-only copies for declared trial slots; never dispatch."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil


def sha(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()


def write(p,value):
    with p.open('x') as f:f.write(json.dumps(value,ensure_ascii=False,indent=2)+'\n')


def materialize(layout_path,variant):
    layout=json.loads(layout_path.read_text());trial_root=Path(layout['root']).resolve()
    slots=[r for r in layout['runs'] if r['variant']==variant]
    if len(slots)!=18 or len({(r['discipline'],r['repeat']) for r in slots})!=18:
        raise ValueError('Expected six projects and three unique repeats')
    sources=layout['sources_observed_now_not_yet_trial_frozen']
    prefix='03_analysis/runs/candidate/'
    # Verify all originals and destinations before any copy. Extra input files
    # are rejected, not silently imported as presumed harmless context.
    for disc in {r['discipline'] for r in slots}:
        source=sources[disc];root=Path(source['root'])
        if any(p.is_symlink() for p in root.rglob('*')):raise ValueError('Source symlink')
        actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
        if actual!=set(source['files']):raise ValueError('Input inventory changed')
        for name,digest in source['files'].items():
            allowed=name in ('01_input/project_info.json','02_work/document.md','02_work/document.pdf','02_work/blocks.json',prefix+'document_graph.json',prefix+'block_context_summary.json') or (name.startswith(prefix+'block_vector_graphs/') and name.endswith('.json')) or (name.startswith(prefix+'blocks_stage02_100/') and (name.endswith('.png') or name==prefix+'blocks_stage02_100/index.json'))
            if not allowed or sha(root/name)!=digest:raise ValueError('Unapproved or changed input: '+name)
    for run in slots:
        work=Path(run['work']).resolve()
        if not work.is_relative_to(trial_root) or (work/'classic_control').exists():
            raise ValueError('Occupied or foreign slot')
        if json.loads((work/'run_slot.json').read_text())!=run:raise ValueError('Run slot changed')
        if Path(run['proposed_version_dir'])!=work/'classic_control/inputs'/run['discipline']/'versions/v001':
            raise ValueError('Unexpected version path')
        if Path(run['proposed_output_dir'])!=Path(run['proposed_version_dir'])/'03_analysis/runs/comparison':
            raise ValueError('Unexpected output path')
    receipts=[]
    for run in slots:
        source=sources[run['discipline']];version=Path(run['proposed_version_dir']);out=Path(run['proposed_output_dir'])
        version.mkdir(parents=True,exist_ok=False);files={}
        for name,digest in source['files'].items():
            relative=name.replace(prefix,'03_analysis/runs/comparison/',1) if name.startswith(prefix) else name
            dest=version/relative;dest.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(Path(source['root'])/name,dest)
            if sha(dest)!=digest:raise ValueError('Copied bytes differ')
            files[relative]=digest
        graph=json.loads((out/'document_graph.json').read_text())
        if graph['source_md_sha256']!=files['02_work/document.md']:raise ValueError('Graph source mismatch')
        index=json.loads((out/'blocks_stage02_100/index.json').read_text())
        for block in index['blocks']:
            if files.get('03_analysis/runs/comparison/blocks_stage02_100/'+block['file'])!=block['sha256']:
                raise ValueError('Image index mismatch')
        root=Path(run['work'])/'classic_control'
        manifest=[{'discipline':run['discipline'],'project':'source-only-'+run['discipline'],
            'version_dir':str(version),'output_dir':str(out),'files':files,'graphics':len(index['blocks'])}]
        write(root/'input_manifest.json',manifest)
        write(root/'input_checks.json',{'status':'passed','scope':'Source copy identity, source-only allowlist, graph and graphic index bindings.',
            'layout_sha256':sha(layout_path),'production_changed':False,'trial_freeze_complete':False})
        receipts.append({'work':run['work'],'discipline':run['discipline'],'repeat':run['repeat'],
                         'files':len(files),'input_manifest_sha256':sha(root/'input_manifest.json')})
    receipt={'status':'private_source_copies_prepared_not_frozen','variant':variant,'runs':receipts,
             'source_layout_sha256':sha(layout_path),'model_calls':0,'runnable_protocols_created':False,
             'remaining':'Implementation, evaluator/trial binding, controls and dispatch order must be frozen separately.'}
    write(trial_root/(variant+'_input_copy_receipt.json'),receipt)
    return receipt

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--layout',type=Path,required=True)
    p.add_argument('--variant',choices=['classic','classic_current','inventory','source_review','candidate'],required=True)
    a=p.parse_args();result=materialize(a.layout,a.variant)
    print(json.dumps({'runs':len(result['runs']),'files':sum(r['files'] for r in result['runs']),'model_calls':0}))
