"""Freeze classic pipeline source inputs from the same six project versions.

No audits/engineer answers copied. Graphic crops rendered only inside IMAGE blocks.
"""
import argparse
import json
from pathlib import Path
import shutil
from PIL import Image
from investigate import Corpus,read,sha,write


def prepare(work,old_manifest):
    root=work/'classic_control';root.mkdir(exist_ok=False)
    original=read(old_manifest);current={p['discipline']:p for p in read(work/'manifest.json')};prepared=[]
    for project in original:
        disc=project['discipline'];source=Path(project['version_dir']);run=Path(project['run_dir'])
        frozen=work/'sources'/disc
        for name,digest in current[disc]['files'].items():
            if sha(frozen/name)!=digest:raise ValueError('Frozen input changed')
        version=root/'inputs'/disc/'versions'/source.name
        for folder in ('01_input','02_work'): (version/folder).mkdir(parents=True)
        for name in ('document.md','document.pdf','blocks.json'):shutil.copyfile(frozen/name,version/'02_work'/name)
        shutil.copyfile(frozen/'document.pdf',version/'01_input/document.pdf')
        info=read(source/'01_input/project_info.json')
        # Normalize embedded local paths only; document content remains byte identical.
        info=json.loads(json.dumps(info,ensure_ascii=False).replace(str(source),str(version)))
        write(version/'01_input/project_info.json',info)
        out=version/'03_analysis/runs/control';out.mkdir(parents=True)
        for name in ('document_graph.json','block_context_summary.json','block_grounding_summary.json'):
            raw=(run/name).read_text().replace(str(run),str(out)).replace(str(source),str(version))
            (out/name).write_text(raw)
        if (run/'block_vector_graphs').is_dir():
            shutil.copytree(run/'block_vector_graphs',out/'block_vector_graphs')
            for path in (out/'block_vector_graphs').rglob('*.json'):
                path.write_text(path.read_text().replace(str(run),str(out)).replace(str(source),str(version)))
        crops=out/'blocks_stage02_100';crops.mkdir()
        index=read(run/'blocks_stage02_100/index.json')
        corpus=Corpus(frozen,root/'render_cache'/disc)
        for i,block in enumerate(index['blocks']):
            bid=block['block_id']
            _,images=corpus.execute({'tool':'image','id':bid})
            target=crops/Path(block['file']).name
            original_size=block.get('render_size')
            with Image.open(images[0]) as im:
                if original_size:im=im.resize(tuple(original_size),Image.Resampling.LANCZOS)
                im.save(target)
                block['render_size']=list(im.size)
            block['file']=target.name;block['size_kb']=round(target.stat().st_size/1024,2)
            block['source']='frozen_pdf_graphic_rerender';block['sha256']=sha(target)
            if (i+1)%10==0:print(disc,'graphics',i+1,'/',len(index['blocks']),flush=True)
        index['source']='frozen_pdf_graphic_rerender'
        index['crop_failover']={'cloud_crop_attempts':0,'local_crop_successes':len(index['blocks'])}
        index['rendition_note']='Rerendered only GRAPHIC regions and resized to original crop pixel dimensions; original PNG bytes unavailable.'
        write(crops/'index.json',index)
        files={str(p.relative_to(version)):sha(p) for p in version.rglob('*') if p.is_file()}
        prepared.append({'discipline':disc,'project':project['project'],'version_dir':str(version),'output_dir':str(out),
            'graphics':len(index['blocks']),'source_md_sha256':sha(frozen/'document.md'),'source_pdf_sha256':sha(frozen/'document.pdf'),'files':files})
        write(root/'input_manifest.json',prepared)
        print('PREPARED',disc,len(index['blocks']),flush=True)
    write(root/'preparation.json',{'status':'prepared','projects':len(prepared),'source_authority':'same frozen authoritative MD and PDF; only IMAGE raster rendered',
        'context':'source-derived graph/context/vector metadata from original same-version preparation; no saved findings or expert feedback copied',
        'models':'Not launched. Next protocol must pin Codex-only models and explicitly forbid Claude/OpenRouter.',
        'comparison_limit':'New graphic renditions differ from lost historical PNGs; archived A/B not a randomized baseline for this run.',
        'production_changed':False})

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);p.add_argument('--old-manifest',type=Path,required=True)
    a=p.parse_args();prepare(a.work,a.old_manifest)
