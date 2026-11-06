"""Finalize and validate source-only classic preparation, with no model calls."""
import argparse
import os
from pathlib import Path
from PIL import Image
from investigate import read,write,sha


def check(work,old_manifest,normalize=False):
    root=work/'classic_control'
    if not (root/'preparation.json').exists():raise ValueError('Preparation process has not finished')
    original={p['discipline']:p for p in read(old_manifest)}
    prepared=read(root/'input_manifest.json')
    if len(prepared)!=6:raise ValueError('Expected all six prepared projects')
    os.environ['AUDIT_DISABLE_DOTENV']='1';os.environ['AUDIT_OPENROUTER_ENABLED']='false'
    from backend.app.pipeline.stages.block_context.contract import validate_block_context_summary
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import crop_index_matches_policy,stage02_crop_policy
    reports=[]
    for project in prepared:
        version=Path(project['version_dir']);out=Path(project['output_dir']);disc=project['discipline']
        if (out/'01_blocks_analysis.json').exists() or (out/'03_findings.json').exists():raise ValueError('Cannot change/check preparation after audit started')
        index_path=out/'blocks_stage02_100/index.json';index=read(index_path)
        old_index=read(Path(original[disc]['run_dir'])/'blocks_stage02_100/index.json')
        originals={b['block_id']:b for b in old_index['blocks']}
        if normalize:
            for block in index['blocks']:
                path=index_path.parent/block['file'];size=originals[block['block_id']]['render_size']
                with Image.open(path) as im:
                    if list(im.size)!=size:
                        im=im.resize(tuple(size),Image.Resampling.LANCZOS);im.save(path)
                block.update(render_size=size,sha256=sha(path),size_kb=round(path.stat().st_size/1024,2))
            index['crop_failover']={'cloud_crop_attempts':0,'local_crop_successes':len(index['blocks'])}
            index['rendition_note']='Only GRAPHIC rerendered from frozen PDF; original crop pixel dimensions restored; original PNG byte identity not claimed.'
            write(index_path,index)
            info=read(version/'01_input/project_info.json')
            info.update(md_file='02_work/document.md',pdf_file='01_input/document.pdf')
            write(version/'01_input/project_info.json',info)
            project['files']={str(p.relative_to(version)):sha(p) for p in version.rglob('*') if p.is_file()}
        for name,digest in project['files'].items():
            if sha(version/name)!=digest:raise ValueError(f'Prepared file changed: {disc}/{name}')
        if sha(version/'02_work/document.md')!=project['source_md_sha256']:raise ValueError('Authoritative MD changed')
        if sha(version/'02_work/document.pdf')!=project['source_pdf_sha256']:raise ValueError('PDF changed')
        context=validate_block_context_summary(out)
        crop_policy=crop_index_matches_policy(index_path,stage02_crop_policy())
        if not context['valid'] or not crop_policy:raise ValueError(f'Classic contract failed: {disc}: context={context.get("reason")}, crop_policy={crop_policy}')
        assert set(originals)=={b['block_id'] for b in index['blocks']}
        reports.append({'discipline':disc,'graphics':len(index['blocks']),'hashes_checked':len(project['files']),'context_valid':True,'crop_policy_valid':True})
    if normalize:write(root/'input_manifest.json',prepared)
    write(root/'input_checks.json',{'status':'passed','projects':reports,'model_calls':0,'limits':'Input integrity and source-preparation contracts only, not audit completion or quality.'})
    for report in reports:print(report)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);p.add_argument('--old-manifest',type=Path,required=True);p.add_argument('--normalize',action='store_true')
    a=p.parse_args();check(a.work,a.old_manifest,a.normalize)
