"""Metadata inventory and reversible research-only reconstruction of block geometry.

No source PDF/Markdown/result is rewritten. Page identity is explicit in the
enclosing result page; block.page_index has historically mixed conventions.
"""
from collections import Counter, defaultdict
from pathlib import Path
import argparse
import hashlib
import re

import fitz

from .run import ROOT,REPO,read,write,sha,immutable,now

OBJECTS=REPO/'projects_v2/objects'
DEV_PROJECTS={'214_Alia_ASTERUS','256_Primavera_K14_Spartak','272_Sadovnicheskaya_76_Balchug_Esteyt'}


def convert_result(raw, pdf_pages):
    blocks=[];pages=[];seen=set()
    for page in raw['pages']:
        if 'page_number' not in page:
            raise ValueError('Explicit one-based enclosing page_number required')
        number=page['page_number']
        if not isinstance(number,int) or not 1<=number<=pdf_pages or number in seen:
            raise ValueError('Invalid or duplicate source page')
        seen.add(number)
        pages.append(dict(page_index=number-1,width_px=page['width'],height_px=page['height'],rotation=0))
        ids=set()
        for i,block in enumerate(page.get('blocks',[]),1):
            bid=block['id'];coords=block.get('coords_norm')
            if bid in ids:raise ValueError('Duplicate block on source page')
            ids.add(bid)
            if not coords or len(coords)!=4 or not all(isinstance(v,(int,float)) and 0<=v<=1 for v in coords):
                raise ValueError('Invalid source geometry')
            blocks.append(dict(block_id=bid,page_index=number-1,page_label=number,ordinal=i,
                block_type='stamp' if block.get('category_code')=='stamp' else block.get('block_type','unknown'),shape_type=block.get('shape_type','rectangle'),
                coords_norm=coords,polygon_points=block.get('polygon_points_norm')))
    return dict(schema_version=1,coordinate_space='normalized_page_top_left',pages=pages,blocks=blocks,
                provenance='Derived losslessly from enclosing result page and source block geometry; no OCR/state inference')


def inventory():
    records=[]
    for project in sorted(OBJECTS.iterdir()):
        if not project.is_dir():continue
        versions=sorted(project.glob('disciplines/*/documents/*/versions/*'))
        versions+=sorted(project.glob('comparison/*/documents/*/versions/*'))
        docs=defaultdict(list)
        availability=Counter()
        for v in versions:
            w=v/'02_work';exists=tuple(f for f in ['document.pdf','document.md','result.json','blocks.json'] if (w/f).is_file())
            availability[' + '.join(exists) or 'NONE']+=1
            if all((w/f).is_file() for f in ['document.pdf','document.md','result.json']):docs[str(v.parent)].append(v)
        records.append(dict(project=project.name,versions=len(versions),availability=dict(availability),
            pdf_result_paired_documents=sum(len(v)>1 for v in docs.values()),
            dev_known=project.name in DEV_PROJECTS,metadata_only=True))
    write(ROOT/'00_STATE/CORPUS_INVENTORY.json',dict(created_at=now(),projects=records,semantic_contents_inspected=False))
    print(records)


def dev_verify():
    # Geometry adapter calibration uses an already spent source document only.
    work=OBJECTS/'214_Alia_ASTERUS/disciplines/OV/documents/13АВ-РД-ОВ1.2-К1_V1/versions/v001/02_work'
    raw=read(work/'result.json');frozen=read(work/'blocks.json')
    converted=convert_result(raw,len(fitz.open(work/'document.pdf')))
    fields=['block_id','page_index','block_type','coords_norm','shape_type']
    canonical=lambda d: sorted([tuple(str(b.get(k)) for k in fields) for b in d['blocks']])
    result=dict(converted_blocks=len(converted['blocks']),existing_blocks=len(frozen['blocks']),
                geometry_equal=canonical(converted)==canonical(frozen),
                result_sha256=sha(work/'result.json'),original_blocks_sha256=sha(work/'blocks.json'),
                label_kind='AUTOMATIC_DEV_GEOMETRY_CONSERVATION',semantic_quality_claim=False)
    if not result['geometry_equal']:raise ValueError('DEV geometry mismatch')
    write(ROOT/'01_CYCLES/03_source_recovery/DEV_VERIFICATION.json',result);print(result)


def select():
    target=ROOT/'03_VALIDATIONS/recovered_documents/MANIFEST.json'
    if target.exists():
        print('Selection already locked:',target);return
    # Previously used document names, including source-only corpus manifests,
    # are excluded. Selection never reads predictions or semantic PDF content.
    known=set()
    for dirname in ['20260913_engineering_subject_multiproject_validation','20260911_semantic_foundation_v3']:
        p=ROOT.parent/dirname/'DOCUMENTS.json'
        if p.exists():
            data=read(p)
            if isinstance(data,list):known.update(d['document_code'] for d in data)
    seed='project-change-master-unseen-documents-20260914-v1'
    eligible=[];excluded=[]
    for project in sorted(OBJECTS.iterdir()):
        if not project.is_dir() or '272_' in project.name:continue
        for document in sorted(project.glob('disciplines/*/documents/*')):
            if document.name in known:continue
            versions=[v for v in sorted((document/'versions').glob('*')) if all((v/'02_work'/f).is_file() for f in ['document.pdf','document.md','result.json'])]
            if len(versions)<2:continue
            a,b=versions[0],versions[-1]
            ap,bp=a/'02_work/document.pdf',b/'02_work/document.pdf'
            ah,bh=sha(ap),sha(bp)
            if ah==bh:
                excluded.append(dict(document=str(document),reason='IDENTICAL_PDF'));continue
            try:counts=[len(fitz.open(p)) for p in [ap,bp]]
            except Exception:
                excluded.append(dict(document=str(document),reason='PDF_UNREADABLE'));continue
            if max(counts)>100:
                excluded.append(dict(document=str(document),reason='PREDECLARED_100_PAGE_RESOURCE_CAP'));continue
            eligible.append(dict(project=project.name,discipline=document.parts[-3],document=document.name,
                old=str(a),new=str(b),pdf_pages=counts,pdf_hashes=[ah,bh],
                selection_key=hashlib.sha256((seed+str(document)).encode()).hexdigest()))
    selected=[];disciplines=Counter();projects=Counter()
    for d in sorted(eligible,key=lambda d:d['selection_key']):
        if disciplines[d['discipline']]>=1:continue
        selected.append(d);disciplines[d['discipline']]+=1;projects[d['project']]+=1
        if len(selected)==6:break
    for d in selected:
        d['sources']={side:{name:dict(path=str(Path(d[side])/'02_work'/name),sha256=sha(Path(d[side])/'02_work'/name))
                           for name in ['document.pdf','document.md','result.json']} for side in ['old','new']}
    immutable(target,dict(locked_at=now(),seed=seed,selected=selected,eligible=eligible,excluded=excluded,
                         exclusion_document_names=sorted(known),predictions_run=False,answers_seen=False,
                         previously_unseen_projects=sum(p not in DEV_PROJECTS for p in projects),
                         limitation='Unseen documents inside previously used project families; not independent-project generalization'))
    print('Locked',[(d['project'],d['discipline'],d['document'],d['pdf_pages']) for d in selected])


def recover():
    validation=ROOT/'03_VALIDATIONS/recovered_documents';manifest=read(validation/'MANIFEST.json');documents=[];pairs=[]
    for pair in manifest['selected']:
        ds={}
        for side in ['old','new']:
            sources=pair['sources'][side]
            for receipt in sources.values():
                if sha(receipt['path'])!=receipt['sha256']:raise ValueError('Validation source drift')
            key=hashlib.sha256(str(Path(pair[side])).encode()).hexdigest()
            pdfpath=sources['document.pdf']['path'];raw=read(sources['result.json']['path'])
            blocks=convert_result(raw,len(fitz.open(pdfpath)))
            bp=validation/'derived_blocks'/(key+'.json');immutable(bp,blocks)
            # Corroborate page/block identities against the untouched Markdown.
            text=Path(sources['document.md']['path']).read_text();page=None;mdids=set()
            for line in text.splitlines():
                p=re.fullmatch(r'## Page\s+(\d+)\s*',line)
                b=re.fullmatch(r'### BLOCK #\S+ \[[^]]+\]:\s*(\S+)\s*',line)
                if p:page=int(p[1])
                if b:mdids.add((page,b[1]))
            bids={(b['page_index']+1,b['block_id']) for b in blocks['blocks']}
            if not mdids or not mdids<=bids:raise ValueError('Markdown/result block identity mismatch')
            artifacts=dict(pdf=sources['document.pdf'],work_md=sources['document.md'],blocks=dict(path=str(bp),sha256=sha(bp)))
            doc=dict(document_version=key,document_code=pair['document'],project=pair['project'],discipline=pair['discipline'],version_id=Path(pair[side]).name,artifacts=artifacts)
            documents.append(doc);ds[side]=doc
        pairs.append(dict(project=pair['project'],discipline=pair['discipline'],pair_key='recovered/'+hashlib.sha256(pair['document'].encode()).hexdigest()[:24],**ds))
    immutable(validation/'DOCUMENTS.json',documents);immutable(validation/'PAIRS.json',pairs)
    print('Recovered',len(documents),'versions into research output only')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['inventory','dev_verify','select','recover'])
    globals()[p.parse_args().action]()
