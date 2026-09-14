"""Small, hashed raster scopes from admitted DEV PDFs; no synthetic imagery."""
from copy import deepcopy
import base64
from pathlib import Path

import fitz

from experiments.project_change_272.inventory import ROOT, read, immutable, sha, now
from experiments.project_change_272.policy import admitted_pairs
from .packets import BASE, digest


def augment(name='dev_visual_packets_v2', source='dev_packets_v7_graphic_scopes'):
    admitted_pairs('DEV')
    pairs={p['index']:p for p in read(ROOT/'sources/DEV/PAIRS.json')}
    input_dir=BASE/source;out=BASE/name
    immutable(out/'MANIFEST.json',dict(partition='DEV',created_at=now(),
        split_sha256=sha(ROOT/'SPLIT.json'),source_manifest_sha256=sha(input_dir/'MANIFEST.json'),
        code_sha256=sha(__file__),source_policy='At most one admitted original page per side; up to four raster tiles per page; no entire project',
        raster_citations='Visual transcriptions require separate multimodal semantic audit; not falsely presented as exact native matches'))
    count=0
    for path in sorted((input_dir/'packets').glob('*.json')):
        p=deepcopy(read(path));pair=pairs[p['pair_index']];p['parent_packet_id']=p.pop('packet_id')
        for side in ['old','new']:
            if not p['evidence'][side]:continue
            # The first anchor defines the local visual scope. Other retrieved
            # pages remain explicit native witnesses, never unseen visual proof.
            page_number=p['evidence'][side][0]['page']
            history=read(input_dir/'history_quarantine'/(str(pair['index'])+'.json'))[side]
            if page_number in pair['embargo_pages'][side] or str(page_number) in history:
                raise ValueError('Forbidden raster source')
            doc=pair[side]
            with fitz.open(doc['artifacts']['pdf']['path']) as pdf:
                page=pdf[page_number-1]
                graphic=any(e['route']=='GRAPHIC' for e in p['evidence'][side] if e['page']==page_number)
                table=any(e['route']=='TABLE' or e.get('requires_visual_scope') for e in p['evidence'][side] if e['page']==page_number)
                route='GRAPHIC' if graphic else ('TABLE' if table else 'TEXT')
                n=2 if max(page.rect.width,page.rect.height)>1000 else 1
                for y in range(n):
                    for x in range(n):
                        # Small overlap preserves labels crossing tile edges.
                        w,h=page.rect.width/n,page.rect.height/n
                        box=fitz.Rect(max(0,x*w-12),max(0,y*h-12),min(page.rect.width,(x+1)*w+12),min(page.rect.height,(y+1)*h+12))
                        scale=min(2.0,1800/max(box.width,box.height))
                        key=digest([doc['document_version'],page_number,list(box),scale])[:28]
                        image_path=out/'rasters'/(key+'.png');image_path.parent.mkdir(parents=True,exist_ok=True)
                        if not image_path.exists():page.get_pixmap(matrix=fitz.Matrix(scale,scale),clip=box).save(image_path)
                        p['evidence'][side].append(dict(evidence_id=side+'_raster_'+key,side=side,
                            document_version=doc['document_version'],page=page_number,bbox=list(box),quote=None,
                            source_kind='PDF_RASTER_CROP',route=route,source_receipt=doc['artifacts']['pdf'],
                            raster=dict(path=str(image_path),sha256=sha(image_path),pixels_per_pdf_point=scale),
                            native_quote_check=False,visual_audit_required=True))
        p['packet_id']=digest(p)[:24]
        immutable(out/'packets'/(p['packet_id']+'.json'),p);count+=1
    immutable(out/'COUNTS.json',dict(packets=count))
    print(out,count,flush=True)
    return out


def image_messages(packet):
    """Return API image blocks and replayable file/hash receipts separately."""
    blocks=[];receipts=[]
    for side in ['old','new']:
        for e in packet['evidence'][side]:
            if e.get('source_kind')!='PDF_RASTER_CROP':continue
            r=e['raster'];path=Path(r['path'])
            if sha(path)!=r['sha256']:raise ValueError('Raster drift')
            label=f"SOURCE IMAGE {e['evidence_id']}; side={side}; PDF page={e['page']}; bbox={e['bbox']}"
            blocks.append(dict(type='text',text=label))
            blocks.append(dict(type='image_url',image_url=dict(url='data:image/png;base64,'+base64.b64encode(path.read_bytes()).decode(),detail='high')))
            receipts.append(dict(evidence_id=e['evidence_id'],label=label,**r))
    assert len(receipts)<=8
    return blocks,receipts


if __name__=='__main__':augment()
