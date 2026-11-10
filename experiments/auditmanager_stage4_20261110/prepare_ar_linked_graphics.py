"""Render only model-requested linked GRAPHIC blocks, keeping versions separate."""
from pathlib import Path
import hashlib
import json
import fitz
from .run_ar_roof_operations import immutable

ROOT=Path('/tmp/auditmanager_ar_linked_graphics_20261111')
SOURCES=[('v001',Path('/home/coder/projects/PDF-proverka/projects_v2/_system/destructive_backups/20260914T143051948760Z_00580716-_5.2_V1_v001__delete_project/01_input')),
 ('v002',Path('/home/coder/projects/PDF-proverka/projects_v2/objects/314_Sobytie_6_1_Donstroy/disciplines/KM/documents/ПД-00580716-КМ5.2/versions/v002/01_input'))]


def main():
    ROOT.mkdir(exist_ok=True);images=[];sources=[]
    for version,directory in SOURCES:
        pdf,=directory.glob('*.pdf');blocks,=directory.glob('*blocks.json');md,=directory.glob('*.md')
        data=json.loads(blocks.read_text())
        assert data['coordinate_space']=='normalized_page_top_left'
        chosen=[b for b in data['blocks'] if b['page_index']==4 and b['block_type']=='image']
        assert len(chosen)==4
        sources.append({'version':version,'files':[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in (pdf,blocks,md)]})
        with fitz.open(pdf) as document:
            page=document[4];assert page.rotation==0
            for b in chosen:
                x0,y0,x1,y1=b['coords_norm'];r=page.rect
                clip=fitz.Rect(x0*r.width,y0*r.height,x1*r.width,y1*r.height)
                pix=page.get_pixmap(matrix=fitz.Matrix(2,2),clip=clip,alpha=False)
                target=ROOT/(version+'_'+b['block_id']+'.png');raw=pix.tobytes('png')
                if target.exists():assert target.read_bytes()==raw
                else:target.write_bytes(raw)
                images.append({'version':version,'document_code':'ПД-00580716-КМ5.2','block_id':b['block_id'],'page_index':4,
                    'clip_page_points':list(clip),'path':str(target),'width':pix.width,'height':pix.height,
                    'sha256':hashlib.sha256(raw).hexdigest()})
    manifest={'sources':sources,'images':images,'selection':'All four original GRAPHIC blocks on sheet 3 requested by navigation, separately per source version.',
        'text_reextracted':False,'text_raster_corrected':False,'technical_applicability_verified':False,'model_called':False}
    immutable(ROOT/'manifest.json',manifest)
    immutable(Path(__file__).with_name('AR_linked_graphics_manifest.json'),manifest)
    print('PREPARED',len(images),'graphics',sum(Path(i['path']).stat().st_size for i in images),'bytes')

if __name__=='__main__':main()
