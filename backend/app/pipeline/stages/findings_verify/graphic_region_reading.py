"""Derive bounded reading views from a review's image references, never TEXT."""
import hashlib
import math
from pathlib import Path
from PIL import Image
from .source_review_evidence import evidence_digest
from .graphic_literal_reading import prepare_literal_reading


def prepare_review_region_readings(source_request, response, output_dir, *, max_regions=4):
    """First usable region per referenced image; no target mark or value selection.

    Coordinates are untrusted model proposals, not proof of the cited proposition.
    Keep provenance and the original image; rotation only assists literal reading.
    """
    if type(max_regions) is not int or not 1 <= max_regions <= 4:
        raise ValueError('invalid_region_budget')
    images={(im['block_id'],im['sha256']):im for im in source_request['image_bindings']}
    root=Path(output_dir);root.mkdir(parents=True,exist_ok=True)
    selected=[];seen=set();skipped=[]
    for claim in response.get('claims',[]):
        for e in claim.get('evidence',[]):
            if e.get('type')!='image':continue
            key=(e.get('block_id'),e.get('image_sha256'))
            if key in seen:continue
            image=images.get(key)
            if not image or image['path'] not in source_request['image_paths']:
                raise ValueError('region_source_not_attached')
            if hashlib.sha256(Path(image['path']).read_bytes()).hexdigest()!=image['sha256']:
                raise ValueError('region_source_changed')
            box=e.get('bbox_norm_xyxy')
            if (not isinstance(box,list) or len(box)!=4
                or any(type(v) not in (int,float) or not 0<=v<=1 for v in box)
                or box[0]>=box[2] or box[1]>=box[3]):raise ValueError('invalid_region_geometry')
            with Image.open(image['path']) as original:
                width,height=original.size
                x0,y0,x1,y1=box[0]*width,box[1]*height,box[2]*width,box[3]*height
                if min(x1-x0,y1-y0)<16:
                    skipped.append({'claim_id':claim.get('claim_id'),'image_sha256':key[1],'reason':'region_under_16_pixels'})
                    continue
                if len(selected)>=max_regions:
                    skipped.append({'claim_id':claim.get('claim_id'),'image_sha256':key[1],'reason':'region_budget'})
                    continue
                # Rendering margins, not engineering tolerances; retain context.
                dx=max(8,(x1-x0)*0.1);dy=max(8,(y1-y0)*0.1)
                pixel_box=[max(0,math.floor(x0-dx)),max(0,math.floor(y0-dy)),min(width,math.ceil(x1+dx)),min(height,math.ceil(y1+dy))]
                crop=original.crop(pixel_box)
                rotation=90 if crop.height>3*crop.width else 0
                if rotation:crop=crop.transpose(Image.Transpose.ROTATE_90)
                if crop.width*crop.height>20_000_000:raise ValueError('region_pixel_budget')
                binding={'block_id':key[0],'parent_image_sha256':key[1],'parent_path':image['path'],
                    'parent_size':[width,height],'proposed_bbox_norm_xyxy':box,'pixel_box':pixel_box,
                    'rotation_ccw':rotation,'selection':'first usable evidence region per attached image',
                    'semantic_verified':False}
                name=evidence_digest(binding);path=root/f'{name}.png'
                import io
                stream=io.BytesIO();crop.save(stream,format='PNG');raw=stream.getvalue()
                if path.exists() and path.read_bytes()!=raw:raise ValueError('region_snapshot_changed')
                path.write_bytes(raw)
                crop_binding={'block_id':key[0],'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),
                    'width':crop.width,'height':crop.height,'view_kind':'review_region'}
                request=prepare_literal_reading({'image_paths':[str(path)],'image_bindings':[crop_binding]},crop_binding['sha256'])
                selected.append({'source':binding,'request':request})
                seen.add(key)
    return {'regions':selected,'skipped':skipped,'source_request_sha256':evidence_digest(source_request),
        'response_sha256':evidence_digest(response),'semantic_verified':False}
