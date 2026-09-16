"""Offline capacity measurement. Does not alter guards or make inference calls."""
import json
import math
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone
from PIL import Image
from .snapshot import ROOT, OUT, BASE, LIVE_A, REPO, read, sha, save
from experiments.project_change_semantic_codex_272.serialization import canonical_json_bytes, prepare_request_bytes

AUDIT=OUT/'pair_b_capacity_check'


def measured_request(system, data, packet):
    # Independently reconstruct transport bytes for measurement only. Never invokes provider.
    images=[]
    for side in ('old','new'):
        for e in packet['evidence'][side]:
            if e.get('source_kind')=='PDF_RASTER_CROP':
                raster=dict(e['raster']);raster['path']=str(BASE/raster['path'])
                assert sha(raster['path'])==raster['sha256']
                images.append(dict(evidence_id=e['evidence_id'],side=side,page=e['page'],bbox=e['bbox'],**raster))
    labels=[dict(image_index=i+1,file=f'image_{i:02d}.png',evidence_id=e['evidence_id'],side=e['side'],page=e['page'],bbox=e['bbox']) for i,e in enumerate(images)]
    prefix=system.encode()+b'\n\nProcess only the supplied evidence. Return the final JSON directly.\n'+b'Attached images are ordered as this source manifest:\n'+canonical_json_bytes(labels)+b'\n\nUNTRUSTED PACKET DATA:\n'
    return prefix+canonical_json_bytes(data),images,len(prefix)


def main():
    sys.path.insert(0,'/tmp/projectchange-capacity-deps')
    import tiktoken
    enc=tiktoken.get_encoding('o200k_base')
    tokens=lambda s:len(enc.encode(s,disallowed_special=()))
    catalog=read(AUDIT/'RUNTIME_MODEL_CATALOG.json')
    m=next(m for m in catalog['models'] if m['slug']=='gpt-6-astra')
    assert m['context_window']==272000 and m['effective_context_window_percent']==95
    effective=m['context_window']*m['effective_context_window_percent']//100
    system=(LIVE_A/'PROMPT.txt').read_text();schema=(LIVE_A/'OUTPUT_SCHEMA.json').read_text()
    base=m['base_instructions'];persistent=m['model_messages']['persistent_instructions']
    debug=(AUDIT/'CLI_INPUT_OVERHEAD.json').read_text()
    # Entire debug rendering and persistent instructions may overlap: overcount intentionally.
    fixed_texts=dict(model_instructions=base,persistent_instructions=persistent,cli_input_overhead=debug,output_schema=schema)
    fixed={k:dict(characters=len(s),bytes=len(s.encode()),estimated_tokens=tokens(s)) for k,s in fixed_texts.items()}
    overhead_tokens=sum(v['estimated_tokens'] for v in fixed.values());overhead_bytes=sum(v['bytes'] for v in fixed.values())
    rows=[];prior_bodies=[];verified=0
    for label,plan_dir in [('A',LIVE_A),('B',OUT/'pair_b_live')]:
        for item in read(plan_dir/'CALL_PLAN.json')['packages']:
            if item['action']!='MODEL_CALL':continue
            key=item['key'];package_path=BASE/item['package'];p=read(package_path)
            data=read(plan_dir/f'inputs/{key}/MODEL_INPUT.json');packet=p['evidence_packet']
            payload,images,prefix_bytes=measured_request(system,data,packet)
            if label=='A':
                assert payload==(plan_dir/f'inputs/{key}/EXACT_PROMPT.txt').read_bytes()
                assert read(plan_dir/f'calls/{key}/SUCCESS.json')['status']=='SUCCESS'
                verified+=1
            details=[]
            for e in images:
                with Image.open(e['path']) as im:w,h=im.size
                patches=math.ceil(w/32)*math.ceil(h/32)
                details.append(dict(evidence_id=e['evidence_id'],side=e['side'],page=e['page'],sha256=e['sha256'],
                    width=w,height=h,patches_original=patches,image_token_upper_estimate=math.ceil(patches*1.2),
                    file_bytes=Path(e['path']).stat().st_size,base64_bytes=4*math.ceil(Path(e['path']).stat().st_size/3)))
            # JSON escaping allowance + envelope reservation; raw original PNG (no resizing).
            body_upper=2*len(payload)+sum(e['base64_bytes'] for e in details)+2*overhead_bytes+65536
            if label=='A':
                prior_bodies.append(dict(package_id=key,body_bytes_upper=body_upper,
                    image_base64_bytes=sum(e['base64_bytes'] for e in details),
                    successful_receipt=str(plan_dir/f'calls/{key}/SUCCESS.json')))
                continue
            text_tokens=tokens(payload.decode());image_tokens=sum(e['image_token_upper_estimate'] for e in details)
            total_estimate=text_tokens+image_tokens+overhead_tokens
            # Tokenizer model mapping unknown: double ALL text, count full-resolution images,
            # add protocol slack, and reserve the documented maximum output/reasoning budget.
            conservative_input=2*(text_tokens+overhead_tokens)+image_tokens+4096
            reserved_output=128000
            chars=len(system)+len(json.dumps(data,ensure_ascii=False))
            evidence=canonical_json_bytes(data['evidence'])
            saved=plan_dir/f'inputs/{key}/EXACT_PROMPT.txt'
            if saved.exists():assert payload==saved.read_bytes()
            rows.append(dict(package_id=key,characters=chars,over_60000=chars>60000,
                canonical_request_characters=len(payload.decode()),canonical_request_bytes=len(payload),
                estimated_text_tokens=text_tokens,estimated_image_tokens_upper=image_tokens,
                estimated_full_input_tokens=total_estimate,exact_model_tokens=None,number_of_images=len(images),images=details,
                prompt_overhead=dict(system_characters=len(system),system_tokens=tokens(system),prefix_and_manifest_bytes=prefix_bytes),
                evidence_overhead=dict(characters=len(evidence.decode()),bytes=len(evidence),estimated_tokens=tokens(evidence.decode()),
                    other_packet_fields_bytes=len(canonical_json_bytes(data))-len(evidence)),
                body_bytes_upper_estimate=body_upper,conservative_input_tokens=conservative_input,
                output_and_reasoning_reserve_tokens=reserved_output,
                safety_headroom_tokens=effective-conservative_input-reserved_output,
                package_file_sha256=sha(package_path),package_hash=p['package_hash'],
                evidence_hash=__import__('hashlib').sha256(canonical_json_bytes(packet['evidence'])).hexdigest(),
                model_input_sha256=sha(plan_dir/f'inputs/{key}/MODEL_INPUT.json'),
                canonical_request_sha256=__import__('hashlib').sha256(payload).hexdigest()))
    assert len(rows)==18 and verified==52
    # Same CLI/provider has successfully carried more image bytes alone than any entire B envelope.
    largest_prior=max(prior_bodies,key=lambda x:x['image_base64_bytes'])
    safe=all(r['safety_headroom_tokens']>15000 and r['body_bytes_upper_estimate']<largest_prior['image_base64_bytes']
             and r['body_bytes_upper_estimate']<512000000 and r['number_of_images']<=8
             and all(i['patches_original']<=30000 for i in r['images']) for r in rows)
    proposed=math.ceil(max(r['characters'] for r in rows)*1.05/1000)*1000
    result=dict(at=datetime.now(timezone.utc).isoformat(),status='CAPACITY_SAFE' if safe else 'CAPACITY_NOT_CONFIRMED',
        capacity_safe=safe,limit_60000='LOCAL_GUARD_ONLY',model='gpt-6-astra',reasoning='xhigh',provider='codex_chatgpt',
        model_calls=0,metadata_only_model_catalog_reads=1,tokenizer=dict(library='tiktoken',version=tiktoken.__version__,
            encoding='o200k_base',exact_model_mapping_available=False,counts='ESTIMATES; conservative text multiplier 2'),
        limits=dict(runtime_context_window=272000,effective_runtime_context_window=effective,api_model_context_window=1050000,
            api_max_output_tokens=128000,api_total_payload_bytes=512000000,
            chatgpt_backend_hard_body_limit='NOT_PUBLISHED; do not equate API limit to backend',
            same_runtime_successful_body_lower_bound=largest_prior['image_base64_bytes'],same_runtime_body_receipt=largest_prior,
            cli_character_limit='No 60000 check; reads stdin; adapter rejects before spawning CLI',
            adapter_text_characters=60000,adapter_image_count=8),
        fixed_overhead=fixed,rows=rows,packages=20,model_eligible=18,over_60000=sum(r['over_60000'] for r in rows),
        maxima={k:max(r[k] for r in rows) for k in ['characters','canonical_request_bytes','estimated_text_tokens','estimated_image_tokens_upper',
            'estimated_full_input_tokens','body_bytes_upper_estimate','conservative_input_tokens','number_of_images']},
        minimum_safety_headroom_tokens=min(r['safety_headroom_tokens'] for r in rows),
        guard_proposal=proposed if safe else None,guard_headroom_characters=proposed-max(r['characters'] for r in rows),
        prior_pair_a_canonical_requests_verified=verified,content_changed=False,
        provenance=dict(local_guard_origin_commit='93e97cd0',serializer_commit='084df378',
            model_catalog_sha256=sha(AUDIT/'RUNTIME_MODEL_CATALOG.json'),cli_overhead_sha256=sha(AUDIT/'CLI_INPUT_OVERHEAD.json'),
            prompt_sha256=sha(LIVE_A/'PROMPT.txt'),code_commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()),
        sources=[dict(url='https://developers.openai.com/api/docs/models/gpt-6-astra',fact='API context 1050000; output 128000; xhigh supported'),
            dict(url='https://developers.openai.com/api/docs/guides/images-vision',fact='512 MB payload; original/auto patch rules and 1.2 multiplier'),
            dict(url='https://learn.chatgpt.com/docs/config-file/config-reference',fact='model_context_window is runtime token capacity')],
        assumptions=['o200k_base is an estimate, not a verified Astra tokenizer',
            'Original raster patch estimate conservatively covers CLI resizing; no input image is modified',
            'Backend hard body maximum unknown; B full upper envelope is below previously accepted A image bytes alone',
            '128000 output reserve includes reasoning; no change to provider output configuration'],
        architecture_modified=False,source_truth_opened=False,validation='NOT OPENED',final_holdout='NOT OPENED')
    save(AUDIT/'PAIR_B_INPUT_CAPACITY_AUDIT.json',result)
    print(json.dumps({k:result[k] for k in ['status','maxima','minimum_safety_headroom_tokens','guard_proposal']},ensure_ascii=False))

if __name__=='__main__':main()
