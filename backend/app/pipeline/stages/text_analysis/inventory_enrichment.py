"""Add source-only inventory to classic analysis without replacing its findings."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from ..findings_verify.source_review_evidence import evidence_digest
from ..prepare.object_inventory_passes import build_object_inventory_passes
from .inventory_candidates import inventory_text_candidates
from .object_registry import reconcile_object_registry
from .inventory_record_ids import separate_colliding_record_ids


def merge_inventory_analysis(base, packets, known_blocks):
    """Executor-owned packet IDs and block boundaries, not model-generated IDs."""
    merged=deepcopy(base)
    if not isinstance(merged.get('text_findings'),list):
        raise ValueError('classic_findings_required')
    if 'inventory_enrichment' in merged:
        raise ValueError('inventory_already_applied')
    candidates=[]; reports=[];seen=set();packet_blocks={}
    existing={o.get('id') for o in merged.get('object_registry',[]) if isinstance(o,dict)}
    for packet in packets:
        pid=packet['packet_id']
        if not isinstance(pid,str) or not pid or pid in seen:
            raise ValueError('duplicate_inventory_packet')
        seen.add(pid)
        data, record_repairs = separate_colliding_record_ids(packet['payload'])
        local=set(packet['block_ids'])
        if not local.issubset(known_blocks):
            raise ValueError('inventory_source_version_mismatch')
        packet_blocks[pid]=local
        coverage=reconcile_object_registry(data,local)
        prefix=pid+':'
        for obj in coverage['objects']:
            oid=prefix+obj['id']
            if oid in existing:
                raise ValueError('inventory_object_id_collision')
            existing.add(oid)
            merged.setdefault('object_registry',[]).append({**deepcopy(obj),'id':oid})
        operation_rows=data.get('object_operation_results',[])
        for record in operation_rows if isinstance(operation_rows,list) else []:
            if isinstance(record,dict) and isinstance(record.get('object_id'),str):
                merged.setdefault('object_operation_results',[]).append({**deepcopy(record),'object_id':prefix+record['object_id']})
        inventory_rows=data.get('object_inventory_coverage',[])
        for record in inventory_rows if isinstance(inventory_rows,list) else []:
            if isinstance(record,dict) and isinstance(record.get('object_ids'),list):
                merged.setdefault('object_inventory_coverage',[]).append({**deepcopy(record),
                    'object_ids':[prefix+oid if isinstance(oid,str) else oid for oid in record['object_ids']]})
        candidates.append({'packet_id':pid,'findings':data.get('text_findings',[])})
        reports.append({'packet_id':pid,'block_ids':sorted(local),
            'raw_payload':deepcopy(packet['payload']),'record_identity_repairs':record_repairs,
            'coverage':coverage})
    adapted=inventory_text_candidates(candidates,known_blocks,
        reserved_ids=[f.get('id') for f in base['text_findings'] if isinstance(f,dict) and isinstance(f.get('id'),str)])
    for candidate in adapted['text_findings']:
        local=packet_blocks[candidate['inventory_origin']['packet_id']]
        candidate['unsupplied_block_ids']=[bid for bid in candidate['related_block_ids'] if bid not in local]
    merged['text_findings'].extend(adapted['text_findings'])
    merged['inventory_enrichment']={'packets':reports,'pending_findings':adapted['pending'],
        'classic_findings_preserved':len(base['text_findings']),
        'added_candidates':len(adapted['text_findings']),'semantic_verified':False}
    return merged


def _write(path,data):
    temporary=path.with_suffix('.tmp')
    temporary.write_text(json.dumps(data,ensure_ascii=False,indent=2))
    temporary.replace(path)


async def run_inventory_enrichment(base, md_path, graphic_dir, work_dir, *,
        invoke, executor_id, max_packets, is_cancelled=None,
        max_source_chars=60000, max_images=4):
    """Explicit experimental callback; no provider default or automatic retry.

    Every generated request contains only source blocks and images. The classic
    findings are retained locally and never included in inventory model input.
    The caller owns dispatch authorization. Results are resumable and bound to
    exact requests, source bytes, baseline and executor identity.
    """
    if not callable(invoke) or not isinstance(executor_id,str) or not executor_id:
        raise ValueError('explicit_inventory_executor_required')
    if type(max_packets) is not int or max_packets<1:
        raise ValueError('positive_inventory_budget_required')
    if any(type(value) is not int or value < 1 for value in (max_source_chars, max_images)):
        raise ValueError('positive_inventory_packet_limits_required')
    passes,manifest=build_object_inventory_passes(md_path,graphic_dir,
        max_chars=max_source_chars,max_images=max_images)
    root=Path(work_dir);root.mkdir(parents=True,exist_ok=True)
    requests=[{'messages':p.messages,'image_paths':[str(path) for path in p.image_paths]} for p in passes]
    binding={'manifest':manifest,'request_hashes':[evidence_digest(r) for r in requests],
        'baseline_sha256':evidence_digest(base),'executor_id':executor_id,'workflow_version':2,
        'packet_limits':{'max_source_chars':max_source_chars,'max_images':max_images}}
    frozen=root/'binding.json'
    if frozen.exists():
        if json.loads(frozen.read_text())!=binding:
            raise ValueError('inventory_workflow_inputs_changed')
    else:
        with frozen.open('x') as handle:json.dump(binding,handle)
    packets=[];calls=[]
    status='finished'
    for targeted,request,entry in zip(passes[:max_packets],requests[:max_packets],manifest['packets'][:max_packets],strict=True):
        if is_cancelled and is_cancelled():
            status='cancelled';break
        if hashlib.sha256(Path(md_path).read_bytes()).hexdigest()!=manifest['source_md_sha256']:
            raise ValueError('inventory_md_changed')
        directory=root/targeted.stage;directory.mkdir(exist_ok=True)
        digest=evidence_digest(request)
        for path,image in zip(request['image_paths'],entry['images'],strict=True):
            if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=image['sha256']:
                raise ValueError('inventory_image_changed')
        output=directory/'result.json';receipt=directory/'result_binding.json'
        if output.exists():
            result=json.loads(output.read_text())
            if not receipt.exists() or json.loads(receipt.read_text())!={'request':digest,'result':evidence_digest(result)}:
                raise ValueError('inventory_result_changed_or_unbound')
            origin='reused'
        else:
            _write(directory/'request.json',request)
            attempt=directory/'attempt.json'
            # Exclusive creation prevents ambiguous timeout/cancellation retries.
            with attempt.open('x') as handle:json.dump({'status':'started','request':digest},handle)
            try:
                result=await invoke(deepcopy(request),targeted.stage)
                _write(output,result)
                _write(receipt,{'request':digest,'result':evidence_digest(result)})
                _write(attempt,{'status':'finished','request':digest})
            except BaseException as exc:
                _write(attempt,{'status':'interrupted_or_failed','request':digest,'error_type':type(exc).__name__})
                raise
            origin='performed'
        if not isinstance(result,dict) or result.get('is_error') or not isinstance(result.get('json_data'),dict):
            raise ValueError('inventory_result_unusable')
        packets.append({'packet_id':targeted.stage,'block_ids':entry['block_ids'],'payload':result['json_data']})
        calls.append({'packet_id':targeted.stage,'origin':origin,'duration_ms':result.get('duration_ms'),
            'input_tokens':result.get('input_tokens'),'output_tokens':result.get('output_tokens')})
        _write(root/'progress.json',{'completed':len(packets),'total':len(passes),'calls':calls,'semantic_verified':False})
    if hashlib.sha256(Path(md_path).read_bytes()).hexdigest()!=manifest['source_md_sha256']:
        raise ValueError('inventory_md_changed')
    known={bid for entry in manifest['packets'] for bid in entry['block_ids']}
    enriched=merge_inventory_analysis(base,packets,known)
    enriched['inventory_enrichment'].update(status=status if len(packets)==len(passes) or status=='cancelled' else 'packet_budget',
        completed_packets=len(packets),total_packets=len(passes),calls=calls,
        source_md_sha256=manifest['source_md_sha256'],input_manifest=manifest)
    _write(root/'enriched_analysis.json',enriched)
    return enriched
