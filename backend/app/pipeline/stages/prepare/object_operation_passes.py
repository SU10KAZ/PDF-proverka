"""Target incomplete object checks using current-run sources, never historical answers."""
import hashlib
import json
import re
from pathlib import Path
from backend.app.pipeline.stages.block_context.contract import resolve_blocks_dir
from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry


def build_object_operation_passes(output_dir, md_path, *, max_requests=8):
    from .codex_targeted_findings import CodexTargetedPass
    output_dir, md_path = Path(output_dir), Path(md_path)
    text_path = output_dir/'02_text_analysis.json'
    if not text_path.is_file():
        return []
    data = json.loads(text_path.read_text())
    md = md_path.read_text(encoding='utf-8')
    headers = list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$',md,re.M|re.I))
    blocks = {h.group(2): {'kind':h.group(1).upper(), 'content':md[h.start():headers[i+1].start() if i+1<len(headers) else len(md)]}
              for i,h in enumerate(headers)}
    graph_path = output_dir/'document_graph.json'
    graph = json.loads(graph_path.read_text()) if graph_path.is_file() else None
    if graph is not None and graph.get('source_md_sha256') != hashlib.sha256(md.encode()).hexdigest():
        graph = None  # Unbound legacy graphs cannot interpret sheet-number queries.
    coverage = reconcile_object_registry(data, set(blocks))
    crops = resolve_blocks_dir(output_dir)
    index_path = crops/'index.json'
    index = json.loads(index_path.read_text()) if index_path.is_file() else {}
    files = {b.get('block_id'):b.get('file') for b in index.get('blocks',[]) if isinstance(b,dict)}
    passes, backlog, planned = [], [], []
    for obj in coverage['objects']:
        operations = [r for r in coverage['operations'] if r['object_id']==obj['id'] and r['status'] in ('not_checked','needs_data')]
        if not operations:
            continue
        if len(passes)>=max_requests:
            backlog.append({'object_id':obj['id'],'operations':operations,'status':'not_checked','reason':'request_budget'})
            continue
        from .recovery_context import search_md_context, expand_operation_queries
        source_ids = list(obj['source_refs'])
        searches, new_by_operation = {}, {}
        eligible = []
        for operation in operations:
            if operation['status'] != 'needs_data':
                eligible.append(operation)
                continue
            queries = operation.get('receipt', {}).get('context_queries', [])
            expansion = expand_operation_queries(queries)
            search = search_md_context(md, expansion['queries'], exclude_block_ids=set(obj['source_refs']), source_graph=graph)
            search['query_expansion'] = expansion
            # Sources are supplied once in source_context. Search receipts keep
            # navigation/provenance only, including hits omitted by the budget.
            searches[operation['operation']] = {**search, 'hits':[
                {k:v for k,v in hit.items() if k!='content'} for hit in search['hits']]}
            new_ids = [h['block_id'] for h in search['hits'] if h['block_id'] in blocks]
            if not new_ids:
                backlog.append({'object_id':obj['id'],'operations':[operation], 'status':'needs_data',
                                'reason':'no_new_context', 'search':search})
                continue
            new_by_operation[operation['operation']] = new_ids
            source_ids.extend(new_ids)
            eligible.append(operation)
        operations = eligible
        if not operations:
            continue
        context, images, image_ids, omitted, size = [], [], [], [], 0
        for bid in dict.fromkeys(source_ids):
            block = blocks[bid]
            if size+len(block['content'])>60000:
                omitted.append({'block_id':bid,'reason':'text_budget'})
                continue
            context.append({'block_id':bid,**block})
            size+=len(block['content'])
            if block['kind'] in ('GRAPHIC','IMAGE'):
                name=files.get(bid)
                image=(crops/name).resolve() if isinstance(name,str) else None
                if image and image.is_relative_to(crops.resolve()) and image.is_file() and len(images)<8:
                    images.append(image);image_ids.append(bid)
                else:
                    omitted.append({'block_id':bid,'reason':'image_unavailable_or_budget'})
        allowed=sorted({b['block_id'] for b in context if b['kind'] in ('TEXT','TABLE')}|set(image_ids))
        eligible = []
        for operation in operations:
            new_ids = new_by_operation.get(operation['operation'])
            if new_ids is not None and not set(new_ids).intersection(allowed):
                backlog.append({'object_id':obj['id'],'operations':[operation],'status':'needs_data',
                                'reason':'new_context_not_supplied', 'omitted':omitted})
            else:
                eligible.append(operation)
        operations = eligible
        if not operations:
            continue
        packet={'object':obj,'operations':operations,'source_context':context,'context_searches':searches,
                'attached_image_ids_in_order':image_ids,'allowed_source_refs':allowed,'omitted':omitted,
                'scope_complete':False}
        key=hashlib.sha256(obj['id'].encode()).hexdigest()[:16]
        instructions='''Perform the listed unresolved operations for this object from current project sources.
MD TEXT/TABLE is authoritative. GRAPHIC summaries are navigation only; inspect the
attached images before making graphic claims. Match instance, execution, scope,
units and physical basis before comparing. Never infer compliance from a missing
comparison. If a counterpart, calculation or applicability source is absent, return
needs_data with exact missing context and context_queries (up to six literal
object marks or short phrases for MD search), not a refuted or confirmed defect.
Return JSON with object_operation_results (object_id, operation, status checked/
not_applicable/needs_data/not_checked, reason in Russian, source_refs) and findings.
Each finding must have object_id, operation, severity, problem, description, solution,
page/sheet when established, evidence with exact block_id, and report_section:
technical_findings for confirmed technical conflicts, questions for conditional,
minor editorial or out-of-profile clarifications. Separate questions
from proven conflicts in the wording; never invent a norm or universal tolerance.
Preserve all listed operations, even those that remain unresolved. Do not declare
whole-project coverage or silently add an operation outside the supplied list.'''
        passes.append(CodexTargetedPass('object_operations_'+key,'03_object_operations_'+key+'.json',
            [{'role':'system','content':instructions},{'role':'user','content':json.dumps(packet,ensure_ascii=False)}],tuple(images)))
        planned.append({'object_id':obj['id'],'stage':passes[-1].stage,'operations':[r['operation'] for r in operations],'omitted':omitted})
    (output_dir/'03_object_operation_plan.json').write_text(json.dumps({
        'requests':planned,'backlog':backlog,'invalid_objects':coverage['invalid_objects'],
        'unresolved_source_blocks':coverage['unresolved_source_blocks'],
        'coverage_complete':False,'history_supplied':False},ensure_ascii=False,indent=2))
    return passes


def validate_object_operation_payload(request,payload):
    packet=json.loads(request.messages[-1]['content'])
    obj=packet['object']; allowed=packet['allowed_source_refs']
    operations={r['operation'] for r in packet['operations']}
    raw_receipts=payload.get('object_operation_results',[])
    receipts, receipt_pending = [], []
    for receipt in raw_receipts if isinstance(raw_receipts,list) else []:
        if not isinstance(receipt,dict):
            receipt_pending.append(receipt)
            continue
        if (receipt.get('object_id')!=obj['id'] or
                not isinstance(receipt.get('operation'),str) or receipt['operation'] not in operations):
            receipt_pending.append(receipt)
            continue
        refs=receipt.get('source_refs')
        if receipt.get('status') in ('checked','not_applicable') and (
                not isinstance(refs,list) or not refs or any(ref not in allowed for ref in refs)):
            receipt_pending.append(receipt)
            receipts.append({**receipt,'status':'not_checked','reason':'Sources not supplied to this operation pass.'})
        else:
            receipts.append(receipt)
    coverage=reconcile_object_registry({'object_registry':[obj],'object_operation_results':receipts},
                                       set(obj['source_refs'])|set(allowed))
    accepted,pending=[],[]
    items=payload.get('findings',[])
    for finding in items if isinstance(items,list) else []:
        valid=isinstance(finding,dict) and finding.get('object_id')==obj['id'] and isinstance(finding.get('operation'),str) and finding['operation'] in operations
        evidence=finding.get('evidence') if isinstance(finding,dict) else None
        if not valid or not isinstance(evidence,list) or not any(isinstance(e,dict) and e.get('block_id') in allowed for e in evidence):
            pending.append({'finding':finding,'reason':'unaddressed_operation_or_evidence'})
        else:
            accepted.append({**finding,'operation_semantic_verified':False})
    return {**payload,'object_operation_results':receipts,'findings':accepted,'operation_pending':pending,'operation_coverage':coverage,
            'requested_operations':sorted(operations),'unverified_operation_receipts':receipt_pending}
