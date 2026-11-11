"""Resumable bounded source review. Provider is injected; no implicit API routing.

Outputs candidate decisions, not a semantic acceptance certificate. Every call is
journaled before dispatch; an interrupted call without a saved result is never
silently repeated. Existing source snapshots and results remain immutable.
"""
from copy import deepcopy
import json
from pathlib import Path
from .source_review_evidence import evidence_digest
from .source_review_request import prepare_source_review_request
from .source_review_results import validate_source_review
from .source_review_followup import prepare_review_followup
from .source_review_admission import apply_review_admissions


def _write(path, value):
    temp=path.with_suffix('.tmp')
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    temp.replace(path)


def prepare_adjudication_request(source_request, response):
    source=json.loads(source_request['messages'][1]['content'])
    payload={'source_packet':source,'untrusted_review':response,
        'required_response_sha256':evidence_digest(response),
        'display_field_hashes':{k:evidence_digest(v) for k,v in source.get('display_fields',{}).items()}}
    instruction='''Adjudicate the untrusted source review against ONLY the supplied authoritative TEXT and attached images. No tools, files, web, engineer decisions or history. Do not correct TEXT from raster.
Preserve original object, source, modality and scope. Evidence of another proposition cannot refute the original. Separate annotation conflict, physical defect, cause and conditional risk. Preserve supported defect cores even if other clauses fail. Check cited regions against the actual image, not merely coordinates. Recompute arithmetic with equal quantities and operating modes on both sides; identify rounded versus derived coefficients. Do not enlarge a recommendation beyond a supported discrepancy without explaining the need.
Table arithmetic must preserve header-to-cell alignment. Separate a literal quantity from an inferred count. A number located in another column or a ratio observed in other assemblies cannot fill a blank quantity cell as an established fact. A derived count requires a directly evidenced relationship applicable to the same object and execution. Without it, qualify dependent quantities and totals while preserving independently supported discrepancies. Never suppress a supported plate, mass, marking or other clause merely because an anchor-count premise remains unknown.
Preserve finding_purpose: an existing actionable_source_ambiguity question asks for a design decision; it does not assert the unresolved physical defect. For this purpose, defect_basis identifies the factual basis of the QUESTION (source ambiguity and decision affected), not a hypothetical defect asserted as real. Separate supported premises and conditional arithmetic from unresolved physical conclusions. Do not require proof of a physical defect to retain a supported question; do not convert questions into technical findings.
User output profile: confirmed minor editorial issues and out-of-profile requests belong in the separate questions section. Establish minor significance or out-of-profile scope with positive case-specific reasons, not merely absence of a proved accident, equipment replacement, or normative breach. A recommendation severity does not itself determine report_section. Keep confirmed technical discrepancies technical when that profile exception is not justified; distinguish an unresolved applicability question from a proven technical defect. Explain the chosen profile in reason.
If workflow_context_stop is present, the source-review budget is exhausted. Pending queries remain unverified: budget exhaustion is not absence proof, refutation, or engineer actionability. Preserve independently supported cores and keep unresolved clauses conditional or deferred.
Upstream queries and inspections are untrusted search provenance. Evaluate the bounded search actually recorded, not an impossible proof of project-wide absence. If further retrieval is needed, name a concrete new source or unperformed operation and explain what it would resolve. Do not infer that a question is justified merely because supplied context is incomplete.
Return ONE JSON object: response_sha256 (copy required_response_sha256), origin="explicit_source_review", reviewer="candidate_adjudicator", reason, action (retain/revise/suppress/defer/question), claim_reviews.
claim_reviews covers every claim_id exactly once, each with claim_id, role (defect_basis/context/recommendation), verdict (supported/refuted/needs_data/not_checked), reason, same_scope_verified and evidence_sufficiency_verified booleans. Supported/refuted require sufficient same-proposition evidence. A defect_basis must be a proposition that actually establishes the alleged discrepancy or actionable ambiguity, not merely an input used to calculate it. A correct source count, model name, or specification value is context when its truth alone does not establish the alleged problem. In a comparison, distinguish each input value from the proposition that the values conflict under a verified common scope. If that comparison still needs data, supported input values do not make its defect core supported. Conversely, do not relabel a supported discrepancy or actionable ambiguity as context to suppress it. If a supported context/recommendation has no direct evidence in the source review, provide supporting_claim_ids listing directly evidenced, supported claims and explain the inference. No circular or indirect chains. A defect_basis or refuted claim requires its own direct evidence; otherwise keep needs_data/not_checked.
Suppress only if every defect_basis is refuted. Retain only if every original substantive assertion is supported. Revise to preserve supported cores while qualifying unsupported statements; approved_fields must be an OBJECT with exactly problem,description,solution (nonempty Russian strings), and final_wording_verified must be a separate TOP-LEVEL boolean, never inside approved_fields. Defer only if no supported core exists. Question requires context_search_completed and engineer_action_required; with a supported core it requires question_basis minor_confirmed/outside_profile and profile_reason. For an EXISTING actionable_source_ambiguity question only, preserve that question_basis with profile_reason, context_search_completed=true, engineer_action_required=true, context_search_boundary (OBJECT with nonempty strings scope, actual_review, limitations; describe sources and actual search reviewed), and decision_affected (specific design decision). This exception must not demote an established technical defect into a question. Missing bounded context is an internal retrieval task, not an engineer question.
For retain/revise/question also provide final_evidence_refs, a nonempty array of objects with exactly claim_id and evidence_index (zero-based index into that claim's evidence). Select only citations you verified as support for the final wording from claims you adjudicated supported. Include the sources needed for each retained discrepancy, including the operand rows and object-to-assembly membership, headers and multiplicities used in retained calculations. A quotation from the right block is insufficient if it omits the relevant relation or quantity. Select these dependencies from supported claim evidence; do not invent new quotations or infer support from nearby rows. Do not select a citation just because it was retrieved. This explicit selection updates report evidence and resolves page/sheet labels from authoritative Markdown; it does not validate itself.
For retain/revise/question provide display_field_reviews as an OBJECT keyed by every supplied display_fields key, with {original_value_sha256:copy corresponding hash, value:string, consistent_with_final_claim:boolean, reason:string}. Review risk, consequence, severity and normative wording against the final text. A conditional risk is not disproved by lack of a recorded accident. An unverified standard remains unverified; do not invent applicability. Severity needs positive rationale, not just absence of contrary evidence. Retain cannot change any display field. An empty severity is invalid. For a revised severity use one exact report category: КРИТИЧЕСКОЕ, СУЩЕСТВЕННОЕ, ЭКОНОМИЧЕСКОЕ, ЭКСПЛУАТАЦИОННОЕ, РЕКОМЕНДАТЕЛЬНОЕ, ПРОВЕРИТЬ ПО СМЕЖНЫМ. Put the rationale in reason, never in the severity value. Questions are routed separately by action; question is not a severity category. A model decision is only a candidate, not automatic semantic acceptance.
'''
    return {'messages':[{'role':'system','content':instruction},{'role':'user','content':json.dumps(payload,ensure_ascii=False)}],
        'image_paths':source_request['image_paths'],'image_bindings':source_request['image_bindings']}


async def run_source_review_workflow(queue, finding, md, graphic_dir, work_dir, *, invoke, executor_id, is_cancelled=None, max_images=8, repair_quote_spacing=False, literal_image_hashes=(), auto_graphic_regions=False, repair_image_refs=False):
    """invoke(request, phase) -> provider result dict; caller owns authorization.

    Maximum five source reviews (initial plus four context revisions) and one
    adjudication, plus up to four explicitly selected source-only image readings.
    Automatic region mode instead adds up to four region readings and up to two re-reviews.
    No retries/fallback. A saved result is reused only for the exact
    same request and executor identity. No original finding is changed here.
    """
    if type(auto_graphic_regions) is not bool or (auto_graphic_regions and literal_image_hashes):
        raise ValueError('invalid_graphic_reading_mode')
    if type(max_images) is not int or not 0 <= max_images <= 16:
        raise ValueError('invalid_image_budget')
    if (not isinstance(literal_image_hashes, (tuple, list)) or len(literal_image_hashes) > 4
        or any(not isinstance(h, str) or len(h) != 64 for h in literal_image_hashes)
        or len(set(literal_image_hashes)) != len(literal_image_hashes)):
        raise ValueError('invalid_literal_image_selection')
    root=Path(work_dir);root.mkdir(parents=True,exist_ok=True)
    if not isinstance(executor_id,str) or not executor_id:raise ValueError('executor_identity_required')
    binding={'queue':evidence_digest(queue),'finding':evidence_digest(finding),
             'md':evidence_digest(md),'executor_id':executor_id,'workflow_version':3,'max_images':max_images}
    if auto_graphic_regions:
        binding['auto_graphic_regions'] = 2
    if literal_image_hashes:
        binding['literal_image_hashes'] = list(literal_image_hashes)
    manifest=root/'binding.json'
    if manifest.exists():
        if json.loads(manifest.read_text())!=binding:raise ValueError('workflow_inputs_changed')
    else:
        with manifest.open('x') as f:json.dump(binding,f)
    current=deepcopy(queue);calls=[]

    async def call(request, phase, step):
        if is_cancelled and is_cancelled():raise ValueError('workflow_cancelled')
        directory=root/f'{step:02d}_{phase}';directory.mkdir(exist_ok=True)
        digest=evidence_digest({'request':request,'executor_id':executor_id})
        stored=directory/'request.json';result_path=directory/'result.json'
        if stored.exists():
            if json.loads(stored.read_text())!={'sha256':digest,'request':request}:raise ValueError('saved_request_changed')
        else:_write(stored,{'sha256':digest,'request':request})
        if result_path.exists():
            result=json.loads(result_path.read_text())
            calls.append({'phase':phase,'step':step,'reused':True})
        else:
            # Exclusive create also prevents duplicate live dispatch by concurrent workers.
            with (directory/'attempt.json').open('x') as f:json.dump({'status':'started','request_sha256':digest},f)
            try:
                result=await invoke(request,phase)
                if not isinstance(result,dict):raise ValueError('invalid_provider_result')
                _write(result_path,result)
                _write(directory/'attempt.json',{'status':'finished','request_sha256':digest,'model_error':bool(result.get('is_error'))})
            except BaseException as exc:
                _write(directory/'attempt.json',{'status':'interrupted_or_failed','request_sha256':digest,'error_type':type(exc).__name__})
                raise
            calls.append({'phase':phase,'step':step,'reused':False})
        if result.get('is_error') or not isinstance(result.get('json_data'),dict):raise ValueError('provider_result_not_usable')
        return result['json_data']

    readings = []
    if literal_image_hashes:
        from .graphic_literal_reading import prepare_literal_reading, bind_literal_reading
        initial = prepare_source_review_request(current,finding,md,max_images=max_images)
        literal_requests = [prepare_literal_reading(initial, h) for h in literal_image_hashes]
        for index, literal_request in enumerate(literal_requests):
            response = await call(literal_request, 'literal_reading', index)
            readings.append(bind_literal_reading(literal_request, response))
        _write(root/'independent_graphic_readings.json', readings)

    for step in range(5):
        request=prepare_source_review_request(current,finding,md,max_images=max_images)
        if readings:
            from .graphic_literal_reading import attach_literal_readings
            request = attach_literal_readings(request, readings)
        response=await call(request,'review',step)
        supplied={(im['block_id'],im['sha256']) for im in request['image_bindings']}
        for packet in current['packets']:
            if packet['finding_id']==finding['id']:
                for receipt in packet.get('graphic_attachments',[]):
                    receipt['attached_to_model']=(receipt['block_id'],receipt['sha256']) in supplied
        validation=validate_source_review(response,current,[finding],md)
        _write(root/f'{step:02d}_validation.json',validation)
        if repair_quote_spacing and validation['errors'] and all(e.endswith(':quote_not_in_source') for e in validation['errors']):
            from .source_review_quote_spacing import repair_table_quote_spacing
            repaired, spacing_receipt = repair_table_quote_spacing(response, md)
            _write(root/f'{step:02d}_quote_spacing.json', spacing_receipt)
            if spacing_receipt['repairs']:
                repaired_validation = validate_source_review(repaired,current,[finding],md)
                _write(root/f'{step:02d}_repaired_validation.json',repaired_validation)
                if not repaired_validation['errors']:
                    response, validation = repaired, repaired_validation
        if validation['errors']:raise ValueError('invalid_source_review')
        if auto_graphic_regions and step == 0:
            from .graphic_region_reading import prepare_review_region_readings, reading_on_parent, region_was_read
            from .graphic_literal_reading import bind_literal_reading, attach_literal_readings
            _write(root/'00_initial_validation.json', validation)
            for region_round in range(2):
                manifest = prepare_review_region_readings(request, response, root/'graphic_regions')
                _write(root/f'graphic_region_manifest_{region_round}.json', manifest)
                novel = [region for region in manifest['regions'] if not region_was_read(region, readings)]
                if not novel:break
                for index, region in enumerate(novel[:max(0,4-len(readings))]):
                    literal = await call(region['request'], f'region_literal_{region_round}_{index}', 0)
                    readings.append(reading_on_parent(region, bind_literal_reading(region['request'], literal)))
                _write(root/'independent_graphic_readings.json', readings)
                request = attach_literal_readings(request, readings)
                response = await call(request, f'region_review_{region_round}', 0)
                validation = validate_source_review(response,current,[finding],md)
                _write(root/f'00_region_validation_{region_round}.json', validation)
                if repair_image_refs and validation['errors'] and all(e.endswith(':graphic_not_supplied') for e in validation['errors']):
                    from .source_review_image_refs import repair_unknown_image_labels
                    repaired, receipt = repair_unknown_image_labels(response, request, md)
                    _write(root/f'00_region_image_refs_{region_round}.json', receipt)
                    if receipt['repairs']:
                        checked=validate_source_review(repaired,current,[finding],md)
                        _write(root/f'00_region_repaired_validation_{region_round}.json', checked)
                        if not checked['errors']:response,validation=repaired,checked
                if validation['errors']:raise ValueError('invalid_region_source_review')
        followup=prepare_review_followup(response,current,[finding],md,graphic_dir,max_images=max_images)
        _write(root/f'{step:02d}_followup.json',followup)
        if followup['status']=='prepared_awaiting_dispatch_authorization':
            if step==4:
                # No further source call. Adjudicate what was actually checked;
                # reaching a budget neither refutes unknowns nor erases cores.
                followup={**followup,'status':'context_budget_exhausted'}
                stop={'reason':'maximum_five_source_reviews',
                    'pending_queries':[q for claim in response.get('claims',[])
                        for q in claim.get('context_queries',[])],
                    'absence_proven':False,'expanded_context_reviewed':False}
                _write(root/'04_budget_stop.json',stop)
                request=deepcopy(request)
                payload=json.loads(request['messages'][1]['content'])
                payload['workflow_context_stop']=stop
                request['messages'][1]['content']=json.dumps(payload,ensure_ascii=False)
                request['request_sha256']=evidence_digest({'messages':request['messages'],'images':request['image_bindings']})
                _write(root/f'{step:02d}_followup.json',followup)
                break
            current=followup['expanded_queue']
            continue
        break
    if auto_graphic_regions:
        from .graphic_region_reading import prepare_review_region_readings, region_was_read
        final_regions=prepare_review_region_readings(request,response,root/'graphic_regions')
        pending=[r['source'] for r in final_regions['regions'] if not region_was_read(r,readings)]
        unresolved_skips=final_regions.get('skipped',[])
        region_coverage={'pending_regions':pending,'skipped':unresolved_skips,
            'semantic_verified':False,'boundary':'Geometry coverage is not factual verification.'}
        _write(root/'graphic_region_completion.json',region_coverage)
        if pending or unresolved_skips:
            report={'status':'not_completed','reason':'graphic_region_reading_budget_or_geometry',
                'calls':calls,'pending_source_findings':[deepcopy(finding)],'queue':current,
                'response':response,'region_coverage':region_coverage,
                'semantic_acceptance':False,'original_findings_mutated':False}
            _write(root/'candidate.json',report)
            return report
    decision=await call(prepare_adjudication_request(request,response),'adjudication',5)
    entry={'queue':current,'response':response,'decision':decision}
    preview,receipts=apply_review_admissions({'findings':[finding]},[entry],md)
    report={'status':'candidate_ready' if receipts[0]['status']=='applied' else 'candidate_rejected',
        'admission_receipts':receipts,'calls':calls,'source_review_stop':followup['status'],
        'semantic_acceptance':False,'original_findings_mutated':False,'entry':entry}
    _write(root/'candidate.json',report)
    return report


async def run_source_review_workflow_phase(output_dir, md, *, invoke, executor_id, max_findings, is_cancelled=None, auto_graphic_regions=False):
    """Explicit experimental opt-in; prepare candidates, never mutate findings.

    Rebuild inputs AFTER absence correction so response hashes bind current text.
    Raw candidates use a separate file, never the reviewed admission inbox.
    """
    if type(auto_graphic_regions) is not bool:
        raise ValueError('invalid_graphic_reading_mode')
    from .source_review_queue import build_source_review_queue
    from .source_review_graphics import attach_graphic_receipts
    if not isinstance(max_findings,int) or isinstance(max_findings,bool) or max_findings<1:
        raise ValueError('positive_explicit_finding_budget_required')
    root=Path(output_dir)
    document=json.loads((root/'03_findings.json').read_text())
    if not isinstance(document,dict) or not isinstance(document.get('findings'),list):
        raise ValueError('unsupported_findings_container')
    findings=document['findings'];queue=build_source_review_queue(findings,md)
    detail_dir = root/'object_graphic_details'
    if (detail_dir/'graphic_detail_manifest.json').is_file():
        queue['_operation_graphic_detail_dir'] = str(detail_dir.resolve())
    attach_graphic_receipts(queue,md,root/'blocks_stage02_100')
    _write(root/'03_source_review_queue.json',queue)
    packets=[p for p in queue['packets'] if p['status']=='pending']
    reports=[]
    target=root/'03_source_review_candidates.json'
    _write(target,{'status':'running','reports':reports,'semantic_acceptance':False})
    for packet in packets[:max_findings]:
        fid=packet['finding_id']
        if is_cancelled and is_cancelled():
            reports.append({'finding_id':fid,'status':'cancelled'});break
        # Hash-based directory: project IDs cannot become paths; changed inputs
        # cannot reuse earlier remote results.
        one={**queue,'packets':[packet],'pending_count':1}
        key_data={'packet':packet,'source':queue['source_md_sha256'],'executor':executor_id}
        if auto_graphic_regions:key_data['auto_graphic_regions']=2
        key=evidence_digest(key_data)
        try:
            report=await run_source_review_workflow(one,packet['finding_snapshot'],md,root/'blocks_stage02_100',
                root/'source_review_workflows'/key,invoke=invoke,executor_id=executor_id,is_cancelled=is_cancelled,
                max_images=16 if queue.get('_operation_graphic_detail_dir') else 8,
                auto_graphic_regions=auto_graphic_regions)
            reports.append({'finding_id':fid,**report})
        except Exception as exc:
            reports.append({'finding_id':fid,'status':'not_completed','error_type':type(exc).__name__,'reason':str(exc)})
        _write(target,{'status':'running','reports':reports,'semantic_acceptance':False})
    summary={'status':'finished','reports':reports,'semantic_acceptance':False,
        'candidate_ready':sum(r['status']=='candidate_ready' for r in reports),
        'not_completed':sum(r['status']=='not_completed' for r in reports),
        'not_attempted':len(packets)-len(reports),'original_findings_mutated':False}
    _write(target,summary)
    return summary


def direct_cli_executor(model, *, project_id, timeout=900, reasoning_effort='high'):
    """Explicit direct-CLI adapter; no OpenRouter, fallback or retry here.

    Creation is local only. Caller must authorize every dispatch under its policy.
    The callback can be injected into PipelineStageContext for experimental runs.
    """
    if model not in ('codex/gpt-6-astra','claude/claude-sonnet-5-5'):
        raise ValueError('unsupported_direct_review_model')
    async def invoke(request, phase):
        from dataclasses import asdict
        if model.startswith('claude/'):
            from backend.app.services.llm.claude_json_runner import run_claude_json_messages
            result=await run_claude_json_messages(request['messages'],model=model,
                image_paths=request['image_paths'],timeout=timeout,reasoning_effort=reasoning_effort,
                stage='source_review_'+phase,project_id=project_id)
        else:
            from backend.app.services.llm.codex_runner import run_codex_json_messages
            result=await run_codex_json_messages(request['messages'],model=model,
                image_paths=request['image_paths'],timeout=timeout,reasoning_effort=reasoning_effort,
                stage='source_review_'+phase,project_id=project_id,allowed_tools='')
        return asdict(result)
    return invoke
