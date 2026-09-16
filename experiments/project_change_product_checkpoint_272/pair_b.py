"""Pair B checkpoint orchestration. All inference/admission components reused verbatim.

No retry, no regeneration, no tuning. Requires Pair A browser PASS before preflight.
"""
import asyncio
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys

from .snapshot import ROOT, OUT, BASE, REPO, LIVE_A, V4, read, sha, build
sys.path.insert(0,str(ROOT/'controlled_inference_f1_f4_f2_v3/runtime_deps'))
spec=importlib.util.spec_from_file_location('frozen_pair_a_live',LIVE_A/'live_run.py')
live=importlib.util.module_from_spec(spec)
# Import the frozen runner without writing a __pycache__ into its artifact tree.
bytecode_policy=sys.dont_write_bytecode;sys.dont_write_bytecode=True
try:spec.loader.exec_module(live)
finally:sys.dont_write_bytecode=bytecode_policy
from experiments.project_change_272.policy import admitted_pairs
from experiments.project_change_f5_272.common import fingerprint, code_hashes
from experiments.project_change_f5_272.run import assert_answer_blind
from experiments.project_change_f5_272.repair_v7 import structural
from experiments.project_change_f5_272.observations_v7 import evaluate as boundary_evaluate
from experiments.project_change_f5_272.boundary_v7 import package_completeness
from experiments.project_change_f5_272.contract_adapters import has_source_text, typed_preparation
from experiments.project_change_contracts_272.witnesses import raster_locator_errors
from experiments.project_change_semantic_codex_272.provider import CodexProvider, runtime_identity
from experiments.project_change_semantic_codex_272.serialization import prepare_request_bytes, request_sha256
from experiments.project_change_post_inference_repair_272.identity import contract
from experiments.project_change_post_inference_repair_272.replay import f2_verdict
from experiments.project_change_post_inference_repair_v4_272.repair import repair
from experiments.project_change_semantic_codex_v2_272.contracts import resolve_ownership

B=OUT/'pair_b_live'
OVERLAY=ROOT/'fresh_dev_sample_f5_pipeline_v7/all_79'
TOOLROOT=Path('/tmp/pair_a_v2_ocr/root')


def now():return datetime.now(timezone.utc).isoformat()
def commit():return subprocess.check_output(['git','-C',str(REPO),'rev-parse','HEAD'],text=True).strip()
def save(name,value):
    p=OUT/name;p.parent.mkdir(parents=True,exist_ok=True)
    data=value if isinstance(value,str) else json.dumps(value,ensure_ascii=False,indent=2,sort_keys=True)+'\n'
    if p.exists():
        if p.read_text()!=data:raise ValueError('Immutable output drift: '+str(p))
    else:p.write_text(data)


def codes():
    found=code_hashes()
    for folder in ('project_change_contracts_v2_272','project_change_semantic_codex_272','project_change_semantic_codex_v2_272',
                   'project_change_post_inference_repair_272','project_change_post_inference_repair_v2_272',
                   'project_change_post_inference_repair_v3_272','project_change_post_inference_repair_v4_272',
                   'project_change_product_checkpoint_272'):
        found.update({str(p.relative_to(REPO)):sha(p) for p in (REPO/'experiments'/folder).glob('*') if p.is_file()})
    return found


def preflight():
    assert read(OUT/'PAIR_A_UI_CHECK.json')['status']=='PASS','PAIR A UI must PASS first'
    assert not (OUT/'PAIR_B_INFERENCE_FREEZE.json').exists(),'Already frozen'
    pair=admitted_pairs('DEV',indices=[8])[0]
    assert pair['pair_key']=='caea6d2810c334ec0368de8e'
    assert (pair['old']['structure']['pages'],pair['new']['structure']['pages'])==(108,188)
    inventories={s:read(BASE/f'pair_8/DOCUMENT_INVENTORY_{s.upper()}.json') for s in ('old','new')}
    baseline=[r for r in read(BASE/'PACKAGE_INDEX.json') if r['package'].startswith('pair_8/')]
    paths=sorted((BASE/'pair_8/packages').glob('*.json'))
    assert {r['package'] for r in baseline}=={str(p.relative_to(BASE)) for p in paths}
    system=(LIVE_A/'PROMPT.txt').read_text();schema=read(LIVE_A/'OUTPUT_SCHEMA.json')
    assert sha(LIVE_A/'PROMPT.txt')==sha(REPO/'experiments/project_change_contracts_v3_272/prompt.txt')
    save('pair_b_live/PROMPT.txt',system);save('pair_b_live/OUTPUT_SCHEMA.json',schema)
    issues=[];index=[];plan=[];inputs={};serial=[];evidence_hashes={}
    for path in paths:
        rel=str(path.relative_to(BASE));p=read(path);o=read(OVERLAY/rel);key=p['candidate_subject']['candidate_id'];local=[]
        def check(ok,code):
            if not ok:local.append(code)
        check(p['provenance']['pair_index']==8 and p['provenance']['pair_key']==pair['pair_key'],'PAIR_ID')
        check(p['package_hash']==next(r['package_hash'] for r in baseline if r['package']==rel),'BASE_HASH')
        check(o['boundary_hash']==fingerprint({k:v for k,v in o.items() if k!='boundary_hash'}),'OVERLAY_HASH')
        check(o['base_file_sha256']==sha(path),'BASE_FILE_HASH')
        local+=structural(p,o['claim_boundary_certificates'])
        certs=boundary_evaluate(p);check(certs==o['claim_boundary_certificates'],'BOUNDARY_REPLAY')
        summary=package_completeness(certs,baseline_status=p['completeness'])
        check(all(o[k]==v for k,v in summary.items()),'BOUNDARY_SUMMARY')
        assert_answer_blind(p)
        body=p['f1_requirement_package'];packet=p['evidence_packet']
        check(body['package_hash']==fingerprint({k:v for k,v in body.items() if k!='package_hash'}),'F1_HASH')
        check(packet['packet_id']==fingerprint({k:v for k,v in packet.items() if k!='packet_id'})[:24],'PACKET_HASH')
        check(packet['source_package_hash']==body['package_hash'],'SOURCE_PACKAGE_HASH')
        check(packet['evidence_coverage']['delivery_hash']==fingerprint(packet['evidence']),'DELIVERY_HASH')
        check(fingerprint(typed_preparation(packet,p['candidate_subject']))==fingerprint(p['typed_state_skeleton']),'V4_BINDING_REPLAY')
        ids={}
        for side in ('old','new'):
            for e in packet['evidence'][side]:
                check(e['evidence_id'] not in ids,'DUPLICATE_EVIDENCE');ids[e['evidence_id']]=e
                check(e['side']==side and e['document_version']==pair[side]['document_version'],'SIDE_VERSION')
                check(e['source_receipt']==pair[side]['artifacts']['pdf'],'PDF_RECEIPT')
                check(inventories[side]['pages'][e['page']-1]['status']=='INDEXED','EXCLUDED_PAGE')
                check(bool(e['raster']) or has_source_text(e['quote']),'EMPTY_EVIDENCE')
                evidence_hashes[key+'/'+e['evidence_id']]=fingerprint(e)
                if e.get('raster'):
                    rp=(BASE/e['raster']['path']).resolve()
                    check(rp.is_relative_to(BASE/'rasters'),'RASTER_PATH')
                    check(rp.is_file() and sha(rp)==e['raster']['sha256'],'RASTER_HASH')
                    inputs[str(rp)]=sha(rp)
        for state in p['typed_state_skeleton']['states'].values():
            for binding in state['evidence_bindings']:
                e=ids.get(binding['evidence_id'])
                check(bool(e and (e.get('raster') or has_source_text(e.get('quote')))),'EMPTY_BINDING')
        runtime_packet=live.packet_runtime(p)
        for g in p['graphic_bindings']:
            e=next(e for es in runtime_packet['evidence'].values() for e in es if e['evidence_id']==g['evidence_id'])
            check(raster_locator_errors(g,e)==g['errors'],'F4_REPLAY')
        data=live.model_data(p,o);assert_answer_blind(data)
        call=o['completeness']!='MISSING'
        input_characters=len(system)+len(json.dumps(data,ensure_ascii=False))
        nimages=sum(e.get('source_kind')=='PDF_RASTER_CROP' for es in packet['evidence'].values() for e in es)
        check(nimages<=8,'IMAGE_BUDGET')
        if call:check(input_characters<=60000,'PROVIDER_TEXT_LIMIT')
        # Invoke the unchanged serializer; a rejection is an audit failure, never truncation.
        try:
            prompt_bytes,images=prepare_request_bytes(system,data,runtime_packet)
        except ValueError as exc:
            prompt_bytes=None
            local.append('SERIALIZATION_REJECTED: '+str(exc))
        save(f'pair_b_live/inputs/{key}/MODEL_INPUT.json',data)
        exact=None;request_hash=None
        input_paths=[path,OVERLAY/rel,B/f'inputs/{key}/MODEL_INPUT.json']
        if prompt_bytes is not None:
            save(f'pair_b_live/inputs/{key}/EXACT_PROMPT.txt',prompt_bytes.decode('utf-8'))
            request_hash=request_sha256(prompt_bytes)
            exact=sha(B/f'inputs/{key}/EXACT_PROMPT.txt')==request_sha256(prepare_request_bytes(system,read(B/f'inputs/{key}/MODEL_INPUT.json'),runtime_packet)[0])
            check(exact,'CANONICAL_REQUEST_BYTES_MISMATCH')
            input_paths.append(B/f'inputs/{key}/EXACT_PROMPT.txt')
        serial.append(dict(package_id=key,sha256=request_hash,exact=exact,
                           input_characters=input_characters,status='PASS' if exact else 'REJECTED_FROZEN_LIMIT'))
        for file in input_paths:
            inputs[str(file)]=sha(file)
        index.append(dict(candidate_id=key,package=rel,base_package_hash=p['package_hash'],boundary_hash=o['boundary_hash'],
                          completeness=o['completeness'],correspondence=p['candidate_subject']['confidence']))
        plan.append(dict(key=key,package=rel,completeness=o['completeness'],action='MODEL_CALL' if call else 'NO_CALL_MISSING_EVIDENCE',
                         exact_prompt_sha256=request_hash,input_characters=input_characters,images=nimages))
        issues.extend(dict(package=rel,issue=x) for x in sorted(set(local)))
    runtime=runtime_identity();prior_runtime=read(LIVE_A/'PAIR_A_INFERENCE_FREEZE.json')['runtime']
    if runtime!=prior_runtime:issues.append(dict(issue='PAIR_A_RUNTIME_DRIFT',current=runtime,previous=prior_runtime))
    # Freeze all reused admission code against the authoritative V4 freeze.
    v4codes=read(V4/'REPAIR_CODE_FREEZE.json')['code']
    for relative,digest in v4codes.items():
        if sha(REPO/relative)!=digest:issues.append(dict(issue='V4_CODE_DRIFT',path=relative))
    tesseract=TOOLROOT/'usr/bin/tesseract'
    assert sha(tesseract)==read(ROOT/'fresh_dev_pair_a_post_inference_repair_v2/DELIVERED_NUMERIC_FACTS.json')['tesseract_sha256']
    inputs[str(tesseract)]=sha(tesseract)
    for file in (LIVE_A/'live_run.py',LIVE_A/'PROMPT.txt',LIVE_A/'OUTPUT_SCHEMA.json',OUT/'PAIR_A_UI_CHECK.json'):
        inputs[str(file)]=sha(file)
    for side in ('old','new'):
        inputs[pair[side]['artifacts']['pdf']['path']]=pair[side]['artifacts']['pdf']['sha256']
    planned=sum(r['action']=='MODEL_CALL' for r in plan)
    save('PAIR_B_PACKAGE_INDEX.json',dict(pair_index=8,pair_key=pair['pair_key'],package_count=len(index),packages=index,
        completeness=dict(Counter(r['completeness'] for r in index)),correspondence=dict(Counter(r['correspondence'] for r in index))))
    save('pair_b_live/CALL_PLAN.json',dict(packages=plan,total_packages=len(plan),planned_model_calls=planned,retries=0,parallelism=1,artificial_limit=None))
    save('PAIR_B_STRUCTURAL_AUDIT.json',dict(status='FAIL' if issues else 'PASS',issues=issues,packages=len(plan),model_calls=0,
        planned_model_calls=planned,frozen_inventory_exact=True,source_audit_opened=False))
    save('PAIR_B_REQUEST_SERIALIZATION.json',serial)
    for file in (OUT/'PAIR_B_PACKAGE_INDEX.json',B/'CALL_PLAN.json',OUT/'PAIR_B_STRUCTURAL_AUDIT.json',B/'PROMPT.txt',B/'OUTPUT_SCHEMA.json'):
        inputs[str(file)]=sha(file)
    freeze=dict(at=now(),status='BLOCKED_STRUCTURAL_FAIL' if issues else 'READY',code_commit=commit(),code=codes(),
        runtime=runtime,prompt_sha256=sha(B/'PROMPT.txt'),input_files=inputs,
        package_hashes={r['candidate_id']:r['base_package_hash'] for r in index},
        boundary_hashes={r['candidate_id']:r['boundary_hash'] for r in index},evidence_hashes=evidence_hashes,
        canonical_request_hashes={r['package_id']:r['sha256'] for r in serial},packages=len(plan),planned_model_calls=planned,
        model='gpt-6-astra',reasoning='xhigh',retries=0,source_audit_opened=False,validation='NOT OPENED',final_holdout='NOT OPENED',
        architecture_modified=False,openrouter_calls=0,claude_calls=0,
        downstream=['normalization','binding V4','applicability V4','NumericConflictGuard','F2','final admission','resolve_ownership'],
        numeric_extractor='Unchanged V2 local OCR on delivered cited crops; output directory only rebound')
    save('PAIR_B_INFERENCE_FREEZE.json',freeze)
    print(json.dumps(dict(status=freeze['status'],packages=len(plan),planned_model_calls=planned,issues=issues),ensure_ascii=False))


def verify():
    f=read(OUT/'PAIR_B_INFERENCE_FREEZE.json')
    assert f['status']=='READY','Structural audit FAIL: STOP'
    assert commit()==f['code_commit'],'CODE COMMIT DRIFT'
    for relative,digest in f['code'].items():assert sha(REPO/relative)==digest,relative
    for file,digest in f['input_files'].items():assert sha(file)==digest,file
    assert runtime_identity()==f['runtime'],'RUNTIME DRIFT'
    return f


def downstream():
    frozen=verify();plan=read(B/'CALL_PLAN.json')['packages']
    assert len(list((B/'calls').glob('*/SUCCESS.json')))==frozen['planned_model_calls']
    # Reuse the exact OCR extractor. It has no admission authority; only detects conflicts.
    from experiments.project_change_post_inference_repair_v2_272 import extract_numeric
    extract_numeric.OUT=B;extract_numeric.LIVE=B
    previous=sys.argv;sys.argv=[previous[0],str(TOOLROOT)]
    try:extract_numeric.main()
    finally:sys.argv=previous
    facts=read(B/'DELIVERED_NUMERIC_FACTS.json')['facts']
    rows=[];events=[];evidence={};display=[]
    for item in plan:
        key=item['key'];p=read(BASE/item['package']);packet=live.packet_runtime(p)
        evidence.update({e['evidence_id']:e for es in packet['evidence'].values() for e in es})
        if item['action']=='MODEL_CALL':
            raw=read(OUT/f'PAIR_B_RAW_RESPONSES/{key}.json')
            n=repair(raw,packet,contract(p['candidate_subject'],packet),[f for f in facts if f['candidate_id']==key])
        else:
            raw={};n=dict(case_token=key,status='NOT_EVALUATED',effective_verdict='REVIEW',call_status='NO_CALL_MISSING_EVIDENCE',
                          reason='Frozen V7 completeness MISSING: insufficient evidence',completeness='MISSING')
        save(f'pair_b_live/normalized_responses/{key}.json',n)
        bound={s:sorted({b['provenance']['page'] for b in n.get(s+'_state',{}).get('evidence_bindings',[])}) for s in ('old','new')}
        row=dict(package_id=key,raw=raw,normalized=n,status=n['effective_verdict'],f2_verdict=f2_verdict(n),evidence_pages=bound,
            source_label=p['candidate_subject']['subject'],completeness=item['completeness'],
            binding_status='NO_CALL_MISSING' if not raw else 'BOUND_WITH_REJECTED_OPTIONAL_SUPPORT' if n.get('optional_rejected_supporting_references') else
                           'REVIEW' if n.get('rejected_evidence_bindings') else 'BOUND')
        rows.append(row)
        if row['status']=='ACCEPT':
            ids=sorted({eid for s in ('old','new') for eid in n[s+'_state']['evidence_ids']})
            events.append(dict(event_id=key,object_id=272,status='ACCEPT',engineering_subject=raw['new_state']['engineering_subject'],
                summary_ru=raw['project_change_summary'],old_state=n['old_state'],new_state=n['new_state'],evidence_ids=ids))
        display.append({'ID':key,'Изменение':raw.get('project_change_summary',row['source_label']),
            'OLD':raw.get('old_state',{}).get('value','NOT_EVALUATED'),'NEW':raw.get('new_state',{}).get('value','NOT_EVALUATED'),
            'Статус':row['status'],'Источник':', '.join(sorted({e['route'] for es in packet['evidence'].values() for e in es})),
            'OLD page':', '.join(map(str,bound['old'])),'NEW page':', '.join(map(str,bound['new'])),
            'Completeness':row['completeness'],'Причина REVIEW':'; '.join(n.get('issues',[])),
            'Evidence references':', '.join(e['evidence_id'] for es in packet['evidence'].values() for e in es)})
    grouped=resolve_ownership(events,[],evidence)
    save('PAIR_B_NORMALIZED_RESULTS.json',rows);save('PAIR_B_PROJECT_CHANGES.json',grouped)
    live.workbook(display,OUT/'PAIR_B_SYSTEM_OUTPUT.xlsx')
    verify()
    files={str(p.relative_to(OUT)):sha(p) for directory in (OUT/'PAIR_B_RAW_RESPONSES',B) for p in directory.rglob('*') if p.is_file()}
    files.update({name:sha(OUT/name) for name in ('PAIR_B_NORMALIZED_RESULTS.json','PAIR_B_PROJECT_CHANGES.json','PAIR_B_SYSTEM_OUTPUT.xlsx')})
    save('PAIR_B_RESULT_FREEZE.json',dict(at=now(),files=files,model_calls=frozen['planned_model_calls'],
        counts=dict(Counter(r['status'] for r in rows)),project_changes=len(grouped['groups']),source_audit_started=False,
        inference_freeze_sha256=sha(OUT/'PAIR_B_INFERENCE_FREEZE.json'),code_commit=commit(),architecture_modified=False))
    build('B',rows,grouped['groups'],plan,B)
    print('RESULT FROZEN AND UI SNAPSHOT PUBLISHED',flush=True)


async def run():
    frozen=verify();plan=read(B/'CALL_PLAN.json')['packages'];provider=CodexProvider(B,config=frozen['runtime'])
    assert not list((B/'calls').glob('*/attempt_*')),'Run already started: no automatic resumption'
    try:
        for item in plan:
            if item['action']!='MODEL_CALL':continue
            verify();key=item['key'];print('START '+key,flush=True)
            raw=await provider.call(key,(B/'PROMPT.txt').read_text(),read(B/f'inputs/{key}/MODEL_INPUT.json'),
                read(B/'OUTPUT_SCHEMA.json'),live.packet_runtime(read(BASE/item['package'])),
                expected_request_sha256=frozen['canonical_request_hashes'][key])
            assert raw['case_token']==key,'CASE TOKEN MISMATCH'
            save(f'PAIR_B_RAW_RESPONSES/{key}.json',raw)
            save(f'pair_b_live/raw_responses/{key}.json',raw)
            for source,suffix in [('final.txt','.txt'),('raw.jsonl','.jsonl')]:
                shutil.copyfile(B/f'calls/{key}/attempt_001/{source}',OUT/f'PAIR_B_RAW_RESPONSES/{key}{suffix}')
            print('COMPLETE '+key+' '+str(len(list((B/'calls').glob('*/SUCCESS.json'))))+'/'+str(frozen['planned_model_calls']),flush=True)
        save('PAIR_B_INFERENCE_COMPLETE.json',dict(at=now(),model_calls=frozen['planned_model_calls'],retries=0))
        downstream()
    except Exception as e:
        save('PAIR_B_STOP.json',dict(status='STOPPED_NO_REPAIR',at=now(),error_type=type(e).__name__,error=str(e),
            invocation_attempts=len(list((B/'calls').glob('*/attempt_*/INVOCATION.json'))),
            successful=len(list((B/'calls').glob('*/SUCCESS.json'))),retries=0,source_audit_started=False))
        print('STOP '+type(e).__name__+': '+str(e),flush=True)
        raise

if __name__=='__main__':
    if sys.argv[1]=='preflight':preflight()
    elif sys.argv[1]=='run':asyncio.run(run())
    elif sys.argv[1]=='verify':verify();print('FREEZE VERIFIED')
