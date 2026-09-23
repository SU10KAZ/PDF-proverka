"""Synthetic end-to-end Stage 01: unchanged ON, explicit Astra/Sol OFF."""
import json
import socket
import subprocess
from pathlib import Path

import pytest
from backend.app.pipeline.stages.block_analysis import gemma_findings_only as gfo, dual_review
from backend.app.pipeline.stages.block_analysis.ensemble_mode import ASTRA_MODEL, SOL_MODEL
from backend.app.models.usage import LLMResult


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    attempts = []
    def forbidden(*args, **kwargs):
        attempts.append(True)
        raise AssertionError('real network forbidden')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.delenv('AUDIT_SECOND_LEG', raising=False)
    monkeypatch.setattr(gfo, 'STAGE01_THIRD_LEG_ENABLED', True)
    monkeypatch.setattr(gfo, 'STAGE01_THIRD_LEG_MODEL', SOL_MODEL)
    monkeypatch.setattr(gfo, 'CODEX_STAGE_MODEL_ID', ASTRA_MODEL)
    monkeypatch.setattr(gfo, 'STAGE01_DUAL_REVIEW_MODEL', SOL_MODEL)
    monkeypatch.setattr(gfo, 'STAGE01_DUAL_REVIEW_ENABLED', True)
    monkeypatch.setattr(gfo, 'STAGE01_DUAL_GAP_SEARCH_ENABLED', True)
    monkeypatch.setattr(gfo, 'STAGE01_PROTECTION_TABLE_CHECK_ENABLED', False)
    monkeypatch.setattr(gfo, 'provider_bridge_active', lambda: False)
    yield
    assert not attempts


def project(tmp_path):
    from backend.app.pipeline.stages.block_context.contract import SCHEMA_VERSION
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import stage02_crop_policy
    output = tmp_path / '_output'
    blocks = output / 'blocks_stage02_100'
    blocks.mkdir(parents=True)
    (blocks / 'B-1.png').write_bytes(b'synthetic-same-image')
    (blocks / 'index.json').write_text(json.dumps({**stage02_crop_policy(), 'blocks':[
        {'block_id':'B-1', 'page':1, 'file':'B-1.png', 'size_kb':1}]}))
    (output / 'document_graph.json').write_text(json.dumps({'pages':[{'page':1,'sheet_no':'1','text_blocks':[]}]}))
    (output / 'block_context_summary.json').write_text(json.dumps({
        'schema_version':SCHEMA_VERSION,'stage':'block_context',
        'reference_catalog':{'runtime_source':'pipeline_stage_embedded_catalog','records_total':1},
        'status':'ok','blocks_total':1,'blocks_ready':1,'blocks_failed':0,'source_counts':{'raw_vector':1},
        'blocks':[{'block_id':'B-1','page':1,'source_kind':'raw_vector','coverage_status':'ready','warnings':[]}]}))
    return output


def finding(label):
    return {'severity':'ЭКСПЛУАТАЦИОННОЕ','category':'marking','finding':f'{label}: missing marking',
            'norm_quote':None,'value_found':'X1','recommendation':'Исправить маркировку.'}


@pytest.mark.asyncio
async def test_on_off_stage_and_identical_detector_inputs(monkeypatch, tmp_path):
    from backend.app.services.llm import codex_runner
    from backend.app.services.llm.openrouter_gate import openrouter_workflow, raise_if_openrouter_stopped
    from backend.app.pipeline.manager import _stage01_model_spends_paid_api
    seen = {'1':[], '0':[]}
    judges = []
    mode = '1'
    fallback_calls = []
    async def unexpected(*args, **kwargs):
        fallback_calls.append(True)
        raise AssertionError('No replacement provider permitted')
    monkeypatch.setattr(gfo, 'call_claude_cli_for_block', unexpected)
    async def gpt(*args, **kwargs):
        assert mode == '1', 'OFF must never even enter the GPT branch'
        seen[mode].append(('openrouter', args[1], kwargs))
        return {'ok':True,'parsed':{'findings':[finding('GPT')]},'input_tokens':0,'output_tokens':0,'elapsed_ms':1}
    async def codex(*args, **kwargs):
        seen[mode].append((kwargs['model'], args, kwargs))
        return {'ok':True,'parsed':{'findings':[finding(kwargs['model'])]},'input_tokens':0,'output_tokens':0,'elapsed_ms':1}
    async def judge(messages, **kwargs):
        payload = json.loads(messages[1]['content'])
        assert kwargs['model'] == SOL_MODEL
        assert kwargs['image_paths'][0].read_bytes() == b'synthetic-same-image'
        assert payload['gap_search_enabled'] is True
        if mode == '1':
            assert messages[0]['content'] == dual_review.REVIEW_SYSTEM_PROMPT
            assert len(payload['detector_findings']) == 3
            assert 'detector_results' not in payload
            relationships = []
        else:
            groups = payload['detector_results']
            assert len(groups) == 2
            assert [g['model'] for g in groups] == [ASTRA_MODEL,SOL_MODEL]
            assert 'gpt_openrouter' not in json.dumps(payload)
            assert 'GPT' not in messages[0]['content']
            relationships = [{'left_ref':groups[0]['findings'][0]['ref'],
                'right_ref':groups[1]['findings'][0]['ref'],'relation':'match','extends':'none',
                'confidence':0.9,'reason':'Same marking'}]
        judges.append((mode,payload))
        data = {'relationships':relationships,
            'gap_findings':[{'severity':'КРИТИЧЕСКОЕ','category':'earthing','finding':'Protective earth connection missing at cabinet enclosure',
                'value_found':'PE chassis','recommendation':'Add protective conductor','norm_quote':None}],
            'gap_search':{'performed':True,'status':'gaps_found'}}
        return LLMResult(text=json.dumps(data),json_data=data,model=SOL_MODEL,input_tokens=0,output_tokens=0,duration_ms=1)
    monkeypatch.setattr(gfo, 'call_gpt_for_block', gpt)
    monkeypatch.setattr(gfo, 'call_codex_for_block', codex)
    monkeypatch.setattr(codex_runner, 'run_codex_json_messages', judge)
    monkeypatch.setattr(gfo, 'build_effective_block_context', lambda *_a, **_k: ('CTX','raw_vector'))
    monkeypatch.setattr(gfo, 'load_version_project_info', lambda *_a, **_k: {'project_id':'synthetic','section':'EOM'})
    output=project(tmp_path)
    for mode in ('1','0'):
        monkeypatch.setenv('AUDIT_OPENROUTER_ENABLED',mode)
        assert _stage01_model_spends_paid_api('ensemble/gpt-codex') == (mode=='1')
        with openrouter_workflow():
            result=await gfo.run_findings_only_for_project(tmp_path,output_dir_override=output,
                model='ensemble/gpt-codex',api_key=None if mode=='0' else 'fake',timeout_s=5,write_run_log=False)
            raise_if_openrouter_stopped()  # Deliberate skip must not poison the global job guard.
        doc=result['output_doc'];summary=result['summary'];receipt=summary['execution_receipt']
        assert summary['blocks_ok']==1 and summary['blocks_failed']==0
        assert summary['blocks_partial']==0
        assert doc['block_analyses'][0]['detectors_failed']==[]
        assert receipt['openrouter_calls']==(1 if mode=='1' else 0)
        assert receipt['astra_calls']==receipt['sol_calls']==receipt['judge_calls']==receipt['gap_search_calls']==1
        assert doc['stage01_meta']['dual_review']['gap_search_blocks']==1
        assert doc['block_analyses'][0]['dual_review']['gap_search']['findings_added']==1
        if mode=='0':
            assert receipt['stage_01_mode']=='TWO_MODEL_NO_OPENROUTER'
            assert receipt['openrouter_enabled'] is False
            assert receipt['skipped_branches']==[{'model':'openai/gpt-5.4','status':'SKIPPED_OPENROUTER_DISABLED'}]
            assert len(doc['stage01_meta']['detectors'])==2
            assert doc['block_analyses'][0]['dual_review']['counts']['matches']==1
        persisted=json.loads((output/gfo.BLOCKS_ANALYSIS_FILENAME).read_text())
        assert persisted['stage01_meta']['execution_receipt']==receipt
    assert [x[0] for x in seen['0']]==[ASTRA_MODEL,SOL_MODEL]
    assert [x[0] for x in seen['1']]==['openrouter',ASTRA_MODEL,SOL_MODEL]
    for model in (ASTRA_MODEL,SOL_MODEL):
        old=next(x for x in seen['1'] if x[0]==model)
        new=next(x for x in seen['0'] if x[0]==model)
        assert old[1]==new[1] and old[2]==new[2]  # Image, prompt, effort and all context unchanged.
    assert len(judges)==2 and not fallback_calls


def test_invalid_flag_never_selects_reduced_mode(monkeypatch):
    from backend.app.pipeline.stages.block_analysis.ensemble_mode import two_model_no_openrouter
    from backend.app.services.llm.openrouter_gate import OpenRouterGateError
    monkeypatch.setenv('AUDIT_OPENROUTER_ENABLED','banana')
    with pytest.raises(OpenRouterGateError,match='openrouter_invalid_enable_flag'):
        two_model_no_openrouter('ensemble/gpt-codex',third_leg_enabled=True)


def test_two_model_normalization_and_deterministic_review_failure():
    combined=gfo.combine_detector_results([
        (ASTRA_MODEL,{'ok':True,'parsed':{'findings':[finding('same')]}}),
        (SOL_MODEL,{'ok':True,'parsed':{'findings':[finding('same')]}})],run_id='synthetic')
    result=dual_review.fallback_dual_review(combined['parsed']['findings'],reviewer_model=SOL_MODEL,
        run_id='synthetic',gap_search_enabled=True,error='synthetic judge failure',comparison_models=[ASTRA_MODEL,SOL_MODEL])
    assert result['report']['counts']['matches']==1
    assert result['report']['gap_search']['performed'] is False
    assert result['report']['comparison_models']==[ASTRA_MODEL,SOL_MODEL]


@pytest.mark.asyncio
async def test_status_api_and_rendered_algorithm(monkeypatch):
    from backend.app.api.routers import audit
    monkeypatch.setenv('AUDIT_OPENROUTER_ENABLED','0')
    monkeypatch.setenv('STAGE01_THIRD_LEG_ENABLED','true')
    monkeypatch.setitem(audit.STAGE_MODEL_CONFIG,'block_batch','ensemble/gpt-codex')
    data=await audit.get_stage_model_config()
    details=data['ensemble_details']['block_batch']
    assert details['parallel_models']==[ASTRA_MODEL,SOL_MODEL]
    assert details['branch_statuses']['openai/gpt-5.4']=='SKIPPED_OPENROUTER_DISABLED'
    source=Path('frontend/static/js/app.js').read_text()
    js=source[source.index('        function stageModelDisplayName('):source.index('        function optimizationAlgorithm(')]
    script='const stageTokens=()=>({}), availableModels={value:[]}, stageModelConfig={value:{block_batch:"ensemble/gpt-codex"}}, stageEnsembleDetails={value:'+json.dumps(data['ensemble_details'])+'};\n'+js+'\nprocess.stdout.write(JSON.stringify(blockAnalysisAlgorithm()));'
    rendered=json.loads(subprocess.check_output(['node','-e',script],text=True))
    branches=rendered['steps'][1]['branches']
    assert len(branches)==3 and sum(bool(b.get('disabled')) for b in branches)==1
    assert 'Временно отключён' in branches[0]['text']
    assert all('активна' in b['text'] for b in branches[1:])


def test_frozen_plan_off_removes_only_stage01_openrouter():
    from backend.app.services.audit_routing import compiler, presets, registry
    plans = []
    for enabled in ("1", "0"):
        plans.append(compiler.AuditRoutingPlanCompiler().compile(compiler.CompilerInputs(
            stage_models=presets.reference_config(presets.PRESET_FULL_CODEX, codex_model_id=ASTRA_MODEL),
            feature_flags={"AUDIT_OPENROUTER_ENABLED": enabled, "STAGE01_THIRD_LEG_ENABLED": "true"},
            claude_default_model_class=registry.MODEL_CLASS_CHEAP, codex_model_id=ASTRA_MODEL,
        )))
    on, off = plans
    assert len([a for a in on.stage("block_batch").actions if a.role == registry.ROLE_DETECTOR]) == 3
    detectors = [a for a in off.stage("block_batch").actions if a.role == registry.ROLE_DETECTOR]
    assert [a.action_id for a in detectors] == ["detector_codex_standard", "detector_codex_strong"]
    assert all(a.provider == "codex" for a in detectors)
    assert [s for s in on.stages if s.stage_id != "block_batch"] == [s for s in off.stages if s.stage_id != "block_batch"]
    assert not [a for s in off.stages for a in s.actions if a.provider == "openrouter"]


@pytest.mark.asyncio
async def test_empty_real_detector_still_has_two_inputs_and_gap_search(tmp_path):
    async def judge(messages, images):
        payload = json.loads(messages[1]["content"])
        assert payload["detector_results"] == [
            {"side":"left", "model":ASTRA_MODEL, "findings":[]},
            {"side":"right", "model":SOL_MODEL, "findings":[]},
        ]
        data = {"relationships": [], "gap_findings": [finding("gap")],
                "gap_search": {"performed": True, "status": "gaps_found"}}
        return LLMResult(text=json.dumps(data), json_data=data, model=SOL_MODEL)
    result = await dual_review.review_dual_findings([], reviewer_model=SOL_MODEL,
        run_id="synthetic", image_path=tmp_path / "fake.png", block_context="context",
        block_id="B-1", page=1, project_id="synthetic", timeout=5, gap_search_enabled=True,
        judge_call=judge, comparison_models=[ASTRA_MODEL, SOL_MODEL])
    assert result["report"]["gap_search"]["performed"] is True
    assert result["report"]["gap_search"]["findings_added"] == 1
