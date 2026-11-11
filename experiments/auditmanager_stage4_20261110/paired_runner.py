"""Common Astra harness for source-isolated classic/candidate comparison.

Preflight configures transport and verifies imports without calling any model.
Execution uses the classic stage harness only after a matching frozen protocol.
Candidate method activation is a separate required contract, not implied here.
"""
import argparse
import asyncio
import json
from pathlib import Path
import sys

REPOSITORY=Path(__file__).resolve().parents[2]
MODEL='codex/gpt-6-astra'
VARIANTS=('classic','classic_current','inventory','source_review','candidate')


def method_profile(variant):
    """Switch isolation only; not proof of equivalence to historical classic.

    Inventory includes its dependent object operations and context retrieval.
    Source review includes its mandatory independent release check. Neither is
    enabled alone without its safety dependencies. Candidate is their union.
    classic_current provides an all-off control on the same candidate source.
    """
    if variant not in VARIANTS:raise ValueError('Unknown comparison variant')
    inventory=variant in ('inventory','candidate')
    review=variant in ('source_review','candidate')
    config={}
    if inventory:
        config.update(inventory_max_packets=128,inventory_max_source_chars=60000,inventory_max_images=4)
    if review:
        config.update(source_review_max_findings=256,source_review_auto_graphic_regions=True,
                      source_final_check_max_findings=256,source_final_check_complete_citations=True)
    enabled='1' if inventory else '0'
    env=dict(AUDIT_OBJECT_CONTEXT_FOLLOWUPS=enabled,AUDIT_OBJECT_GRAPHIC_DETAILS=enabled,
             AUDIT_OBJECT_TERMINAL_DISPOSITION=enabled,
             AUDIT_OBJECT_OPERATION_MAX_REQUESTS='128' if inventory else '8')
    return config,env


def load_harness(source_root, variant="classic"):
    if variant not in VARIANTS:
        raise ValueError("Unknown comparison variant")
    source_root=Path(source_root).resolve()
    if any(name=='backend' or name.startswith('backend.') for name in sys.modules):
        raise RuntimeError('Select source root before importing backend')
    if not (source_root/'backend/app/pipeline/context.py').is_file():
        raise ValueError('Missing implementation root')
    sys.path.insert(0,str(REPOSITORY/'experiments/classic_investigator'))
    import investigate
    import classic_control
    # The harness is shared; algorithm imports and prompts belong to the arm.
    investigate.ROOT=source_root
    classic_control.ROOT=source_root
    sys.path.insert(0,str(source_root))
    classic_control.MODELS={key:(value if value.startswith('ensemble/') else MODEL)
                            for key,value in classic_control.MODELS.items()}
    classic_control.ALLOWED={'gpt-6-astra'}
    classic_control.REASONING_EFFORT='high'
    classic_control.MAX_REQUEST_ATTEMPTS=3
    classic_control.ENV.update(STAGE01_CODEX_ONLY_LEG_MODELS=MODEL+','+MODEL,
        STAGE01_CODEX_ONLY_LEG_EFFORT='high',STAGE01_CODEX_ONLY_JUDGE_MODEL=MODEL,
        STAGE01_CODEX_ONLY_JUDGE_EFFORT='high')
    classic_control.METHOD_CONFIG, method_env=method_profile(variant)
    classic_control.ENV.update(method_env)
    return classic_control


def preflight(source_root,work,variant="classic"):
    harness=load_harness(source_root,variant)
    work=Path(work).resolve();work.mkdir(parents=True,exist_ok=True)
    harness.configure(work,'PREFLIGHT')
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as graphics
    from backend.app.core import config
    from backend.app.pipeline.context import PipelineStageContext
    # Import every stage used by run() now, before any expensive detector call.
    from backend.app.pipeline.stages.block_analysis.blocks_for_text import write_blocks_for_text_compact
    from backend.app.pipeline.stages.text_analysis.runner import run_text_analysis
    from backend.app.pipeline.stages.findings_merge.runner import run_findings_merge
    from backend.app.pipeline.stages.findings_verify.runner import run_findings_verify
    options=harness.method_context_options('PREFLIGHT')
    assert set(options)<=set(PipelineStageContext.__dataclass_fields__)
    if not Path(graphics.__file__).resolve().is_relative_to(harness.ROOT):
        raise RuntimeError('Imported algorithm from wrong arm')
    loaded={name:str(Path(module.__file__).resolve()) for name,module in sys.modules.items()
            if (name=='backend' or name.startswith('backend.')) and getattr(module,'__file__',None)}
    foreign={name:path for name,path in loaded.items() if not Path(path).is_relative_to(harness.ROOT)}
    if foreign:raise RuntimeError('Cross-arm imports: '+json.dumps(foreign))
    assert graphics.STAGE01_CODEX_ONLY_LEG_MODELS==(MODEL,MODEL)
    assert graphics.STAGE01_CODEX_ONLY_JUDGE_MODEL==MODEL
    assert graphics.STAGE01_CODEX_ONLY_LEG_EFFORT=='high'
    assert graphics.STAGE01_CODEX_ONLY_JUDGE_EFFORT=='high'
    assert config.STAGE_MODEL_CONFIG==harness.MODELS
    report={'status':'shared_transport_preflight_passed','source_root':str(harness.ROOT),
        'algorithm_module':str(Path(graphics.__file__).resolve()),'backend_imports_verified':len(loaded),'cross_arm_imports':foreign,
        'variant':variant,'method_config':harness.METHOD_CONFIG,'context_option_keys':sorted(options),
        'stage_models':harness.MODELS,'environment':harness.ENV,
        'reasoning_effort':harness.REASONING_EFFORT,'max_request_attempts':harness.MAX_REQUEST_ATTEMPTS,
        'retry_delay_seconds':10,'model_calls':0,
        'boundary':'Configuration/import check only. Candidate methods and execution/evaluator freeze are not complete.'}
    (work/'transport_preflight.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('command',choices=['preflight','run'])
    p.add_argument('--source-root',type=Path,required=True);p.add_argument('--work',type=Path,required=True)
    p.add_argument('--variant',choices=VARIANTS,required=True)
    p.add_argument('--discipline',choices=['AK','ITP','AR','KM','VK','EOM'])
    a=p.parse_args()
    if a.command=='preflight':preflight(a.source_root,a.work,a.variant)
    else:
        if not a.discipline:p.error('--discipline is required for run')
        from experiments.auditmanager_stage4_20261110.paired_protocol import verify_shared_protocol, verify_trial_binding
        verify_shared_protocol(REPOSITORY,a.source_root,a.work,a.variant)
        verify_trial_binding(a.work,a.variant,a.discipline)
        harness=load_harness(a.source_root,a.variant)
        asyncio.run(harness.run(a.work,a.discipline))
