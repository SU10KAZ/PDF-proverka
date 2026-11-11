"""Common Astra harness for source-isolated classic/candidate comparison.

Preflight configures transport and verifies imports without calling any model.
Execution uses the classic stage harness only after a matching frozen protocol.
Candidate method activation is a separate required contract, not implied here.
"""
import argparse
import json
from pathlib import Path
import sys

REPOSITORY=Path(__file__).resolve().parents[2]
MODEL='codex/gpt-6-astra'


def load_harness(source_root):
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
    return classic_control


def preflight(source_root,work):
    harness=load_harness(source_root)
    work=Path(work).resolve();work.mkdir(parents=True,exist_ok=True)
    harness.configure(work,'PREFLIGHT')
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as graphics
    from backend.app.core import config
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
        'stage_models':harness.MODELS,'environment':harness.ENV,
        'reasoning_effort':harness.REASONING_EFFORT,'max_request_attempts':harness.MAX_REQUEST_ATTEMPTS,
        'retry_delay_seconds':10,'model_calls':0,
        'boundary':'Configuration/import check only. Candidate methods and execution/evaluator freeze are not complete.'}
    (work/'transport_preflight.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('command',choices=['preflight'])
    p.add_argument('--source-root',type=Path,required=True);p.add_argument('--work',type=Path,required=True)
    a=p.parse_args();preflight(a.source_root,a.work)
