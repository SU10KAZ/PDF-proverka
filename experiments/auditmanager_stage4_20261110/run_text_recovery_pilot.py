"""Astra-only remaining text candidate recovery pilot. Uses Codex CLI; never OpenRouter/Claude."""
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
from backend.app.pipeline.stages.prepare.candidate_recovery import build_recovery_passes, validate_recovery_payload
from backend.app.pipeline.stages.block_context.contract import resolve_blocks_dir
from backend.app.services.llm.codex_model_substitution import substitute_codex_model
from backend.app.services.llm.codex_runner import run_codex_json_messages

BASE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs')
RESULTS = Path('/tmp/auditmanager_text_recovery_astra_20261110')

async def main(*, results_dir=RESULTS, source_ids=None, follow_context=False):
    assert substitute_codex_model('gpt-6-astra') == 'gpt-6-astra', 'Astra substitution forbidden for this pilot'
    results_dir.mkdir(exist_ok=True)
    for disc in ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'):
        version = next((BASE / disc).glob('versions/*'))
        source = version / '03_analysis/runs/control'
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp)
            for name in ('01_blocks_analysis.json', '02_text_analysis.json', '03_findings.json', 'document_graph.json'):
                (out / name).symlink_to(source / name)
            blocks = resolve_blocks_dir(source)
            (out / blocks.name).symlink_to(blocks)
            passes = build_recovery_passes(out, version / '02_work/document.md')
            for request in passes:
                packet = json.loads(request.messages[-1]['content'])
                sid = packet['candidate']['source_id']
                if packet['candidate']['origin'] != 'text' or (source_ids and sid not in source_ids):
                    continue
                suffix = '_context' if request.stage.endswith('_context') else ''
                name = f'{disc}_{sid}{suffix}'
                target = results_dir / f'{name}.json'
                if target.exists():
                    print(f'SKIP existing {sid}', flush=True)
                    continue
                (results_dir / f'{name}_request.json').write_text(json.dumps({
                    'messages': request.messages, 'image_paths': [str(p.resolve()) for p in request.image_paths],
                    'model': 'gpt-6-astra', 'scope': 'known-case recovery pilot, not blind evaluation'}, ensure_ascii=False, indent=2))
                for attempt in range(1, 4):
                    assert substitute_codex_model('gpt-6-astra') == 'gpt-6-astra'
                    print(f'START {disc}/{sid} attempt={attempt} model=gpt-6-astra images={len(request.image_paths)}', flush=True)
                    result = await run_codex_json_messages(request.messages, model='codex/gpt-6-astra',
                        timeout=900, stage='candidate_recovery_pilot', project_id=f'{disc}-{sid}',
                        image_paths=request.image_paths)
                    receipt = asdict(result)
                    (results_dir / f'{name}_attempt{attempt}.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2))
                    if not result.is_error and isinstance(result.json_data, dict):
                        receipt['validated_payload'] = validate_recovery_payload(request, result.json_data)
                        if follow_context:
                            from backend.app.pipeline.stages.prepare.recovery_context import build_context_followup
                            followup, search = build_context_followup(request, receipt['validated_payload'])
                            receipt['context_retrieval'] = search
                            if followup is not None:
                                passes.append(followup)
                        target.write_text(json.dumps(receipt, ensure_ascii=False, indent=2))
                        print(f'DONE {disc}/{sid} findings={len(receipt["validated_payload"]["findings"])} duration_ms={result.duration_ms}', flush=True)
                        break
                    print(f'ERROR {sid}: {result.error_message}; retry after 10s', flush=True)
                    if attempt < 3:
                        await asyncio.sleep(10)
                else:
                    raise RuntimeError(f'{sid}: exhausted; inspect saved attempts before resuming')
    print('PILOT COMPLETE', flush=True)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-dir', type=Path, default=RESULTS)
    parser.add_argument('--source-ids', nargs='*')
    parser.add_argument('--follow-context', action='store_true')
    args = parser.parse_args()
    asyncio.run(main(results_dir=args.output_dir, source_ids=args.source_ids, follow_context=args.follow_context))
