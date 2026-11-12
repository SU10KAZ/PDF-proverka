"""Run one source-bound final check on a prepared local publication edit."""
import argparse
import asyncio
import json
import os
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check
from backend.app.pipeline.stages.findings_verify.source_review_release_workflow import run_final_release_check
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor
from backend.app.services.llm.codex_model_substitution import substitute_codex_model
from .run_primary_text_runtime_probe import sha, write


async def run(root):
    manifest = json.loads((root / 'preparation.json').read_text())
    for name in ('source_report', 'source_md'):
        assert sha(Path(manifest[name + '_path'])) == manifest[name + '_sha256']
    assert sha(root / 'entry.json') == manifest['entry_sha256']
    assert sha(root / 'request.json') == manifest['request_sha256']
    entry = json.loads((root / 'entry.json').read_text())
    finding_id = entry['response']['finding_id']
    document = json.loads((root / 'input_document.json').read_text())
    md = Path(manifest['source_md_path']).read_text()
    assert prepare_final_check(entry, document, md)['request'] == json.loads((root / 'request.json').read_text())
    assert substitute_codex_model('gpt-6-astra') == 'gpt-6-astra'
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    write(root / 'process.json', {'pid': os.getpid(), 'status': 'running',
          'model': 'codex/gpt-6-astra', 'production_acceptance': False})
    try:
        after, receipt = await run_final_release_check(entry, document, md, root / 'workflow',
            invoke=direct_cli_executor('codex/gpt-6-astra', project_id=f"{manifest.get('project_id', 'AK')}-{finding_id}-publication-final-check"),
            executor_id='codex/gpt-6-astra:high:publication-final-check-v1')
    except Exception as exc:
        write(root / 'process.json', {'pid': os.getpid(), 'status': 'failed',
              'error_type': type(exc).__name__, 'error': str(exc),
              'result_applied': False, 'production_acceptance': False})
        raise
    write(root / 'output_preview.json', after)
    write(root / 'receipt.json', receipt)
    write(root / 'process.json', {'pid': os.getpid(), 'status': 'finished',
          'result': receipt['status'], 'production_acceptance': False})
    print(receipt['status'], flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    asyncio.run(run(parser.parse_args().root.resolve()))
