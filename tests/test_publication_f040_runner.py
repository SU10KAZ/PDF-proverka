import asyncio
import json
from pathlib import Path
import pytest
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from experiments.auditmanager_stage4_20261110 import run_publication_f040_admission as runner


def test_failed_final_check_is_terminal_and_never_creates_applied_output(tmp_path, monkeypatch):
    source=tmp_path/'source.md';source.write_text('source')
    report=tmp_path/'sealed.json';report.write_text('{}')
    for name in ('entry.json','input_document.json','request.json'):
        (tmp_path/name).write_text('{}')
    manifest={'source_report_path':str(report),'source_report_sha256':runner.sha(report),
              'source_md_path':str(source),'source_md_sha256':runner.sha(source),
              'entry_sha256':runner.sha(tmp_path/'entry.json'),
              'request_sha256':runner.sha(tmp_path/'request.json')}
    (tmp_path/'preparation.json').write_text(json.dumps(manifest))
    monkeypatch.setattr(runner,'prepare_final_check',lambda *args:{'request':{}})
    monkeypatch.setattr(runner,'substitute_codex_model',lambda model:model)
    monkeypatch.setattr(runner,'direct_cli_executor',lambda *args,**kwargs:None)
    async def fail(*args,**kwargs):
        raise ValueError('stale_final_check')
    monkeypatch.setattr(runner,'run_final_release_check',fail)
    with pytest.raises(ValueError,match='stale_final_check'):
        asyncio.run(runner.run(tmp_path))
    status=json.loads((tmp_path/'process.json').read_text())
    assert status['status']=='failed' and status['error']=='stale_final_check'
    assert status['result_applied'] is False
    assert not (tmp_path/'output_preview.json').exists()
    assert not (tmp_path/'receipt.json').exists()
