"""Validate requirement receipts against the exact supplied MD; no dispatch."""
import json
from pathlib import Path
from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks,reconcile_requirement_checks
from experiments.auditmanager_stage4_20261110.run_requirement_scope_claude_pilot import ROOT


def main():
    result=json.loads((ROOT/'result.json').read_text());request=json.loads((ROOT/'request.json').read_text())
    if result['is_error'] or not isinstance(result['json_data'],dict):raise ValueError('No successful structured response')
    md=json.loads(request['messages'][1]['content'])['authoritative_md']
    receipt=reconcile_requirement_checks(result['json_data'],authoritative_text_blocks(md))
    (ROOT/'validation.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ('text','json_data')},ensure_ascii=False,indent=2))
    print(json.dumps(result['json_data'],ensure_ascii=False,indent=2))
    print('RECEIPT',json.dumps(receipt,ensure_ascii=False))


if __name__=='__main__':main()
