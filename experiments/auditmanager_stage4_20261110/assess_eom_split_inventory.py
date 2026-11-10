"""Evaluate sealed extraction only; never imported by the model executor.

A literal quotation check is not semantic verification or proof of completeness.
Missing result files are pending, not failed, and this evaluator never dispatches.
"""
import hashlib
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT = Path('/tmp/auditmanager_eom_balance_inventory_control_20261110_split/EOM')
OUT = Path(__file__).with_name('EOM_balance_inventory_split_assessment.json')


def assess(root=ROOT):
    manifest = json.loads((root / 'manifest.json').read_text())
    packets = []
    for spec, digest in zip(manifest['packets'], manifest['request_digests'], strict=True):
        stage = spec['stage']
        request = json.loads((root / (stage + '.json')).read_text())
        assert evidence_digest(request) == digest, 'Request changed'
        blocks = {b['block_id']: b for b in json.loads(request['messages'][1]['content'])['source_blocks']}
        result_path = root / 'execution' / stage / 'result.json'
        row = {'stage': stage, 'request_sha256': digest, 'status': 'pending'}
        packets.append(row)
        if not result_path.exists():
            continue
        raw = result_path.read_bytes()
        result = json.loads(raw)
        row.update(result_sha256=hashlib.sha256(raw).hexdigest(),
                   duration_ms=result.get('duration_ms'), input_tokens=result.get('input_tokens'),
                   output_tokens=result.get('output_tokens'), cost_source=result.get('cost_source'))
        if result.get('is_error') or not isinstance(result.get('json_data'), dict):
            row.update(status='unusable_result', error=result.get('error_message'))
            continue
        data = result['json_data']
        exact, failures, graphic, missing = 0, [], 0, 0
        for obj in data.get('object_registry', []):
            for parameter in obj.get('observed_parameters', []):
                quote = parameter.get('quote')
                if not isinstance(quote, str) or not quote:
                    missing += 1
                    continue
                source = blocks.get(parameter.get('source_ref'))
                if source and source['kind'] in ('TEXT', 'TABLE'):
                    if quote in source['content']:
                        exact += 1
                    else:
                        failures.append({'object_id': obj['id'], 'parameter': parameter.get('name'),
                                         'source_ref': parameter.get('source_ref'), 'quote': quote})
                elif source:
                    graphic += 1
                else:
                    failures.append({'object_id': obj['id'], 'reason': 'unresolved_source_ref',
                                     'source_ref': parameter.get('source_ref')})
        row.update(status='usable_json', objects=len(data.get('object_registry', [])),
                   exact_text_quotes=exact, quotation_failures=failures,
                   graphic_quotes_require_visual_review=graphic, parameters_without_literal_quote=missing,
                   inventory_coverage=data.get('object_inventory_coverage', []),
                   findings=data.get('text_findings', []))
    return {'manifest_sha256': evidence_digest(manifest), 'packets': packets,
            'completed': sum(r['status'] != 'pending' for r in packets), 'total': len(packets),
            'usable': sum(r['status'] == 'usable_json' for r in packets),
            'exact_text_quotes': sum(r.get('exact_text_quotes', 0) for r in packets),
            'quotation_failures': sum(len(r.get('quotation_failures', [])) for r in packets),
            'semantic_verified': False, 'whole_project_coverage': False,
            'known_source_control_not_blind_discovery': True,
            'boundary': 'Quoted text must match authoritative MD. Graphic quotations require image review. '
                        'Valid JSON, exact quotes and model coverage claims do not prove complete or correct inventory. '
                        'No expected circuit marks are supplied to the executor.'}


if __name__ == '__main__':
    report = assess()
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k != 'packets'}, ensure_ascii=False))
