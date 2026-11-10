"""Evaluation-only known-mark diagnostic; never imported by source executors.

A mark appearing in an object is not proof of its identity, role or connectivity.
The four known marks are case-specific evaluation targets, not runtime rules.
"""
import json
import re
import hashlib
from pathlib import Path

ROOTS = {
    'astra_grid': Path('/tmp/auditmanager_eom_graphic_recovery_20261110_grid'),
    'claude_grid': Path('/tmp/auditmanager_eom_graphic_grid_claude_control_20261110'),
}


def inspect():
    reports = {}
    for provider, root in ROOTS.items():
        path = root/'result.json'
        report = {'status': 'result_pending', 'semantic_verified': False}
        reports[provider] = report
        if not path.exists():
            continue
        raw = path.read_bytes(); data = json.loads(raw)
        report.update(status='unusable_result' if data.get('is_error') else 'response_received',
            result_sha256=hashlib.sha256(raw).hexdigest(), duration_ms=data.get('duration_ms'),
            input_tokens=data.get('input_tokens'), output_tokens=data.get('output_tokens'))
        if data.get('is_error') or not isinstance(data.get('json_data'),dict):
            continue
        objects = data['json_data'].get('object_registry',[])
        report['objects'] = len(objects)
        report['known_mark_hits'] = {}
        for number in ('2325','2326','2327','2328'):
            # This reviewed case uses visually equivalent Latin C / Cyrillic С.
            # Do not apply this case-specific diagnostic to production matching.
            pattern = re.compile(r'(?<![\w])[CС]' + number + r'(?!\d)')
            report['known_mark_hits']['C'+number] = [o for o in objects if pattern.search(json.dumps(o,ensure_ascii=False))]
        report['coverage'] = data['json_data'].get('object_inventory_coverage')
        report['findings'] = data['json_data'].get('text_findings')
    return {'scope': 'Known EOM27 source-control diagnostic, not a blind test.',
        'providers': reports, 'control_requirements': [
            'Distinct addressed records of active C2326 and C2327 and reserves C2325 and C2328.',
            'Check upstream panel, protective apparatus, load and cable parameters against original graphic.',
            'Compare separately with model-extracted RP3 table scope, not hand-filled historical inventory.',
            'Do not count reserves as missing active loads or infer unaccounted aggregate4kW without ruling out aggregation.'
        ], 'semantic_acceptance': False,
        'boundary': 'Presence of strings and coverage claims are diagnostic only; engineering verification remains separate.'}


if __name__=='__main__':
    result=inspect()
    Path(__file__).with_name('EOM_grid_branch_diagnostic.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:{'status':v['status'],'marks':{m:len(rows) for m,rows in v.get('known_mark_hits',{}).items()}} for k,v in result['providers'].items()}))
