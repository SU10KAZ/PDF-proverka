"""Evaluate explicit duplicate links after sealed Astra responses, without inference."""
import copy
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted

HERE = Path(__file__).resolve().parent
RESULTS = Path('/tmp/auditmanager_duplicate_links_astra_20261110')
BASE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs')


def main():
    expected = {('AK','T-004'): ['F-004','F-005'], ('AK','T-008'): ['F-009','F-010'],
                ('AK','T-017'): ['F-023','F-024'], ('ITP','T-016'): ['F-016','F-017']}
    rows = []
    for (disc, sid), targets in expected.items():
        path = RESULTS / f'{disc}_{sid}.json'
        result = json.loads(path.read_text())
        payload = result['validated_payload']
        assert result['model'] == 'codex/gpt-6-astra'
        assert not payload['findings'] and not payload['recovery_pending']
        assert sorted(u['target_id'] for u in payload['recovery_link_updates']) == targets
        base_path = next((BASE / disc).glob('versions/*/03_analysis/runs/control/03_findings.json'))
        original = json.loads(base_path.read_text())
        combined = combine_findings_with_targeted(copy.deepcopy(original), [('candidate_recovery_links', payload)])
        repairs = combined['candidate_recovery_reports'][-1]['link_repairs']
        assert not repairs['pending'] and len(repairs['applied']) == 2
        assert len(original['findings']) == len(combined['findings'])
        for old, new in zip(original['findings'], combined['findings']):
            assert {k:v for k,v in old.items() if k != 'source_finding_ids'} == {
                k:v for k,v in new.items() if k not in ('source_finding_ids','recovery_link_receipts')}
            if new['id'] in targets:
                assert sid in new['source_finding_ids']
                assert set(old.get('source_finding_ids', [])) <= set(new['source_finding_ids'])
            else:
                assert old == new
        rows.append({'discipline':disc,'source_id':sid,'target_ids':targets,
                     'claim_and_severity_preserved':True,'new_findings':0,
                     'model_result':result,'repair_report':repairs,
                     'base':{'path':str(base_path),'sha256':hashlib.sha256(base_path.read_bytes()).hexdigest()}})
    report = {'kind':'known_case_duplicate_link_repair_pilot','rows':rows,'restored_links':8,
              'new_findings':0,'boundary':'Candidate-to-final provenance restored, not independent validation of every target claim.',
              'stage4_complete':False}
    (HERE/'duplicate_link_repair_pilot.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print('4 compound candidates, 8 explicit links, zero added findings; claim fields unchanged.')

if __name__ == '__main__':
    main()
