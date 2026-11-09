"""Exercise real deterministic verifier in isolated copies; no model requests."""
import copy
import hashlib
import json
import tempfile
from pathlib import Path
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
from backend.app.pipeline.stages.text_analysis.absence_guard import _is_absence_claim

HERE = Path(__file__).resolve().parent

def main():
    pilot = json.loads((HERE / 'AK_graphic_recovery_pilot.json').read_text())
    payload = pilot['raw_runs'][-1]['result']['validated_payload']
    source = next(Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/AK').glob('versions/*/03_analysis/runs/control'))
    original = json.loads((source / '03_findings.json').read_text())
    candidate = combine_findings_with_targeted(copy.deepcopy(original), [('candidate_recovery_grouped',payload)])
    outcomes = []
    for label, data in [('baseline', original), ('candidate', candidate)]:
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp)
            for name in ('01_blocks_analysis.json', 'document_graph.json'):
                (out / name).symlink_to(source / name)
            (out / '03_findings.json').write_text(json.dumps(data, ensure_ascii=False))
            stats = _run_deterministic_phase(out, 'AK-recovery-replay')
            assert not stats['critic_error'] and not stats['corrector_error']
            outcomes.append({'label':label,'stats':stats,
                'findings':json.loads((out/'03_findings.json').read_text())['findings'],
                'review':json.loads((out/'03_findings_review.json').read_text())})
    before, after = outcomes
    recovered = after['findings'][-1]
    assert after['findings'][:-1] == before['findings']
    assert recovered == candidate['findings'][-1]
    assert not _is_absence_claim(recovered)
    result = {'scope':'actual deterministic verifier/corrector and absence eligibility, no LLM',
              'outcomes':outcomes,'recovered_finding_unchanged':True,'baseline_outputs_preserved':True,
              'recovered_absence_candidate':False,
              'boundary':'Not a semantic LLM pass or complete production release test. Baseline findings may still need absence checks.',
              'sources':[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in
                         [HERE/'AK_graphic_recovery_pilot.json',source/'03_findings.json', source/'01_blocks_analysis.json',source/'document_graph.json']]}
    (HERE/'AK_recovered_verifier_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'baseline':before['stats'],'candidate':after['stats'],'recovered_unchanged':True},ensure_ascii=False))

if __name__ == '__main__':
    main()
