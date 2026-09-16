"""New generation for the explicitly authorized guard-only Pair B run."""
import asyncio
import sys
from . import pair_b
from .snapshot import OUT, read, sha
from .capacity import AUDIT

RUN=AUDIT/'run_01'

def configure():
    audit=read(AUDIT/'PAIR_B_INPUT_CAPACITY_AUDIT.json')
    assert audit['capacity_safe'] and audit['guard_proposal']==pair_b.MAX_TEXT_CHARACTERS==83000
    assert len(audit['rows'])==18 and audit['minimum_safety_headroom_tokens']>15000
    pair_b.OUT=RUN;pair_b.B=RUN/'pair_b_live'


def preflight():
    configure()
    assert not RUN.exists(),'Single-use generation; no automatic replacement'
    RUN.mkdir()
    # The original UI receipt is copied byte-for-byte, never re-created.
    (RUN/'PAIR_A_UI_CHECK.json').write_bytes((OUT/'PAIR_A_UI_CHECK.json').read_bytes())
    pair_b.preflight()
    freeze=read(RUN/'PAIR_B_INFERENCE_FREEZE.json')
    prior=read(OUT/'PAIR_B_INFERENCE_FREEZE.json')
    for field in ('package_hashes','boundary_hashes','evidence_hashes','prompt_sha256','runtime'):
        assert freeze[field]==prior[field],field+' changed'
    audit=read(AUDIT/'PAIR_B_INPUT_CAPACITY_AUDIT.json')
    for row in audit['rows']:
        key=row['package_id']
        assert freeze['canonical_request_hashes'][key]==row['canonical_request_sha256']
    assert freeze['status']=='READY' and freeze['planned_model_calls']==18
    pair_b.save('CAPACITY_GATE.json',dict(status='PASS',preflight='18/18 PASS',capacity_audit_sha256=sha(AUDIT/'PAIR_B_INPUT_CAPACITY_AUDIT.json'),
        previous_inference_freeze_sha256=sha(OUT/'PAIR_B_INFERENCE_FREEZE.json'),
        changed_only='Local text guard 60000 -> 83000; orchestration output generation',
        package_hashes='PASS',evidence_hashes='PASS',prompt_semantic_hash='PASS',runtime_configuration='UNCHANGED',model_calls=0))


def main():
    if sys.argv[1]=='preflight':preflight()
    elif sys.argv[1]=='run':
        configure()
        gate=read(RUN/'CAPACITY_GATE.json')
        assert gate['status']=='PASS' and sha(AUDIT/'PAIR_B_INPUT_CAPACITY_AUDIT.json')==gate['capacity_audit_sha256']
        asyncio.run(pair_b.run())
    elif sys.argv[1]=='verify':configure();pair_b.verify();print('FREEZE VERIFIED')

if __name__=='__main__':main()
