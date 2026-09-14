"""Freezeable whole-cipher research pipeline with source-first reserve evaluation."""
import argparse
import asyncio
from pathlib import Path

from experiments.project_change_272.inventory import ROOT,read,immutable,sha,now
from experiments.project_change_272.policy import freeze
from experiments.project_change_272.run import prepare as prepare_sources
from experiments.project_change_272.candidate import run as typed_run,PACKAGES
from .access import authorize,prepared_pairs
from .packets import BASE,prepare as native_prepare
from .vision import augment
from .typed_seeds import prepare as typed_prepare
from .run import run,MODEL
from .merge_runs import merge
from .closure import prepare as closure_prepare,CLOSURE_PROMPT
from .ownership import group
from .evaluation import READINESS,PROTOCOL

CONFIG=dict(authoritative_object='272_Sadovnicheskaya_76_Balchug_Esteyt',old='stage_1',new='stage_2',
    baseline='v002 logical comparison baseline',unit='COMPLETE_DOCUMENT_PAIR',
    architecture='EvidenceScope -> EngineeringSubject -> OLD/NEW state -> ProjectChange',
    proposal_routes=['ALL_ELIGIBLE_NATIVE_AND_GRAPHIC_PAGES','TYPED_STATE_SOURCE_SEEDS'],
    acceptance='Source exactness + separate semantic audit + counter-state search + condition admission + ownership audit',
    model=MODEL,temperature=0,reasoning_effort='low',max_request_characters=60000,max_images=8,
    source_images='At most one original PDF page per side, up to four tiles per page',
    counter_context='Bounded physical continuations from admitted native pools, no jump over embargo/history',
    model_independence='Same-model sequential checks; source adjudication required',
    readiness=READINESS,evaluation=PROTOCOL,
    source_audit_frame='Whole reserved cipher pairs: enumerate author-reported engineering changes and source-body/graphic scopes, corroborate OLD and NEW independently, record source-scan coverage and all unresolved areas; lock before inference',
    historical_blind=False,prospective_cipher_isolation=True)


def prepare(name,partition='DEV',candidate=None):
    authorize(partition,candidate)
    source_root=ROOT/'sources'/partition
    if not (source_root/'PAIRS.json').exists():prepare_sources(partition,candidate)
    prepared_pairs(partition,candidate)
    native_prepare(name+'_native',partition,candidate)
    augment(name+'_visual',name+'_native',partition,candidate)
    return BASE/(name+'_visual')


def lock_truth(path,partition,out):
    if path is None:
        if partition!='DEV':raise PermissionError('Reserve inference requires a source-only truth lock')
        return None
    truth=read(path)
    if truth.get('partition')!=partition or truth.get('predictions_seen') is not False:
        raise PermissionError('Truth must be source-only and in this cipher partition')
    if not truth.get('source_scan_coverage') or not isinstance(truth.get('changes'),list):
        raise PermissionError('Truth needs source scan coverage, confirmed changes and explicit unknowns')
    receipt=dict(locked_at=now(),path=str(Path(path).resolve()),sha256=sha(path),
        partition=partition,predictions_seen=False,source_scan_coverage=truth['source_scan_coverage'])
    immutable(out/'TRUTH_LOCK.json',receipt)
    immutable(out/'TRUTH_SNAPSHOT.json',truth)
    return receipt


async def infer(name,partition='DEV',candidate=None,truth=None):
    authorize(partition,candidate)
    out=BASE/'pipelines'/name
    lock=lock_truth(truth,partition,out)
    visual=BASE/(name+'_visual')
    if not (visual/'COUNTS.json').exists():raise ValueError('Prepare admitted source scopes before inference')
    immutable(out/'MANIFEST.json',dict(started_at=now(),partition=partition,config=CONFIG,
        candidate_manifest=str(candidate) if candidate else None,split_sha256=sha(ROOT/'SPLIT.json'),truth_lock=lock))
    typed_name=name+'_typed'
    typed_run(typed_name,partition,candidate)
    typed_prepare(name+'_seeds',typed_name,partition,candidate)
    augment(name+'_seed_visual',name+'_seeds',partition,candidate)
    # Both complete routes execute; no selection based on an accepted-count target.
    await run(name+'_primary',visual,10**9,partition,candidate)
    await run(name+'_seed_inference',BASE/(name+'_seed_visual'),10**9,partition,candidate)
    merged,packets=merge(name+'_routes',[name+'_primary',name+'_seed_inference'],partition,candidate)
    closure_dir=closure_prepare(name+'_counter_packets',merged.name,packets.name,partition,candidate)
    await run(name+'_counter',closure_dir,10**9,partition,candidate,proposal_prompt=CLOSURE_PROMPT)
    owned=await group(name+'_owners',name+'_counter',closure_dir.name,partition,candidate)
    immutable(out/'OUTPUT.json',dict(completed_at=now(),project_changes=dict(path=str(owned/'PROJECT_CHANGES.json'),sha256=sha(owned/'PROJECT_CHANGES.json')),
        source_truth_lock=lock,status='REQUIRES_SOURCE_ADJUDICATION',quality_decision=None,
        costs='Original inference and ownership receipts; derived unions issue no provider calls'))
    return owned


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','infer','freeze']);p.add_argument('--name',required=True)
    p.add_argument('--partition',default='DEV',choices=['DEV','VALIDATION','FINAL_HOLDOUT'])
    p.add_argument('--candidate',type=Path);p.add_argument('--truth',type=Path);a=p.parse_args()
    if a.action=='freeze':print(freeze(a.name,a.partition,CONFIG,PACKAGES+['project_change_semantic_272']))
    elif a.action=='prepare':print(prepare(a.name,a.partition,a.candidate))
    else:print(asyncio.run(infer(a.name,a.partition,a.candidate,a.truth)))
