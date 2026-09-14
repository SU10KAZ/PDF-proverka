"""Frozen route union with source-verified duplicate ownership and score gates."""
import argparse
from collections import Counter
from copy import deepcopy
from pathlib import Path
import time

from experiments.project_change_text_v1.engine import compare as text_compare
from experiments.project_change_text_v1.contract import validate

from .inventory import ROOT, REPO, read, sha, immutable, now
from .policy import admitted_pairs, freeze, verify_candidate
from .grouped_demand import compare as demand_compare
from .compound_states import observations, compare as compound_compare
from .semantic_states import compare as semantic_compare
from .graphic_ports import extract as graphic_extract, compare as graphic_compare

PACKAGES = ['project_change_272', 'project_change_master', 'project_change_text_v1',
            'table_project_change_v1', 'table_materialization_v3', 'engineering_subject_resolver_v1',
            'evidence_scope_binding_v1', 'text_comparison_v1', 'text_alignment_v2',
            'text_old_scope_recovery_v1', 'text_safe_coverage', 'semantic_foundation_v3']
CONFIG = dict(routes=['TEXT', 'TABLE', 'GRAPHIC'],
    automatic_additions=False, automatic_removals=False,
    state='complete explicit scoped declarations; conditional addresses; paired graphic circuit states',
    grouping='explicit engineering owner; no row/fragment random splits',
    model_calls=0,
    readiness=dict(project_change_precision_min=.95, high_value_coverage_min=.70,
                   accepted_events_min=8, represented_validation_ciphers_min=3,
                   false_additions_max=0, false_removals_max=0,
                   duplicate_fraction_max=.05, over_grouping_max=0, traceability_min=1.0,
                   review_to_correct_accepted_max=5.0,
                   required='Source-only high-value truth inventory with explicit unknowns; report all requested error categories. Zero predictions never establish precision.'))


def fuse(changes):
    """Absorb a temperature-pair event only when the diagram states prove it."""
    graphics = [c for c in changes if 'GRAPHIC' in c['routes']]
    absorbed = []
    kept = []
    for c in changes:
        if c['routes'] != ['TEXT'] or c['change_type'] != 'SYSTEM_MODE_CHANGED':
            kept.append(c)
            continue
        facts = c['supporting_fact_changes']
        if len(facts) != 1 or facts[0]['property'] != 'flow_return_temperature':
            kept.append(c)
            continue
        f = facts[0]
        owners = []
        for g in graphics:
            if c['comparison_scope'] != g['comparison_scope'] or g['status'] != 'PROVEN':
                continue
            if not all({e['evidence_id'] for e in c['evidence_' + s]} <= {e['evidence_id'] for e in g['evidence_' + s]} for s in ['old', 'new']):
                continue
            temps = {x['property'].split(':')[0]: x for x in g['supporting_fact_changes'] if x['new']['unit'] == '°c'}
            if set(temps) != {'supply', 'return'}:
                continue
            if all('/'.join(temps[role][s]['value'] for role in ['supply', 'return']) == f[s]['value'] for s in ['old', 'new']):
                owners.append(g)
        if len(owners) == 1:
            absorbed.append(dict(child_event=c['project_change_id'], owner_event=owners[0]['project_change_id'],
                                 reason='Same evidence and exact corresponding supply/return temperatures; one circuit event owns both routes'))
        else:
            kept.append(c)
    seen = set()
    for c in kept:
        if c['project_change_id'] in seen:
            raise ValueError('Duplicate event ID after route union')
        seen.add(c['project_change_id'])
        validate(c, text_only=False)
    return sorted(kept, key=lambda c: c['project_change_id']), sorted(absorbed, key=lambda r: r['child_event'])


def run(name, partition='DEV', candidate=None):
    admitted_pairs(partition, candidate)
    base = ROOT / 'sources' / partition
    out = ROOT / 'runs' / name
    immutable(out / 'MANIFEST.json', dict(started_at=now(), partition=partition,
        config=CONFIG, split_sha256=sha(ROOT/'SPLIT.json'),
        candidate_manifest=str(candidate) if candidate else None,
        code={str(p.relative_to(REPO)): sha(p) for p in Path(__file__).parent.glob('*.py')}))
    started = time.monotonic()
    events = []
    summaries = []
    for pair in read(base/'PAIRS.json'):
        units = {s: read(base/'narrative'/(pair[s]['document_version']+'.json'))['units'] for s in ['old', 'new']}
        pools = {s: read(base/'pools'/(pair[s]['document_version']+'.json'))['pool'] for s in ['old', 'new']}
        text = text_compare(pair['pair_key'], units['old'], units['new'])
        demand = demand_compare(pools['old'], pools['new'])
        compound = compound_compare(pair['pair_key'], observations(pair['old']), observations(pair['new']))
        semantic = semantic_compare(pair['pair_key'], units['old'], units['new'])
        graphics = {s: graphic_extract(pair[s], pair['embargo_pages'][s]) for s in ['old', 'new']}
        graphic = graphic_compare(pair['pair_key'], graphics['old'], graphics['new'])
        candidates = text['changes'] + demand['project_changes'] + compound['project_changes'] + semantic['project_changes'] + graphic['project_changes']
        changes, absorbed = fuse(candidates)
        summary = dict(index=pair['index'], old=pair['old']['document_code'], new=pair['new']['document_code'],
            candidates=len(changes), statuses=dict(Counter(c['status'] for c in changes)),
            accepted_routes=dict(Counter('+'.join(c['routes']) for c in changes if c['status']=='PROVEN')),
            absorbed_duplicate_events=len(absorbed),
            precision=None, high_value_coverage=None, adjudication='NOT_ADJUDICATED')
        immutable(out/'pairs'/(str(pair['index'])+'.json'), dict(pair=pair, summary=summary, project_changes=changes,
            absorbed=absorbed, text=text, demand=demand, compound=compound, semantic=semantic, graphic=graphic,
            graphic_sources=graphics))
        summaries.append(summary)
        events += [dict(pair_index=pair['index'], change=c) for c in changes]
        print(summary, flush=True)
    immutable(out/'RESULTS.json', dict(pairs=summaries, project_changes=events,
        seconds=time.monotonic()-started, config=CONFIG, adjudication='NOT_ADJUDICATED',
        accepted=sum(x['change']['status']=='PROVEN' for x in events),
        review=sum(x['change']['status']=='REVIEW' for x in events),
        scope='Within object 272 only; historically exposed, prospectively cipher-isolated'))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['run', 'freeze'])
    p.add_argument('--name', required=True)
    p.add_argument('--partition', default='DEV', choices=['DEV', 'VALIDATION', 'FINAL_HOLDOUT'])
    p.add_argument('--candidate', type=Path)
    args = p.parse_args()
    if args.action == 'freeze':
        print(freeze(args.name, args.partition, CONFIG, PACKAGES))
    else:
        run(args.name, args.partition, args.candidate)
