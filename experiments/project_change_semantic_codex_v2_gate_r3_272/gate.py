"""Counter pass V2: unavailable escalation levels are not performed searches.

Frozen R2 inputs and responses remain unchanged. This module is a separately
frozen deterministic candidate, not a repair applied inside the running R2.
"""
from copy import deepcopy

from experiments.project_change_semantic_codex_v2_272.contracts import evaluate


def evaluate_r3(packet, output):
    old = [e for e in packet['sources'] if e['side']=='old']
    available = {level: [e['evidence_id'] for e in old if
        (bool(e.get('raster')) if level==3 else e.get('level',0)==level)] for level in range(4)}
    adapted=deepcopy(output);steps=[];trace=[]
    for s in output['counter_evidence']['steps']:
        level=s['level']
        unavailable = (level in {1,2,3} and not s['evidence_ids'] and
                       not available[level] and bool(s['reason'].strip()))
        if not unavailable:
            steps.append(deepcopy(s))
        trace.append(dict(**s, execution_status='UNAVAILABLE' if unavailable else 'PERFORMED_OR_INVALID',
                          available_evidence_ids=available.get(level,[])))
    adapted['counter_evidence']['steps']=steps
    gate=evaluate(packet,adapted)
    gate['versions']=dict(gate['versions'],CounterEvidenceEscalation=2)
    gate['deterministic_revision']='CODEX_V2_DIAGNOSTIC_GATE_R3'
    gate['counter_execution_trace']=trace
    gate['raw_model_output_unchanged']=True
    return gate
