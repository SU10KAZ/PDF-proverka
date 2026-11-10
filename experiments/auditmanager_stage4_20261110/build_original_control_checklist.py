"""Track every originally selected control without promoting nearby pilot evidence."""
import hashlib
import json
from pathlib import Path

SOURCE=Path('experiments/auditmanager_stage3_20261110/selected_hypotheses.json')
OUT=Path(__file__).parent
# Explicit evidence mappings only. Unmapped controls are review work, not proof
# that no evidence exists anywhere in the historical research corpus.
EVIDENCE={
 ('H01','negative_controls_and_limits',0):('local_contract_verified',['tests/test_text_candidate_lineage.py']),
 ('H01','negative_controls_and_limits',1):('local_contract_verified',['tests/test_text_candidate_lineage.py']),
 ('H01','negative_controls_and_limits',2):('bounded_control_recorded',['duplicate_link_repair_pilot.json']),
 ('H03','negative_controls_and_limits',0):('bounded_control_recorded',['KM_identity_known_relation_pair.json','AK_terminal_graphic_assessment.json','AK_cabinet_003_source_review.json']),
 ('H07','positive_controls',0):('bounded_control_recorded',['metadata_gate_candidate_replay.json']),
 ('H07','negative_controls_and_limits',0):('synthetic_contract_only',['tests/test_stage01_evidence_context.py']),
 ('H07','negative_controls_and_limits',1):('explicit_evidence_limit',['metadata_gate_candidate_replay.json']),
 ('H01','positive_controls',0):('bounded_control_recorded',['AK_graphic_recovery_pilot.json','AK_recovered_verifier_replay.json']),
 ('H02','positive_controls',0):('bounded_control_recorded',['AR_deferred_assessment.json','AR_question_output_replay.json']),
 ('H02','positive_controls',1):('bounded_control_recorded',['KM_node_navigation_summary.json']),
 ('H02','negative_controls_and_limits',0):('bounded_control_recorded',['AR_deferred_assessment.json']),
 ('H02','negative_controls_and_limits',1):('bounded_control_recorded',['KM_node_navigation_summary.json']),
 ('H03','positive_controls',0):('bounded_control_recorded',['AK_systemic_output_replay.json','AK_terminal_graphic_assessment.json','AK_cabinet_001_operation_v4_validated.json']),
 ('H03','positive_controls',2):('bounded_control_recorded',['AR_unlabelled_inventory_v2_assessment.json','AR_region_operation_status.json','AR_region05_context2_validated.json']),
 ('H04','positive_controls',0):('partial_discovery_pending_reconciliation',['EOM_cable_graphic_0_assessment.json','EOM_cable_graphic_1_assessment.json']),
 ('H04','positive_controls',2):('partial_control_failed_quantitative_basis',['KM_LK6_quantity_assessment.json','KM_LK6_repeated_instances_source_review.json']),
 ('H04','negative_controls_and_limits',2):('negative_control_failed',['KM_LK6_quantity_assessment.json','KM_LK6_quantity_context3_validated.json']),
 ('H04','positive_controls',1):('bounded_control_recorded',['EOM_model_extracted_balance_control.json','EOM_model_extracted_balance_claude_control.json']),
 ('H04','negative_controls_and_limits',0):('bounded_control_recorded',['EOM_model_extracted_balance_control.json']),
 ('H05','positive_controls',0):('bounded_control_recorded',['KM_OGM20_geometry_assessment.json','KM_OGM20_geometry_validated.json']),
 ('H05','negative_controls_and_limits',0):('disputed_requires_readjudication',['benchmark_errata.json','KM_adjacent_flights_validated.json']),
 ('H06','positive_controls',0):('bounded_control_recorded',['ITP_requirement_claude_assessment.json']),
 ('H07','positive_controls',1):('bounded_control_recorded',['EOM_prescan_recovery_assessment.json']),
}

def main():
    raw=SOURCE.read_bytes();hypotheses=json.loads(raw)['hypotheses'];rows=[]
    for h in hypotheses:
        for group in ('positive_controls','negative_controls_and_limits'):
            for i,claim in enumerate(h[group]):
                status,files=EVIDENCE.get((h['id'],group,i),('requires_explicit_evidence_review',[]))
                refs=[]
                for name in files:
                    p=Path(name) if name.startswith('tests/') else OUT/name
                    assert p.is_file(),str(p)
                    refs.append({'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
                rows.append({'id':f"{h['id']}-{'P' if group=='positive_controls' else 'N'}{i+1:02d}",
                    'hypothesis':h['id'],'kind':group,'original_control':claim,
                    'evidence_state':status,'evidence':refs,'full_acceptance_proven':False})
    report={'source':str(SOURCE),'source_sha256':hashlib.sha256(raw).hexdigest(),
        'scope':'Every named original control including composite and negative/limitation items; no replacement by easier controls.',
        'rows':rows,'counts':{s:sum(r['evidence_state']==s for r in rows) for s in sorted({r['evidence_state'] for r in rows})},
        'boundaries':['Unmapped means not explicitly reconciled in this checklist, not absent from all evidence.',
            'Bounded control recorded is not proof of complete audit improvement.',
            'For composite items verify every clause; do not close from one matching fragment.',
            'Historical stage3 status/model policy fields are superseded by sequential_plan.json; original control wording remains binding.'],
        'stage4_complete':False}
    (OUT/'original_control_checklist.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'controls':len(rows),'counts':report['counts']},ensure_ascii=False))

if __name__=='__main__':main()
