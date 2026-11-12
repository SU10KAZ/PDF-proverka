"""Account for every original control; neither an uncertain case nor a test is a pass."""
from collections import Counter
import json
from pathlib import Path
from .run_primary_text_runtime_probe import sha, write

HERE = Path(__file__).parent
SOURCE = Path('experiments/auditmanager_stage3_20261110/selected_hypotheses.json')
# Outcomes describe the original requirement, not the confidence of a model.
# Each tuple is outcome, evidence files, remaining boundary.
ROWS = {
'H01': [
 ('bounded_verified', ['H01_dimension_chain_release_assessment.json'], 'One dimension-chain finding reaches final Excel; not whole-project recall.'),
 ('partial_profile_disputed', ['eom_current_profile_evidence/assessment.json','EOM_F032_output_replay.json','EOM_F001_F030_current_profile_disagreement.json'], 'Facts retained; automatic question routing differs from the original locally assigned label. No separate engineer section decision.'),
 ('contract_verified', ['stage4_current_gate_tests.json'], 'Unknown references rejected by contract tests; existence of a reference is not truth.'),
 ('bounded_verified', ['stage4_current_gate_tests.json','publication_identity_adjudication_pair_assessment.json'], 'Source and severity require separate review; no general semantic certification.'),
 ('bounded_verified', ['H01_duplicate_controls_assessment.json'], 'Four original duplicates exercised against current recovery and merge; no new duplicate rows.')],
'H02': [
 ('bounded_verified', ['AR_deferred_summary.json','AR_question_output_replay.json'], 'Existing upper-roof plan retrieved; conditional question retained.'),
 ('bounded_verified', ['KM_node_navigation_summary.json'], 'Required node located; unresolved applicability and detail boundaries preserved.'),
 ('bounded_verified', ['AR_question_output_replay.json'], 'No unsupported categorical removal of the third aerator.'),
 ('bounded_verified', ['KM_node_navigation_summary.json'], '3100/2900 remains unresolved rather than a confirmed dimension defect.')],
'H03': [
 ('partial_manual_grouping', ['AK_five_cabinet_control_status.json','AK_systemic_output_replay.json','grouping_scope_pair_evidence/assessment.json'], 'Five addresses checked and retained in local replay. Automatic grouping is not accepted and stays disabled.'),
 ('inconclusive_identity', ['EOM_instance_identity_validated.json','EOM_identity_question_output_replay.json'], 'Repeated K3.2.406 labels established; distinct physical targets not resolved.'),
 ('bounded_verified', ['AR_region_operation_status.json'], 'Six location-bound regions receive context requests; not six established defects.'),
 ('partial_no_general_filter', ['AK_terminal_graphic_assessment.json','stage4_current_gate_tests.json'], 'Overlapping tiles do not add instances. PE/N historical negative pair unconfirmed; no broad terminal suppression.'),
 ('bounded_verified', ['EOM_instance_identity_validated.json'], 'Same model alone not treated as conflicting identity.'),
 ('explicit_no_filter_limit', ['stage4_hypothesis_current_audit.json'], 'No engineer-confirmed historical negative pair for a general terminal filter.')],
'H04': [
 ('partial_applicability_unresolved', ['eom_f050_stamp_evidence/assessment.json','eom_f051_released_evidence/assessment.json'], 'Literal cable execution facts retained; scheme/specification applicability remains unresolved for F050.'),
 ('bounded_verified', ['EOM_model_extracted_balance_control.json'], 'Active C2326/C2327 compared with all24 RP3 rows; aggregation is not excluded.'),
 ('partial_profile_and_identity', ['KM_LK6_preservation_verification.json','KM_complete_literal_quantity_trace.json'], 'Repeated OGM22/25 and conditional quantities retained; exact OGM26/25 correspondence and profile remain unresolved.'),
 ('bounded_verified', ['EOM_model_extracted_balance_control.json'], 'C2325/C2328 treated as reserve, not omitted active loads.'),
 ('bounded_verified', ['EOM_model_extracted_balance_control.json','eom_f050_stamp_evidence/assessment.json'], 'No mandatory160m procurement or4kW addition inferred from missing separate rows.'),
 ('bounded_verified', ['KM_complete_literal_quantity_trace.json','KM_LK6_preservation_verification.json'], '172 known anchors;228/40 remain conditional on unknown OGM21/24 counts.')],
'H05': [
 ('bounded_verified', ['KM_OGM20_geometry_assessment.json'], 'Same landing -2.350/-2.500 conflict retained; correct elevation and final release remain unverified.'),
 ('partial_pipe_mapping_unresolved', ['ITP_GVS_current_preservation_verification.json','itp_t312_released_evidence/assessment.json','ITP_H72_independent_mapping_search.json'], 'Supply/circulation discrepancies and K15 context preserved; same-pipe3/3.5 mapping still unknown. Neither hydraulic failure nor pump inadequacy established.'),
 ('bounded_question_only', ['AR_roof_reviewed_question_assessment.json'], 'Conditional comparison retained as question; point elevations are not asserted support heights.'),
 ('disputed_reference_excluded', ['benchmark_errata.json'], 'Prior refutation explanation for F014 contradicted by source. Neither original defect nor refutation established; no filter credit.'),
 ('bounded_verified', ['KM_handrail_claim_check_assessment.json'], '3240 is not accepted as proven full spatial development.'),
 ('bounded_verified', ['AR_roof_reviewed_question_assessment.json'], 'Drain elevations not generalized to every support.')],
'H06': [
 ('bounded_scope_only', ['ITP_requirement_claude_assessment.json'], '819 is row quantity sum, not violation count. Conditional model findings not admitted as proven defects.'),
 ('inconclusive_execution', ['EOM_manufacturer_applicability_assessment.json'], 'Manufacturer/article for actual QF3.20 unresolved; preserve addressable question, not incompatibility.'),
 ('bounded_question_only', ['AR_rope_current_release_assessment.json'], 'Automatic question and final Excel verified; applicable KM8/KM9.2 calculation remains unavailable.'),
 ('bounded_verified', ['EOM_shunt_identity_negative_pair.json'], 'QF3.17 and QF3.20 scopes kept separate; series designation alone does not establish poles.'),
 ('bounded_verified', ['ITP_pipe_distinct_materials_assessment.json'], 'VGP65x4 and electric-welded76x3.5 not silently identified.'),
 ('bounded_verified', ['AR_rope_strength_inference_pair.json'], 'Decorative label establishes neither inadequate strength nor adequacy.')],
'H07': [
 ('bounded_verified', ['metadata_shared_profile_assessment.json','metadata_gate_candidate_replay.json'], 'Project room discrepancy preserved despite context wording; not a new unique finding if already reported.'),
 ('bounded_verified', ['prescan_curtain_candidate_replay.json','EOM_prescan_recovery_assessment.json'], 'Prescan emits a review request, not automatic renaming; limited context stays pending.'),
 ('synthetic_control_only', ['metadata_shared_profile_assessment.json'], 'Mixed synthetic request excludes internal markup correction while retaining project discrepancy.'),
 ('explicit_no_filter_limit', ['metadata_shared_profile_assessment.json'], 'Synthetic variant is not an independent historical pair; no broad suppression accepted.')],
}


def main():
    original = json.loads(SOURCE.read_text())
    rows = []
    for hypothesis in original['hypotheses']:
        requirements = [(kind, i+1, text) for kind in ('positive_controls','negative_controls_and_limits')
                        for i, text in enumerate(hypothesis[kind])]
        reviewed = ROWS[hypothesis['id']]
        assert len(requirements) == len(reviewed), hypothesis['id']
        for (kind, number, text), (outcome, names, boundary) in zip(requirements, reviewed):
            refs = []
            for name in names:
                path = HERE/name
                json.loads(path.read_text())
                refs.append({'path': str(path), 'sha256': sha(path)})
            rows.append({'id': f"{hypothesis['id']}-{'P' if kind=='positive_controls' else 'N'}{number:02}",
                         'original_requirement': text, 'outcome': outcome, 'evidence': refs,
                         'boundary': boundary,
                         'original_requirement_fully_proven': outcome in ('bounded_verified','contract_verified','explicit_no_filter_limit')})
    assert len(rows) == 37 and len({r['id'] for r in rows}) == 37
    report = {'source': {'path': str(SOURCE), 'sha256': sha(SOURCE)},
              'rows': rows, 'original_controls': len(rows),
              'outcomes': dict(Counter(r['outcome'] for r in rows)),
              'unproven_original_controls': [r['id'] for r in rows if not r['original_requirement_fully_proven']],
              'stage4_complete': False, 'production_acceptance': False,
              'interpretation': 'All37 controls accounted for. Tested is not passed; partial, synthetic and disputed controls remain unproven. Bounded pass does not establish full-project improvement.',
              'next_action': 'Decide method eligibility from these outcomes before freezing the next candidate; retain all unresolved source obligations and original benchmark provenance. Do not repeat calls merely to obtain desired labels.'}
    write(HERE/'stage4_original_control_matrix.json', report)
    print(json.dumps({k: v for k, v in report.items() if k != 'rows'}, ensure_ascii=False))


if __name__ == '__main__':
    main()
