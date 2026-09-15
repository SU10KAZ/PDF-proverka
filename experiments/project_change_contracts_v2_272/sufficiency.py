"""Claim-type requirements, independent of full supporting-package completeness."""
VERSION = 'EVIDENCE_SUFFICIENCY_PROFILE/2'
PROFILES = {
    'TOPOLOGY_DECLARATION': dict(required=['same_subject', 'old_declaration', 'new_declaration', 'no_relevant_conflict'], graphics='SUPPORTING'),
    'INPUT_CRITERION': dict(required=['same_subject', 'comparable_scope', 'old_value', 'new_value'], graphics='SUPPORTING', downstream_equipment=False),
    'REQUIREMENT': dict(required=['same_subject', 'comparable_scope', 'old_value', 'new_value'], graphics='SUPPORTING', downstream_equipment=False),
    'SELECTED_EQUIPMENT': dict(required=['same_functional_subject', 'old_selected_equipment', 'new_selected_equipment'], source_label_identity=False),
    'AGGREGATE_CALCULATED_RESULT': dict(required=['same_subject', 'comparable_scope', 'composition', 'role', 'basis', 'calculation_conditions']),
    'NOVEL_SYSTEM': dict(required=['new_positive', 'bounded_old_negative']),
    'ADDED_FUNCTION': dict(required=['new_positive', 'bounded_old_negative']),
    'TOPOLOGY': dict(required=['same_subject', 'old_state', 'new_state', 'claim_graphic_counterparts']),
    'ROUTING': dict(required=['same_subject', 'old_state', 'new_state', 'no_relevant_conflict']),
}


def evaluate(profile, transition, *, evidence_forms, negative=None, comparison=None):
    errors = []
    roles = {
        'TOPOLOGY_DECLARATION': {'TOPOLOGY'}, 'INPUT_CRITERION': {'INPUT_CRITERION'},
        'REQUIREMENT': {'REQUIREMENT'}, 'SELECTED_EQUIPMENT': {'SELECTED_EQUIPMENT'},
        'AGGREGATE_CALCULATED_RESULT': {'CALCULATED_RESULT', 'CAPACITY', 'COUNT'},
        'NOVEL_SYSTEM': {'TOPOLOGY', 'INSTALLED_CONFIGURATION'},
        'ADDED_FUNCTION': {'TOPOLOGY', 'INSTALLED_CONFIGURATION'},
        'TOPOLOGY': {'TOPOLOGY'}, 'ROUTING': {'ROUTING'},
    }
    if profile not in PROFILES:
        errors.append('UNKNOWN_SUFFICIENCY_PROFILE')
    if any(s.state_role not in roles.get(profile, set()) for s in (transition.old, transition.new)):
        errors.append('STATE_ROLE_DOES_NOT_MATCH_CLAIM_PROFILE')
    if comparison and comparison['status'] != 'COMPARABLE':
        errors.append('COMPARABLE_STATE_CONDITIONS_REQUIRED')
    if transition.state_support != 'SOURCE_SUPPORTED':
        errors.append('SOURCE_SUPPORTED_STATES_REQUIRED')
    if profile == 'TOPOLOGY_DECLARATION' and any(evidence_forms.get(s) != 'DECLARATION' for s in ('old', 'new')):
        errors.append('UNAMBIGUOUS_DECLARATIONS_REQUIRED')
    if profile == 'SELECTED_EQUIPMENT' and any(evidence_forms.get(s) != 'SELECTED_EQUIPMENT' for s in ('old', 'new')):
        errors.append('SELECTED_EQUIPMENT_EVIDENCE_REQUIRED')
    if profile in {'NOVEL_SYSTEM', 'ADDED_FUNCTION'} and (negative or {}).get('status') != 'PROVEN_ABSENT_IN_BOUNDED_SCOPE':
        errors.append('BOUNDED_OLD_NEGATIVE_REQUIRED')
    return dict(schema=VERSION, profile=profile, sufficient=not errors, reasons=errors,
                graphics_complete_required=profile == 'TOPOLOGY', downstream_equipment_required=False)
