"""Structured witnesses with separate literal and visual validation routes."""


def obj(properties):
    return dict(type='object', properties=properties, required=list(properties), additionalProperties=False)


def arr(items):
    return dict(type='array', items=items)


STRING = dict(type='string')
BOOL = dict(type='boolean')
WITNESS = obj(dict(evidence_id=STRING,
    kind=dict(type='string', enum=['TEXT_LITERAL', 'RASTER_LOCATOR']),
    literal_quote=STRING, visual_locator=STRING, bbox_norm=arr(dict(type='number')),
    binding_reason=STRING, route=dict(type='string', enum=['TEXT', 'TABLE', 'GRAPHIC'])))
FACT = obj(dict(property=STRING, old_value=STRING, new_value=STRING,
                old_witnesses=arr(WITNESS), new_witnesses=arr(WITNESS)))
EVENT = obj(dict(event_id=STRING, engineering_subject=STRING, identity_basis=STRING,
    old_state=STRING, new_state=STRING, summary_ru=STRING,
    change_type=dict(type='string', enum=['SYSTEM_CONFIGURATION_CHANGED', 'SYSTEM_MODE_CHANGED',
        'CAPACITY_CHANGED', 'EQUIPMENT_REPLACED', 'REQUIREMENT_CHANGED', 'ENGINEERING_SOLUTION_CHANGED']),
    confidence=dict(type='string', enum=['HIGH', 'MEDIUM', 'LOW']),
    importance=dict(type='string', enum=['HIGH', 'LOW']), facts=arr(FACT)))
PROPOSAL = obj(dict(events=arr(EVENT), unknowns=arr(STRING), unchanged=arr(STRING)))
VERIFICATION = obj(dict(decisions=arr(obj(dict(event_id=STRING,
    verdict=dict(type='string', enum=['ACCEPT', 'REVIEW', 'REJECT']), reason=STRING,
    scope_correct=BOOL, states_entailed=BOOL, material_change=BOOL, grouping_correct=BOOL)))))


def validate(value, schema):
    # The local validator is mandatory even when the server promises structured output.
    import jsonschema
    jsonschema.Draft202012Validator(schema).validate(value)
