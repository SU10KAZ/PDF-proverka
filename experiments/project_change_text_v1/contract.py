"""Shared route-neutral ProjectChange contract, with TEXT V1 safety checks."""
from experiments.text_comparison_v1.common import digest
import re

TYPES = ['EQUIPMENT_REPLACED', 'EQUIPMENT_ADDED', 'EQUIPMENT_REMOVED',
         'EQUIPMENT_COUNT_CHANGED', 'SYSTEM_TYPE_CHANGED', 'SYSTEM_CONFIGURATION_CHANGED',
         'SYSTEM_MODE_CHANGED', 'CAPACITY_CHANGED', 'LAYOUT_CHANGED', 'ROUTING_CHANGED',
         'REQUIREMENT_CHANGED', 'ENGINEERING_SOLUTION_CHANGED', 'OTHER_ENGINEERING_CHANGE']


def obj(properties, required=None):
    return dict(type='object', additionalProperties=False, properties=properties,
                required=list(properties) if required is None else required)


STRING = dict(type='string', minLength=1)
NULL_STRING = dict(type=['string', 'null'])
STRINGS = dict(type='array', items=STRING)
REF = obj(dict(document_version=STRING, line_id=dict(type='integer', minimum=0),
               page=dict(type='integer', minimum=1), block_id=STRING,
               markdown_line=dict(type='integer', minimum=1),
               within_block_line=dict(type='integer', minimum=0),
               line_sha256=dict(type='string', pattern='^[a-f0-9]{64}$'), edge=STRING))
RECEIPT = obj(dict(path=STRING, sha256=dict(type='string', pattern='^[a-f0-9]{64}$')))
EVIDENCE = obj(dict(evidence_id=STRING, route=dict(enum=['TEXT', 'TABLE', 'GRAPHIC']),
                    document_version=STRING, document_code=STRING, quote=NULL_STRING,
                    source_refs=dict(type='array', items=REF),
                    source_receipts=obj(dict(work_md=RECEIPT, blocks=RECEIPT, pdf=RECEIPT), required=['pdf']),
                    local_unit_id=STRING, section_titles=STRINGS,
                    locator=dict(type=['object', 'null']), text_purity_basis=STRINGS))
EVIDENCE['allOf'] = [dict(
    **{'if': dict(properties=dict(route=dict(const='TEXT'))),
       'then': dict(properties=dict(quote=STRING, source_refs=dict(minItems=1),
                                   source_receipts=dict(required=['work_md','blocks','pdf']))),
       'else': dict(properties=dict(locator=dict(type='object', minProperties=1)))})]
VALUE = obj(dict(value=STRING, quote=STRING, unit=NULL_STRING))
FACT = obj(dict(fact_id=STRING, property=STRING, old=VALUE, new=VALUE,
               evidence_old=STRINGS, evidence_new=STRINGS))
ENTITY = obj(dict(entity_id=STRING, system=NULL_STRING, equipment_class=NULL_STRING,
                  mark=NULL_STRING, equipment_model_old=NULL_STRING,
                  equipment_model_new=NULL_STRING, room=NULL_STRING, floor=NULL_STRING,
                  engineering_function=NULL_STRING, semantic_subject=STRING,
                  scope_key=STRING, identity_basis=STRINGS,
                  resolution=dict(enum=['EXPLICIT', 'LOCAL', 'AMBIGUOUS'])))
CHANGE = obj(dict(project_change_id=STRING, comparison_scope=STRING,
                 change_type=dict(enum=TYPES), engineering_subject=ENTITY,
                 old_state=dict(type=['string', 'null']), new_state=dict(type=['string', 'null']),
                 short_summary_ru=STRING, importance=dict(enum=['MATERIAL', 'UNASSESSED']),
                 scope=dict(enum=['EQUIPMENT', 'SYSTEM', 'LOCAL_ASSERTION']),
                 status=dict(enum=['PROVEN', 'REVIEW']), confidence=dict(enum=['HIGH', 'MEDIUM', 'LOW']),
                 evidence_old=dict(type='array', items=EVIDENCE),
                 evidence_new=dict(type='array', items=EVIDENCE),
                 supporting_fact_changes=dict(type='array', items=FACT),
                 decision_reasons=STRINGS, review_reasons=STRINGS,
                 event_key=STRING, routes=STRINGS,
                 conflicts=dict(type='array', items=obj(dict(
                     conflict_type=dict(enum=['CROSS_SOURCE_CONFLICT']), property=STRING,
                     evidence_ids=STRINGS, status=dict(enum=['OPEN', 'RESOLVED']))))))
SCHEMA = dict(**{'$schema': 'https://json-schema.org/draft/2020-12/schema',
                 '$id': 'urn:pdf-proverka:project-change:1'}, **CHANGE)


def validate(change, text_only=True):
    """Semantic invariants beyond the separately exported JSON Schema."""
    import jsonschema
    jsonschema.Draft202012Validator(SCHEMA).validate(change)
    old, new = change['evidence_old'], change['evidence_new']
    assert old or new, 'Evidence is required even for REVIEW'
    evidence = old + new
    ids = {e['evidence_id'] for e in evidence}
    assert len(ids) == len(evidence), 'Duplicate evidence'
    assert set(change['routes']) == {e['route'] for e in evidence}
    if text_only:
        assert change['routes'] == ['TEXT']
        assert not change['conflicts'], 'V1 has no conflict inference'
    for side in ('old', 'new'):
        side_ids = {e['evidence_id'] for e in change['evidence_' + side]}
        for f in change['supporting_fact_changes']:
            assert f['evidence_' + side] and set(f['evidence_' + side]) <= side_ids
            normalize = lambda s: re.sub(r'\s+', ' ', s.casefold().replace('ё', 'е')).strip()
            supporting = [e for e in change['evidence_' + side] if e['evidence_id'] in f['evidence_' + side]]
            quotes = [normalize(e['quote']) for e in supporting if e['quote'] is not None]
            if text_only or all(e['route']=='TEXT' for e in supporting):
                assert any(normalize(f[side]['quote']) in q for q in quotes)
            # Future non-TEXT evidence is structurally expressible; route-specific
            # claim verification is deliberately not implemented by TEXT V1.
    if change['status'] == 'PROVEN':
        assert old and new, 'TEXT V1 never infers addition/removal from absence'
        assert change['confidence'] == 'HIGH' and not change['review_reasons']
        assert change['engineering_subject']['resolution'] != 'AMBIGUOUS'
        assert change['supporting_fact_changes']
    if change['confidence'] == 'LOW':
        assert change['status'] == 'REVIEW'
    if change['change_type'] == 'EQUIPMENT_REPLACED' and change['status'] == 'PROVEN':
        assert any(f['property'] == 'model' for f in change['supporting_fact_changes'])
    assert change['project_change_id'] == 'pc_' + digest(change['event_key'])[:24]
