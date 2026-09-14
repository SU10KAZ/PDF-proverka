"""Standalone schema; no changes to EngineeringSubject or ProjectChange."""
from .core import DIMENSIONS, SOURCE_TYPES, APPROACHES


def obj(properties, required=None):
    return dict(type='object', additionalProperties=False, properties=properties,
                required=list(properties) if required is None else required)


STRING = dict(type='string', minLength=1)
STRINGS = dict(type='array', items=STRING, uniqueItems=True)
STATUS = dict(enum=['PROVEN','REVIEW'])
EVIDENCE = dict(type='object', required=['document_version','page','quote','source_receipts'],
                properties=dict(document_version=STRING,page=dict(type='integer',minimum=1),
                                quote=dict(type='string'),source_receipts=dict(type='object')))
NODE = obj(dict(scope_id=STRING,document_version=STRING,dimension=dict(enum=list(DIMENSIONS)),
                value=STRING,interval=dict(type='array',minItems=2,maxItems=2,items=dict(type='integer',minimum=0)),
                pages=dict(type='array',minItems=1,uniqueItems=True,items=dict(type='integer',minimum=1)),
                parent_scope_ids=STRINGS,scope_evidence=dict(type='array',minItems=1,items=EVIDENCE),
                status=STATUS,container_id=dict(type=['string','null'])))
SCHEMA = {'$schema':'https://json-schema.org/draft/2020-12/schema',
          '$id':'urn:pdf-proverka:evidence-scope:1',
          **obj(dict(schema=dict(const='evidence-scope.v1'),fragment_id=STRING,document_version=STRING,
                     source_type=dict(enum=list(SOURCE_TYPES)),route=dict(enum=['TEXT','TABLE']),
                     container_id=dict(type=['string','null']),parent_scope_ids=STRINGS,
                     dimensions=obj({d:dict(anyOf=[STRINGS,dict(type='null')]) for d in DIMENSIONS}),
                     scope_nodes=dict(type='array',items=NODE),source_evidence=dict(type='array',minItems=1,items=EVIDENCE),
                     status=STATUS,confidence=dict(enum=['HIGH','LOW']),reasons=STRINGS,method=dict(enum=list(APPROACHES))))}
