"""Public resolver schema; the established ProjectChange schema is imported read-only."""
from experiments.project_change_text_v1.contract import obj, EVIDENCE, STRING, STRINGS
from .core import FIELDS, RELATIONS

SUBJECT=obj(dict(schema={'const':'engineering-subject.v1'},subject_id=STRING,
    source_type={'enum':['TEXT','TABLE','GRAPHIC']},side={'enum':['OLD','NEW']},
    document_version=STRING,comparison_scope=STRING,text=STRING,
    clues=obj({f:{'type':'array','items':{'type':'string'}} for f in FIELDS}),
    context={'type':'array','items':{'type':'object'}},
    evidence={'type':'array','minItems':1,'items':EVIDENCE},
    purity={'enum':['PROVEN','TABLE','UNPROVEN']},state=obj({'model_is_property':{'const':True}}),
    unit={'type':'object'},purity_audit={'type':'object'},cells={'type':'array','items':{'type':'string'}},
    headers={'type':'array','items':{'type':'array','items':{'type':'string'}}},
    header_evidence={'type':'array','items':EVIDENCE},table_key=STRING,row_key=STRING,
    page={'type':'integer','minimum':1},table_boundary_complete={'const':False},project_scope=STRING),
    required=['schema','subject_id','source_type','side','document_version','comparison_scope','text',
              'clues','context','evidence','purity','state'])
WITNESS=obj(dict(old_subject_id=STRING,new_subject_id=STRING,old_quote=STRING,new_quote=STRING))
RELATION=obj(dict(schema={'const':'engineering-subject-relation.v1'},candidate_id=STRING,
    packet_hash=STRING,source_type={'enum':['TEXT','TABLE','GRAPHIC']},relation={'enum':RELATIONS},
    old_subject_ids=STRINGS,new_subject_ids=STRINGS,confidence={'enum':['HIGH','MEDIUM','LOW']},
    method=STRING,reason=STRING,witnesses={'type':'array','items':WITNESS},
    state_relation={'const':'UNASSESSED'},review_required={'type':'boolean'},absence_proven={'const':False},
    identity_basis={'type':'string'},alternatives_reason={'type':'string'},restructuring_basis={'type':'string'}),
    required=['schema','candidate_id','packet_hash','source_type','relation','old_subject_ids','new_subject_ids',
              'confidence','method','reason','witnesses','state_relation','review_required','absence_proven'])
SCHEMA={'$schema':'https://json-schema.org/draft/2020-12/schema',
        '$id':'urn:pdf-proverka:engineering-subject:1',
        '$defs':{'EngineeringSubject':SUBJECT,'SubjectRelation':RELATION},
        'oneOf':[{'$ref':'#/$defs/EngineeringSubject'},{'$ref':'#/$defs/SubjectRelation'}]}
