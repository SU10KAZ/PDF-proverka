"""Local narrative scope adapter; no changes to the frozen TEXT producer.

A narrative heading describing a scheme does not make its prose a drawing.
Structured tables, actual scheme captions and ambiguous blocks remain excluded.
"""
from collections import defaultdict,Counter
import re
from experiments.text_comparison_v1.common import read,file_hash
from experiments.text_safe_coverage.sections import materialize
from experiments.text_alignment_v2.units import from_materialization
from experiments.project_change_text_v1.narrative import purity_reasons,NON_TEXT

NARRATIVE_HEADING=re.compile(r'описани|обосновани|сведени|мероприяти|требовани',re.I)
HARD_NON_TEXT=re.compile(r'таблиц|спецификац|экспликац|условн\w*\s+обознач|легенд',re.I)


def non_narrative_heading(title):
    return bool(HARD_NON_TEXT.search(title) or (NON_TEXT.search(title) and not NARRATIVE_HEADING.search(title)))


def build(document):
    for name in ('work_md','blocks','pdf'):
        r=document['artifacts'][name]
        assert file_hash(r['path'])==r['sha256']
    mat=materialize(document);base=from_materialization(document,mat)
    types=defaultdict(set)
    for b in read(document['artifacts']['blocks']['path']).get('blocks',[]):
        types[(int(b['page_index'])+1,str(b['block_id']))].add(b.get('block_type','').lower())
    bad_blocks=defaultdict(bool)
    for s in mat['sections']:
        if non_narrative_heading(s['section_title']):
            for ref in s['source_refs']:bad_blocks[(ref['page'],ref['block_id'])]=True
    accepted=[];quarantined=[]
    for u in base['units']:
        probe={**u,'section_context':[s for s in u.get('section_context',[]) if non_narrative_heading(s['title'])],
               'nearest_heading':u.get('nearest_heading') if non_narrative_heading(u.get('nearest_heading') or '') else None}
        reasons=purity_reasons(probe,types,bad_blocks)
        if reasons:
            quarantined.append(dict(unit_id=u['unit_id'],reasons=reasons,source_refs=u['source_refs']))
            continue
        accepted.append({**u,'text_purity_basis':['FOUNDATION_TEXT_ROUTE','RAW_TEXT_BLOCK','LOCAL_NARRATIVE_ASSERTION',
                                                 'NARRATIVE_HEADING_DESCRIBES_SOLUTION_NOT_DRAWING'],
                         'source_receipts':{k:{x:document['artifacts'][k][x] for x in ('path','sha256')} for k in ('work_md','blocks','pdf')}})
    return dict(document_version=document['document_version'],units=accepted,quarantined=quarantined,
                quality=dict(input_paragraphs=len(base['units']),accepted_paragraphs=len(accepted),
                             quarantine_reasons=dict(Counter(z for q in quarantined for z in q['reasons']))))
