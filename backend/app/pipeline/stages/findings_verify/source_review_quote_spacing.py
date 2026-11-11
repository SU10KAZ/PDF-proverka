"""Recover exact table-row citation spacing, never change cells or review verdicts."""
from copy import deepcopy
from ..text_analysis.requirement_checks import authoritative_text_blocks


def _cells(row):
    if not isinstance(row,str) or '\n' in row or '\r' in row or not row.startswith('|') or not row.endswith('|'):
        return None
    return tuple(cell.strip(' \t') for cell in row[1:-1].split('|'))


def repair_table_quote_spacing(response,md):
    result=deepcopy(response);repairs=[]
    blocks=authoritative_text_blocks(md)
    for ci,claim in enumerate(result.get('claims',[])):
        if not isinstance(claim,dict):continue
        for ei,evidence in enumerate(claim.get('evidence',[])):
            if not isinstance(evidence,dict) or evidence.get('type')!='text':continue
            text=blocks.get(evidence.get('block_id'),'');quote=evidence.get('quote')
            cells=_cells(quote)
            if cells is None or not text or quote in text:continue
            # Require one literal source row with identical ordered cell values,
            # including every empty cell. Never move numbers between columns.
            matches=[line for line in text.splitlines() if _cells(line)==cells]
            if len(matches)!=1:continue
            evidence['quote']=matches[0]
            repairs.append({'claim_index':ci,'evidence_index':ei,'block_id':evidence['block_id'],
                            'original_quote':quote,'exact_source_quote':matches[0],
                            'reason':'unique_table_row_same_ordered_cells_only_outer_cell_spacing'})
    return result,{'repairs':repairs,'raw_response_mutated':False,'semantic_verified':False}
