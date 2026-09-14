"""Source-authored change claims with independent OLD-source retrieval.

NEW's before-column is a claim, never an OLD witness. Every claim stays REVIEW
until independent current OLD and NEW evidence is adjudicated. This produces a
bounded coverage audit and tests whether the current architecture sees valuable
project changes; retrieval scores are not correctness probabilities.
"""
from collections import Counter
import hashlib
from pathlib import Path
import re

import fitz

from .inventory import ROOT, read, immutable, sha, now
from .policy import admitted_pairs


def tokens(text):
    # Conservative prefix stems are retrieval only, never subject certificates.
    return {w[:7] for w in re.findall(r'[а-яёa-z]{4,}', text.casefold())}


def claims(document):
    lines = Path(document['artifacts']['work_md']['path']).read_text().splitlines()
    page = None
    result = []
    headers = None
    for n, line in enumerate(lines, 1):
        match = re.fullmatch(r'## Page (\d+)\s*', line)
        if match:
            page = int(match[1])
            headers = None  # No assumed cross-page continuation.
        if not line.startswith('|'):
            continue
        cells = [c.strip() for c in line.strip('|').split('|')]
        if any('Ранее разработанные' in c for c in cells) and any('Суть изменения' in c for c in cells):
            headers = (next(i for i, c in enumerate(cells) if 'Ранее разработанные' in c),
                       next(i for i, c in enumerate(cells) if 'Суть изменения' in c), n)
            continue
        if headers and len(cells) > max(headers[:2]):
            old, new = [cells[i] for i in headers[:2]]
            if len(old) < 15 or len(new) < 15 or not re.search('[а-яА-Я]', old + new):
                continue
            result.append(dict(claim_id=hashlib.sha256((document['document_version'] + ':' + str(n)).encode()).hexdigest()[:24],
                old_state_claim_in_NEW=old, new_state_claim_in_NEW=new,
                reference=cells[-1], page=page, markdown_line=n, header_markdown_line=headers[2],
                document_version=document['document_version'], source_receipts=document['artifacts']))
    return result


def native_sources(document, embargo):
    """All nonembargo PDF text blocks, including labels located on drawings.

    The bounding box and native quote support retrieval; a text label alone
    does not establish drawing topology, connection or object existence.
    """
    pdf = fitz.open(document['artifacts']['pdf']['path'])
    blocks = read(document['artifacts']['blocks']['path'])['blocks']
    images = {}
    for block in blocks:
        if block.get('block_type') == 'image':
            images.setdefault(block['page_index'] + 1, []).append(block['coords_norm'])
    out = []
    for pageno, page in enumerate(pdf, 1):
        if pageno in embargo:
            continue
        for block in page.get_text('blocks'):
            quote = block[4].strip()
            if len(quote) < 25 or block[6] != 0:
                continue
            box = fitz.Rect(block[:4])
            graphic = any((box & fitz.Rect(c[0]*page.rect.width, c[1]*page.rect.height,
                                          c[2]*page.rect.width, c[3]*page.rect.height)).get_area() >= box.get_area()*.7
                          for c in images.get(pageno, []))
            out.append(dict(page=pageno, bbox=list(box), quote=quote,
                            document_version=document['document_version'],
                            source_kind='PDF_NATIVE_DRAWING_LABEL' if graphic else 'PDF_NATIVE_TEXT',
                            source_receipt=document['artifacts']['pdf']))
    pdf.close()
    return out


def retrieve(query, pool, k=4):
    q = tokens(query)
    rows = []
    for item in pool:
        words = tokens(item['quote'])
        overlap = len(q & words)
        if overlap < 2:
            continue
        score = overlap / max(1, len(q)) + overlap / max(1, len(words)) * .2
        rows.append(dict(score=round(score, 4), evidence=item))
    return sorted(rows, key=lambda r: (-r['score'], r['evidence']['page'], r['evidence']['bbox']))[:k]


def run():
    admitted_pairs('DEV')
    base = ROOT / 'sources/DEV'
    out = ROOT / 'cycles/04_source_authored_claims'
    immutable(out / 'MANIFEST.json', dict(started_at=now(), partition='DEV',
        split_sha256=sha(ROOT / 'SPLIT.json'), code_sha256=sha(__file__),
        purpose='Independent OLD-source retrieval for NEW-authored change claims; source-only coverage inventory',
        acceptance_rule='NONE: retrieval never proves engineering state; adjudication required',
        excluded='No implied absence, no identity from similarity, no cross-project data'))
    all_cases = []
    for pair in read(base / 'PAIRS.json'):
        declared = claims(pair['new'])
        if not declared:
            print(pair['index'], 'no explicit change-table header', flush=True)
            continue
        old = native_sources(pair['old'], pair['embargo_pages']['old'])
        new = native_sources(pair['new'], pair['embargo_pages']['new'])
        claim_pages = {c['page'] for c in declared}
        # Independent NEW-body candidates exclude the explicit claim pages.
        new = [e for e in new if e['page'] not in claim_pages]
        cases = []
        for c in declared:
            q = c['old_state_claim_in_NEW'] + ' ' + c['new_state_claim_in_NEW']
            cases.append(dict(pair_index=pair['index'], claim=c,
                old_candidates=retrieve(q, old), new_body_candidates=retrieve(q, new),
                status='REVIEW_SOURCE_CLAIM_NOT_CORROBORATED', absence_proven=False))
        immutable(out / 'pairs' / (str(pair['index']) + '.json'), dict(cases=cases))
        all_cases += cases
        print(pair['index'], 'claims', len(cases), 'native OLD/NEW blocks', len(old), len(new), flush=True)
    immutable(out / 'RESULTS.json', dict(cases=all_cases, count=len(all_cases), accepted=0,
        adjudication='NOT_ADJUDICATED', limitation='Explicit first-page table rows only; no undocumented continuation. Native text cannot establish drawing topology.'))


if __name__ == '__main__':
    run()
