"""Quarantine source revision histories, including unheaded continuations."""
from pathlib import Path
import re


def pages_from_markdown(text):
    pages={}
    for match in re.finditer(r'^## Page (\d+)\s*\n(.*?)(?=^## Page \d+\s*$|\Z)',text,re.M|re.S):
        pages[int(match[1])]=match[2]
    return pages


def history_pages(text,embargo=()):
    excluded={}
    active=False
    for page,body in sorted(pages_from_markdown(text).items()):
        if page in embargo:
            # The supplied text has already been blanked. Never read through an
            # embargo just to identify a boundary; retain conservative state.
            continue
        normalized=re.sub(r'\s+',' ',body).casefold()
        rows=[[c.strip() for c in line.strip().strip('|').split('|')] for line in body.splitlines()
              if line.strip().startswith('|') and not re.fullmatch(r'[\s|:\-]+',line)]
        substantive=[r for r in rows if len(r)>=3 and sum(len(c) for c in r)>35]
        references=sum(bool(re.search(r'\b(?:тч|гч|том|лист\w*)\b',r[-1],re.I)) for r in substantive)
        header=('ранее разработан' in normalized and 'суть изменени' in normalized)
        title=bool(re.search(r'справка\s+о\s+(?:внесенн\w*\s+)?изменени',normalized))
        continuation=active and bool(substantive)
        orphan=len(substantive)>=2 and references/max(1,len(substantive))>=.4
        if header or title or continuation or orphan:
            excluded[page]='EXPLICIT_HISTORY' if header or title else 'CONTINUED_OR_LOCATOR_COLUMN_HISTORY'
            active=True
        elif body.strip():
            active=False
    return excluded


def document_history(document,embargo):
    return history_pages(Path(document['artifacts']['work_md']['path']).read_text(),embargo)
