"""Source-declared paired flow/return temperatures are one engineering state."""
from collections import defaultdict, Counter
from pathlib import Path
import re

from experiments.project_change_text_v1.engine import analyze, canonical, make_change, evidence
from experiments.project_change_text_v1.contract import validate
from experiments.text_comparison_v1.common import digest

from .inventory import ROOT, read, immutable, sha, now
from .policy import admitted_pairs

SCHEDULE = re.compile(r'(?<![\w\d])(?P<a>\d{2,3})\s*[-/]\s*(?P<b>\d{2,3})\s*°\s*[cс]', re.I)
NAMED_PAIR = re.compile(r'\b[тtхx]\d{1,3}\s*/\s*[тtхx]\d{1,3}\b', re.I)


def observations(document):
    lines = Path(document['artifacts']['work_md']['path']).read_text().splitlines()
    page = None
    block_id = None
    block_type = None
    block_line = 0
    out = []
    for n, text in enumerate(lines, 1):
        p = re.fullmatch(r'## Page (\d+)\s*', text)
        b = re.fullmatch(r'### BLOCK #\S+ \[([^]]+)\]:\s*(\S+)\s*', text)
        if p:
            page = int(p[1])
        if b:
            block_type, block_id, block_line = b[1].upper(), b[2], n
        if block_type != 'TEXT' or text.startswith(('>', '|', '#', '**Description:', '**Summary:', '**Entities:')):
            continue
        normalized = canonical(text)
        hits = list(SCHEDULE.finditer(normalized))
        if len(hits) != 1 or not NAMED_PAIR.search(normalized):
            continue
        if not re.search(r'подающ\w*\s*/\s*обратн\w*', normalized):
            continue
        m = hits[0]
        template = normalized[:m.start()] + '<flow_return_temperature>' + normalized[m.end():]
        unit = dict(unit_id='decl_' + digest([document['document_version'], n, text])[:24],
            document_version=document['document_version'], document_code=document['document_code'], text=text,
            source_refs=[dict(document_version=document['document_version'], line_id=n-1, page=page, block_id=block_id,
                              markdown_line=n, within_block_line=n-block_line,
                              line_sha256=__import__('hashlib').sha256(text.encode()).hexdigest(), edge='LINE')],
            source_receipts={k: {z: v[z] for z in ['path', 'sha256']} for k, v in document['artifacts'].items()},
            section_context=[], text_purity_basis=['EXPLICIT_TEXT_BLOCK', 'NAMED_FLOW_RETURN_FUNCTION_DECLARATION'])
        out.append(dict(template=template, unit=unit, value=m['a'] + '/' + m['b'], quote=m[0]))
    return out


def compare(scope, old, new):
    aa, bb = defaultdict(list), defaultdict(list)
    for o in old:
        aa[o['template']].append(o)
    for o in new:
        bb[o['template']].append(o)
    changes = []
    outcomes = Counter()
    for template in sorted(aa.keys() & bb.keys()):
        a, b = aa[template], bb[template]
        if len({x['value'] for x in a}) != 1 or len({x['value'] for x in b}) != 1:
            outcomes['CONFLICTING_NAMED_FUNCTION_STATE'] += 1
            continue
        if a[0]['value'] == b[0]['value']:
            outcomes['UNCHANGED'] += 1
            continue
        x, y = a[0], b[0]
        ua, ub = x['unit'], y['unit']
        fact = dict(fact_id='fact_' + digest([scope, template, x['value'], y['value']])[:24],
            property='flow_return_temperature',
            old=dict(value=x['value'], quote=x['quote'], unit='°C'),
            new=dict(value=y['value'], quote=y['quote'], unit='°C'),
            evidence_old=[evidence(ua)['evidence_id']], evidence_new=[evidence(ub)['evidence_id']])
        sa, sb = analyze(ua['text']), analyze(ub['text'])
        sa['template'] = sb['template'] = template
        change = make_change(scope, ua, ub, sa, sb, [fact], [], [], 'SYSTEM_MODE_CHANGED')
        change['short_summary_ru'] = 'Изменен температурный график: ' + x['quote'] + ' → ' + y['quote'] + '.'
        change['decision_reasons'] = ['Same complete named flow/return function declaration; compound temperature state compared as a whole.']
        for side, observations_ in [('old', a), ('new', b)]:
            change['evidence_' + side] = [evidence(o['unit']) for o in observations_]
        validate(change)
        changes.append(change)
        outcomes['NAMED_COMPOUND_STATE_CHANGED'] += 1
    return dict(project_changes=changes, outcomes=dict(outcomes))


def run():
    admitted_pairs('DEV')
    base = ROOT / 'sources/DEV'
    out = ROOT / 'cycles/06_compound_states'
    immutable(out / 'MANIFEST.json', dict(started_at=now(), partition='DEV', split_sha256=sha(ROOT/'SPLIT.json'),
        code_sha256=sha(__file__), change='Paired flow/return state attached to an explicit full function declaration'))
    events = []
    for pair in read(base / 'PAIRS.json'):
        old, new = observations(pair['old']), observations(pair['new'])
        result = compare(pair['pair_key'], old, new)
        immutable(out / 'pairs' / (str(pair['index']) + '.json'), dict(old=old, new=new, result=result))
        events += [dict(pair_index=pair['index'], change=c) for c in result['project_changes']]
        print(pair['index'], len(old), len(new), result['outcomes'], flush=True)
    immutable(out / 'RESULTS.json', dict(project_changes=events, adjudication='NOT_ADJUDICATED'))


if __name__ == '__main__':
    run()
