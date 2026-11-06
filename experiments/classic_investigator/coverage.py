"""Reconstruct source access from successful host actions, not model self-reports."""
import argparse
import json
from pathlib import Path
from investigate import Corpus, write, sha


def assess(work):
    rows=[]
    for status in sorted((work/'runs').glob('*/status.json')):
        state=json.loads(status.read_text());disc=status.parent.name.split('_')[0]
        corpus=Corpus(work/'sources'/disc,work/'evaluation/coverage_scratch')
        read_ranges={};viewed=set();calculations=0;errors=[]
        for path in sorted(status.parent.glob('*_evidence.json')):
            for entry in json.loads(path.read_text()):
                action=entry.get('action',{});result=entry.get('result',{})
                if result.get('error'):
                    errors.append({'step':path.name,'action':action,'error':result['error']});continue
                if action.get('tool')=='read' and not result.get('navigation_only'):
                    offset=int(action.get('offset',0));read_ranges.setdefault(action['id'],[]).append((offset,offset+len(result.get('text',''))))
                elif action.get('tool')=='image':viewed.add(action['id'])
                elif action.get('tool')=='calculate':calculations+=1
        fully_read=[];partial=[];unread=[]
        for bid,b in corpus.blocks.items():
            if b['kind']!='TEXT':continue
            end=0
            for start,stop in sorted(read_ranges.get(bid,[])):
                if start>end:break
                end=max(end,stop)
            if end>=len(b['text']):fully_read.append(bid)
            elif bid in read_ranges:partial.append(bid)
            else:unread.append(bid)
        graphic={bid for bid,b in corpus.blocks.items() if b['kind']!='TEXT'}
        rows.append({'run':status.parent.name,'source_status_sha256':sha(status),'status':state['status'],
            'findings':len(state['findings']),'questions':sum(f['status']=='question' for f in state['findings']),
            'text_total':len(fully_read)+len(partial)+len(unread),'text_fully_read':fully_read,
            'text_partial':partial,'text_not_explicitly_read':unread,'graphic_total':len(graphic),
            'graphic_requested':sorted(viewed),'graphic_not_requested':sorted(graphic-viewed),
            'calculator_calls':calculations,'tool_errors':errors,
            'self_reported_unfinished':[op for op in state['operations'] if op.get('status') in ('not_checked','needs_data')],
            'limits':'Full read means delivered text ranges, not understood or checked. Image request means bytes supplied, not correct reading. Search snippets excluded from full-block coverage.'})
    write(work/'evaluation/coverage.json',rows)
    for r in rows:print(r['run'],'TEXT',len(r['text_fully_read']),'/',r['text_total'],'GRAPHIC',len(r['graphic_requested']),'/',r['graphic_total'],'calculations',r['calculator_calls'])

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);assess(p.parse_args().work)
