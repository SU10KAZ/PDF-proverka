"""Priority inventory and cheap local retrieval; scores never prove identity."""
from collections import Counter, defaultdict
from math import log
import re
from experiments.project_change_text_v1.engine import analyze, canonical, titles
from experiments.text_comparison_v1.common import digest

TOP_K=6
MAX_PACKET_CHARS=24000
MAX_CONTEXT_CHARS=400
STOP=set('и в на с по для из до от при что как или не более менее же то но а к у со во за'.split())
GENERIC=re.compile(r'подлежащ\w* обязательн\w* сертификац|должны иметь подтверждение|должны соответствовать требованиям|следует предусмотреть следующие мероприятия',re.I)
EQUIPMENT=re.compile(r'насос|фанкойл|теплообменник|чиллер|холодильн\w* машин|котел|котл|вытяжн\w* установ|приточн\w* установ',re.I)
CONFIG=re.compile(r'трубн\w* систем|зон\w* водоснабжен|режим|резервир|управлен|газоанализатор|концев\w* выключател',re.I)
SPECIFIC_SAFETY=re.compile(r'пожар|дым|безопасн|огнестойк',re.I)
MODEL=re.compile(r'(?<!\w)(?=[a-zа-я0-9_./-]*[a-zа-я])(?=[a-zа-я0-9_./-]*\d)[a-zа-я][a-zа-я0-9_./-]{2,}',re.I)


def priority(change):
    text=change['new_state'] or '';a=analyze(text)
    if GENERIC.search(text):return 'LOW',['GENERIC_COMPLIANCE_OR_INTRODUCTION']
    if EQUIPMENT.search(text):return 'HIGH',['EQUIPMENT_OR_SYSTEM_SOLUTION']
    if CONFIG.search(text):return 'HIGH',['SYSTEM_TYPE_CONFIGURATION_OR_MODE']
    if any(s['unit'] in ('W','m3/h','m3/day') for s in a['slots']) and re.search(r'суммар|расчетн|потребност|производительност',text,re.I):
        return 'HIGH',['SYSTEM_CAPACITY_OR_DEMAND']
    if SPECIFIC_SAFETY.search(text) and a['slots']:
        return 'HIGH',['SPECIFIC_SAFETY_REQUIREMENT']
    if a['slots']:return 'MEDIUM',['ENGINEERING_PARAMETER_OR_REQUIREMENT']
    return 'LOW',['SECONDARY_OR_GENERIC_ASSERTION']


def tokens(text):
    raw=canonical(text).replace('²','2').replace('³','3')
    words=re.findall(r'[а-яa-z]+|[+−-]?\d+(?:[.,]\d+)?',raw)
    # Retrieval-only stem approximation; never applied to evidence/decisions.
    return [w[:6] if len(w)>6 else w for w in words if w not in STOP]


def shingles(text):
    words=[w for w in tokens(text) if not re.fullmatch(r'[+−-]?\d+(?:[.,]\d+)?',w)]
    return {' '.join(words[i:i+2]) for i in range(len(words)-1)}


class Index:
    def __init__(self, units):
        self.units=units;self.postings=defaultdict(set)
        self.features=[]
        for i,u in enumerate(units):
            ts=Counter(tokens(u['text']));self.features.append((ts,shingles(u['text']),analyze(u['text'])))
            for t in ts:self.postings[t].add(i)
        self.idf={t:log(1+(len(units)-len(ids)+.5)/(len(ids)+.5)) for t,ids in self.postings.items()}
        self.mean_length=sum(sum(f[0].values()) for f in self.features)/max(1,len(units))

    def retrieve(self,new):
        query=set(tokens(new['text']));qsh=shingles(new['text']);qa=analyze(new['text'])
        qmodels=set(MODEL.findall(canonical(new['text'])))
        pool=set().union(*(self.postings.get(t,set()) for t in query)) if query else set()
        scored=[]
        for i in sorted(pool):
            u=self.units[i];ts,sh,a=self.features[i];length=sum(ts.values())
            lexical=sum(self.idf.get(t,0)*ts[t]*2.2/(ts[t]+1.2*(.25+.75*length/max(1,self.mean_length))) for t in query if ts[t])
            phrase=len(qsh&sh)/max(1,len(qsh|sh))
            models=set(MODEL.findall(canonical(u['text'])))
            bonuses=dict(phrase=phrase*3,mark=2*len(set(qa['marks'])&set(a['marks'])),
                         model=2*len(qmodels&models),heading=.5*bool(set(titles(new))&set(titles(u))))
            score=lexical+sum(bonuses.values())
            if score>0:scored.append(dict(unit_id=u['unit_id'],ordinal=i,score=round(score,6),lexical=round(lexical,6),bonuses=bonuses))
        scored.sort(key=lambda x:(-x['score'],x['unit_id']))
        # Preserve a repeated assertion only once per explicit local heading;
        # alternative scopes remain visible and are not resolved by ranking.
        result=[];seen=set()
        for row in scored:
            u=self.units[row['ordinal']];key=(canonical(u['text']),tuple(titles(u)))
            if key in seen:continue
            seen.add(key);result.append(row)
            if len(result)==TOP_K:break
        return result,len(pool)


def local_view(units,i):
    u=units[i]
    contexts=[]
    for n in (i-1,i+1):
        if not 0<=n<len(units):continue
        v=units[n]
        # Context must be local in the source, not merely adjacent after filtering.
        distance=min(abs(a['markdown_line']-b['markdown_line']) for a in u['source_refs'] for b in v['source_refs'])
        same_page=bool(set(u['page_span'])&set(v['page_span']))
        if same_page and distance<=5:
            contexts.append(dict(unit_id=v['unit_id'],text=v['text'][:MAX_CONTEXT_CHARS],
                                 truncated=len(v['text'])>MAX_CONTEXT_CHARS,position='before' if n<i else 'after'))
    a=analyze(u['text'])
    return dict(unit_id=u['unit_id'],text=u['text'],headings=titles(u),context=contexts,
                engineering_marks=a['marks'],equipment_classes=a['classes'],
                source_refs=u['source_refs'])


def packet(change,old,new,new_paragraphs=None):
    index=Index(old);new_index=next(i for i,u in enumerate(new) if u['unit_id']==change['evidence_new'][0]['local_unit_id'])
    query=local_view(new,new_index);rows,pool=index.retrieve(new[new_index]);candidates=[]
    if new_paragraphs:
        for u in new_paragraphs:
            if query['text'] in u['text'] and set(r['line_id'] for r in u['source_refs']) & set(r['line_id'] for r in query['source_refs']):
                if u['text']!=query['text']:
                    query['context'].append(dict(unit_id=u['unit_id'],text=u['text'],truncated=False,position='containing_paragraph'))
                break
    chars=len(query['text'])+sum(len(c['text']) for c in query['context'])
    for row in rows:
        v=local_view(old,row['ordinal']);cost=len(v['text'])+sum(len(c['text']) for c in v['context'])
        if chars+cost>MAX_PACKET_CHARS:break
        candidates.append(dict(**v,retrieval=row));chars+=cost
    p=dict(schema='old-scope-local-packet.v1',project_change_id=change['project_change_id'],
           comparison_scope=change['comparison_scope'],new=query,old_candidates=candidates,
           old_search_pool=pool,old_eligible_units=len(old),local_text_characters=chars,
           full_old_scope_coverage_proven=False,table_content_included=False,graphic_content_included=False)
    p['packet_hash']=digest(p)
    return p
