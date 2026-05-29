import json, re, unicodedata, sys
base='/home/coder/projects/PDF-proverka/comparison/sessions/ba413a93c5754f6c/pairs/p2ef68719'
res=json.load(open(base+'/enriched_comparison/comparison_result.json'))
L=open(base+'/text_enrichment/left_enriched.md',encoding='utf-8',errors='replace').read()
R=open(base+'/text_enrichment/right_enriched.md',encoding='utf-8',errors='replace').read()
def norm(s):
    s=unicodedata.normalize('NFKC',s or '').replace('ё','е').replace('Ё','Е')
    return re.sub(r'\s+',' ',s).strip().lower()
NL,NR=norm(L),norm(R)
def grounded(q,hay):
    q=norm(q)
    if len(q)<8: return None
    if q in hay: return 'exact'
    toks=[t for t in re.split(r'[\s,;:]+',q) if len(t)>=4]
    if not toks: return 'short'
    return f'{sum(1 for t in toks if t in hay)/len(toks):.2f}'
print("=== RESULT CONTRACT ===")
for k in ('status','strategy','fallback'): print(f"  {k}: {res.get(k)}")
print(f"  changes: {len(res.get('changes') or [])}")
d=res.get('diagnostics') or {}
print("  diagnostics:",json.dumps({k:d.get(k) for k in ('deterministic_changes','llm_changes_raw','llm_changes_dropped_ungrounded','duplicates_removed','final_changes','shared_header_chars')},ensure_ascii=False))
print("  chunks:",[(c.get('chunk_id'),c.get('total_chars')) for c in d.get('chunks',[])])
print("  chunk_results status:",[(c.get('chunk_id'),c.get('status'),c.get('changes_count')) for c in d.get('chunk_results',[])])
print("  input_stats:",json.dumps(res.get('input_stats'),ensure_ascii=False))
print("\n=== CHANGES + GROUNDING ===")
prov={}
for c in res.get('changes') or []:
    prov[c.get('provenance')]=prov.get(c.get('provenance'),0)+1
    el=(c.get('evidence_left') or {}).get('quote') or ''
    er=(c.get('evidence_right') or {}).get('quote') or ''
    gl=grounded(el,NL) if el else '∅'
    gr=grounded(er,NR) if er else '∅'
    print(f"  [{c.get('provenance')}/{c.get('source')}/{c.get('type')}/ver={c.get('evidence_verified')}] {c.get('title')[:62]}")
    print(f"      L({gl}) R({gr})")
print("\n provenance breakdown:",prov)
