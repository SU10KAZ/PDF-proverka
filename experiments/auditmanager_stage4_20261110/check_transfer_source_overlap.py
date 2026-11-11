"""Exact-byte overlap screen. Hash source bytes locally; never emit source text."""
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re

BASE=Path(__file__).parent
KNOWN=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/input_manifest.json')


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()


def name_key(name):
    # Search hint only: never a proof of source identity.
    return re.sub(r'[-_ ]','',re.sub(r'_V\d+$','',name,flags=re.I)).casefold()


def main():
    inventory_path=BASE/'transfer_source_inventory.json'
    inventory=json.loads(inventory_path.read_text())
    known=json.loads(KNOWN.read_text())
    hashes={r['source_md_sha256']:r['discipline'] for r in known}
    names={name_key(r['project']):r['discipline'] for r in known}
    rows=[];groups=defaultdict(list);known_objects=set();errors=[]
    for r in inventory['rows']:
        if not r['prepared_md_present'] or not r['valid_version_selector']:continue
        path=Path(r['path'])/'versions'/r['current_version']/'02_work/document.md'
        try:sha=digest(path)
        except OSError as exc:
            errors.append({'path':str(path),'error':str(exc)});continue
        exact=hashes.get(sha)
        hint=names.get(name_key(r['document_directory']))
        if exact or hint:known_objects.add(r['object_id'])
        item={k:r[k] for k in ['object_id','storage_discipline','document_directory','current_version','path','prepared_pdf_present','explicit_AK_filename_marker']}
        item.update(source_md_sha256=sha,exact_known_MD_match=exact,known_name_hint=hint,
                    prior_exposure_verified=False,reserve_eligible=None)
        rows.append(item);groups[sha].append(r['path'])
    for r in rows:
        r['object_contains_known_control_or_name_hint']=r['object_id'] in known_objects
    report={'status':'overlap_screen_not_blind_reserve_certification',
        'inventory_sha256':digest(inventory_path),'known_manifest_sha256':digest(KNOWN),
        'source_content_handling':'MD bytes streamed through local SHA256 only; no text, findings or engineer answers emitted or inspected.',
        'rows':rows,'read_errors':errors,
        'known_object_ids_by_hash_or_name_hint':sorted(known_objects),
        'exact_known_matches':sum(bool(r['exact_known_MD_match']) for r in rows),
        'known_name_hints':sum(bool(r['known_name_hint']) for r in rows),
        'counts_by_object':dict(Counter(r['object_id'] for r in rows)),
        'duplicate_MD_groups':[{'sha256':sha,'paths':paths} for sha,paths in groups.items() if len(paths)>1],
        'limits':['Different MD bytes do not establish unseen project content or different source PDF.',
                  'Name normalization is only a hint; no Cyrillic/Latin substitutions were made.',
                  'Known object siblings are exposed-group candidates, not automatically unseen projects.',
                  'Historical access/exposure audit, source-PDF deduplication and engineering profile checks remain required.',
                  'The original minimum of eighteen audit project packages is unchanged; statistical independence must use construction-object clusters, not count ciphers as independent observations.'],
        'selected_calibration':[],'selected_reserve':[],'production_acceptance':False}
    (BASE/'transfer_source_overlap.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ['known_object_ids_by_hash_or_name_hint','exact_known_matches','known_name_hints','counts_by_object','read_errors']},ensure_ascii=False))

if __name__=='__main__':main()
