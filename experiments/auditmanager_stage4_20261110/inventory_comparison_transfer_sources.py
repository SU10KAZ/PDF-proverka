"""Survey source-only comparison-stage directories without opening source content."""
import json
import re
from pathlib import Path
from collections import Counter

ROOT = Path('/srv/auditmanager/data/projects_v2/objects')
PROFILES = {'АК':'AK','AK':'AK','ИТП':'ITP','ITP':'ITP','ТМ':'ITP','АР':'AR','AR':'AR',
            'КМ':'KM','KM':'KM','ВК':'VK','VK':'VK','ЭОМ':'EOM','EOM':'EOM'}


def main():
    rows = []
    for obj in sorted(ROOT.iterdir()):
        if not obj.is_dir() or obj.name.startswith('_'):
            continue
        for stage in ('stage_1', 'stage_2'):
            documents = obj/'comparison'/stage/'documents'
            if not documents.is_dir():
                continue
            for doc in sorted(documents.iterdir()):
                if not doc.is_dir():
                    continue
                selector = doc/'current_version.txt'
                version = selector.read_text().strip() if selector.is_file() else None
                versions = sorted(p.name for p in (doc/'versions').glob('v*') if p.is_dir() and re.fullmatch(r'v\d+',p.name))
                mode = 'current_version_selector'
                if version is None and len(versions)==1:
                    version = versions[0];mode = 'sole_version_not_publication_certificate'
                valid = bool(version and re.fullmatch(r'v\d+',version) and version in versions)
                base = doc/'versions'/version if valid else None
                tokens = re.findall(r'(?:^|[-_ .])('+'|'.join(PROFILES)+r')(?=\d|[-_ .]|$)',doc.name,re.I)
                rows.append({'object_directory':obj.name,'stage_directory':stage,'document_directory':doc.name,
                             'path':str(doc),'selected_version':version if valid else None,'selector_mode':mode,
                             'available_versions':versions,'profile_hints':sorted({PROFILES[t.upper()] for t in tokens}),
                             'prepared_md_present':bool(base and (base/'02_work/document.md').is_file()),
                             'prepared_pdf_present':bool(base and (base/'02_work/document.pdf').is_file()),
                             'blocks_present':bool(base and (base/'02_work/blocks.json').is_file()),
                             'prior_exposure':'unchecked_comparison_storage','reserve_eligible':None})
    counts = Counter((r['object_directory'].split('_')[0],r['stage_directory']) for r in rows if r['prepared_md_present'] and r['prepared_pdf_present'])
    report = {'status':'additional_source_inventory_not_reserve_assignment',
              'read_scope':['Directory names','current_version.txt metadata','file existence'],
              'not_read':['Source MD/PDF contents','comparison outputs','findings','engineer decisions'],
              'rows':rows,'prepared_counts':{'/'.join(k):v for k,v in sorted(counts.items())},
              'limits':['Two stages or multiple versions of one construction object are not independent projects.',
                        'Directory identity and filename discipline hints need verification; foreign documents may be stored under another object.',
                        'Comparison storage indicates potential prior exposure. Audit eligibility before assigning a reserve.',
                        'Object 272 has separate ProjectChange restrictions; no ProjectChange development, truth or reserve is accessed or changed by this metadata survey.',
                        'No earlier source-availability estimate should be generalized beyond its surveyed storage layout.'],
              'selected_calibration':[],'selected_reserve':[],'production_acceptance':False}
    Path(__file__).with_name('comparison_transfer_source_inventory.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'rows':len(rows),'prepared_counts':report['prepared_counts']},ensure_ascii=False))


if __name__=='__main__':main()
