"""Metadata-only source availability survey; never read documents or audit answers.

Directory disciplines are storage labels, not engineering-profile certification.
No candidate in this inventory is declared unseen or assigned to a reserve.
"""
import json
from pathlib import Path
from collections import Counter
import re

ROOT=Path('/srv/auditmanager/data/projects_v2/objects')


def inventory(root=ROOT):
    rows=[]
    for obj in sorted(root.iterdir()):
        disciplines=obj/'disciplines'
        if obj.name.startswith('_') or not disciplines.is_dir():
            continue
        for discipline in sorted(disciplines.iterdir()):
            documents=discipline/'documents'
            if not documents.is_dir():
                continue
            for doc in sorted(documents.iterdir()):
                if not doc.is_dir():
                    continue
                current=doc/'current_version.txt'
                version=current.read_text().strip() if current.is_file() else None
                # The version selector is metadata; never follow a path outside
                # this document into an answer-bearing or foreign directory.
                valid=bool(version and re.fullmatch(r'v\d+',version))
                base=doc/'versions'/version if valid else None
                rows.append({'object_id':obj.name.split('_')[0], 'object_directory':obj.name,
                    'storage_discipline':discipline.name,'document_directory':doc.name,
                    'path':str(doc),'current_version':version,'valid_version_selector':valid,
                    'prepared_md_present':bool(base and (base/'02_work/document.md').is_file()),
                    'prepared_pdf_present':bool(base and (base/'02_work/document.pdf').is_file()),
                    'blocks_metadata_present':bool(base and (base/'02_work/blocks.json').is_file()),
                    'explicit_AK_filename_marker':bool(re.search(r'(?:^|[-_. ])(?:АК|AK)(?:\d|[-_. ]|$)',doc.name)),
                    'prior_exposure':'not_checked','reserve_eligible':None})
    return {'status':'metadata_inventory_not_reserve_selection',
        'read_scope':['Directory names','current_version.txt'],
        'not_read':['Prepared MD/PDF content','Findings','Engineer answers','Document metadata free text'],
        'rows':rows,'counts_by_storage_discipline':dict(Counter(r['storage_discipline'] for r in rows)),
        'explicit_AK_filename_candidates':sum(r['explicit_AK_filename_marker'] for r in rows),
        'prepared_md_and_pdf':sum(r['prepared_md_present'] and r['prepared_pdf_present'] for r in rows),
        'requirements_before_selection':[
            'Verify engineering discipline; AI is not automatically AK.',
            'Exclude six known controls and previously exposed documents, versions and sibling packages that leak answers.',
            'Group by actual construction project; multiple ciphers or revisions do not create independent projects.',
            'Confirm at least 18 eligible projects under the original trial definition; six calibration, twelve reserve, all six disciplines.',
            'Freeze assignment and access restrictions before opening reserve source content.',
        ],'selected_calibration':[],'selected_reserve':[], 'production_acceptance':False}


if __name__=='__main__':
    result=inventory()
    Path(__file__).with_name('transfer_source_inventory.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ['rows','requirements_before_selection','not_read','read_scope']},ensure_ascii=False))
