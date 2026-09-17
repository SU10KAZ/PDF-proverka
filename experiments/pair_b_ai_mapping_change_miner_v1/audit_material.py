"""Post-freeze source packets and literal quote checks; no correctness verdicts."""
import re
from .run import OUT, read, write, sha, now, verify_freeze


def compact(text):
    return re.sub(r'\s+', ' ', text).strip()


def main():
    freeze=read(OUT/'CHANGE_MINER_FREEZE.json')
    assert freeze.get('evaluation_truth_opened') is False and freeze.get('hashes')
    verify_freeze('CHANGE_MINER_FREEZE.json')
    source=read(OUT/'SOURCE_ADMISSION.json')
    results=read(OUT/'CHANGE_MINER_RESULTS.json')
    queue=[]
    for change in results['concrete_changes']:
        checks=[]
        for ref in change['evidence_refs']:
            pagefile=OUT/'pages'/ref['side']/f"p{ref['page']:03d}.json"
            page=read(pagefile)
            quote=compact(ref['quote_or_visual_observation'])
            checks.append(dict(**ref,
                literal_present_native=bool(quote) and quote in compact(page['native']),
                literal_present_ocr=bool(quote) and quote in compact(page['ocr']),
                page_json=str(pagefile),page_json_sha256=sha(pagefile),
                original_pdf=source[ref['side']]['artifacts']['pdf']['path'],
                raster=page['raster'],
                warning='Literal presence is not a semantic correctness or identity verdict'))
        item=dict(prediction=change,source_checks=checks,required_audit=change['confidence']>=0.85,
                  verdict='NOT_ASSESSED')
        write(OUT/'audit_material'/f"{change['change_id']}.json",item)
        queue.append(dict(change_id=change['change_id'],confidence=change['confidence'],
            required_audit=item['required_audit'],engineering_subject=change['engineering_subject'],
            old_state=change['old_state'],new_state=change['new_state'],
            old_pages=change['old_pages'],new_pages=change['new_pages'],
            packet=str(OUT/'audit_material'/f"{change['change_id']}.json")))
    write(OUT/'SOURCE_AUDIT_QUEUE.json',dict(at=now(),rows=queue,
        method='Mechanical references only; requires independent source-first adjudication'))
    print(f'Prepared {len(queue)} source packets; no verdicts assigned')


if __name__=='__main__':
    main()
