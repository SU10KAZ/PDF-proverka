"""Local OCR of delivered, cited table crops. No provider or source-audit calls.

Two segmentation passes must agree on one isolated printed total.
This extractor can introduce a conflict, never admit a claim.
"""
import csv
import io
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from PIL import Image
from .numeric import groups, dec

ROOT = Path('/home/coder/auditmanager/corpus-audits/20260914_project_change_272')
OUT = ROOT / 'fresh_dev_pair_a_post_inference_repair_v2'
BASE = ROOT / 'fresh_dev_sample_f5_pipeline_v6'
LIVE = ROOT / 'fresh_dev_pair_a_live_v2'


def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p): return json.loads(Path(p).read_text())


def candidates(text):
    # Isolated table total, not a numbered row. No proximity/tolerance matching.
    return set(str(dec(m)) for m in re.findall(r'^\s*[|]?\s*(\d+[.,]\d{2})\s*[|]?\s*$', text, re.M))


def main():
    if (OUT / 'DELIVERED_NUMERIC_FACTS.json').exists(): raise ValueError('Numeric facts already frozen')
    toolroot = Path(sys.argv[1])
    binary = toolroot / 'usr/bin/tesseract'
    env = os.environ | {'LD_LIBRARY_PATH': str(toolroot / 'usr/lib/x86_64-linux-gnu'),
                       'TESSDATA_PREFIX': str(toolroot / 'usr/share/tesseract-ocr/5/tessdata'), 'OMP_THREAD_LIMIT': '1'}
    audit, facts = [], []
    work = OUT / 'local_ocr'; work.mkdir(exist_ok=True)
    for row in read(LIVE / 'CALL_PLAN.json')['packages']:
        if row['action'] != 'MODEL_CALL': continue
        raw = read(LIVE / f"raw_responses/{row['key']}.json")
        packet = read(BASE / row['package'])['evidence_packet']
        for side in ('old', 'new'):
            state = raw[side + '_state']
            for group in groups(state['value']):
                for index, w in enumerate(raw.get('witnesses', [])):
                    if w.get('side') != side or w.get('kind') != 'RASTER_LOCATOR' or w['evidence_id'] not in state['evidence_ids']: continue
                    loc = w.get('visual_locator', '')
                    if not re.search(r'таблиц|экспликац', loc, re.I): continue
                    if group['scope'] != 'state':
                        number = group['scope'].split()[-1]
                        title = re.search(r'«[^»]*экспликац[^»]*»', loc, re.I)
                        scope_text = title[0] if title else re.split(r'\b(?:над|под|непосредственно)\b', loc, flags=re.I)[0]
                        scopes = set(re.findall(r'корпус[а]?\s+(\d+(?:\.\d+)?)', scope_text, re.I))
                        if scopes != {number}: continue
                    elif not re.search(r'МОП', loc):
                        continue
                    e = next(e for e in packet['evidence'][side] if e['evidence_id'] == w['evidence_id'])
                    if not e.get('raster'): continue
                    path = BASE / e['raster']['path']
                    assert sha(path) == e['raster']['sha256']
                    image = Image.open(path); box = w['bbox_norm']
                    crop = image.crop(tuple(round(x*d) for x,d in zip(box, [image.width,image.height]*2)))
                    crop = crop.resize((crop.width*4,crop.height*4))
                    key = f"{row['key']}_{side}_{index}"
                    crop_path = work / (key + '.png')
                    if not crop_path.exists(): crop.save(crop_path)
                    outputs = []
                    for psm in ('6', '11'):
                        result = subprocess.run([str(binary),str(crop_path),'stdout','-l','rus+eng','--psm',psm],env=env,capture_output=True,text=True,check=True)
                        text_path = work / (key + '_' + psm + '.txt'); text_path.write_text(result.stdout)
                        outputs.append(dict(psm=psm, path=str(text_path), sha256=sha(text_path), candidates=sorted(candidates(result.stdout))))
                    common = set(outputs[0]['candidates']) & set(outputs[1]['candidates'])
                    # Verify the isolated number independently of table layout.
                    # Segmentation agreement alone can repeat the same OCR error.
                    digit_check = []
                    if len(common) == 1:
                        tsv = subprocess.run([str(binary),str(crop_path),'stdout','-l','rus+eng','--psm','6','tsv'],env=env,capture_output=True,text=True,check=True).stdout
                        (work / (key + '.tsv')).write_text(tsv)
                        for token in csv.DictReader(io.StringIO(tsv), delimiter='\t'):
                            if token['text'].strip().replace(',', '.') not in common: continue
                            x,y,width,height = [int(token[k]) for k in ('left','top','width','height')]
                            digit_image = crop.crop((max(0,x-10),max(0,y-10),min(crop.width,x+width+10),min(crop.height,y+height+10)))
                            digit_path = work / (key + '_digits.png'); digit_image.save(digit_path)
                            for mode in ('7','13'):
                                digits = subprocess.run([str(binary),str(digit_path),'stdout','--psm',mode,'-c','tessedit_char_whitelist=0123456789.,'],env=env,capture_output=True,text=True,check=True).stdout.strip()
                                digit_check.append(dict(psm=mode,text=digits))
                        if len(digit_check) != 2 or any(d['text'].replace(',', '.') not in common for d in digit_check):
                            common = set()
                    receipt = dict(candidate_id=row['key'], side=side, scope=group['scope'], evidence_id=e['evidence_id'],
                        page=e['page'], document_version=e['document_version'], source_receipt=e['source_receipt'],
                        raster=e['raster'], bbox_norm=box, witness_index=index, crop_sha256=sha(crop_path), outputs=outputs,
                        method='LOCAL_TESSERACT_5.5.0_TABLE_AND_ISOLATED_DIGIT_AGREEMENT', digit_check=digit_check, grounded=len(common)==1,
                        unit=state['unit'], unit_provenance='Bound table metric from saved response', rounding_rule=None)
                    audit.append(receipt)
                    if len(common)==1: facts.append(receipt | dict(printed_value=next(iter(common))))
    result = dict(model_api_calls=0, local_ocr=True, new_evidence_delivered=False, source_audit_opened=False,
                  tesseract_sha256=sha(binary), facts=facts, attempts=audit)
    (OUT / 'DELIVERED_NUMERIC_FACTS.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(attempts=len(audit), facts=[{k:f[k] for k in ('candidate_id','side','scope','printed_value')} for f in facts]),ensure_ascii=False))

if __name__ == '__main__': main()
