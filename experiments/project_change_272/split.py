"""Prospective whole-cipher isolation; no historical blindness claim."""
from collections import Counter, defaultdict
import hashlib
import re

from .inventory import ROOT, OBJECT, read, sha, now, immutable

# Allocation is chosen from exposure receipts and structural metadata, before
# any candidate tuning. Index 1 is foreign ALIA; 23 is a fragment of index 10.
ALLOCATION = {
    'DEV': [2, 5, 6, 7, 8, 9, 10, 13, 14, 15, 16, 19, 21],
    'VALIDATION': [3, 4, 17, 20],
    'FINAL_HOLDOUT': [11, 12, 18, 22],
}
DISCIPLINES = {
    2: 'architecture_plans', 3: 'architecture_facades', 4: 'daylight',
    5: 'water_supply', 6: 'drainage', 7: 'heating', 8: 'ventilation',
    9: 'cooling', 10: 'electrical', 11: 'communications_5.5',
    12: 'structures', 13: 'accessibility', 14: 'environment',
    15: 'general_explanatory_note', 16: 'site_plan', 17: 'construction_organization',
    18: 'communications_5.1', 19: 'communications_5.2',
    20: 'communications_5.3', 21: 'communications_5.4', 22: 'energy_efficiency',
}


def fingerprints(text):
    """Identify substantial exact shared evidence without exposing its text.

    Whole pages and long content lines are hashed; envelope/boilerplate lines
    are excluded. This cannot prove absence of paraphrase or raster overlap.
    """
    pages = defaultdict(list)
    page = None
    for line in text.splitlines():
        match = re.fullmatch(r'## Page (\d+)\s*', line)
        if match:
            page = int(match[1])
        elif page is not None and not line.startswith(('>', '### BLOCK', 'Generated:', 'Path:')):
            norm = re.sub(r'\s+', ' ', line.casefold()).strip()
            if norm:
                pages[page].append(norm)
    result = defaultdict(set)
    for page, lines in pages.items():
        for value in ['\n'.join(lines)] + lines:
            if len(value) >= 300:
                result[hashlib.sha256(value.encode()).hexdigest()].add(page)
    return result


def build():
    from pathlib import Path
    inv = read(ROOT / 'INVENTORY.json')
    pairs = {r['index']: r for r in inv['pairs']}
    assigned = [i for v in ALLOCATION.values() for i in v]
    assert len(assigned) == len(set(assigned)) == 21
    assert set(assigned) == set(range(2, 23))
    # Content is only machine-hashed and syntactically counted for selection;
    # no OLD/NEW differences, answers, or engineering passages are inspected.
    hashes = defaultdict(list)
    route_stats = {}
    for i in assigned:
        row = pairs[i]
        tables = 0
        for side in ['old', 'new']:
            text = Path(row[side]['artifacts']['work_md']['path']).read_text()
            tables += sum(bool(re.match(r'^\s*\|?\s*:?-{3,}:?\s*\|', line))
                          for line in text.splitlines())
            tables += len(re.findall(r'<table\b', text, re.I))
            for h, pages in fingerprints(text).items():
                hashes[h].append(dict(index=i, side=side, pages=sorted(pages)))
        graphic = sum(row[s]['structure']['block_areas'].get('image', 0) for s in ['old', 'new'])
        text_area = sum(row[s]['structure']['block_areas'].get('text', 0) for s in ['old', 'new'])
        route_stats[i] = dict(table_envelopes=tables, graphic_area=round(graphic, 2),
                              text_area=round(text_area, 2),
                              graphic_heavy=graphic > text_area,
                              text_heavy=text_area >= graphic, table_present=tables > 0)
    membership = {i: name for name, indices in ALLOCATION.items() for i in indices}
    overlaps = [dict(sha256=h, references=refs) for h, refs in hashes.items()
                if len({membership[r['index']] for r in refs}) > 1]
    # Embargo every page with exact evidence shared across partitions. The
    # assignment still consists of whole pairs; no fragment changes partition.
    embargo = defaultdict(set)
    for item in overlaps:
        for ref in item['references']:
            embargo[(ref['index'], ref['side'])].update(ref['pages'])
    immutable(ROOT / 'OVERLAP_AUDIT.json', dict(
        method='exact normalized page or content line >=300 characters; all cross-partition occurrences embargoed',
        semantic_inspection=False, overlaps=overlaps,
        residual_risk='Paraphrase, shared engineering subjects and image-only duplicates may escape exact OCR fingerprints. All documents already historically exposed.'))
    split = dict(schema='project-change-272-split.v1', frozen_at=now(), object=OBJECT,
                 logical_baseline=inv['logical_baseline'], old='stage_1', new='stage_2',
                 inventory_sha256=sha(ROOT / 'INVENTORY.json'),
                 overlap_audit_sha256=sha(ROOT / 'OVERLAP_AUDIT.json'),
                 historical_blind=False, prospective_isolation=True,
                 allocation=ALLOCATION, pairs=[],
                 excluded=[dict(index=1, reason='FOREIGN_ALIA_CONFIRMED_BY_OBJECT_ADDRESS_LETNAYA_AND_ASTERUS_TITLE_PAGE',
                                inspection='Only membership metadata/title pages; no ProjectChange inference'),
                           dict(index=23, alias_of=10, partition='DEV',
                                reason='PAGE_EXTRACT_NOT_INDEPENDENT_PAIR; never extra evaluation support')],
                 unpaired_policy='Six unmatched baseline documents are quarantined; no inferred additions/removals',
                 rationale='13 DEV / 4 VALIDATION / 4 FINAL. Keep heavily used subject/state ciphers in DEV; reserve lower documented exposure while preserving graphic, narrative, tables and different systems. Both reserve sets remain historically DEV-known. Full original v002 source receipts retained.',
                 leakage_policy='No reserve sources, examples, predictions, labels, failure analysis, thresholds or architecture feedback before the appropriate candidate freeze. DEV processing excludes cross-partition exact duplicate pages. After validation exposure the set is spent; subsequent claims must identify reuse and cannot call it blind.',
                 architecture='EvidenceScope -> EngineeringSubject -> OLD/NEW state -> ProjectChange')
    for i in sorted(assigned):
        row = pairs[i]
        split['pairs'].append(dict(index=i, pair_id=row['pair_id'], partition=membership[i],
                                  discipline=DISCIPLINES[i], structure=route_stats[i],
                                  historical_exposure='DEV_KNOWN', historical_blind=False,
                                  embargo_pages={s: sorted(embargo[(i, s)]) for s in ['old', 'new']}))
    immutable(ROOT / 'SPLIT.json', split)
    (ROOT / 'SPLIT.sha256').write_text(sha(ROOT / 'SPLIT.json') + '\n')
    for name, indices in ALLOCATION.items():
        print(name, indices, [(i, route_stats[i], {s: len(embargo[(i, s)]) for s in ['old', 'new']}) for i in indices])


if __name__ == '__main__':
    build()
