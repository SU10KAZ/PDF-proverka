"""Prepare full-field publication packets from current-run links, without gold."""
import argparse
import json
import re
from pathlib import Path

from backend.app.pipeline.stages.findings_merge.publication_reconciliation import (
    INSTRUCTIONS, build_packet, fingerprint,
)
from .run_primary_text_runtime_probe import sha, write


def text_blocks(md):
    boundaries = list(re.finditer(r'^## Page \d+[^\n]*$|^### BLOCK[^\n]*$', md, re.M))
    result = {}
    for i, boundary in enumerate(boundaries):
        match = re.match(r'### BLOCK[^\n]*?\[(TEXT|TABLE)\]:\s*(\S+)', boundary.group())
        if match:
            ref = match[2]
            if ref in result:
                raise ValueError(f'Duplicate MD block: {ref}')
            result[ref] = md[boundary.start():boundaries[i+1].start() if i+1 < len(boundaries) else len(md)]
    return result


def linked_refs(finding):
    refs = []
    for field in ('source_block_ids', 'related_block_ids'):
        value = finding.get(field, [])
        if isinstance(value, list):
            refs.extend(value)
    for evidence in finding.get('evidence', []):
        if isinstance(evidence, dict):
            refs.append(evidence.get('block_id'))
    for addendum in finding.get('recovery_addenda', []):
        refs.extend(addendum.get('source_refs', []))
    return list(dict.fromkeys(r for r in refs if isinstance(r, str) and r))


def prepare(findings_path, md_path, images_dir, output):
    findings = json.loads(findings_path.read_text())['findings']
    if len({f['id'] for f in findings}) != len(findings):
        raise ValueError('Duplicate finding IDs')
    blocks = text_blocks(md_path.read_text())
    output.mkdir(parents=True, exist_ok=False)
    rows = []
    for number, finding in enumerate(findings, 1):
        texts, images, unavailable = {}, {}, []
        text_size = image_size = 0
        for ref in linked_refs(finding):
            if ref in blocks:
                if text_size + len(blocks[ref]) <= 100000:
                    texts[ref] = blocks[ref]
                    text_size += len(blocks[ref])
                else:
                    unavailable.append({'ref': ref, 'reason': 'text_budget'})
            else:
                image = images_dir / f'block_{ref}.png'
                if not image.resolve().is_relative_to(images_dir.resolve()) or not image.is_file():
                    unavailable.append({'ref': ref, 'reason': 'not_found_in_supplied_source_indexes'})
                elif len(images) >= 8 or image_size + image.stat().st_size > 16000000:
                    unavailable.append({'ref': ref, 'reason': 'image_budget'})
                else:
                    images[str(image.resolve())] = sha(image)
                    image_size += image.stat().st_size
        image_refs = [Path(path).stem.removeprefix('block_') for path in images]
        packet = build_packet(finding, texts, image_refs)
        stage = f'publication_reconciliation_{number:03d}'
        envelope = {'packet_sha256': fingerprint(packet), 'packet': packet,
                    'context_complete': False, 'unavailable_linked_sources': unavailable,
                    'context_limit': 'Only current-run linked sources. Broader object identity and normative applicability are not presumed checked.'}
        request = {'stage': stage, 'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
                   'allowed_tools': '', 'image_hashes': images,
                   'messages': [{'role': 'system', 'content': INSTRUCTIONS},
                                {'role': 'user', 'content': json.dumps(envelope, ensure_ascii=False)}]}
        path = output / f'{stage}.json'
        write(path, request)
        # Missing linked evidence requires context inspection before dispatch,
        # not automatic filtering of the finding or a claim of full coverage.
        rows.append({'path': str(path), 'sha256': sha(path), 'finding_id': finding['id'],
                     'input_units': len(packet['input_units']), 'text_blocks': len(texts),
                     'images': len(images), 'image_bytes': image_size,
                     'unavailable_linked_sources': unavailable,
                     'transport_size_ready': not unavailable and bool(texts or images)
                     and sum(len(m['content']) for m in request['messages']) < 1000000})
    record = {'history_supplied': False, 'engineer_answers_supplied': False,
              'coordinator_review_supplied': False,
              'selection': 'All saved AK findings, DEV-known; source selection uses current-run links only, no review or gold files.',
              'sources': {'sealed_findings_path': str(findings_path), 'sealed_findings_sha256': sha(findings_path),
                          'md_path': str(md_path), 'md_sha256': sha(md_path)},
              'requests': rows, 'production_acceptance': False, 'dispatched': False}
    write(output / 'preparation.json', record)
    return {'prepared': len(rows), 'ready': sum(r['transport_size_ready'] for r in rows),
            'held': sum(not r['transport_size_ready'] for r in rows),
            'input_units': sum(r['input_units'] for r in rows), 'output': str(output),
            'preparation_sha256': sha(output / 'preparation.json')}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('findings', 'md', 'images', 'output'):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(*(getattr(args, n).resolve() for n in ('findings', 'md', 'images', 'output'))), ensure_ascii=False))
