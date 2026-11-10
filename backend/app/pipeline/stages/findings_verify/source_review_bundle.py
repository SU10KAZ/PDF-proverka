"""Version-separated source bundles for the existing source-review contract.

Only block headers are namespaced; literal source bodies remain unchanged except
an end-of-block newline needed to separate Markdown headers. No evidence is
created, no applicability asserted, and no provider is called here.
"""
from collections import Counter
from copy import deepcopy
import hashlib
from pathlib import Path
import re

HEADER = re.compile(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$', re.M | re.I)


def build_source_review_bundle(sources):
    pieces, catalogue, origins, bindings = [], [], {}, []
    identities = set()
    for source in sources:
        code, version = source['document_code'], source['version']
        if not all(isinstance(v, str) and v.strip() for v in (code, version)):
            raise ValueError('bundle_document_identity_required')
        if (code, version) in identities:
            raise ValueError('bundle_duplicate_document_version')
        identities.add((code, version))
        path = Path(source['md_path']).resolve()
        md = path.read_text()
        digest = hashlib.sha256(md.encode()).hexdigest()
        namespace = 'doc_' + hashlib.sha256((code+'\0'+version+'\0'+digest).encode()).hexdigest()[:24]
        headers = list(HEADER.finditer(md))
        counts = Counter(h[2] for h in headers)
        selected = source.get('block_ids', list(counts))
        if (not isinstance(selected, list) or not selected
                or any(not isinstance(bid, str) or counts[bid] != 1 for bid in selected)
                or len(selected) != len(set(selected))):
            raise ValueError('bundle_missing_or_ambiguous_block')
        catalogue.append({'namespace':namespace, 'document_code':code, 'version':version, 'source_md_sha256':digest})
        bindings.append({'document_code':code, 'version':version, 'md_path':str(path),
                         'block_ids':selected, 'source_md_sha256':digest})
        for i, h in enumerate(headers):
            if h[2] not in selected:
                continue
            kind = h[1].upper()
            if kind not in ('TEXT', 'TABLE', 'IMAGE', 'GRAPHIC'):
                raise ValueError('bundle_unsupported_block_kind')
            body = md[h.end():headers[i+1].start() if i+1 < len(headers) else len(md)]
            body = re.split(r'^##[ \t]+(?:Page|СТРАНИЦА)\b', body, maxsplit=1, flags=re.M | re.I)[0]
            bid = 'blk_' + namespace + '_' + hashlib.sha256(h[2].encode()).hexdigest()[:24]
            if bid in origins:
                raise ValueError('bundle_block_collision')
            origins[bid] = {'namespace':namespace, 'original_block_id':h[2]}
            pieces.append(f'### BLOCK #{len(pieces)+1} [{kind}]: {bid}' + body + ('' if body.endswith('\n') else '\n'))
    if not pieces:
        raise ValueError('bundle_empty')
    md = ''.join(pieces)
    return {'md':md, 'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),
            'source_catalogue':catalogue, 'source_origins':origins, 'source_bindings':bindings}


def bind_source_review_bundle(queue, bundle):
    if queue.get('source_md_sha256') != bundle['source_md_sha256']:
        raise ValueError('bundle_queue_version_mismatch')
    result = deepcopy(queue)
    result['source_catalogue'] = deepcopy(bundle['source_catalogue'])
    result['source_origins'] = deepcopy(bundle['source_origins'])
    result['_bundle_source_bindings'] = deepcopy(bundle['source_bindings'])
    verify_source_review_bundle(result)
    return result


def verify_source_review_bundle(queue):
    if '_bundle_source_bindings' not in queue:
        if queue.get('source_catalogue') or queue.get('source_origins'):
            raise ValueError('unbound_source_catalogue')
        return
    bindings = queue['_bundle_source_bindings']
    current = build_source_review_bundle(bindings)
    if (current['source_bindings'] != bindings or any(
        current[key] != queue.get(key) for key in ('source_md_sha256', 'source_catalogue', 'source_origins'))):
        raise ValueError('bundle_source_version_changed')
