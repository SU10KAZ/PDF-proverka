"""Pre-inference adapters; F1/F2/F4/V4 implementations remain unchanged.

V4's reference resolver accepts an explicit reference manifest in its raw-input
shape. Here that manifest is produced by discovery, never by a model. Its legacy
raw_response_hash names the manifest hash and is explicitly tagged as such. Only
identity/scope/context are bound: no STATE_VALUE or CHANGE_OBSERVATION is invented.
"""
from dataclasses import replace
from pathlib import Path
import re

import fitz

from experiments.project_change_272.inventory import sha
from experiments.project_change_contracts_272.witnesses import raster_locator_errors
from experiments.project_change_contracts_v3_272.delivery import semantic_packet
from experiments.project_change_f2_binding_v4_272.binding import bind
from experiments.project_change_f2_binding_v4_272.contract import EngineeringState
from .common import fingerprint
from .requirement_builder import TYPES


def has_source_text(quote):
    """Transport labels alone are not delivered source content."""
    return bool(re.sub(r'(?m)^(?:PDF NATIVE:|OCR \(fallible\):)\s*$', '', quote or '').strip())


def region_delivery(evidence, region):
    """Mechanical payload availability; never a semantic completeness flag."""
    kind = region['evidence_type']
    raster = bool(evidence.get('raster'))
    quote = evidence.get('quote', '')
    text = has_source_text(quote)
    rows = [line for line in quote.splitlines() if line.strip().startswith('|')
            and not re.fullmatch(r'[\s|:\-]+', line)]
    if kind in {'GRAPHIC_REGION', 'GRAPHIC_SCHEME'}:
        delivered = raster
    elif kind in {'TABLE_COMPLETE', 'EQUIPMENT_SCHEDULE'}:
        # A heading or prose about a table cannot substitute for its rows.
        delivered = raster or len(rows) >= 3
    else:
        delivered = raster or text
    return dict(payload_delivered=delivered,
        payload_form='RASTER_AND_TEXT' if delivered and raster and text else 'RASTER'
                     if delivered and raster else 'TEXT' if delivered else 'NONE',
        boundary_complete=bool(delivered and region['boundary_verified']),
        reason='' if delivered else 'REQUIRED_ROUTE_PAYLOAD_NOT_DELIVERED')


class DiscoveryPageLoader:
    def __init__(self, pair, inventories, regions, raster_dir):
        self.pair, self.inventories, self.regions = pair, inventories, regions
        self.raster_dir = Path(raster_dir)
        self.pdfs, self.rasters = {}, {}

    def __call__(self, requirement):
        r = self.regions[requirement.requirement_id]
        if r is None:
            return None
        side = requirement.side.lower()
        doc, inv = self.pair[side], self.inventories[side]
        if (requirement.document != doc['document_code'] or requirement.document_version != doc['document_version']
                or r['document_version'] != requirement.document_version or r['page'] != requirement.page
                or r['side'] != requirement.side or r['region_id'] != requirement.scope_binding):
            raise PermissionError('Discovery region binding mismatch')
        if inv['pages'][r['page'] - 1]['status'] != 'INDEXED':
            raise PermissionError('Excluded source page')
        if side not in self.pdfs:
            if sha(doc['artifacts']['pdf']['path']) != doc['artifacts']['pdf']['sha256']:
                raise ValueError('Source PDF drift')
            self.pdfs[side] = fitz.open(doc['artifacts']['pdf']['path'])
        page = self.pdfs[side][r['page'] - 1]
        raster_key = (side, r['page'])
        if raster_key not in self.rasters:
            # One rendered page per source/physical version. Relative content-
            # addressed paths keep model-package hashes independent of output root.
            scale = min(2.0, 2400 / max(page.rect.width, page.rect.height))
            name = fingerprint([doc['artifacts']['pdf']['sha256'], r['page'], scale, fitz.VersionBind]) + '.png'
            path = self.raster_dir / name
            self.raster_dir.mkdir(parents=True, exist_ok=True)
            if not path.exists():
                page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False).save(path)
            pix = fitz.Pixmap(str(path))
            self.rasters[raster_key] = dict(path=str(path.relative_to(self.raster_dir.parent)), sha256=sha(path), width=pix.width, height=pix.height,
                page=r['page'], source_pdf_sha256=doc['artifacts']['pdf']['sha256'])
        evidence_types = [TYPES[r['source_type']]]
        if r['notes']:
            evidence_types.append('NOTE')
        return dict(native=r['native_text'], ocr=r['ocr_text'], raster=self.rasters[raster_key],
            bbox=list(page.rect), scope_binding=r['region_id'], evidence_types=evidence_types,
            # Full raster delivery does not establish a semantic region boundary.
            full_page=r['boundary_verified'], content_kind=r['source_type'],
            type_provenance=dict(method=r['method'], region_id=r['region_id'], bbox_norm=r['bbox_norm']),
            provenance=dict(pdf=doc['artifacts']['pdf'], region_id=r['region_id'],
                            discovery_method=r['method'], bbox_norm=r['bbox_norm'],
                            boundary_verified=r['boundary_verified']))

    def close(self):
        for pdf in self.pdfs.values():
            pdf.close()


def source_packet(body, pair, candidate):
    packet = semantic_packet(body, pair)
    packet['proposal_kind'] = 'DOCUMENT_DISCOVERED_FUNCTIONAL_SUBJECT'
    packet['proposal_locator'] = dict(candidate_id=candidate['candidate_id'], scope=candidate['scope'])
    # Existing delivery deduplicates full-page rasters. Preserve ALL region forms
    # and explicit requirement->region links when several share the same raster.
    for side in ('old', 'new'):
        for e in packet['evidence'][side]:
            units = [u for u in body['evidence'] if u['side'].lower() == side and u['page'] == e['page']
                     and (u['raster']['sha256'] if u['raster'] else None) == (e['raster']['sha256'] if e['raster'] else None)]
            e['evidence_types'] = sorted({t for u in units for t in u['evidence_types']})
            e['region_bindings'] = [dict(requirement_id=u['requirement_id'],
                region_id=u['provenance']['region_id'], bbox_norm=u['provenance']['bbox_norm'],
                evidence_type=u['selected_evidence_type'], boundary_verified=u['provenance']['boundary_verified']) for u in units]
            for region in e['region_bindings']:
                region.update(region_delivery(e, region))
            e['route'] = ('GRAPHIC' if 'GRAPHIC_REGION' in e['evidence_types'] else
                          'TABLE' if 'TABLE_COMPLETE' in e['evidence_types'] else 'TEXT')
            e['delivered_routes'] = sorted({
                'GRAPHIC' if r['evidence_type'] in {'GRAPHIC_REGION', 'GRAPHIC_SCHEME'} else
                'TABLE' if r['evidence_type'] in {'TABLE_COMPLETE', 'EQUIPMENT_SCHEDULE'} else 'TEXT'
                for r in e['region_bindings'] if r['payload_delivered']})
    # Empty F1 units remain traceable in requirements/gaps, but are not source
    # evidence. Do not hand V4 a resolvable ID for a metadata-only delivery.
    packet['delivery_omissions'] = []
    for side in ('old', 'new'):
        retained = []
        for e in packet['evidence'][side]:
            if e['raster'] or has_source_text(e['quote']):
                retained.append(e)
            else:
                packet['delivery_omissions'].append(dict(evidence_id=e['evidence_id'],
                    side=side, page=e['page'], document_version=e['document_version'],
                    requirement_ids=[r['requirement_id'] for r in e['region_bindings']],
                    reason='NO_SOURCE_TEXT_OR_RASTER_DELIVERED'))
        packet['evidence'][side] = retained
    # F1's generic page coverage can alias several region types. Resolve each
    # requirement to its own delivered region, not every unit on the same page.
    links = {}
    for side in ('old', 'new'):
        for e in packet['evidence'][side]:
            for r in e['region_bindings']:
                if r['payload_delivered']:
                    links.setdefault(r['requirement_id'], set()).add(e['evidence_id'])
    coverage = packet['evidence_coverage']
    for row in coverage['requirements']:
        row['evidence_ids'] = sorted(links.get(row['requirement']['requirement_id'], set()))
        row['delivery']['payload_delivered'] = bool(row['evidence_ids'])
    coverage['subjects'] = [dict(subject=subject, **{
        side + '_evidence': [r for r in coverage['requirements']
            if r['requirement']['subject'] == subject and r['requirement']['side'] == side.upper()]
        for side in ('old', 'new')}) for subject in sorted({r['subject'] for r in body['requirements']})]
    packet['evidence_coverage']['delivery_hash'] = fingerprint(packet['evidence'])
    packet['packet_id'] = fingerprint({k: v for k, v in packet.items() if k != 'packet_id'})[:24]
    return packet


def graphic_bindings(packet, candidate, raster_root):
    bindings = []
    for side in ('old', 'new'):
        for e in packet['evidence'][side]:
            for region in e['region_bindings']:
                if region['evidence_type'] != 'GRAPHIC_REGION':
                    continue
                witness = dict(evidence_id=e['evidence_id'], bbox_norm=region['bbox_norm'],
                    visual_locator=candidate['subject'],
                    binding_reason='Document discovery functional context; region=' + region['region_id'])
                local = dict(e)
                if e['raster']:
                    local['raster'] = e['raster'] | dict(path=str(Path(raster_root) / e['raster']['path']))
                errors = raster_locator_errors(witness, local)
                bindings.append(dict(**witness, side=side, page=e['page'], document_version=e['document_version'],
                    source_receipt=e['source_receipt'], raster=e['raster'], region_id=region['region_id'],
                    requirement_id=region['requirement_id'], errors=errors,
                    f4_status='ACCEPTED' if not errors else 'UNAVAILABLE' if not e['raster'] else 'REJECTED',
                    functional_binding_status='CANDIDATE_NOT_SEMANTICALLY_VERIFIED'))
    return bindings


def typed_preparation(packet, candidate):
    manifest = dict(input_kind='F5_DISCOVERY_REFERENCE_MANIFEST', case_token=candidate['candidate_id'])
    subject = candidate['candidate_id'] + ': ' + candidate['subject']
    scope = ','.join(candidate['scope'])
    for side in ('old', 'new'):
        ids = [e['evidence_id'] for e in packet['evidence'][side]
               if (e.get('raster') or has_source_text(e.get('quote')))
               and any(r.get('payload_delivered', True) for r in e['region_bindings'])]
        manifest[side + '_state'] = dict(engineering_subject=subject, evidence_ids=[],
            subject_identity=dict(scope=scope, evidence_ids=ids))
    delivered_old = {e['evidence_id'] for e in packet['evidence']['old']
                     if e.get('raster') or has_source_text(e.get('quote'))}
    manifest['old_counter_evidence'] = [dict(evidence_id=eid) for eid in sorted({eid
        for row in packet['evidence_coverage']['requirements'] if row['requirement']['evidence_role'] == 'COUNTER'
        for eid in row['evidence_ids'] if eid in delivered_old})]
    bound = bind(manifest, packet, set())
    if bound['rejected']:
        raise ValueError('V4 binding rejected discovery references: ' + str(bound['issues']))
    states = {}
    for side in ('old', 'new'):
        links = tuple(replace(b, provenance=b.provenance | dict(
            raw_response_kind='F5_DISCOVERY_REFERENCE_MANIFEST_NOT_MODEL_RESPONSE',
            relation='EXPLICIT_DISCOVERY_REFERENCE_TO_SAME_SUBJECT_DELIVERED_SOURCE')) for b in bound['states'][side])
        card = candidate['cardinality']
        # Base F2 has no zero-sided cardinality enum. Keep a typed empty skeleton
        # with an explicit adapter status; never represent it as confirmed 1:1.
        state = EngineeringState(engineering_subject=subject, scope=scope, state_role='OTHER', value='UNKNOWN',
            functional_role=candidate['subject'], comparison_cardinality=card if card != 'UNRESOLVED' else '1→1',
            members=tuple(candidate[side]), evidence_ids=tuple(dict.fromkeys(b.evidence_id for b in links)),
            evidence_bindings=links, provenance=dict(producer='F5_PRE_INFERENCE_SKELETON',
                input_manifest_hash=fingerprint(manifest), comparison_cardinality_status=card,
                state_role_status='UNKNOWN', unknown_representation='None means UNKNOWN',
                no_state_value_assertion=True))
        states[side] = state.to_dict() | dict(applicable_conditions='UNKNOWN',
            comparison_cardinality_status=card, state_role_status='UNKNOWN')
    return dict(states=states, reference_manifest=manifest, binding_issues=bound['issues'],
                semantic_verdict=None, normalization='F2_TYPED_CONSTRUCTOR_AND_V4_REFERENCE_RESOLVER',
                value_normalization='DEFERRED_UNTIL_INFERENCE')
