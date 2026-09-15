"""Offline, bounded requirement-driven package builder (no inference command).

Sources must already have passed the frozen DEV guard. Source-audit page maps
are manual retrieval scaffolding, not an autonomous relevance classifier. Full
page raster delivery can fulfill an explicitly audited page region; native/OCR
alone cannot certify table, note, or diagram completeness.
"""
from dataclasses import asdict
from pathlib import Path
import json
import re

import fitz

from experiments.project_change_272.inventory import sha
from .evidence import EvidenceType, coverage_receipt, fingerprint


def section_units(text):
    """Keep whole paragraphs, markdown tables (including notes), and sections.

    This deliberately over-collects through the next heading rather than cutting
    a paragraph or dropping a table's trailing notes at a character boundary.
    """
    return [s for s in re.split(r'(?=^#{1,6}\s)', text, flags=re.M) if s.strip()]


def select_sections(text, anchors, budget):
    """Exact anchors rank sections; no prefix slice and no partial paragraph."""
    units = section_units(text)
    needles = [a.casefold() for a in anchors if a.strip()]
    ranked = sorted(enumerate(units), key=lambda x: (
        -sum(a in x[1].casefold() for a in needles), x[0]))
    selected, omitted = [], []
    for index, unit in ranked:
        cost = len(unit) + bool(selected)
        if cost <= budget:
            selected.append((index, unit))
            budget -= cost
        else:
            omitted.append(index)
    return dict(text='\n'.join(u for _, u in sorted(selected)),
                boundary_complete=not omitted, omitted_sections=omitted)


def same_subject_counter(requirements, evidence):
    """Only explicit subject/document/OLD-version scopes; a miss proves nothing."""
    result = []
    for r in requirements:
        if r.evidence_type != EvidenceType.COUNTER_EVIDENCE:
            continue
        result.extend(e for e in evidence if e['side'] == 'OLD' and e['subject'] == r.subject
                      and e['document'] == r.document and e['document_version'] == r.document_version
                      and e['page'] == r.page)
    return dict(evidence_ids=sorted({e['evidence_id'] for e in result}), absence_proven=False,
                search_scope='EXPLICIT_SAME_SUBJECT_OLD_REQUIREMENTS')


def package(requirements, page_loader, *, text_budget=28000, raster_budget=8):
    """Build from complete admitted pages, budget whole units, then measure delivery.

    page_loader validates scope before IO and returns content + verified raster.
    Requirements include supporting notes and related nodes as independent page
    requirements. A missing counterpart remains visible to future model consumers.
    """
    if text_budget < 0 or raster_budget < 0:
        raise ValueError('Nonnegative package budgets required')
    evidence, gaps, rasters = [], [], set()
    # Counter-evidence and alternating directions precede redundant context.
    ordered = sorted(requirements, key=lambda r: (
        r.evidence_type != EvidenceType.COUNTER_EVIDENCE, r.page, r.side, r.requirement_id))
    for r in ordered:
        page = page_loader(r)
        if page is None:
            gaps.append(dict(requirement_id=r.requirement_id, reason='NOT_ADMITTED_OR_UNAVAILABLE'))
            continue
        native, ocr = page.get('native', ''), page.get('ocr', '')
        text = 'PDF NATIVE:\n' + native + '\nOCR (fallible):\n' + ocr
        selected = select_sections(text, [r.expected_semantic_content], text_budget)
        text_budget -= len(selected['text'])
        raster = page.get('raster')
        # Equal image bytes in distinct OLD/NEW sources still need distinct
        # labeled source inputs in the existing delivery contract.
        key = (r.side, r.document_version, r.page, raster.get('sha256')) if raster else None
        if raster and key not in rasters and len(rasters) >= raster_budget:
            raster = None
        if raster:
            rasters.add(key)
        # The audit maps the subject to a bounded region on this page. Only
        # delivery of its full source raster fulfills ALL visual/table parts.
        grounded = bool(r.scope_binding and page.get('scope_binding') == r.scope_binding)
        full_visual = bool(raster and page.get('full_page') and grounded)
        parts = sorted(r.parts) if full_visual else []
        e = dict(evidence_id='e_' + fingerprint(asdict(r))[:24], subject=r.subject,
                 side=r.side, document=r.document, document_version=r.document_version,
                 page=r.page, text=selected['text'], raster=raster,
                 bbox=page.get('bbox'), content_kind=page.get('content_kind', 'PAGE'),
                 scope_binding=r.scope_binding if grounded else '', delivered_parts=parts,
                 boundary_complete=full_visual, truncated=bool(selected['omitted_sections']) and not full_visual,
                 text_complete=selected['boundary_complete'],
                 omitted_sections=selected['omitted_sections'],
                 provenance=page['provenance'])
        evidence.append(e)
        if not full_visual:
            gaps.append(dict(requirement_id=r.requirement_id,
                             reason='RASTER_BUDGET_OR_UNVERIFIED_SEMANTIC_REGION'))
    coverage = coverage_receipt(requirements, evidence)
    body = dict(schema='PROJECTCHANGE_REQUIREMENT_PACKAGE/1', inference_executed=False,
                requirements=[asdict(r) for r in requirements], evidence=evidence,
                evidence_coverage=coverage, coverage_complete=coverage['complete'], gaps=gaps,
                counter_evidence=same_subject_counter(requirements, evidence),
                scope_discovery='SOURCE_AUDIT_SCAFFOLDING_NOT_AUTONOMOUS',
                limits=dict(remaining_text_characters=text_budget, raster_budget=raster_budget),
                model_instruction='Read evidence_coverage. Incomplete evidence cannot prove a '
                                  'direct comparison or novelty. COMPLETE means delivery, not truth.')
    body['package_hash'] = fingerprint(body)
    return body


def write_package(directory, body):
    """Every package has a separately readable coverage receipt and integrity hash."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    for name, value in [('PACKAGE.json', body), ('EVIDENCE_COVERAGE.json', body['evidence_coverage'])]:
        path = directory / name
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + '\n'
        if path.exists() and path.read_text() != payload:
            raise ValueError('Existing package drift; use a fresh output directory')
        if not path.exists():
            path.write_text(payload)


class AdmittedPageLoader:
    """Read only explicit pages of already admitted documents, before rendering.

    excluded is the union of embargo and revision-history pages from isolated DEV
    sources. An excluded requirement remains MISSING; it is never opened to fill
    a package. A content hash is checked before each document's first source read.
    """
    def __init__(self, documents, excluded, output):
        self.documents, self.excluded = documents, excluded
        self.output = Path(output)
        self.cache = {}
        self.rasters = {}
        self.opened = []

    def __call__(self, r):
        key = (r.document, r.document_version, r.side)
        doc = self.documents.get(key)
        if doc is None or r.page in self.excluded.get(key, set()):
            return None
        if key not in self.cache:
            receipt = doc['artifacts']['pdf']
            if sha(receipt['path']) != receipt['sha256']:
                raise ValueError('Source PDF drift')
            self.cache[key] = fitz.open(receipt['path'])
        pdf = self.cache[key]
        if not 1 <= r.page <= len(pdf):
            raise ValueError('Page outside admitted document')
        page = pdf[r.page - 1]
        native = page.get_text()
        image_id = fingerprint(dict(source=doc['artifacts']['pdf'], page=r.page,
                                    renderer=fitz.VersionBind, scale=2))
        path = self.output / (image_id + '.png')
        if image_id not in self.rasters:
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                pixmap = fitz.Pixmap(str(path))
            else:
                pixmap = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                pixmap.save(path)
            self.rasters[image_id] = dict(path=str(path), sha256=sha(path), page=r.page,
                source_pdf_sha256=doc['artifacts']['pdf']['sha256'],
                width=pixmap.width, height=pixmap.height)
        raster = self.rasters[image_id]
        self.opened.append(dict(document=r.document, side=r.side, page=r.page))
        return dict(native=native, ocr='', raster=raster, full_page=True,
                    bbox=list(page.rect), scope_binding=r.scope_binding,
                    provenance=dict(pdf=doc['artifacts']['pdf'], native_sha256=fingerprint(native),
                                    scope_requirement=r.provenance, raster=raster))

    def close(self):
        for pdf in self.cache.values():
            pdf.close()
