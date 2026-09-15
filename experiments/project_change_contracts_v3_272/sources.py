"""Conservative source-form discovery within already admitted page requirements.

Native scheme titles identify the form, never the engineering truth. Scope is
still the preexisting subject/page binding; unknown forms cannot satisfy graphics.
"""
from dataclasses import replace
import re

from experiments.project_change_contracts_272.packages import AdmittedPageLoader
from .evidence import EvidenceType, fingerprint


def source_types(native, bbox):
    text = ' '.join(native.split()).casefold()
    types = {'COUNTER_EVIDENCE'} if text else set()
    if text:
        types.add('TEXT_SECTION')
    if re.search(r'примечани|условные обозначения', text):
        types.add('NOTE')
    # Require a drawing-sized page AND an actual drawing title, rather than a
    # mention of a scheme in prose or the column «Схема исп.» in a schedule.
    width, height = bbox[2] - bbox[0], bbox[3] - bbox[1]
    title = re.search(r'принципиальн\w* схем\w*|схема (?:подключения|разводки|систем)', text)
    if min(width, height) >= 590 and max(width, height) >= 840 and title:
        types.update(('GRAPHIC_SCHEME', 'GRAPHIC_REGION'))
    numeric_lines = sum(bool(re.fullmatch(r'[\d.,+−\-/ ]+', s.strip())) for s in native.splitlines() if s.strip())
    if 'таблица' in text or numeric_lines >= 20:
        types.add('TABLE_COMPLETE')
    if ('вентилятор' in text and 'электродвигатель' in text and
            ('воздухонагреватель' in text or 'характеристика систем' in text)):
        types.update(('TABLE_COMPLETE', 'EQUIPMENT_SCHEDULE'))
    return sorted(types)


class TypedPageLoader(AdmittedPageLoader):
    def __call__(self, requirement):
        page = super().__call__(requirement)
        if page:
            page['evidence_types'] = source_types(page['native'], page['bbox'])
            page['type_provenance'] = dict(method='BOUNDED_NATIVE_FORM_DISCOVERY/3',
                native_sha256=fingerprint(page['native']), pdf=page['provenance']['pdf'],
                page=requirement.page, scope_binding=requirement.scope_binding)
        return page


def refine_requirements(requirements, pages):
    """Keep every old requirement; specialize genuine diagrams to schemes.

    Mislabelled table pages retain an unsatisfied GRAPHIC_REGION requirement.
    A matching table never erases that gap or satisfies a mandatory scheme.
    """
    return [replace(r, required_type=EvidenceType.GRAPHIC_SCHEME)
            if r.evidence_type == 'GRAPHIC_REGION' and
            'GRAPHIC_SCHEME' in (pages.get(r.requirement_id) or {}).get('evidence_types', [])
            else r for r in requirements]
