"""Explicit report routing is independent of severity and must survive dedup."""


def report_section_key(finding):
    section = finding.get('report_section')
    return section if section in ('technical_findings', 'questions') else 'unclassified'
