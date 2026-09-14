"""Product-level exclusions: source changes do not establish design changes."""
import re


def normalize(value):
    return re.sub(r'\s+',' ',str(value)).casefold().replace('ё','е')


def metadata_property(name):
    name=normalize(name)
    return bool(re.search(
        r'основани[ея].{0,60}(?:корректиров|разработк)|'
        r'(?:дата|датировка).{0,70}(?:состояни|нанесени|выпуска|выдачи|документ|заключени|согласован)|'
        r'(?:номер|дата).{0,30}(?:договор|соглашени|заключени)|'
        r'(?:source|document|revision|issue) (?:date|number)|basis of (?:revision|design)',name))


def decide(event,allow_condition_review=False):
    """These are engineering-domain gates, never positive truth certificates.

    A construction deadline can be an engineering state. A date of a source
    survey or a different agreement reference, without changed design content,
    cannot. A legend's missing entry is not closed-world component absence.
    """
    facts=event.get('facts',[])
    substantive=[i for i,f in enumerate(facts) if not metadata_property(f.get('property',''))]
    issues=[]
    if facts and not substantive:
        return dict(status='REJECT_NOT_PROJECT_CHANGE',issues=['ADMINISTRATIVE_STATE_ONLY'],substantive_fact_indices=[])
    text=normalize(' '.join(str(event.get(k,'')) for k in ['engineering_subject','old_state','new_state','summary_ru']))
    absence=bool(re.search(r'не (?:виден|видна|видно|видны|показан|указан|представлен|приведен)|отсутству|'
                           r'not (?:visible|shown|listed|present)|no .{0,55}(?:visible|shown)',text))
    existence=bool(re.search(r'добавл|добави|исключ|удален|появи|дополнительно указан|'
                            r'\b(?:added|removed|introduced|excluded)\b',text))
    if absence and existence:issues.append('EXISTENCE_INFERRED_FROM_INCOMPLETE_REPRESENTATION')
    if re.search(r'(?:перестал\w* описыв|вместо описания|заменен\w* ссылк|из текста исключен\w* (?:указани|информаци))',text):
        issues.append('DOCUMENTATION_OR_REFERENCE_CHANGE_WITHOUT_PROVEN_DESIGN_STATE')
    if len(substantive)<len(facts):issues.append('MIXED_ADMINISTRATIVE_FACTS_REQUIRE_SUMMARY_REPAIR')
    conditional=[]
    for f in facts:
        markers=[bool(re.search(r'(?<!\*)\*(?!\*)',str(f.get(s+'_value','')))) for s in ['old','new']]
        if markers[0]!=markers[1]:conditional.append('ASYMMETRIC_SOURCE_FOOTNOTE_REQUIRES_COMPARABLE_DIMENSIONS')
    if not allow_condition_review:issues+=conditional
    return dict(status='REVIEW' if issues else 'ACCEPTED_FOR_SOURCE_AUDIT',
                issues=issues,condition_review_required=sorted(set(conditional)),substantive_fact_indices=substantive)
