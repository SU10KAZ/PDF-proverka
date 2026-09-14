import {describe, expect, it} from 'vitest';
import {createRequire} from 'node:module';
const require = createRequire(import.meta.url);
const V = require('../static/js/project-change-view.js');
const evidence = (source_type = 'TEXT', side = 'OLD', page = 21, extra = {}) => ({
    id: `${source_type}-${side}-${page}`, source_type, side, page, pair_id: 'pair-272',
    document: {id: `doc-${side}`, label: 'ИОС4.1', version: 'v002', pdf_path: `/272/${side}/v002/document.pdf`},
    image_url: `/static/272/${side}-${page}.png`, short_explanation_ru: 'Проверьте схему подключения.', ...extra,
});
const change = (extra = {}) => ({id: 'change-272', summary_ru: 'Изменена система', change_type: 'SYSTEM',
    status: 'REVIEW', importance: 'HIGH', cipher: 'ИОС4.1', discipline: 'Кондиционирование',
    engineering_system: 'Фанкойлы', engineering_subject: 'Фанкойльная система',
    old_state: 'Двухтрубная', new_state: 'Четырёхтрубная',
    details: [{label: 'Мощность', old: '10', new: '12'}],
    evidence: [evidence(), evidence('TABLE', 'NEW', 26), evidence('GRAPHIC', 'NEW', 27)],
    technical_provenance: ['AMBIGUOUS_ENTITY'], ...extra});
const envelope = (items, extra = {}) => ({schema_version: 'project-change-view/1', object_id: V.OBJECT,
    origin: 'PRODUCTION', revision: 'v002-r1', items, ...extra});
const adapt = (c, extra) => V.fromEnvelope(envelope([c], extra), V.OBJECT)[0];

describe('ProjectChangeView presentation boundary', () => {
    it('preserves one grouped event with atomic facts only in details', () => {
        const v = adapt(change());
        expect(v.id).toBe('change-272'); expect(v.details).toHaveLength(1);
        expect(v.summary_ru).toBe('Изменена система');
    });
    it('does not mutate source data', () => {
        const source = change(); const serialized = JSON.stringify(source);
        V.applyDecision(adapt(source), 'CONFIRMED'); expect(JSON.stringify(source)).toBe(serialized);
    });
    it('does not accept a foreign project or raw atomic diff array', () => {
        expect(V.fromEnvelope(envelope([change()]), 'other')).toEqual([]);
        expect(V.fromEnvelope({diffs: [change()]}, V.OBJECT)).toEqual([]);
    });
    it('rejects wrong contract versions and foreign envelope scope', () => {
        expect(V.fromEnvelope(envelope([change()], {schema_version: '2'}), V.OBJECT)).toEqual([]);
        expect(V.fromEnvelope(envelope([change()], {object_id: 'other'}), V.OBJECT)).toEqual([]);
    });
    it('never converts research PROVEN or CONFIRMED to production truth', () => {
        for (const status of ['PROVEN', 'CONFIRMED', 'REVIEW']) {
            expect(adapt(change({status}), {origin: 'RESEARCH'}).status).toBe('REVIEW');
        }
    });
    it('defaults unknown statuses to review', () => {
        expect(adapt(change({status: 'APPROVED_UNKNOWN'})).status).toBe('REVIEW');
    });
    it('does not trust confirmation from an unspecified origin', () => {
        expect(adapt(change({status: 'CONFIRMED'}), {origin: undefined}).status).toBe('REVIEW');
    });
    it('handles incomplete optional evidence and details without crashing', () => {
        const c = adapt(change({evidence: [null], conflicts: [null], details: [null]}));
        expect(c.evidence[0].page).toBeNull(); expect(c.conflicts).toEqual([]); expect(c.details).toEqual([]);
    });
    it('deduplicates event ids and rejects missing ids', () => {
        expect(V.fromEnvelope(envelope([change(), change(), change({id: ''})]), V.OBJECT)).toHaveLength(1);
    });
    it('preserves all evidence routes and multiple screenshots per side', () => {
        const c = adapt(change({evidence: [evidence(), evidence('TEXT', 'OLD', 22), evidence('TABLE', 'NEW', 26), evidence('GRAPHIC', 'NEW', 27)]}));
        expect(c.evidence).toHaveLength(4);
        expect(new Set(c.evidence.map(e => e.source_type))).toEqual(new Set(V.SOURCES));
        expect(c.evidence.filter(e => e.side === 'OLD')).toHaveLength(2);
    });
    it('does not turn absent image, page, or route into invented evidence', () => {
        const c = adapt(change({evidence: [evidence('UNKNOWN', 'OLD', 0, {image_url: ''})]}));
        expect(c.evidence[0]).toMatchObject({source_type: null, page: null, image_url: ''});
        expect(V.destination(c)).toBeNull();
    });
    it('rejects remote, script and protocol-relative image URLs', () => {
        for (const image_url of ['javascript:alert(1)', '//remote/image.png', 'https://remote/image.png', '/\\remote/image.png']) {
            expect(adapt(change({evidence: [evidence('TEXT', 'OLD', 1, {image_url})]})).evidence[0].image_url).toBe('');
        }
    });
    it('only accepts bounded explicitly normalized regions', () => {
        const region = {units: 'normalized', x: 0.2, y: 0.3, width: 0.1, height: 0.2};
        expect(adapt(change({evidence: [evidence('TEXT', 'OLD', 1, {region})]})).evidence[0].region).toEqual(region);
        expect(adapt(change({evidence: [evidence('TEXT', 'OLD', 1, {region: {...region, x: 2}})]})).evidence[0].region).toBeNull();
    });
    it.each(['CONFIRMED', 'REJECTED', 'UNDETERMINED', 'PROBLEM'])('applies review action %s', status => {
        expect(V.applyDecision(adapt(change()), status)).toMatchObject({status, local_decision: true});
    });
    it('ignores invalid actions', () => {
        const c = adapt(change()); expect(V.applyDecision(c, 'PROVEN')).toBe(c);
    });
    it('conflicts override confirmation and cannot be confirmed implicitly', () => {
        const c = adapt(change({status: 'CONFIRMED', conflicts: [{values: [
            {source_type: 'TEXT', value: '3'}, {source_type: 'TABLE', value: '2'}, {source_type: 'GRAPHIC', value: '2'},
        ]}]}));
        expect(c.status).toBe('CONFLICT'); expect(c.conflicts[0].values).toHaveLength(3);
        expect(V.applyDecision(c, 'CONFIRMED')).toBe(c); expect(V.report([c])).toEqual([]);
    });
    it('report includes only confirmed events, including after a rejected decision', () => {
        const c = adapt(change()); const yes = V.applyDecision(c, 'CONFIRMED');
        expect(V.report([c, yes, V.applyDecision(c, 'REJECTED')])).toEqual([yes]);
        expect(V.report([V.applyDecision(yes, 'REJECTED')])).toEqual([]);
    });
    it('computes compact summary independently of filters', () => {
        const c = adapt(change());
        expect(V.summary([c, V.applyDecision({...c, id: '2'}, 'CONFIRMED')])).toEqual({confirmed: 1, review: 1, conflicts: 0, high: 2});
    });
    it.each(['cipher','system','type','status','source'])('filters by %s', key => {
        const c = adapt(change());
        const correct = {cipher: 'ИОС4.1 · Кондиционирование', system: 'Фанкойлы', type: 'SYSTEM', status: 'REVIEW', source: 'GRAPHIC'};
        expect(V.filter([c], {[key]: correct[key]})).toEqual([c]);
        expect(V.filter([c], {[key]: 'missing'})).toEqual([]);
    });
    it('combines filters conjunctively', () => {
        expect(V.filter([adapt(change())], {source: 'TABLE', status: 'CONFIRMED'})).toEqual([]);
    });
    it('navigates selected source and exact opposite page together', () => {
        const c = adapt(change()); const target = V.destination(c, c.evidence[2]);
        expect(target).toMatchObject({pair_id: 'pair-272', OLD: {page: 21}, NEW: {page: 27}});
        expect(target.NEW.document.version).toBe('v002');
    });
    it('does not reuse the viewer page when one side is unbound', () => {
        const c = adapt(change({evidence: [evidence('TEXT', 'NEW', 13)]}));
        expect(V.destination(c)).toMatchObject({OLD: null, NEW: {page: 13}});
    });
    it('never chooses opposite evidence from a different document pair', () => {
        const c = adapt(change({evidence: [evidence(), evidence('TEXT','NEW',2,{pair_id:'other'})]}));
        expect(V.destination(c).NEW).toBeNull();
    });
    it('refuses unversioned evidence navigation', () => {
        const c = adapt(change({evidence: [evidence('TEXT','OLD',1,{document:{id:'a'}})]}));
        expect(V.destination(c)).toBeNull();
    });
    it('research adapter separates human text, technical reasons and facts', () => {
        const raw = [{project_change_id: 'c', status: 'PROVEN', short_summary_ru: 'Изменён насос', change_type: 'EQUIPMENT_REPLACED',
            engineering_subject: {mark: 'П1'}, review_reasons: ['OLD_SCOPE_NOT_ESTABLISHED'],
            supporting_fact_changes: [{property: 'flow', old: {quote: '10'}, new: {quote: '12'}}],
            evidence_new: [{evidence_id: 'e', route: 'GRAPHIC', quote: 'П1'}]}];
        const context = {object_id: V.OBJECT, partition: 'DEV', revision: 'v002',
            bindings: {e: [evidence('GRAPHIC', 'NEW', 1)]}, property_labels: {flow: 'Расход'}};
        const env = V.fromResearch(raw, context); const c = V.fromEnvelope(env, V.OBJECT)[0];
        expect(c.status).toBe('REVIEW'); expect(c.change_type).toBe('EQUIPMENT');
        expect(c.review_explanation_ru).toContain('Область поиска в OLD');
        expect(c.review_explanation_ru).not.toContain('OLD_SCOPE_NOT_ESTABLISHED');
        expect(c.details[0].label).toBe('Расход'); expect(c.evidence[0].source_type).toBe('GRAPHIC');
        expect(V.fromResearch(raw, {...context, partition: 'VALIDATION'})).toBeNull();
    });
});
