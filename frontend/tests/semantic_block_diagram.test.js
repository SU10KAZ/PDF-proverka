import {describe, it, expect} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const context = vm.createContext({});
for (const file of ['human-mapping-core.js', 'stage-block-mapping.js']) {
    vm.runInContext(readFileSync(new URL('../static/js/' + file, import.meta.url), 'utf8'), context);
}
const api = context.StageBlockMapping;
const region = (id, old, next) => api.fromIndexRegion({id, domain: id,
    members: {OLD: old.map(([id, page]) => ({id, page, type: 'TEXT'})),
        NEW: next.map(([id, page]) => ({id, page, type: 'TEXT'}))}});

describe('simple semantic block diagram', () => {
    it('keeps all document blocks, deduplicates shared blocks and renders 1:N / N:1 links', () => {
        const model = api.buildModel({regions: [
            region('one-to-many', [['a', 1]], [['b', 9], ['c', 12]]),
            region('many-to-one', [['a', 1], ['d', 30]], [['e', 6]]),
            region('old-only', [['f', 31]], []),
        ]});
        const diagram = api.diagramData(model);
        expect(diagram.OLD.map(b => b.id)).toEqual(['a', 'd', 'f']);
        expect(diagram.NEW.map(b => b.id)).toEqual(['e', 'b', 'c']);
        expect(diagram.edges.map(e => e.key).sort()).toEqual(['a|b', 'a|c', 'a|e', 'd|e']);
        expect(diagram.OLD[0].titles).toEqual(['one-to-many', 'many-to-one']);
    });

    it('never invents all-to-all links for an unresolved N:N group', () => {
        const diagram = api.diagramData(api.buildModel({regions: [
            region('group', [['a', 1], ['b', 2]], [['c', 3], ['d', 4]]),
        ]}));
        expect(diagram.edges).toHaveLength(0);
        expect(diagram.OLD).toHaveLength(2);
        expect(diagram.NEW.every(b => b.groupOnly)).toBe(true);
    });

    it('does not display rejected or conflicting links as accepted correspondences', () => {
        const model = api.buildModel({regions: [region('r', [['a', 1]], [['b', 2]])]});
        model.edges.get('a|b').state = 'FORBIDDEN';
        expect(api.diagramData(model).edges).toHaveLength(0);
        model.edges.get('a|b').state = 'CROSS_REGION_CONFLICT';
        expect(api.diagramData(model).edges).toHaveLength(0);
        expect(api.diagramData(model).OLD).toHaveLength(1);
    });

    it('crops normalized coordinates using the original page aspect ratio', () => {
        expect(api.diagramCrop({bbox: [.25, .1, .75, .6]}, {width_px: 1000, height_px: 2000}))
            .toEqual({ratio: .5, image: {width: '200%', height: '200%', left: '-50%', top: '-20%'}});
        for (const bbox of [null, [0, 0, 0, 1], [-1, 0, 1, 1], [0, 0, 2, 1]])
            expect(api.diagramCrop({bbox}, null)).toBeNull();
    });
});
