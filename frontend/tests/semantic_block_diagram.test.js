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
    it('draws a common orange bus for unresolved members without creating pair edges', () => {
        const model = api.buildModel({regions:[region('g', [['a',1],['b',2]], [['c',3],['d',4]])]});
        const data = api.diagramData(model), groups = api.diagramPendingGroups(model,data);
        const ends = new Map([['OLD|a',{x:50,y:50}],['OLD|b',{x:50,y:150}],['NEW|c',{x:450,y:100}],['NEW|d',{x:450,y:250}]]);
        const lines = api.diagramPendingRoutes(groups,ends,150,350);
        expect(lines).toHaveLength(4); // one arm per member, never four claimed pair links
        expect(new Set(lines.map(l=>l.laneX)).size).toBe(1);
        expect(lines.every(l=>l.pending && !l.old_block_id && !l.new_block_id)).toBe(true);
        expect(lines.some(l=>l.path.includes(' Q '))).toBe(true);
        expect(data.edges).toHaveLength(0);
        expect(model.edges.size).toBe(0);
        ends.delete('NEW|c'); ends.delete('NEW|d');
        expect(api.diagramPendingRoutes(groups,ends,150,350)).toHaveLength(0);
    });

    it('keeps concretely linked blocks out of orange membership lines', () => {
        const model = api.buildModel({regions:[region('g', [['a',1],['b',2]], [['c',3],['d',4]]), region('pair', [['a',1]], [['d',4]])]});
        const groups = api.diagramPendingGroups(model,api.diagramData(model));
        expect(groups).toHaveLength(1);
        expect(groups[0].members.map(b=>b.key)).toEqual(['OLD|b','NEW|c']);
        const single = api.buildModel({regions:[region('single',[['a',1]],[])]});
        expect(api.diagramPendingGroups(single,api.diagramData(single))).toHaveLength(0);
        const rejected = api.buildModel({regions:[region('rejected',[['a',1]],[['b',2]])]});
        rejected.edges.get('a|b').state = 'FORBIDDEN';
        expect(api.diagramPendingGroups(rejected,api.diagramData(rejected))).toHaveLength(0);
        model.latest.set('g',{status:'HUMAN_REJECTED'});
        expect(api.diagramPendingGroups(model,api.diagramData(model))).toHaveLength(0);
    });

    it('shows the saved group reason and the roles of the selected endpoints only', () => {
        const r = api.fromIndexRegion({id:'r',domain:'Насосы',scope:'Пуск и давление',reason:'Обе стороны описывают управление насосами.',
            members:{OLD:[{id:'a',page:10,type:'TEXT',relevance:'Пуск задвижек'}, {id:'b',page:12,type:'TEXT',relevance:'Другая роль'}],
                NEW:[{id:'c',page:151,type:'TABLE',relevance:'Уставки давления'}]}});
        const unrelated = region('unrelated', [['a',10]], [['d',200]]);
        unrelated.reason = 'Другая причина';
        const edge = api.diagramData(api.buildModel({regions:[r,unrelated]})).edges.find(e=>e.key==='a|c');
        expect(edge.groups).toEqual([{id:'r',title:'Насосы',scope:'Пуск и давление',reason:'Обе стороны описывают управление насосами.',
            oldRelevance:'Пуск задвижек',newRelevance:'Уставки давления'}]);
        const legacy = api.diagramData(api.buildModel({regions:[region('old', [['x',1]], [['y',2]])]}));
        expect(legacy.edges[0].groups[0].reason).toBe('');
    });

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

    it('adds explained content proposals to the diagram without adding review anchors', () => {
        const r = region('group', [['a', 1], ['b', 2]], [['c', 3], ['d', 4]]);
        r.block_link_proposals = [{old_block_id:'a',new_block_id:'d',reason:'Близкий текст',method:'TEXT_OVERLAP'}];
        const model = api.buildModel({regions:[r]});
        const diagram = api.diagramData(model);
        expect(diagram.edges.map(e => e.key)).toEqual(['a|d']);
        expect(diagram.edges[0].reason).toBe('Близкий текст');
        expect(diagram.edges[0].state).toBe('CONTENT_PROPOSED');
        expect(model.edges.size).toBe(0);
        expect(model.decided).toHaveLength(0);
    });

    it('does not resurrect deleted, reviewed or cross-region rejected links with proposals', () => {
        const r = region('group', [['a', 1], ['b', 2]], [['c', 3], ['d', 4]]);
        r.block_link_proposals = [{old_block_id:'a',new_block_id:'d',reason:'text'}];
        const model = api.buildModel({regions:[r]});
        model.edits = [{region_id:'group',event_type:'DELETE_BLOCK_LINK',old_block_id:'a',new_block_id:'d'}];
        expect(api.diagramData(model).edges).toHaveLength(0);
        model.edits = [];
        model.reviews = [{region_id:'group'}];
        expect(api.diagramData(model).edges).toHaveLength(0);
        model.reviews = [];
        model.edges.set('a|d',{key:'a|d',state:'FORBIDDEN'});
        expect(api.diagramData(model).edges).toHaveLength(0);
    });

    it('rejects proposal endpoints outside the saved region membership', () => {
        const r = region('group', [['a', 1], ['b', 2]], [['c', 3], ['d', 4]]);
        r.block_link_proposals = [{old_block_id:'a',new_block_id:'foreign'}];
        expect(api.diagramData(api.buildModel({regions:[r]})).edges).toHaveLength(0);
    });

    it('keeps a currently effective manual link even if it was deleted earlier', () => {
        const model = api.buildModel({regions:[region('r', [['a',1]], [['b',2]])]});
        model.edits = [{region_id:'r',event_type:'DELETE_BLOCK_LINK',old_block_id:'a',new_block_id:'b'}];
        model.edges.get('a|b').links = [{source:'HUMAN_MANUAL'}];
        expect(api.diagramData(model).edges.map(e => e.key)).toEqual(['a|b']);
    });

    it('does not display rejected or conflicting links as accepted correspondences', () => {
        const model = api.buildModel({regions: [region('r', [['a', 1]], [['b', 2]])]});
        model.edges.get('a|b').state = 'FORBIDDEN';
        expect(api.diagramData(model).edges).toHaveLength(0);
        model.edges.get('a|b').state = 'CROSS_REGION_CONFLICT';
        expect(api.diagramData(model).edges).toHaveLength(0);
        expect(api.diagramData(model).OLD).toHaveLength(1);
    });

    it('positions block frames in the original normalized page coordinates', () => {
        expect(api.diagramBox({bbox: [.25, .1, .75, .6]}))
            .toEqual({width: '50%', height: '50%', left: '25%', top: '10%'});
        for (const bbox of [null, [0, 0, 0, 1], [-1, 0, 1, 1], [0, 0, 2, 1]])
            expect(api.diagramBox({bbox})).toBeNull();
    });

    it('shows each sheet once with all its blocks in physical page order', () => {
        const pages = api.diagramPages([{id:'a',page:8},{id:'b',page:2},{id:'c',page:8},{id:'missing',page:null}]);
        expect(pages.map(p=>p.page)).toEqual([2,8]);
        expect(pages[1].blocks.map(b=>b.id)).toEqual(['a','c']);
    });

    it('separates overlapping connections into parallel gutter lanes with small rounded turns', () => {
        const edges = [
            {key:'one', a:{x:80,y:100}, b:{x:400,y:200}},
            {key:'two', a:{x:90,y:150}, b:{x:410,y:300}},
            {key:'back', a:{x:80,y:500}, b:{x:400,y:400}},
            {key:'level', a:{x:80,y:700}, b:{x:400,y:700}},
            {key:'close', a:{x:80,y:800}, b:{x:400,y:802}},
        ];
        const routes = api.diagramRoutes(edges, 160, 320);
        expect(routes[0].laneX).not.toBe(routes[1].laneX);
        expect(routes[2].laneX).toBe(routes[0].laneX); // disjoint spans can share a lane
        for (const route of routes) {
            expect(route.laneX).toBeGreaterThan(160);
            expect(route.laneX).toBeLessThan(320);
            expect(route.radius).toBeLessThanOrEqual(8);
            expect(route.path).toMatch(/^M /);
            expect(route.path).not.toContain(' C ');
        }
        expect(routes[0].path.match(/ Q /g)).toHaveLength(2);
        expect(routes[2].path).toContain(`V ${400 + 8} Q`); // upward route rounds the other way
        expect(routes[3].path).toBe('M 80 700 H 400');
        expect(routes[4].radius).toBe(1); // close endpoints never double back
        expect(api.diagramRoutes([...edges].reverse(),160,320)).toEqual(routes);
    });

    it('preserves a concave polygon instead of substituting its bounding rectangle', () => {
        const points = [[.1,.1],[.9,.1],[.9,.25],[.4,.25],[.4,.4],[.1,.4]];
        const shape = api.diagramShape({shape_type:'polygon', polygon_points:points, bbox:[.1,.1,.9,.4]});
        expect(shape.points).toBe('0.1,0.1 0.9,0.1 0.9,0.25 0.4,0.25 0.4,0.4 0.1,0.4');
        expect(api.diagramAnchor(points,'OLD')).toEqual([.9,.175]);
        expect(api.diagramAnchor(points,'NEW')).toEqual([.1,.25]);
        expect(api.diagramShape({shape_type:'polygon',bbox:[0,0,1,1]})).toBeNull();
        expect(api.diagramShape({polygon_points:[[0,0],[1,1]],bbox:[0,0,1,1]})).toBeNull();
        expect(api.diagramShape({shape_type:'rectangle',polygon_points:[],bbox:[0,0,1,1]}).points)
            .toBe('0,0 1,0 1,1 0,1');
    });
});
