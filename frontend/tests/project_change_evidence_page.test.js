import {describe, expect, it, vi} from 'vitest';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import vm from 'node:vm';
const require = createRequire(import.meta.url);
const V = require('../static/js/project-change-view.js');
const vue = readFileSync(new URL('../static/js/vue.global.prod.js', import.meta.url), 'utf8');
const ui = readFileSync(new URL('../static/js/project-change-ui.js', import.meta.url), 'utf8');
const evidence = id => ({id, page:19, quote:'Pipe insulation 10 mm', image_url:'/crop/'+id,
    page_view_url:'/page-view/'+id, document:{label:'Document'}, side:'OLD'});
const location = id => ({evidence_id:id, image_url:'/page-image/'+id, page_width:600, page_height:400,
    kind:'EXACT_QUOTE', message:'Quote located', highlights:[{x:.1,y:.2,width:.3,height:.05}]});
const response = data => ({ok:true, json:async()=>data});
function mount(fetch) {
    const context = vm.createContext({ProjectChangeView:V, fetch});
    vm.runInContext(vue,context); vm.runInContext(ui,context);
    let component; context.ProjectChangeUI.register({component:(_n,c)=>{component=c;}});
    const view = component.setup({changes:[],pairs:[]},{emit:()=>{}});
    view.imageDialog.value = {open:false,showModal:vi.fn()};
    return {view, context, component};
}

describe('source page with quote highlight',()=>{
    it('opens a page and positions normalized boxes in the PDF coordinate system',async()=>{
        const fetch=vi.fn(async()=>response(location('a'))), {view}=mount(fetch);
        await view.enlarge(evidence('a'));
        expect(fetch).toHaveBeenCalledWith('/page-view/a');
        expect(view.pageView.value.evidence_id).toBe('a');
        expect(view.pageBoxStyle(view.pageView.value.highlights[0])).toEqual({x:60,y:80,width:180,height:20});
        expect(view.pageLoading.value).toBe(false);
        expect(view.imageDialog.value.showModal).toHaveBeenCalledOnce();
    });
    it('starts the full page image while quote location is still pending',async()=>{
        let resolve;const {view,context,component}=mount(()=>new Promise(r=>{resolve=r;}));
        const e={...evidence('a'),page_view_url:'/evidence/a/page-view?run_id=one'};
        const pending=view.enlarge(e);await context.Vue.nextTick();
        expect(view.pageLoading.value).toBe(true);
        expect(view.pageImageUrl.value).toBe('/evidence/a/page-image?run_id=one');
        expect(component.template).toContain('<img v-if="pageLoading" :src="pageImageUrl || selectedImage.image_url"');
        resolve(response(location('a')));await pending;
        expect(view.pageView.value.evidence_id).toBe('a');
    });
    it('an older request cannot overwrite the newly selected evidence',async()=>{
        const pending={}; const {view,context}=mount(url=>new Promise(resolve=>{pending[url]=resolve;}));
        const first=view.enlarge(evidence('a'));await context.Vue.nextTick();
        const second=view.enlarge(evidence('b'));await context.Vue.nextTick();
        pending['/page-view/b'](response(location('b')));await second;
        pending['/page-view/a'](response(location('a')));await first;
        expect(view.pageView.value.evidence_id).toBe('b');
        expect(view.selectedImage.value.id).toBe('b');
    });
    it('closing the dialog discards a pending location response',async()=>{
        let resolve;const {view,context}=mount(()=>new Promise(r=>{resolve=r;}));
        const pending=view.enlarge(evidence('a'));await context.Vue.nextTick();
        view.imageClosed();resolve(response(location('a')));await pending;
        expect(view.pageView.value).toBeNull();expect(view.pageLoading.value).toBe(false);
    });
    it.each([
        {...location('other')},
        {...location('a'),image_url:'https://other.test/image.png'},
        {...location('a'),image_url:'//other.test/image.png'},
        {...location('a'),highlights:[{x:.9,y:0,width:.5,height:1}]},
        {...location('a'),page_width:0},
    ])('invalid evidence identity or geometry never produces a highlight',async data=>{
        const {view}=mount(async()=>response(data));await view.enlarge(evidence('a'));
        expect(view.pageView.value).toBeNull();expect(view.pageError.value).toContain('Не удалось');
    });
    it('network failure leaves an explicit fallback without highlights',async()=>{
        const {view}=mount(async()=>({ok:false}));await view.enlarge(evidence('a'));
        expect(view.pageView.value).toBeNull();expect(view.pageError.value).toContain('сохранённый фрагмент');
    });
    it('old evidence without the new page endpoint still opens its crop',async()=>{
        const fetch=vi.fn(),{view}=mount(fetch),e=evidence('old');delete e.page_view_url;
        await view.enlarge(e);expect(fetch).not.toHaveBeenCalled();expect(view.pageView.value).toBeNull();
        expect(view.selectedImage.value.image_url).toBe('/crop/old');
    });
    it('the SVG template compiles and quotes are escaped Vue text bindings',()=>{
        const {context,component}=mount(vi.fn());
        expect(typeof context.Vue.compile(component.template, {decodeEntities:value=>value})).toBe('function');
        expect(component.template).toContain('{{ selectedImage.quote }}');
        expect(component.template).toContain(':key="selectedImage.id"');
    });
});
