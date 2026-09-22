import {describe, expect, it} from 'vitest';
import {createRequire} from 'node:module';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const require=createRequire(import.meta.url);
const V=require('../static/js/project-change-view.js');
const item={id:'pc-current',status:'CONFIRMED',research_status:'PROVEN',decision_key:'stable',binding_signature:'binding',candidate_version:'candidate',
    decision_state:'ACTIVE',effective_decision:{decision:'CONFIRM',decision_key:'stable',binding_signature:'binding',candidate_version:'candidate',actor:'engineer'}};
const envelope=(c=item)=>({schema_version:'project-change-view/1',object_id:V.OBJECT,origin:'RESEARCH',mode:'BACKEND_PREVIEW',revision:'revision',items:[c]});
const view=(c)=>V.fromEnvelope(envelope(c),V.OBJECT)[0];
describe('backend preview presentation authority',()=>{
    it('accepts a bound effective human decision while retaining research status',()=>{
        expect(view(item)).toMatchObject({status:'CONFIRMED',research_status:'PROVEN',research:true,decision_key:'stable'});
        expect(V.report([view(item)])).toHaveLength(1);
    });
    it.each(['STALE_DECISION','NONE'])('%s never carries a confirmation',decision_state=>{
        expect(view({...item,decision_state}).status).toBe('REVIEW');
        expect(V.report([view({...item,decision_state})])).toHaveLength(0);
    });
    it.each(['decision_key','binding_signature','candidate_version'])('mismatched %s fails closed',key=>{
        expect(view({...item,effective_decision:{...item.effective_decision,[key]:'changed'}}).status).toBe('REVIEW');
    });
    it('research PROVEN alone is REVIEW, even with an untrusted CONFIRMED status',()=>{
        expect(view({...item,effective_decision:null}).status).toBe('REVIEW');
    });
    it.each([['CONFIRM','CONFIRMED'],['NOT_A_CHANGE','REJECTED'],['UNSURE','UNDETERMINED'],['BROKEN_CASE','PROBLEM']])('maps %s server action', (decision,status)=>{
        expect(view({...item,effective_decision:{...item.effective_decision,decision}}).status).toBe(status);
    });
    it('open conflict keeps values and excludes approval from report',()=>{
        const c=view({...item,conflicts:[{resolved:false,values:[{source_type:'TEXT',value:'3'},{source_type:'TABLE',value:'2'}]}]});
        expect(c.status).toBe('CONFLICT');expect(c.conflicts[0].values).toHaveLength(2);expect(V.report([c])).toHaveLength(0);
    });
    it('PAGE_LEVEL preserves no coordinates; TEXT TABLE GRAPHIC retain all PDF targets',()=>{
        const evidence=['TEXT','TABLE','GRAPHIC'].map((source_type,n)=>({id:String(n),source_type,side:n===0?'OLD':'NEW',page:n+5,pair_id:'pair',
            document:{id:'doc'+n,version:'v002',pdf_path:'/exact/'+n},crop_precision:'PAGE_LEVEL',region:null,image_url:'/api/crop/'+n}));
        const c=view({...item,evidence});
        expect(c.evidence.map(e=>e.source_type)).toEqual(['TEXT','TABLE','GRAPHIC']);
        expect(c.evidence.every(e=>e.region===null&&e.crop_precision==='PAGE_LEVEL')).toBe(true);
        const target=V.destination(c,c.evidence[2]);
        expect(target.NEW.document.pdf_path).toBe('/exact/2');expect(target.NEW.page).toBe(7);expect(target.OLD.page).toBe(5);
    });
});
const app=readFileSync(new URL('../static/js/app.js',import.meta.url),'utf8');
describe('global shell and object-scoped presentation',()=>{
    it.each(['4f3e5916','OTHER_OBJECT'])('same shell for %s with or without the retired flag', objectId=>{
        for (const search of ['', '?projectChangeUi=1', '?projectChangeUi=0']) {
            const begin=app.indexOf('// One Stage Comparison shell'),end=app.indexOf('const pcEnvelope =',begin);
            const context={window:{location:{search},ProjectChangeView:V},URLSearchParams,
                currentObjectId:{value:objectId},computed:fn=>({get value(){return fn();}}),ref:value=>({value})};
            vm.createContext(context);vm.runInContext(app.slice(begin,end)+';this.enabled=pcUiEnabled.value;this.api=pcApi.value;',context);
            expect(context.enabled).toBe(true);
            expect(context.api).toBe('/api/project-change-preview/objects/'+objectId);
        }
    });
    it('accepts a different object only with its own explicit presentation contract',()=>{
        expect(V.fromEnvelope(envelope(),'OTHER_OBJECT')).toEqual([]);
        expect(V.fromEnvelope({...envelope(),object_id:'OTHER_OBJECT'},'OTHER_OBJECT')).toHaveLength(1);
        expect(V.fromEnvelope({object_id:'OTHER_OBJECT',discrepancies:[item]},'OTHER_OBJECT')).toEqual([]);
    });
});
const start=app.indexOf('async function pcDecide(');
const code=app.slice(start,app.indexOf('function pcResetDecisions()',start));
function harness(){
    let reply;
    const c=view(item), calls=[];
    const context={pcBridgeActive:{value:true},pcUiEnabled:{value:true},pcSaving:{value:false},pcError:{value:''},
        pcBaseChanges:{value:[c]},pcEnvelope:{value:{revision:'source',decision_revision:2,capabilities:{decisions:true}}},pcApi:{value:'/preview'},currentObjectId:{value:V.OBJECT},
        pcContextEpoch:0,pcBridgeEnvelope:{value:null},pcHistory:{value:{}},
        pcBridgeUnavailable:{value:false},
        fetch:(url,options)=>{calls.push({url,options});return new Promise(r=>{reply=r;});}};
    vm.createContext(context);vm.runInContext(code,context);
    return {context,calls,save:()=>context.pcDecide({id:c.id,status:'CONFIRMED',comment:'checked'}),
        reply:(data={},ok=true,status=ok?200:409)=>reply({ok,status,json:async()=>data})};
}
describe('persistent preview UI saves',()=>{
    it('sends identity, source and optimistic revision; actor is server-owned',async()=>{
        const h=harness(),pending=h.save();
        expect(h.context.pcSaving.value).toBe(true);
        const body=JSON.parse(h.calls[0].options.body);
        expect(body).toMatchObject({change_id:'pc-current',action:'CONFIRM',decision_key:'stable',binding_signature:'binding',expected_source_revision:'source',expected_decision_revision:2,comment:'checked'});
        expect(body).not.toHaveProperty('actor');h.reply({items:['server']});await pending;
        expect(h.context.pcBridgeEnvelope.value.items).toEqual(['server']);expect(h.context.pcSaving.value).toBe(false);
    });
    it('does not double-save while pending',async()=>{
        const h=harness(),pending=h.save();await h.save();expect(h.calls).toHaveLength(1);h.reply();await pending;
    });
    it('does not apply a late response after object context changes',async()=>{
        const h=harness(),pending=h.save();h.context.pcContextEpoch++;h.reply({items:['stale']});await pending;
        expect(h.context.pcBridgeEnvelope.value).toBeNull();
    });
    it('a conflict is visible and never applied optimistically',async()=>{
        const h=harness(),pending=h.save();h.reply({detail:'Решения обновились'},false);await pending;
        expect(h.context.pcError.value).toBe('Решения обновились');expect(h.context.pcBridgeEnvelope.value).toBeNull();
    });
    it('source-unavailable response invalidates the displayed snapshot',async()=>{
        const h=harness(),pending=h.save();h.reply({detail:'Source drift'},false,503);await pending;
        expect(h.context.pcBridgeUnavailable.value).toBe(true);expect(h.context.pcBridgeEnvelope.value).toBeNull();
    });
});

const loadStart=app.indexOf('async function pcLoadBridge(');
const loadCode=app.slice(loadStart,app.indexOf('const pcSheetFilter',loadStart));
function loadHarness(){
    let reply;
    const context={PC:V,pcUiEnabled:{value:true},pcCatalogFocus:{value:null},pcLoadToken:0,pcContextEpoch:0,pcApi:{value:'/preview'},currentObjectId:{value:V.OBJECT},
        pcBridgeActive:{value:true},pcBridgeEnvelope:{value:{revision:'source',decision_revision:5}},
        pcBridgeUnavailable:{value:false},pcHistory:{value:{}},pcError:{value:''},scSession:{value:{id:'viewer'}},
        fetch:()=>new Promise(r=>{reply=r;})};
    vm.createContext(context);vm.runInContext(loadCode,context);
    return {context,load:()=>context.pcLoadBridge(),reply:(status,data={})=>reply({status,ok:status===200,json:async()=>data})};
}
describe('preview refresh failure and stale responses',()=>{
    it('continues ordinary document workflow when no dataset endpoint is available',async()=>{
        const h=loadHarness();h.context.pcBridgeActive.value=false;const p=h.load();h.reply(404);
        expect(await p).toBe(false);expect(h.context.pcBridgeUnavailable.value).toBe(false);
    });
    it('no dataset leaves document workflow writable and does not borrow a snapshot',async()=>{
        const h=loadHarness();h.context.pcBridgeActive.value=false;const p=h.load();
        h.reply(200,{schema_version:'project-change-view/1',object_id:V.OBJECT,availability:'UNAVAILABLE',items:[]});
        expect(await p).toBe(false);expect(h.context.pcBridgeEnvelope.value).toBeNull();
        expect(h.context.pcBridgeUnavailable.value).toBe(false);
    });
    it('ordinary data outage does not consume document session loading',async()=>{
        const h=loadHarness();h.context.pcBridgeActive.value=false;const p=h.load();h.reply(503,{detail:'Unavailable'});
        expect(await p).toBe(false);expect(h.context.pcBridgeUnavailable.value).toBe(true);
    });
    it('foreign object data fails closed',async()=>{
        const h=loadHarness(),p=h.load();h.reply(200,{...envelope(),object_id:'OTHER',viewer_session:{id:'viewer'}});await p;
        expect(h.context.pcBridgeUnavailable.value).toBe(true);
        expect(h.context.pcBridgeEnvelope.value.items).toBeUndefined();
    });
    it('late absent-dataset response cannot trigger loading for a different object',async()=>{
        const h=loadHarness();h.context.pcBridgeActive.value=false;const p=h.load();h.context.pcContextEpoch++;
        h.reply(404);expect(await p).toBe(true);
    });
    it('disabled or unavailable active bridge invalidates its cached report',async()=>{
        const h=loadHarness(),p=h.load();h.reply(503,{detail:'Pinned source changed'});expect(await p).toBe(true);
        expect(h.context.pcBridgeUnavailable.value).toBe(true);expect(h.context.pcError.value).toBe('Pinned source changed');
    });
    it('a late refresh cannot replace a newer decision response',async()=>{
        const h=loadHarness(),p=h.load();h.reply(200,{...envelope(),revision:'source',decision_revision:4,viewer_session:{id:'viewer'}});await p;
        expect(h.context.pcBridgeEnvelope.value.decision_revision).toBe(5);
    });
    it('an obsolete object request cannot publish its failure into the new object',async()=>{
        const h=loadHarness(),p=h.load();h.context.pcContextEpoch++;h.reply(503,{detail:'Old failure'});await p;
        expect(h.context.pcBridgeUnavailable.value).toBe(false);expect(h.context.pcError.value).toBe('');
    });
});

describe('read-only production snapshot',()=>{
    it.each([false,undefined])('does not write without explicit decision capability: %s',async capability=>{
        const h=harness();h.context.pcEnvelope.value.capabilities={decisions:capability};
        await h.save();expect(h.calls).toHaveLength(0);expect(h.context.pcSaving.value).toBe(false);
    });
});

const objectsStart=app.indexOf('async function scLoadObjects()');
const objectsCode=app.slice(objectsStart,app.indexOf('function scBuildStageFolderCandidates',objectsStart));
describe('ordinary document loading across object switches',()=>{
    it.each([true,false])('ignores a stale ordinary object listing (HTTP success: %s)',async ok=>{
        let reply,refreshes=0;
        const context={pcContextEpoch:0,pcLoadBridge:async()=>false,
            scObjectsLoading:{value:false},scObjectsError:{value:''},scObjects:{value:['current']},
            fetch:()=>new Promise(resolve=>{reply=resolve;}),scRefreshSession:async()=>{refreshes++;}};
        vm.createContext(context);vm.runInContext(objectsCode,context);
        const pending=context.scLoadObjects();await new Promise(setImmediate);
        context.pcContextEpoch++;
        reply({ok,status:ok?200:503,json:async()=>({items:['stale'],detail:'Previous object failure'})});
        await pending;
        expect(context.scObjects.value).toEqual(['current']);
        expect(context.scObjectsError.value).toBe('');expect(refreshes).toBe(0);
    });
});
