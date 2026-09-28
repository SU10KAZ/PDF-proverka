import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import vm from 'node:vm';
const require=createRequire(import.meta.url), V=require('../static/js/project-change-view.js');
function harness() {
    let resolve;
    const events=[], requests=[];
    const ctx=vm.createContext({ProjectChangeView:V, fetch:(url, options)=>{
        requests.push({url,body:JSON.parse(options.body)});return new Promise(r=>{resolve=r;});
    }});
    for(const name of ['vue.global.prod.js','project-change-ui.js'])
        vm.runInContext(readFileSync(new URL('../static/js/'+name,import.meta.url),'utf8'),ctx);
    let component;ctx.ProjectChangeUI.register({component:(_,c)=>{component=c;}});
    const c={id:'v3:pair:run:PC-1',display_id:'PC-001',projectchange_id:'PC-1',session_id:'session',pair_id:'pair',source_run_id:'run',
        expert_review_available:true,expert_review:null,pair_ids:['pair'],conflicts:[],status:'REVIEW',evidence:[]};
    const props=ctx.Vue.reactive({changes:[c],pairs:[{id:'pair'}],selectedPairId:'pair',objectId:'object'});
    const view=component.setup(props,{emit:(...args)=>events.push(args)});
    return {props,c:props.changes[0],view,events,requests,tick:ctx.Vue.nextTick,
        reply:(ok=true,status=200)=>resolve({ok,status,json:async()=>ok?{items:[{...requests[0].body.updates[0],revision:1}]}:{detail:'Server error'}})};
}
describe('ProjectChange expert review',()=>{
    it('requires a rejection reason and sends exact run identity and revision',async()=>{
        const h=harness();h.view.setExpertDecision(h.c,'rejected');
        expect(h.view.expertInvalid.value).toBe(true);
        await h.view.saveExpertReview();expect(h.requests).toHaveLength(0);
        h.view.setExpertReason(h.c,'Нет изменения на плане');
        const promise=h.view.saveExpertReview();
        expect(h.requests[0]).toEqual({url:'/api/stage-comparison/objects/object/project-change-expert-review',body:{updates:[{
            session_id:'session',pair_id:'pair',run_id:'run',change_id:'PC-1',decision:'rejected',reason:'Нет изменения на плане',expected_revision:0}]}});
        expect(h.events).toHaveLength(0);h.reply();await promise;
        expect(h.events[0][0]).toBe('expert-saved');expect(h.view.expertPending.value).toHaveLength(0);
    });
    it('keeps failed edits available for retry without showing saved',async()=>{
        const h=harness();h.view.setExpertDecision(h.c,'accepted');
        let p=h.view.saveExpertReview();h.reply(false,503);await p;
        expect(h.view.expertPending.value).toHaveLength(1);expect(h.view.expertMessage.value).toBe('');
        expect(h.view.expertError.value).toBe('Server error');expect(h.events).toHaveLength(0);
        p=h.view.saveExpertReview();h.reply();await p;expect(h.events).toHaveLength(1);
    });
    it('clears a saved decision by clicking it again, preserving its revision',async()=>{
        const h=harness();h.c.expert_review={decision:'accepted',reason:'Проверено',revision:7};
        h.view.setExpertDecision(h.c,'accepted');const p=h.view.saveExpertReview();
        expect(h.requests[0].body.updates[0]).toMatchObject({decision:null,reason:'',expected_revision:7});h.reply();await p;
    });
    it('keeps the captured revision when a newer assessment arrives during editing',()=>{
        const h=harness();h.c.expert_review={decision:'accepted',reason:'',revision:7};
        h.view.setExpertDecision(h.c,'rejected');h.c.expert_review={decision:'accepted',reason:'Updated',revision:8};
        h.view.setExpertReason(h.c,'Причина');expect(h.view.expertValue(h.c).expected_revision).toBe(7);
    });
    it('late replies retain the original object and do not clear another object draft',async()=>{
        const h=harness();h.view.setExpertDecision(h.c,'accepted');const p=h.view.saveExpertReview();
        h.props.objectId='new-object';h.reply();await p;
        expect(h.events[0][1].objectId).toBe('object');expect(h.view.expertMessage.value).toBe('');
        expect(h.view.expertValue(h.c).decision).toBeNull();
    });
    it('does not offer expert writes for sealed read-only cards',()=>{
        const h=harness();h.c.expert_review_available=false;h.view.setExpertDecision(h.c,'accepted');
        expect(h.view.expertAvailable.value).toBe(false);expect(h.view.expertPending.value).toHaveLength(0);
    });
});

describe('saved expert assessment presentation',()=>{
    const change={id:'v3:pair:run:PC-1',candidate_version:'projectchange_v3',status:'REVIEW',expert_review_available:true,
        expert_review:{decision:'rejected',reason:'Не подтверждено',revision:1},conflicts:[]};
    it('renders saved decisions after reload and includes accepted changes in report',()=>{
        const envelope={schema_version:'project-change-view/1',object_id:'object',origin:'PRODUCTION',items:[change]};
        expect(V.fromEnvelope(envelope,'object')[0]).toMatchObject({status:'REJECTED',expert_review:change.expert_review});
        envelope.items=[{...change,expert_review:{decision:'accepted',revision:2}}];
        expect(V.report(V.fromEnvelope(envelope,'object'))).toHaveLength(1);
        envelope.items=[{...change,expert_review:{decision:null,revision:3}}];
        expect(V.fromEnvelope(envelope,'object')[0].status).toBe('REVIEW');
    });
    it('does not treat research cards as writable expert assessments',()=>{
        const envelope={schema_version:'project-change-view/1',object_id:'object',origin:'RESEARCH',items:[{...change,candidate_version:'snapshot'}]};
        expect(V.fromEnvelope(envelope,'object')[0]).toMatchObject({status:'REVIEW',expert_review_available:false,expert_review:null});
    });
});
