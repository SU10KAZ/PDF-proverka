import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const start = app.indexOf('async function scSaveDocumentPairing()');
const source = app.slice(start, app.indexOf('async function scAutoMatchDocumentProjects()', start));
function harness() {
    let resolve;
    const calls = [];
    const context = {
        scSession: {value: {id:'old-session'}}, scPairingSaving: {value:false}, scPairingMatching:{value:false},
        scPairingSaveError:{value:''}, scPairingSaveMessage:{value:''}, scPairingDirty:{value:true},
        payload:{left_order:['a'],right_order:['b']},
        scPersistDocumentOrder:()=>{},
        fetch: (url, options) => { calls.push({url,options}); return new Promise(r=>{resolve=r;}); },
    };
    context.scDocumentPairingPayload = () => context.payload;
    vm.createContext(context); vm.runInContext(source,context);
    return {context, calls, save:()=>context.scSaveDocumentPairing(),
        reply: (ok=true) => resolve({ok,status:ok?200:503,json:async()=>ok?{version:1}:{detail:'Unavailable'}})};
}
describe('ProjectChange upload uses safe existing autosave',()=>{
    it('saves a snapshot and exposes saved state only after success',async()=>{
        const h=harness(), promise=h.save();
        expect(h.context.scPairingSaving.value).toBe(true);
        expect(h.calls[0].url).toContain('/old-session/document-pairing');
        expect(JSON.parse(h.calls[0].options.body)).toEqual(h.context.payload);
        h.reply(); await promise;
        expect(h.context.scPairingDirty.value).toBe(false);
        expect(h.context.scSession.value.document_pairing.version).toBe(1);
    });
    it('does not apply a late save to another session',async()=>{
        const h=harness(), promise=h.save();
        h.context.scSession.value={id:'new-session'}; h.reply(); await promise;
        expect(h.context.scSession.value).toEqual({id:'new-session'});
        expect(h.context.scPairingSaveMessage.value).toBe('');
    });
    it('keeps edits made during the save dirty for the next autosave',async()=>{
        const h=harness(), promise=h.save();
        h.context.payload={left_order:['c'],right_order:['b']}; h.reply(); await promise;
        expect(h.context.scPairingDirty.value).toBe(true);
    });
    it('exposes a failed save and allows a retry',async()=>{
        const h=harness(), promise=h.save(); h.reply(false); await promise;
        expect(h.context.scPairingSaveError.value).toBe('Unavailable');
        expect(h.context.scPairingDirty.value).toBe(true);
        expect(h.context.scPairingSaving.value).toBe(false);
    });
});
