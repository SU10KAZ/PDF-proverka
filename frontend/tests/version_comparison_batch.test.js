import {describe, it, expect, vi, afterEach} from 'vitest';
import {readFileSync} from 'node:fs';
import {randomUUID} from 'node:crypto';
import vm from 'node:vm';

const source=readFileSync(new URL('../static/js/version-comparison-batch.js',import.meta.url),'utf8');

function setup(extra={}) {
    let definition;
    const mounted=[],unmounted=[];
    const sandbox={crypto:{randomUUID},setTimeout,clearTimeout,FormData:class{append(){}},
        addEventListener(){},removeEventListener(){},confirm:()=>true,
        Vue:{ref:value=>({value}),reactive:value=>value,onMounted:fn=>mounted.push(fn),onBeforeUnmount:fn=>unmounted.push(fn)},
        VersionComparison:{API:'/api/version-comparison',api:async()=>({batches:[]})},...extra};
    vm.runInNewContext(source,sandbox);
    sandbox.VersionComparisonBatch.register({component:(name,def)=>definition=def});
    const events=[];
    return {sandbox,helpers:sandbox.VersionComparisonBatch,definition,mounted,unmounted,
        state:definition.setup({objectId:'obj'},{emit:event=>events.push(event)}),events};
}
const file=(path,size=10,mtime=1)=>({name:path.split('/').at(-1),webkitRelativePath:path,size,lastModified:mtime,
    slice:(start,end)=>({size:end-start})});
const pick=(state,files,parent=false)=>state.pick({target:{files,value:'selected'}},parent);

describe('folder batches',()=>{
    afterEach(()=>vi.useRealTimers());
    it('refreshes an existing project when its series or queue state changes, not on every progress tick',async()=>{
        vi.useFakeTimers();
        const {sandbox,mounted,unmounted,events}=setup();
        const item={project_id:'project',published:true,series_id:'old',state:'done'};
        sandbox.VersionComparison.api=async()=>({batches:[{items:[{...item}]}]});
        mounted.forEach(fn=>fn());
        await vi.advanceTimersByTimeAsync(0);
        expect(events).toEqual(['updated']);
        await vi.advanceTimersByTimeAsync(2500);
        expect(events).toHaveLength(1);
        item.series_id='new'; item.state='comparing';
        await vi.advanceTimersByTimeAsync(2500);
        expect(events).toHaveLength(2);
        item.compare_progress={done:1,total:6};
        await vi.advanceTimersByTimeAsync(2500);
        expect(events).toHaveLength(2);
        item.state='done';
        await vi.advanceTimersByTimeAsync(2500);
        expect(events).toHaveLength(3);
        unmounted.forEach(fn=>fn());
    });

    it('splits a parent directory into projects and preserves nested paths',()=>{
        const {helpers}=setup();
        const groups=helpers.groupFiles([file('Root/A/nested/v2.pdf'),file('Root/A/v1.PDF'),file('Root/B/v1.pdf'),
            file('Root/loose.pdf'),file('Root/B/notes.txt')],true);
        expect(groups.map(g=>g.source_root)).toEqual(['A','B']);
        expect(groups[0].files.map(f=>f.relative_path)).toEqual(['nested/v2.pdf','v1.PDF']);
        expect(groups[1].files).toHaveLength(1);
    });

    it('appends separate selections and ignores repeated selection of the same folder',async()=>{
        const {state}=setup();
        await pick(state,[file('A/v1.pdf')]);
        await pick(state,[file('B/v1.pdf')]);
        await pick(state,[file('A/v1.pdf')]);
        expect(state.groups.value.map(g=>g.title)).toEqual(['A','B']);
        await pick(state,[file('A/v1.pdf',11)]);
        expect(state.groups.value).toHaveLength(3);
        expect(new Set(state.groups.value.map(g=>g.item_id)).size).toBe(3);
    });

    it('reads every directory-reader chunk, including more than 100 files',async()=>{
        const {helpers}=setup();
        const chunks=[Array.from({length:100},(_,i)=>({isFile:true,name:`v${i}.pdf`,file:ok=>ok(file(`v${i}.pdf`))})),
            [{isDirectory:true,name:'nested',createReader:()=>{let n=0;return {readEntries:ok=>ok(n++ ? [] : [{isFile:true,name:'last.pdf',file:ok=>ok(file('last.pdf'))}])};}}],[]];
        const entry={isDirectory:true,name:'Project',createReader:()=>({readEntries:ok=>ok(chunks.shift())})};
        const files=await helpers.readEntry(entry);
        expect(files).toHaveLength(101);
        expect(files.at(-1).path).toBe('Project/nested/last.pdf');
    });

    it('captures drop entries before async traversal and keeps folder boundaries',async()=>{
        const {state}=setup();
        let captured=0;
        const entry=name=>({webkitGetAsEntry(){captured++;return {isDirectory:true,name,createReader:()=>{let n=0;return {
            readEntries:ok=>{expect(captured).toBe(2);ok(n++ ? [] : [{isFile:true,name:'v.pdf',file:ok=>ok(file('v.pdf'))}]);}};}};}});
        await state.drop({dataTransfer:{items:[entry('A'),entry('B')]}});
        expect(state.groups.value.map(g=>g.title)).toEqual(['A','B']);
    });

    it.each([true, false])('uploads selected projects and starts after every file succeeds, admin=%s',async(canOperate)=>{
        const calls=[];
        class XHR {
            upload={}; status=200; responseText=JSON.stringify({received:true});
            open(method,url){this.url=url;}
            send(){calls.push(['file',this.url]); this.onload();}
        }
        const {state,sandbox}=setup({XMLHttpRequest:XHR, portalCan: {
            operate: () => canOperate, uploadProjects: section => section === 'version_comparison',
        }});
        sandbox.VersionComparison.api=async(path,options)=>{
            calls.push([options?.method,path,options?.body]);
            if(options?.body?.items) return {...options.body,started:false,created_at:'2026-10-15T00:00:00Z',
                items:options.body.items.map(i=>({...i,state:'uploading',files:i.files.map(f=>({...f,received:false}))}))};
            return {...state.current.value,started:true};
        };
        await pick(state,[file('Root/A/v.pdf'),file('Root/B/v.pdf'),file('Root/C/v.pdf')],true);
        state.groups.value[1].selected=false;
        state.compare.value=true;
        state.move(state.groups.value,2,-2);
        await state.send();
        expect(state.error.value).toBe('');
        expect(calls[0][2].items.map(i=>i.source_root)).toEqual(['C','A']);
        expect(calls[0][2].compare_neighbors).toBe(canOperate);
        expect(calls.filter(c=>c[0]==='file')).toHaveLength(2);
        expect(calls.at(-1)[1]).toMatch(/\/start$/);
        expect(state.current.value.started).toBe(true);
    });

    it('does not start processing after a failed file and resumes missing files',async()=>{
        const calls=[]; let fail=true, stored;
        class XHR {
            upload={};status=200;responseText=JSON.stringify({received:true});
            open(method,url){this.url=url;}
            send(){calls.push(this.url); if(fail && calls.length===2) this.onerror(); else this.onload();}
        }
        const {state,sandbox}=setup({XMLHttpRequest:XHR});
        sandbox.VersionComparison.api=async(path,options)=>{
            if(options?.body?.items) {stored={...options.body,started:false,created_at:'2026-10-15T00:00:00Z',
                items:options.body.items.map(i=>({...i,state:'uploading',files:i.files.map(f=>({...f,received:false}))}))}; return stored;}
            if(path.endsWith('/start')) {stored.started=true;return stored;} return stored;
        };
        await pick(state,[file('A/v1.pdf'),file('A/v2.pdf')]);
        await state.send();
        expect(state.error.value).toMatch(/прервана/);
        expect(stored.started).toBe(false);
        expect(stored.items[0].files[0].received).toBe(true);
        fail=false;
        await state.send();
        expect(calls).toHaveLength(3);
        expect(stored.started).toBe(true);
    });

    it('retries the same manifest when the create response was lost',async()=>{
        const seen=[];
        const {state,sandbox}=setup();
        sandbox.VersionComparison.api=async(path,options)=>{seen.push(options.body);throw new Error('offline');};
        await pick(state,[file('A/v1.pdf')]);
        await state.send(); await state.send();
        expect(seen).toHaveLength(2);
        expect(seen[0].batch_id).toBe(seen[1].batch_id);
    });

    it('matches reselected folders by source name and file metadata after reload',async()=>{
        const requests=[];
        class XHR {
            upload={};status=200;responseText=JSON.stringify({received:true});
            open(method,url){requests.push(url);}
            send(){this.onload();}
        }
        const {state,sandbox}=setup({XMLHttpRequest:XHR});
        const b={batch_id:'existing',created_at:'2026-10-15T00:00:00Z',started:false,
            items:[{item_id:'i1',title:'Renamed project',source_root:'Original',state:'uploading',
                files:[{file_id:'f1',relative_path:'v.pdf',size:10,last_modified:1,received:false}]}]};
        state.saved.value=[b];state.selectedId.value='existing';state.openSaved();
        sandbox.VersionComparison.api=async path=>path.endsWith('/start') ? {...b,started:true} : b;
        await pick(state,[file('Original/v.pdf')]);
        await state.send();
        expect(state.error.value).toBe('');
        expect(requests).toEqual(['/api/version-comparison/objects/obj/batches/existing/items/i1/files/f1/chunks/0']);
    });

    it('offers stamp labels, preserves edits on refresh and submits the displayed labels',async()=>{
        const {state,sandbox}=setup();
        const b={batch_id:'existing',started:true,items:[{item_id:'i1',state:'review',files:[
            {file_id:'f1',file_name:'v1.pdf',suggested_label:'V1 Изм.2 09.07.2025'},
            {file_id:'f2',file_name:'v2.pdf',suggested_label:'V2 10.07.2025',label:'Моя версия'},
            {file_id:'f3',file_name:'unparsed.pdf'},
        ]}]};
        state.saved.value=[b];state.selectedId.value='existing';state.openSaved();
        expect(state.drafts.i1.map(f=>f.label)).toEqual(['V1 Изм.2 09.07.2025','Моя версия','unparsed.pdf']);
        state.drafts.i1[0].label='Уточнённая версия';
        state.openSaved();
        const calls=[];
        sandbox.VersionComparison.api=async(path,options)=>{calls.push(options.body);return b;};
        await state.confirmOne(b.items[0]);
        expect(calls[0].files).toEqual([
            {file_id:'f1',label:'Уточнённая версия'}, {file_id:'f2',label:'Моя версия'},
            {file_id:'f3',label:'unparsed.pdf'},
        ]);
    });

    it('splits a 264 MB file and resumes only missing chunks',async()=>{
        const requests=[];let fail=true;
        const size=263527206,chunkSize=8*1024*1024, chunks={};
        class XHR {
            upload={};status=200;
            open(method,url){this.index=Number(url.split('/').at(-1));}
            send(){
                requests.push(this.index);
                if(this.index===2 && fail) {this.onerror();return;}
                chunks[this.index]={size:Math.min(chunkSize,size-this.index*chunkSize)};
                this.responseText=JSON.stringify({chunks,received:Object.keys(chunks).length===Math.ceil(size/chunkSize)});
                this.onload();
            }
        }
        const {state,sandbox}=setup({XMLHttpRequest:XHR});
        let batch;
        sandbox.VersionComparison.api=async(path,options)=>{
            if(options?.body?.items) batch={...options.body,chunk_size:chunkSize,started:false,created_at:'2026-10-15T00:00:00Z',
                items:options.body.items.map(i=>({...i,state:'uploading',files:i.files.map(f=>({...f,received:false}))}))};
            if(path.endsWith('/start')) batch.started=true;
            return batch;
        };
        await pick(state,[file('A/large.pdf',size)]);
        await state.send();
        expect(batch.started).toBe(false);
        expect(requests).toEqual([0,1,2]);
        fail=false;
        await state.send();
        expect(batch.started).toBe(true);
        expect(requests.slice(3)).toEqual(Array.from({length:30},(_,i)=>i+2));
        expect(state.error.value).toBe('');
    });

    it.each(['stop','timeout'])('releases a stuck upload after %s and allows removal and a fresh upload',async mode=>{
        vi.useFakeTimers();
        let request;
        class XHR {
            upload={};open(){} send(){request=this;} abort(){this.onabort();}
        }
        const {state,sandbox}=setup({XMLHttpRequest:XHR});
        const calls=[];
        sandbox.VersionComparison.api=async(path,options)=>{
            calls.push([path,options?.method]);
            if(options?.method==='DELETE') return {deleted:true};
            return {...options.body,started:false,created_at:'2026-10-15T00:00:00Z',
                items:options.body.items.map(i=>({...i,state:'uploading',files:i.files.map(f=>({...f,received:false}))}))};
        };
        await pick(state,[file('A/v.pdf')]);
        const uploading=state.send();
        await vi.advanceTimersByTimeAsync(0);
        expect(request).toBeTruthy();
        expect(state.busy.value).toBe(true);
        if(mode==='stop') state.stopUpload(); else await vi.advanceTimersByTimeAsync(120000);
        await uploading;
        expect(state.busy.value).toBe(false);
        expect(state.error.value).toMatch(mode==='stop' ? /остановлена/ : /двух минут/);
        expect(calls.some(([path])=>path.endsWith('/start'))).toBe(false);
        const old=state.current.value.batch_id;
        state.saved.value=[state.current.value];
        await state.removeBatch();
        expect(calls.at(-1)).toEqual(['/objects/obj/batches/'+old,'DELETE']);
        expect(state.current.value).toBe(null);
        expect(state.saved.value).toEqual([]);
        expect(state.selectedId.value).toBe('');
        state.openNew();
        expect(state.open.value).toBe(true);
        expect(state.locked.value).toBe(false);
    });
});
