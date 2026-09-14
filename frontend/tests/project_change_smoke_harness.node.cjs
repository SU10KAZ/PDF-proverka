/* Run with node --test. Synthetic same-origin HTTP server, no production. */
const {test}=require('node:test');
const assert=require('node:assert/strict');
const http=require('node:http');
const {chromium}=require('playwright');
const {MUTATIONS,newAudit,createContext}=require('./project_change_smoke_harness.cjs');

test('global shell permits session writes and blocks only snapshot decisions',async()=>{
    const received=[];
    const server=http.createServer((req,res)=>{
        if(req.url.startsWith('/api/')){
            received.push({method:req.method,url:req.url});
            res.writeHead(200,{'Content-Type':'application/json'});res.end('{"id":"fixture-session"}');
        }else{res.writeHead(200,{'Content-Type':'text/html'});res.end('<!doctype html><title>Harness regression</title>');}
    });
    await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
    const base='http://127.0.0.1:'+server.address().port;
    const browser=await chromium.launch({headless:true,executablePath:process.env.CHROME_BIN||'/opt/google/chrome/chrome',args:['--no-sandbox']});
    const audit=newAudit();
    try{
        for(const [id,flag] of [['4f3e5916',''],['OTHER','?projectChangeUi=1']]){
            const c=await createContext(browser,{base,objectId:id,audit}),p=await c.newPage();
            assert.equal(p.url(),'about:blank');await p.evaluate(()=>0);assert.deepEqual(audit.errors,[]);
            await p.goto(base+'/'+flag);
            assert.equal(await p.evaluate(()=>sessionStorage.getItem('currentObjectId')),id);
            assert.equal(await p.evaluate(async()=> (await fetch('/api/stage-comparison/sessions',{method:'POST'})).status),200);
            await c.close();
        }
        const c=await createContext(browser,{base,objectId:'4f3e5916',audit}),p=await c.newPage();
        await p.goto(base+'/?projectChangeUi=1');
        for(const method of MUTATIONS){
            const forwarded=await p.evaluate(async method=>{
                try{await fetch('/api/project-change-preview/objects/4f3e5916/decisions',{method});return true;}catch{return false;}
            },method);
            assert.equal(forwarded,false);
        }
        assert.deepEqual(audit.writes.filter(r=>r.url.endsWith('/decisions')).map(r=>r.method),MUTATIONS);
        assert.equal(received.length,2,'No synthetic preview mutation may reach the server');
        // Changing the actual selected object changes classification, despite this
        // context having originally been created for the canonical preview object.
        await p.evaluate(()=>sessionStorage.setItem('currentObjectId','OTHER'));
        assert.equal(await p.evaluate(async()=> (await fetch('/api/stage-comparison/sessions',{method:'POST'})).status),200);
        assert.equal(received.length,3);assert.equal(audit.legacyWrites.length,2);
        assert(audit.legacyWrites.every(r=>r.status===200));
        assert.deepEqual(audit.errors,[]);assert.deepEqual(audit.harnessErrors,[]);
        // Real page exceptions are observed, including in a second page.
        const second=await c.newPage();await second.goto(base+'/');
        const error=second.waitForEvent('pageerror');
        await second.evaluate(()=>setTimeout(()=>{throw new Error('intentional regression sentinel');},0));
        await error;assert.equal(audit.errors.length,1);
        assert.match(audit.errors[0].message,/intentional regression sentinel/);
        await c.close();
    }finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
});
