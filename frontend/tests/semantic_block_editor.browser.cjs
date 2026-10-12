// Real Vue + store + HTTP history on loopback; synthetic documents, no production writes/model calls.
const {test}=require('node:test'),assert=require('node:assert/strict'),http=require('node:http');
const fs=require('node:fs'),path=require('node:path');
const {chromium}=require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const root=path.resolve(__dirname,'..'),RUN='a'.repeat(32);

test('edit semantic links on sheets, persist decisions, reassign, recover rejection and keep scope',async()=>{
    const history={reviews:[],edits:[]},posts=[],errors=[];
    let writes=true,fail=false;
    const members={OLD:[{id:'o1',page:1,type:'TEXT'},{id:'o2',page:1,type:'TEXT'}],
        NEW:[{id:'n1',page:1,type:'TEXT'},{id:'n2',page:1,type:'TEXT'}]};
    const region={id:'R',domain:'Тестовая тема',members,pages:{OLD:[1],NEW:[1]},
        member_cardinality:'N:N',mapping_state:'MAPPED',membership_state:{OLD:'MAPPED',NEW:'MAPPED'},
        allowed:{OLD:['o1','o2'],NEW:['n1','n2']}};
    const server=http.createServer(async(req,res)=>{
        const url=new URL(req.url,'http://localhost');
        const json=value=>{res.setHeader('Content-Type','application/json');res.end(JSON.stringify(value));};
        if(req.url.startsWith('/static/')){
            res.setHeader('Content-Type',req.url.endsWith('.css')?'text/css':'text/javascript');
            return res.end(fs.readFileSync(path.join(root,req.url)));
        }
        if(req.method==='POST'){
            let raw='';for await(const chunk of req)raw+=chunk;
            const body=JSON.parse(raw);posts.push({url:url.pathname,query:Object.fromEntries(url.searchParams),body});
            if(fail){fail=false;res.statusCode=503;return json({detail:'retry'});}
            const row={...body,object_id:'object',pair_key:'pair',comparison_id:'pair',run_id:RUN,
                timestamp:new Date(Date.now()+posts.length*1000).toISOString()};
            if(body.status){row.review_id='review'+posts.length;history.reviews.push(row);}
            else{row.event_id='event'+posts.length;history.edits.push(row);}
            return json(row);
        }
        if(url.pathname.endsWith('/status'))return json({object_id:'object',session_id:'session',pair_id:'pair',
            current_run_id:RUN,capabilities:{block_mapping_writes:writes},snapshots:[],source_blocks:{OLD:{},NEW:{}},
            runs:[{run_id:RUN,state:'COMPLETED_FROZEN',frozen:true,is_current:true,hm_available:true,pdf_match:true,
                writable:writes,write_block_reason:writes?null:'WRITES_DISABLED',write_block_reasons:writes?[]:['WRITES_DISABLED'],
                history:{reviews:history.reviews.length,block_link_events:history.edits.length,poisoned:false}}]});
        if(url.pathname.endsWith('/region-index'))return json({regions:[region]});
        if(url.pathname.endsWith('/reviews'))return json(history.reviews);
        if(url.pathname.endsWith('/block-links'))return json(history.edits);
        if(url.pathname.endsWith('/page-blocks'))return json({pages:[{physical_page:1,available:true,geometry:{width_px:1000,height_px:1400},
            blocks:members[url.searchParams.get('side')].map((b,i)=>({block_id:b.id,bbox:[.1,.1+i*.45,.85,.4+i*.45]}))}]});
        if(url.pathname.endsWith('/page-preview')){
            res.setHeader('Content-Type','image/svg+xml');return res.end('<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="1400"><rect width="1000" height="1400" fill="white"/><text x="120" y="250" font-size="40">Synthetic sheet</text></svg>');
        }
        if(url.pathname!=='/'){res.statusCode=404;return json({});}
        res.setHeader('Content-Type','text/html; charset=utf-8');res.end(`<!doctype html><html><link rel="stylesheet" href="/static/css/styles.css"><link rel="stylesheet" href="/static/css/stage-block-mapping.css">
        <style>#app{height:95vh;display:flex;flex-direction:column}.sbm-simple{border:1px solid #ccc}</style>
        <div id="app"><semantic-block-comparison v-if="ready" :store="store" /></div>
        <script src="/static/js/vue.global.prod.js"></script><script src="/static/js/human-mapping-core.js"></script><script src="/static/js/stage-block-mapping.js"></script>
        <script>const store=Vue.markRaw(StageBlockMapping.createStore({Vue,fetch:window.fetch.bind(window)}));
        window.store=store;const ready=Vue.ref(false);const app=Vue.createApp({setup:()=>({store,ready})});StageBlockMapping.register(app);app.mount('#app');
        store.load({sessionId:'session',pairId:'pair'}).then(()=>{ready.value=true;});</script></html>`);
    });
    await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
    const browser=await chromium.launch({headless:true,executablePath:'/opt/google/chrome/chrome',args:['--no-sandbox']});
    try{
        const page=await browser.newPage({viewport:{width:1440,height:1050}});page.setDefaultTimeout(7000);
        const base='http://127.0.0.1:'+server.address().port;
        await page.route('**/*',route=>new URL(route.request().url()).origin===base?route.continue():route.abort());
        page.on('pageerror',e=>errors.push(e.message));
        const button=name=>page.locator('[data-sbm-write="'+name+'"]');
        const block=id=>page.locator('[data-block="'+id+'"]');
        const pick=async id=>block(id).click({position:{x:25,y:25}});
        const idle=()=>page.waitForFunction(()=>!store.state.edit.busy);
        const choose=async(o,n)=>{await pick(o);await pick(n);};
        const selectLink=async key=>{
            // Explanation links share the same public handler as the actual SVG line; keyboard operates SVG hit target.
            const line=page.locator('[data-link="'+key+'"]');await line.focus();await line.press('Enter');
            await button('link-confirm').waitFor();
        };
        const decision=async kind=>{await button(kind).click();await button('dialog-commit').click();await idle();};
        await page.goto(base);await block('o1').waitFor();
        await button('edit-toggle').click();
        await choose('o1','n1');assert.equal(await page.locator('.is-selected[data-block]').count(),2);
        await button('connect-one').click();await idle();assert.equal(history.edits.length,1);
        assert.equal(history.edits[0].event_type,'ADD_BLOCK_LINK');
        await selectLink('o1|n1');await decision('link-confirm');assert.equal(history.reviews.at(-1).status,'HUMAN_CONFIRMED');
        assert(await page.locator('.sbm-simple__editor').innerText().then(t=>t.includes('История решений (2)')));
        // Uncertain and rejected persist; a rejected line is recoverable from the local list.
        await selectLink('o1|n1');await decision('link-uncertain');assert.equal(history.reviews.at(-1).status,'HUMAN_UNCERTAIN');
        await selectLink('o1|n1');await decision('link-reject');assert.equal(history.reviews.at(-1).status,'HUMAN_REJECTED');
        await page.getByText('Отклонённые и противоречивые связи (1)',{exact:true}).click();
        await page.locator('.sbm-simple__editor details button').first().click();
        await decision('link-confirm');assert.equal(history.reviews.at(-1).status,'HUMAN_CONFIRMED');
        // Replace both ends through sheet clicks; previous review does not bless a new edge.
        await selectLink('o1|n1');await button('reassign-new').click();await pick('n2');await idle();
        assert.equal(history.edits.at(-1).event_type,'REASSIGN_BLOCK_LINK');assert.equal(history.edits.at(-1).new_block_id,'n2');
        await selectLink('o1|n2');await button('reassign-old').click();await pick('o2');await idle();
        assert.equal(history.edits.at(-1).old_block_id,'o2');
        await selectLink('o2|n2');await button('delete').click();await idle();assert.equal(history.edits.at(-1).event_type,'DELETE_BLOCK_LINK');
        assert.equal(await page.locator('[data-link="o2|n2"]').count(),0);
        // Failed save is visible, retry persists once; cancellation writes nothing.
        await choose('o1','n1');fail=true;await button('connect-one').click();await idle();
        assert(await page.locator('.sbm-write-status.is-error').count());
        await page.locator('.sbm-write-status button').click();await idle();
        await selectLink('o1|n1');let count=posts.length;await button('link-confirm').click();await button('dialog-cancel').click();
        assert.equal(posts.length,count);
        await page.screenshot({path:'/tmp/semantic-block-editor.png'});
        await page.reload();await block('o1').waitFor();await button('edit-toggle').click();
        assert.equal(await page.locator('[data-link="o1|n1"]').count(),1);
        await selectLink('o1|n1');assert(await button('delete').isEnabled());
        assert(posts.every(p=>p.query.session_id==='session' && p.query.run_id===RUN));
        assert(posts.every(p=>p.url.startsWith('/api/human-mapping/objects/object/comparisons/pair/')));
        writes=false;await page.reload();await block('o1').waitFor();assert(await button('edit-toggle').isDisabled());
        assert.equal(errors.length,0,errors.join('\n'));
    }finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
});
