/* Local acceptance: start scripts/project_change_ui_smoke_server.py.
   Authorized production: SMOKE_PRODUCTION=1, SMOKE_BASE and SMOKE_AUTH_FILE.
   Production uses the real portal/API, with no response fixtures. */
const {chromium}=require('playwright');
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {MUTATIONS,newAudit,createContext}=require('./project_change_smoke_harness.cjs');
const base=process.env.SMOKE_BASE||'http://127.0.0.1:8991';
const production=process.env.SMOKE_PRODUCTION==='1';
assert(['127.0.0.1','localhost'].includes(new URL(base).hostname));
assert(production ? base==='http://127.0.0.1:8081' && process.env.SMOKE_AUTH_FILE : new URL(base).port!=='8081',
    'Production smoke requires explicit mode and authentication file');
const out=process.env.SMOKE_OUTPUT||'/tmp/project-change-ui-v2-1-review';fs.mkdirSync(out,{recursive:true});
const object='4f3e5916',api=base+'/api/project-change-preview/objects/'+object;
const checks=[],audit=newAudit(),{errors,writes,failed,requests}=audit;let browser,page;
function save(status,error){fs.writeFileSync(path.join(out,'browser-results.json'),JSON.stringify({status,error,base,
    preview_api:'real frozen production router',portal_shell:production?'real production':'isolated fixture',
    backend_mocking:!production,checks,...audit,
    preview_mutations:Object.fromEntries(MUTATIONS.map(m=>[m,writes.filter(r=>r.method===m).length])),
    preview_legacy_session_requests:requests.filter(r=>new URL(r.url).pathname.startsWith('/api/stage-comparison/sessions')),
},null,2));}
async function check(name,fn){await fn();checks.push({name,status:'PASS'});console.log('PASS '+name);save('RUNNING');}
const shot=async name=>{await page.evaluate(()=>document.fonts.ready);await page.screenshot({path:path.join(out,name+'.png'),animations:'disabled'});};
const rows=()=>page.locator('.pc-table > tbody > .pc-row');
const row=c=>page.locator('#pc-'+c.id);
const tab=n=>page.getByRole('button',{name:n,exact:true}).click();
async function count(n){await page.waitForFunction(n=>document.querySelectorAll('.pc-table > tbody > .pc-row').length===n,n);}
async function newContext(id){
    const cookies=production?JSON.parse(fs.readFileSync(process.env.SMOKE_AUTH_FILE)).cookies:[];
    return createContext(browser,{base,objectId:id,cookies,audit});
}
(async()=>{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROME_BIN||'/opt/google/chrome/chrome',args:['--no-sandbox']});
    const c=await newContext(object);page=await c.newPage();
    await page.goto(base+'/?projectChangeUi=1#/stage-comparison');await page.locator('.sc-pair-board__row').first().waitFor();
    await check('canonical object plus flag opens read-only preview Page 1',async()=>{
        assert.equal(await page.evaluate(()=>sessionStorage.getItem('currentObjectId')),object);
        assert((await page.locator('.pc-preview-label').innerText()).includes('Исследовательский предпросмотр'));
        assert(await page.getByRole('button',{name:'Запустить анализ проекта',exact:true}).isDisabled());
    });
    const env=await(await page.request.get(api+'?projectChangeUi=1')).json();
    const pairs=env.viewer_session.pairs;
    const pairChanges=id=>env.items.filter(c=>c.evidence.some(e=>e.pair_id===id));
    async function openPair(id){
        const pair=pairs.find(p=>p.id===id);assert(pair);
        await tab('1. Загрузка документации');
        await page.locator('.sc-pair-board__row')
            .filter({has:page.getByText(pair.left.filename,{exact:true})})
            .filter({has:page.getByText(pair.right.filename,{exact:true})})
            .getByRole('button',{name:'Открыть',exact:true}).click();
        await page.locator('.sc-vector-pane-head__document').first().waitFor();
        await page.waitForFunction(id=>{
            const imgs=[...document.querySelectorAll('.sc-page-preview')];
            return imgs.length===2&&imgs.every(i=>i.naturalWidth>0&&i.src.includes('/viewer/pairs/'+id+'/'));
        },id);
        const names=await page.locator('.sc-vector-pane-head__document').allTextContents();
        assert(names.some(n=>n.includes(pair.left.filename)));assert(names.some(n=>n.includes(pair.right.filename)));
    }
    async function scopedRows(id){
        const expected=pairChanges(id);await count(expected.length);
        assert.deepEqual(await rows().evaluateAll(x=>x.map(e=>e.dataset.productionTargetId)),expected.map(x=>x.id));
        assert.deepEqual(await page.locator('.pc-summary b').allTextContents(),[
            String(expected.length),String(expected.filter(c=>c.status==='REVIEW').length),
            String(expected.filter(c=>c.conflicts.some(x=>!x.resolved)).length),String(expected.filter(c=>c.importance==='HIGH').length)]);
    }
    await check('Page 1 removes service text and visibly connects OLD to NEW with compact layout',async()=>{
        assert.equal(await page.locator('.pc-upload-save').count(),0);
        const text=await page.locator('body').innerText();
        for(const t of ['RESEARCH / PREVIEW','закреплённые пары документов','Доступен просмотр исходных PDF.'])assert(!text.includes(t));
        const layout=await page.locator('.sc-pair-board__row').evaluateAll(rows=>rows.map(r=>{
            const [old,newer]=[...r.querySelectorAll('.sc-pair-document')].map(e=>e.getBoundingClientRect());
            const arrow=r.querySelector('.sc-pair-board__arrow').getBoundingClientRect();
            return {height:r.getBoundingClientRect().height,gap:newer.left-old.right,oldWidth:old.width,newWidth:newer.width,
                arrowBetween:arrow.left>=old.right-1&&arrow.right<=newer.left+1,
                actionsRight:r.querySelector('.sc-pair-row-actions').getBoundingClientRect().left>=newer.right,
                names:[...r.querySelectorAll('.sc-pair-document__name')].map(e=>({text:e.textContent,title:e.title,width:e.clientWidth,fullWidth:e.scrollWidth}))};
        }));
        assert.equal(layout.length,13);
        for(const r of layout){
            assert(r.height<=53);assert(r.gap<20);assert(r.oldWidth<=420);assert(r.newWidth<=420);
            assert(r.arrowBetween&&r.actionsRight);assert(r.names.every(n=>n.text===n.title&&n.width>=n.fullWidth&&n.width>200));
        }
        assert((await page.locator('.sc-pair-board__arrow').allTextContents()).every(t=>t==='→'));
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
        fs.writeFileSync(path.join(out,'page1-layout.json'),JSON.stringify(layout,null,2));
    });await shot('A-page1-compact-pairs');
    await check('Page 1 stays compact at 1280px and retains full filename tooltips',async()=>{
        await page.setViewportSize({width:1280,height:1000});
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
        assert(await page.locator('.sc-pair-board__row').evaluateAll(rows=>rows.every(r=>
            r.getBoundingClientRect().height<=53&&[...r.querySelectorAll('.sc-pair-document__name')].every(e=>e.clientWidth>170&&e.title===e.textContent))));
        await page.setViewportSize({width:1600,height:1100});
    });
    const single=pairs.find(p=>pairChanges(p.id).length===1);assert(single);
    await check('Page 1 → Page 2 → Page 3 shows only the selected one-change pair',async()=>{
        await openPair(single.id);await tab('3. Изменения проекта');await scopedRows(single.id);
        assert.equal(await rows().count(),1);
    });await shot('B-page3-one-change');
    await check('Page 3 has no pair selector, all-pairs mode, filters or service banner',async()=>{
        const workspace=page.locator('.pc-workspace');
        assert.equal(await workspace.locator('select,.pc-pair-toolbar,.pc-filters,.pc-scope-switch,.pc-notice').count(),0);
        for(const name of ['Все пары объекта','Текущая пара','Сбросить'])assert(!(await workspace.innerText()).includes(name));
        for(const p of pairs)for(const side of ['left','right'])assert(!(await workspace.innerText()).includes(p[side].filename));
        assert.deepEqual(await workspace.locator('thead th').allTextContents(),['ID','Важность','Раздел / шифр','Система','Тип','Изменение','OLD → NEW','Источник','Статус','Подробности']);
        const heading=await workspace.locator('h2').boundingBox(),table=await workspace.locator('.pc-table-scroll').boundingBox();
        assert(table.y-heading.y<75);
    });

    const water=env.items.filter(c=>c.cipher==='ИОС2.1'),heat=env.items.filter(c=>c.cipher==='ИОС4.1');
    const waterId=water[0].evidence[0].pair_id,heatId=heat[0].evidence[0].pair_id;
    await check('Page 1 → Page 2 → Page 3 inherits canonical water pair',async()=>{
        await openPair(waterId);
        await page.waitForFunction(()=>[...document.querySelectorAll('.sc-page-preview')].length===2&&[...document.querySelectorAll('.sc-page-preview')].every(i=>i.naturalWidth>0));
        assert((await page.locator('.sc-vector-pane-head__document').first().innerText()).includes('ИОС-2.1'));await shot('page2');
        await tab('3. Изменения проекта');await count(10);
        await scopedRows(waterId);
        assert.deepEqual(await rows().evaluateAll(x=>x.map(e=>e.dataset.productionTargetId)),water.map(x=>x.id));
        assert.equal(await page.locator('.pc-crop img').count(),0);
        assert.equal(requests.filter(r=>r.url.endsWith('/crop')).length,0);
    });await shot('C-page3-multiple-changes');
    const textBoth=water.filter(x=>x.evidence.some(e=>e.side==='OLD')).sort((a,b)=>a.evidence.length-b.evidence.length)[0];
    await check('expand one ProjectChange: OLD/NEW states and evidence, human controls disabled',async()=>{
        await row(textBoth).click();await page.locator('.pc-expanded-row').waitFor();
        assert.equal(await page.locator('.pc-expanded-row').count(),1);
        assert.equal(await rows().count(),10);
        assert.equal(await page.locator('.pc-states p').nth(0).innerText(),textBoth.old_state);
        assert.equal(await page.locator('.pc-states p').nth(1).innerText(),textBoth.new_state);
        for(const side of ['OLD','NEW']){
            const img=page.locator(`[aria-label="${side} доказательства"] img`).first();
            await img.scrollIntoViewIfNeeded();await img.evaluate(i=>i.decode());
        }
        for(const label of ['Подтвердить','Не изменение','Не могу определить','Проблема'])assert(await page.getByRole('button',{name:label,exact:true}).isDisabled());
        assert((await page.locator('.pc-review').innerText()).includes('Подтверждается ли это изменение по исходным документам?'));
        await page.locator('.pc-details > summary').click();
        assert.deepEqual(await page.locator('.pc-details tbody tr').evaluateAll(rows=>rows.map(r=>[...r.querySelectorAll('td')].map(e=>e.textContent))),textBoth.details.map(d=>[d.label,d.old||'—',d.new||'—']));
        await page.locator('.pc-details > summary').click();

    });await page.locator('.pc-table-scroll').evaluate(s=>{s.scrollTop+=s.querySelector('.pc-expanded-row').getBoundingClientRect().top-s.getBoundingClientRect().top-s.querySelector('thead').offsetHeight-s.querySelector('.pc-row.is-expanded').offsetHeight-5;});await shot('D-expanded-old-new');
    await check('collapse removes all screenshot elements',async()=>{
        await row(textBoth).getByRole('button').click();assert.equal(await page.locator('.pc-crop img').count(),0);
    });
    await check('opening pair B on Page 1 replaces pair A on Page 2 and Page 3',async()=>{
        await openPair(heatId);await tab('3. Изменения проекта');await scopedRows(heatId);
        await tab('2. Сопоставление листов');
        assert((await page.locator('.sc-vector-pane-head__document').first().innerText()).includes('ИОС-4.1'));
        await tab('3. Изменения проекта');await scopedRows(heatId);
        assert.equal(await page.locator('.pc-expanded-row').count(),0);
    });
    await check('reload restores the same canonical pair without choosing another',async()=>{
        await page.reload();await page.locator('.sc-vector-pane-head__document').first().waitFor();
        assert((await page.locator('.sc-vector-pane-head__document').first().innerText()).includes('ИОС-4.1'));
        await tab('3. Изменения проекта');await scopedRows(heatId);
    });
    const graphic=env.items.find(c=>c.evidence.some(e=>e.source_type==='GRAPHIC'&&e.side==='OLD'));
    const ge=graphic.evidence.find(e=>e.source_type==='GRAPHIC'&&e.side==='OLD');
    await check('TEXT/TABLE/GRAPHIC badges reflect selected pair evidence without duplicate rows',async()=>{
        await openPair(ge.pair_id);await tab('3. Изменения проекта');await scopedRows(ge.pair_id);
        for(const item of pairChanges(ge.pair_id))assert.deepEqual(await row(item).locator('.pc-source').allTextContents(),['TEXT','TABLE','GRAPHIC'].filter(s=>item.evidence.some(e=>e.source_type===s)));
        assert.deepEqual(await row(graphic).locator('.pc-source').allTextContents(),['TEXT','TABLE','GRAPHIC'].filter(s=>graphic.evidence.some(e=>e.source_type===s)));
    });
    await check('exact crop enlarge opens and closes with Escape; keyboard expansion works',async()=>{
        await row(graphic).getByRole('button').focus();await page.keyboard.press('Enter');
        const figure=page.locator('[aria-label="OLD доказательства"] figure').filter({hasText:'GRAPHIC'}).first();
        await figure.locator('.pc-crop').click();await page.waitForFunction(()=>document.querySelector('dialog[open] img')?.naturalWidth>0);
        await shot('enlarge-exact-crop');await page.keyboard.press('Escape');assert.equal(await page.locator('dialog[open]').count(),0);
    });
    await check('PDF deep-link opens selected pair/version/page with overlay',async()=>{
        await page.locator('[aria-label="OLD доказательства"] figure').filter({hasText:'GRAPHIC'}).first().getByRole('button',{name:'Открыть в PDF',exact:true}).click();
        await page.locator('.sc-production-evidence-banner').waitFor();
        await page.waitForFunction(()=>document.querySelector('.sc-production-evidence-overlay')&&document.querySelector('.sc-page-preview')?.naturalWidth>0);
        const src=await page.locator('.sc-page-preview').first().getAttribute('src'),url=new URL(src,base);
        assert(url.pathname.includes('/viewer/pairs/'+ge.pair_id+'/'));assert.equal(url.searchParams.get('page'),String(ge.page));
        const pair=await(await page.request.get(api+'/viewer/pairs/'+ge.pair_id+'?projectChangeUi=1')).json();
        assert.equal(pair.pair.left.pdf_path,ge.document.pdf_path);assert.equal(pair.pair.left.version_id,ge.document.version);
        assert(await page.locator('.sc-production-evidence-overlay').count()>0);
    });await shot('pdf-deep-link-overlay');
    await check('PDF return reopens the same ProjectChange details',async()=>{
        await page.getByRole('button',{name:'← Вернуться к той же строке',exact:true}).click();
        await page.locator('#pc-detail-'+graphic.id).waitFor();
        assert.equal(await row(graphic).getByRole('button').getAttribute('aria-expanded'),'true');
    });
    await tab('3. Изменения проекта');await count(env.items.filter(c=>c.evidence.some(e=>e.pair_id===ge.pair_id)).length);
    await openPair(waterId);await tab('3. Изменения проекта');await scopedRows(waterId);
    const missing=water.find(c=>!c.evidence.some(e=>e.side==='OLD'));
    await check('unbound OLD is explicit, NEW page fallback labeled, no white OLD evidence image',async()=>{
        assert((await row(missing).innerText()).includes('OLD не установлен'));
        await row(missing).click();
        const old=page.locator('[aria-label="OLD доказательства"]');
        assert((await old.innerText()).includes('Надёжная привязка к OLD не установлена.'));
        assert.equal(await old.locator('img').count(),0);
        assert(await page.locator('.pc-page-fallback').count()>0);
        const crop=page.locator('[aria-label="NEW доказательства"] .pc-crop');await crop.first().scrollIntoViewIfNeeded();
        await crop.first().locator('img').evaluate(i=>i.decode());
        assert((await crop.first().innerText()).includes('Открыть страницу'));
        await crop.first().click();await page.waitForFunction(()=>document.querySelector('dialog[open] img')?.naturalWidth>0);
        await page.keyboard.press('Escape');
    });await shot('missing-old-binding');
    await check('missing OLD PDF deep-link keeps OLD viewer empty',async()=>{
        await page.locator('[aria-label="NEW доказательства"] figure').first().getByRole('button',{name:'Открыть в PDF',exact:true}).click();
        await page.locator('.sc-production-evidence-banner').waitFor();
        await page.waitForFunction(()=>document.querySelectorAll('.sc-page-preview').length===1&&document.querySelector('.sc-page-preview')?.naturalWidth>0);
        assert((await page.locator('.sc-page-preview').getAttribute('src')).includes('side=right'));
    });
    await check('Page 4 remains empty; research PROVEN is never human confirmation',async()=>{
        await tab('4. Отчёт');await page.locator('.pc-empty').waitFor();assert.equal(await rows().count(),0);
        assert.equal((await(await page.request.get(api+'/report?projectChangeUi=1')).json()).items.length,0);
        assert.deepEqual(env.summary.research_statuses,{PROVEN:14,REVIEW:59});
    });await shot('page4-empty');
    await check('direct Page 3 entry has no current pair and offers only navigation to Page 1',async()=>{
        const fresh=await newContext(object),p=await fresh.newPage();await p.goto(base+'/?projectChangeUi=1#/stage-comparison');
        await p.locator('.sc-pair-board__row').first().waitFor();await p.getByRole('button',{name:'3. Изменения проекта',exact:true}).click();
        await p.locator('.pc-pair-prompt').waitFor();assert.equal(await p.locator('.pc-row,.pc-summary, .pc-workspace select').count(),0);
        assert.equal(await p.locator('.pc-pair-prompt p').innerText(),'Сначала откройте пару документов на вкладке «Загрузка документации».');
        await p.screenshot({path:path.join(out,'E-page3-no-current-pair.png'),animations:'disabled'});
        const before=requests.filter(r=>new URL(r.url).pathname.includes('/viewer/pairs/')).length;
        await p.getByRole('button',{name:'Перейти к загрузке документации',exact:true}).click();
        await p.locator('.sc-pair-board__row').first().waitFor();
        assert.equal(requests.filter(r=>new URL(r.url).pathname.includes('/viewer/pairs/')).length,before);
        await p.getByRole('button',{name:'3. Изменения проекта',exact:true}).click();
        await p.locator('.pc-pair-prompt').waitFor();await fresh.close();
    });
    await check('selected pair with no changes stays distinct from no current pair',async()=>{
        const empty=pairs.find(p=>pairChanges(p.id).length===0);assert(empty);
        await openPair(empty.id);await tab('3. Изменения проекта');await scopedRows(empty.id);
        assert.equal(await page.locator('.pc-pair-prompt').count(),0);
        assert.equal(await page.locator('.pc-empty').innerText(),'Для этой пары изменений в preview нет.');
    });
    const registry=await(await page.request.get(base+'/api/objects')).json();
    const other=registry.objects.find(o=>o.id!==object);assert(other,'A second object is required for the gate control');
    for(const [id,flag] of [[object,''],[other.id,'?projectChangeUi=1']]){
        await check(id===object?'object 272 without flag: legacy UI and session POST allowed':'other object plus flag keeps legacy UI',async()=>{
            const ctx=await newContext(id),p=await ctx.newPage(),preview=[];
            p.on('request',r=>{if(r.url().includes('/api/project-change-preview/'))preview.push(r.url());});
            const session=id===object?p.waitForResponse(r=>r.request().method()==='POST'
                && new URL(r.url()).pathname==='/api/stage-comparison/sessions'):null;
            await p.goto(base+'/'+flag+'#/stage-comparison');await p.getByRole('button',{name:'3. Расхождения',exact:true}).waitFor();
            await p.getByRole('button',{name:'3. Расхождения',exact:true}).click();
            if(session){const response=await session;assert.equal(response.status(),200);assert((await response.json()).id);}
            assert.equal(await p.locator('.pc-workspace,.pc-preview-label').count(),0);assert.deepEqual(preview,[]);
            assert.equal(await p.evaluate(()=>sessionStorage.getItem('currentObjectId')),id);
            await p.waitForLoadState('networkidle');await p.screenshot({path:path.join(out,id===object?'legacy-no-flag.png':'legacy-other-flag.png')});
            await ctx.close();
        });
    }
    await check('no preview mutations, decision persistence, legacy API calls or runtime errors',async()=>{
        assert.deepEqual(writes,[]);assert.deepEqual(errors,[]);assert.deepEqual(failed,[]);
        assert.deepEqual(audit.harnessErrors,[]);
        assert(!requests.some(r=>r.url.includes('/api/stage-comparison/sessions')));
        const decisions=await page.evaluate(()=>[...Object.keys(sessionStorage),...Object.keys(localStorage)].filter(k=>k.startsWith('project-change-ui:demo:')));
        assert.deepEqual(decisions,[]);
        assert.deepEqual(await(await page.request.get(api+'?projectChangeUi=1')).json(),env);
    });
    await check('decision capability remains read-only and production health is 200',async()=>{
        assert.equal(env.capabilities.decisions,false);assert.equal(env.decision_revision,0);
        assert(env.items.every(i=>i.effective_decision===null));
        if(production){
            const health=await page.request.get(base+'/api/info');assert.equal(health.status(),200);
            assert.equal((await health.json()).base_dir,process.env.SMOKE_EXPECTED_RELEASE+'/app');
        }
    });
    save('PASS');await browser.close();
})().catch(async e=>{console.error(e);save('FAIL',String(e.stack||e));if(page)await shot('failure').catch(()=>{});if(browser)await browser.close();process.exitCode=1;});
