/* Genuine FastAPI bridge integration. Requires a FRESH isolated preview DB.
 * Never use a production host or modify research truth to prepare examples.
 */
const {chromium}=require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const base=process.env.PC_BRIDGE_URL || 'http://127.0.0.1:8974';
if(!/^http:\/\/127\.0\.0\.1:\d+$/.test(base))throw Error('Loopback preview only');
const object='272_Sadovnicheskaya_76_Balchug_Esteyt';
const api=base+'/api/project-change-preview/objects/'+object;
const out=path.resolve(__dirname,'../../deliverables/project_change_backend_bridge_v1');
fs.mkdirSync(out+'/screenshots',{recursive:true});
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.PC_CHROME,args:['--no-sandbox']});
 const page=await browser.newPage({viewport:{width:1600,height:1100}});
 const errors=[],writes=[],checks=[];
 page.on('pageerror',e=>errors.push(e.message));
 page.on('request',r=>{if(['POST','PUT','PATCH','DELETE'].includes(r.method()))writes.push({method:r.method(),url:r.url()});});
 const check=async(name,f)=>{await f();checks.push(name);console.log('PASS '+name);};
 const tab=async(name)=>{await page.getByRole('button',{name,exact:true}).click();};
 const card=id=>page.locator('#pc-'+id);
 const screenshot=async name=>{await page.evaluate(()=>window.scrollTo(0,0));await page.screenshot({path:out+'/screenshots/'+name+'.png'});};
 const filters=()=>page.locator('.pc-filters select');
 const loaded=async c=>{await c.locator('.pc-crop img').evaluateAll(images=>Promise.all(images.map(img=>{img.loading='eager';return img.complete?Promise.resolve():new Promise(r=>{img.onload=r;img.onerror=r;});})));assert(await c.locator('.pc-crop img').evaluateAll(images=>images.every(i=>i.naturalWidth>0)));};
 const action=async(id,label,status)=>{await card(id).getByRole('button',{name:label,exact:true}).click();await page.waitForFunction(({id,status})=>document.getElementById('pc-'+id)?.dataset.status===status,{id,status});};
 try {
  const initial=await(await page.request.get(api)).json();
  assert.equal(initial.decision_revision,0,'Use a fresh state directory; never reset history destructively');
  const proven=initial.items.find(c=>c.research_status==='PROVEN'&&c.evidence.some(e=>e.source_type==='GRAPHIC'));
  const review=initial.items.find(c=>c.research_status==='REVIEW');
  const table=initial.items.find(c=>c.evidence.some(e=>e.source_type==='TABLE'));
  await page.goto(base+'/?projectChangeUi=1#/stage-comparison');
  await page.locator('.sc-pair-board__row').first().waitFor();
  await check('13 frozen document pairs; launch and matching disabled',async()=>{
   assert.equal(await page.locator('.sc-pair-board__row').count(),13);
   assert(await page.getByRole('button',{name:'Запустить анализ проекта'}).isDisabled());
   assert.equal(await page.locator('[draggable="true"]').count(),0);
  });
  await tab('3. Изменения проекта');await card(review.id).waitFor();
  await check('73 real changes, 14 PROVEN and 59 REVIEW; no automatic approvals',async()=>{
   assert.equal(await page.locator('.pc-card').count(),73);
   assert.equal(await page.locator('.pc-card[data-status="REVIEW"]').count(),73);
   assert.deepEqual(initial.summary.research_statuses,{REVIEW:59,PROVEN:14});
   assert((await page.locator('.pc-notice').innerText()).includes('RESEARCH / PREVIEW'));
   assert.equal(await page.getByRole('button',{name:'Сбросить решения',exact:true}).count(),0);
  });
  await loaded(card(review.id));await screenshot('page-3-review');
  await check('real REVIEW preserves missing OLD and explicit page-level fallback',async()=>{
   assert.equal(review.evidence.filter(e=>e.side==='OLD').length,0);
   assert((await card(review.id).innerText()).includes('Привязка к OLD не установлена'));
   assert((await card(review.id).innerText()).includes('Открыть страницу'));
  });
  await filters().nth(4).selectOption('TABLE');await loaded(card(table.id));
  await check('TABLE route retains several evidence items on both sides',async()=>{
   assert.equal(await card(table.id).locator('figure').count(),table.evidence.length);
   assert(table.evidence.filter(e=>e.side==='OLD').length>1&&table.evidence.filter(e=>e.side==='NEW').length>1);
   assert.equal(await card(table.id).getAttribute('data-status'),'REVIEW');
  });
  await screenshot('page-3-table');
  await filters().nth(4).selectOption('GRAPHIC');await loaded(card(proven.id));
  await check('real PROVEN renders TEXT and GRAPHIC with exact source regions',async()=>{
   assert.equal(await card(proven.id).locator('figure').count(),proven.evidence.length);
   assert((await card(proven.id).innerText()).includes('TEXT ✓'));
   assert((await card(proven.id).innerText()).includes('GRAPHIC ✓'));
   assert(proven.evidence.some(e=>e.region&&e.crop_precision==='EXACT_REGION'));
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  });
  await screenshot('page-3-proven-multiple');
  const selected=proven.evidence.find(e=>e.side==='OLD'&&e.source_type==='GRAPHIC');
  const index=proven.evidence.findIndex(e=>e.id===selected.id);
  await check('real crop enlargement loads source PDF raster',async()=>{
   await card(proven.id).locator('.pc-crop').nth(index).click();await page.locator('dialog[open] img').waitFor();
   await page.waitForFunction(()=>document.querySelector('dialog[open] img')?.naturalWidth>0);
   await page.screenshot({path:out+'/screenshots/real-evidence-region.png'});await page.keyboard.press('Escape');
  });
  await check('PDF navigation uses exact OLD document/version/page and normalized region',async()=>{
   await card(proven.id).locator('figcaption .pc-link').nth(index).click();
   await page.locator('.sc-production-evidence-banner').waitFor();
   await page.waitForFunction(()=>{const images=[...document.querySelectorAll('.sc-page-preview')];return images.length===2&&images.every(i=>i.complete&&i.naturalWidth>0);});
   const images=await page.locator('.sc-page-preview').evaluateAll(x=>x.map(i=>i.src));
   const old=new URL(images.find(u=>u.includes('side=left')));
   assert(old.pathname.includes('/viewer/pairs/'+selected.pair_id+'/'));
   assert.equal(old.searchParams.get('page'),String(selected.page));
   assert((await page.locator('.sc-vector-pane-head__document').first().innerText()).includes(selected.document.id));
   assert.equal(await page.locator('.sc-production-evidence-overlay').count(),1);
   assert(await page.locator('.sc-sheet-map__page-select').first().isDisabled());
  });
  await screenshot('pdf-provenance');
  await check('viewer thumbnails use real read-only preview pages',async()=>{
   await page.getByRole('button',{name:'Показать миниатюры',exact:true}).click();
   await page.waitForFunction(()=>{const images=[...document.querySelectorAll('.sc-thumbs img')];return images.length>0&&images.every(i=>i.complete&&i.naturalWidth>0);});
  });
  await page.getByRole('button',{name:'← Вернуться к той же строке',exact:true}).click();await card(proven.id).waitFor();
  await check('CONFIRM is persisted by backend with server actor and evidence binding',async()=>{
   await card(proven.id).locator('.pc-decision-comment input').fill('Integration demonstration only; not research adjudication.');
   await action(proven.id,'Подтвердить','CONFIRMED');
   const env=await(await page.request.get(api)).json();const c=env.items.find(c=>c.id===proven.id);
   assert.equal(c.effective_decision.decision,'CONFIRM');assert.equal(c.effective_decision.actor,'codex-local-preview');
   assert.equal(c.effective_decision.decision_key,c.decision_key);
  });
  await check('NOT_A_CHANGE keeps preceding decision history',async()=>{
   await action(review.id,'Не могу определить','UNDETERMINED');
   await action(review.id,'Не изменение','REJECTED');
   await card(review.id).locator('.pc-details > summary').click();
   await card(review.id).locator('.pc-technical > summary').click();
   await card(review.id).getByRole('button',{name:'История решений',exact:true}).click();
   await card(review.id).locator('.pc-technical').getByText('#2', {exact:false}).waitFor();
   const history=(await(await page.request.get(api+'/decisions/'+review.decision_key)).json()).items;
   assert.deepEqual(history.map(h=>h.decision),['UNSURE','NOT_A_CHANGE']);
   assert(history.every(h=>h.evidence_snapshot));
  });
  await tab('4. Отчёт');
  await check('Page 4 and report API include only one effective human confirmation',async()=>{
   assert.equal(await page.locator('.pc-card').count(),1);assert.equal(await card(proven.id).getAttribute('data-status'),'CONFIRMED');
   const report=await(await page.request.get(api+'/report')).json();assert.deepEqual(report.items.map(c=>c.id),[proven.id]);
   for(const name of ['Excel ↓','PDF ↓','HTML ↓'])assert(await page.getByRole('button',{name,exact:true}).isDisabled());
  });
  await screenshot('page-4-confirmed-report');
  await check('decisions survive reload without browser storage authority',async()=>{
   await page.evaluate(()=>sessionStorage.clear());await page.reload();await page.locator('.sc-vector-pane-head').first().waitFor();
   await tab('3. Изменения проекта');assert.equal(await card(proven.id).getAttribute('data-status'),'CONFIRMED');
   assert.equal(await card(review.id).getAttribute('data-status'),'REJECTED');
  });
  await check('simulated source outage hides cached approvals until a successful refresh',async()=>{
   await page.route(api,route=>route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({detail:'Test-only source outage'})}));
   await page.getByRole('button',{name:'Обновить данные',exact:true}).click();
   await page.waitForFunction(()=>document.querySelectorAll('.pc-card').length===0);
   assert((await page.locator('.pc-workspace [role="alert"]').innerText()).includes('Test-only source outage'));
   await tab('4. Отчёт');assert.equal(await page.locator('.pc-card').count(),0);
   await page.unroute(api);await page.getByRole('button',{name:'Обновить данные',exact:true}).click();
   await card(proven.id).waitFor();assert.equal(await page.locator('.pc-card').count(),1);
  });
  await check('all 213 evidence URLs render real PNG; physical crop count deduplicates pages',async()=>{
   const evidence=new Map(initial.items.flatMap(c=>c.evidence).map(e=>[e.id,e]));
   const images=[];
   for(const e of evidence.values()){
    const r=await page.request.get(base+e.image_url);assert.equal(r.status(),200);const bytes=await r.body();
    assert(bytes.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10])));
    images.push({id:e.id,side:e.side,page:e.page,precision:e.crop_precision,pair_id:e.pair_id,
     sha256:require('node:crypto').createHash('sha256').update(bytes).digest('hex')});
   }
   fs.writeFileSync(out+'/crop-receipts.json',JSON.stringify(images,null,2));
   assert.equal(evidence.size,213);
  });
  await check('all writes are local preview decisions; no comparator or truth mutations',async()=>{
   assert.equal(writes.length,3);assert(writes.every(r=>r.method==='POST'&&r.url===api+'/decisions'));
  });
  await check('feature flag off preserves legacy UI and makes no bridge request',async()=>{
   const requests=[];const listener=r=>requests.push(r.url());page.on('request',listener);
   await page.goto(base+'/#/stage-comparison');await page.getByRole('button',{name:'2. Связь блоков',exact:true}).waitFor();
   assert.equal(await page.locator('.pc-workspace').count(),0);assert(!requests.some(u=>u.startsWith(api)));page.off('request',listener);
  });
  await check('no browser application errors',async()=>assert.deepEqual(errors,[]));
  const manifest=await(await page.request.get(api+'/manifest')).json();
  const final=await(await page.request.get(api)).json();
  fs.writeFileSync(out+'/integration-results.json',JSON.stringify({passed:checks.length,checks,errors,writes,
   source_run_id:manifest.source_run_id,candidate_version:manifest.candidate_version,candidate_status:manifest.candidate_status,
   summary:final.summary,demonstration:{proven:proven.id,review:review.id,table:table.id},
   decisions:final.items.filter(c=>c.effective_decision).map(c=>({id:c.id,decision:c.effective_decision})),
   note:'Local integration demonstration by codex-local-preview; not independent engineer adjudication or research truth.'},null,2));
  console.log(checks.length+' browser checks PASS');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
