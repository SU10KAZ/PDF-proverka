// Browser-independent flow checks with synthetic cases and memory-only storage.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(`${__dirname}/dev_ui.js`, 'utf8');
const store = new Map();
let exported;
const panel = {page:1,markdown_line:1,context_start_line:1,context:['Synthetic source']};
const cases = [
  {case_id:'test-section',stratum:'S1',kind:'SECTION',document_code:'Synthetic',question:'Boundary?',pdf:'test.pdf',panels:[panel,panel],allowed_choices:['SAME','NEW','UNSURE']},
  {case_id:'test-owner',stratum:'T7',kind:'OWNER',document_code:'Synthetic',question:'Owner?',pdf:'test.pdf',panels:[panel,panel],allowed_choices:['OWNER_PRECEDING_SECTION','OWNER_FOLLOWING_HEADING','UNSURE']}
];
function element() {return {children:[],classList:{toggle(){}},setAttribute(){},append(...xs){this.children.push(...xs);},replaceChildren(){this.children=[];},click(){if(this.onclick)this.onclick();}};}
async function start() {
  const elements = new Map();
  const sandbox = {document:{getElementById(id){if(!elements.has(id))elements.set(id,element());return elements.get(id);},createElement:element},
    fetch:async()=>({ok:true,json:async()=>({namespace:'SEMANTIC_FOUNDATION_V3_DEV',packet_sha256:'synthetic',cases})}),
    localStorage:{getItem:k=>store.get(k),setItem:(k,v)=>store.set(k,v)},
    Blob:class {constructor(parts){this.parts=parts;}}, URL:{createObjectURL(blob){exported=JSON.parse(blob.parts.join(''));return 'blob:test';},revokeObjectURL(){}},setTimeout:f=>f()};
  await vm.runInNewContext(source,sandbox);
  return {elements,sandbox};
}
(async()=>{
  let {elements,sandbox}=await start();
  assert.match(elements.get('progress').textContent,/Ответов: 0/);
  elements.get('answers').children[0].click();
  elements.get('next').click();
  elements.get('answers').children[1].click();
  elements.get('export').click();
  assert.equal(exported.answers['test-section'],'SAME');
  assert.equal(exported.ownership_answers['test-owner'],'OWNER_FOLLOWING_HEADING');
  assert.equal(exported.namespace,'SEMANTIC_FOUNDATION_V3_DEV');
  assert.equal(exported.packet_sha256,'synthetic');
  ({elements,sandbox}=await start());
  assert.match(elements.get('progress').textContent,/Ответов: 2/);
  elements.get('clear').click();
  assert.match(elements.get('progress').textContent,/Ответов: 1/);
  sandbox.localStorage.setItem=()=>{throw Error('quota');};
  elements.get('answers').children[2].click();
  assert.match(elements.get('error').textContent,/Ответ не сохранён/);
  elements.get('export').click();
  assert.equal(exported.answers['test-section'],null);
  assert.equal(exported.ownership_answers['test-owner'],'OWNER_FOLLOWING_HEADING');
  console.log('PASS: initial blank state, answer choices, persistence, namespace export, clear, storage failure; synthetic only');
})().catch(e=>{console.error(e);process.exitCode=1;});
