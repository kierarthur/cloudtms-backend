// Real Chromium component tests with an isolated adapter. This is not Office
// integration, a native financial fixture or database/API acceptance evidence.
import test from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createServer} from 'node:http';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {mkdirSync} from 'node:fs';
const require=createRequire(import.meta.url),{chromium}=require(process.env.HEALTHROSTER_PLAYWRIGHT_PACKAGE||'playwright');
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const id=n=>`00000000-0000-0000-0000-${String(n).padStart(12,'0')}`,sha='a'.repeat(64),version='DAILY_RATE_CORRECTION_V1';
const review={version,operation_id:id(7),review_id:id(8),review_digest:sha,state_revision:'1',ready_to_confirm:true,current_timesheet_id:id(1),summary:{worked_facts_preserved:true,pay_effect:{visibility:'FULL',text:'The native payment comparison is ready.'},invoice_effect:{visibility:'FULL',text:'Invoice drafts will be prepared.'}},known_blockers:[],confirm:{request_version:version,review_id:id(8),review_digest:sha,expected_state_revision:'1'},links:[]};
const result={version,purpose:'DAILY_RATE_CORRECTION',operation_id:id(7),state:'COMPLETE',state_revision:'4',placement:{status:'COMMITTED',original_timesheet_id:id(1),current_timesheet_id:id(1),placement_revision:'1',receipt_id:id(9)},financial:{status:'REGISTERED',command_id:id(10),cut_id:id(11),summary:{visibility:'FULL',text:'The native pay revision is registered.'}},billing:{status:'PREPARING_DRAFTS',revision:'1',group_id:id(14),visible_summary:{visibility:'FULL',text:'Invoice drafts are being prepared.'}},next_actions:[],links:[]};
let browser,server,origin;
test.before(async()=>{
 server=createServer((request,response)=>{response.writeHead(200,{'content-type':'text/html'});response.end('<!doctype html><html><head><title>Isolated Daily component test</title></head><body><button id="more">Timesheet More</button></body></html>');});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));origin='http://127.0.0.1:'+server.address().port;
 browser=await chromium.launch({headless:true});
});
test.after(async()=>{await browser?.close();if(server)await new Promise(resolve=>server.close(resolve));});
async function mount(page,{clear=true,uncertain=false,blocked=false,redacted=false}={}){
 await page.goto(origin);
 await page.addScriptTag({path:path.join(root,'forward/office/js/correction-contract-v3.js')});
 await page.addScriptTag({path:path.join(root,'forward/office/js/daily-rate-correction.js')});
 await page.addStyleTag({path:path.join(root,'forward/office/css/daily-rate-correction.css')});
 await page.evaluate(({review,result,clear,uncertain,blocked,redacted,tid,actor})=>{
  if(clear)sessionStorage.clear();window.calls=[];window.nativeResult=result;window.nativeReview=review;window.uncertain=uncertain;
  if(blocked){nativeReview.ready_to_confirm=false;nativeReview.confirm=null;nativeReview.known_blockers=[{code:'DAILY_RATE_USED_RATE_MISSING',native_owner:'HEALTH',field_key:'pay_day',permitted_action:null}];}
  if(redacted){nativeReview.summary.pay_effect={visibility:'REDACTED'};nativeReview.summary.invoice_effect={visibility:'REDACTED'};}
  document.getElementById('more').focus();
  window.adapter={actorUserId:actor,preview:async body=>{calls.push({kind:'preview',body});return nativeReview;},accept:async body=>{calls.push({kind:'accept',body});if(window.uncertain)throw {code:'DAILY_RATE_RESULT_UNCERTAIN'};return nativeResult;},get:async operation=>{calls.push({kind:'get',operation});return nativeResult;},runAction:async action=>calls.push({kind:'action',action})};
  window.modal=CloudTMSDailyRateCorrectionV1.open({timesheetId:tid,adapter});
 },{review:structuredClone(review),result:structuredClone(result),clear,uncertain,blocked,redacted,tid:id(1),actor:id(20)});
}
test('phone component has one accessible Daily dialog, native summaries and no Contract choice',async()=>{
 const page=await browser.newPage({viewport:{width:390,height:844}});
 try{
  await mount(page);assert.equal(await page.getByRole('dialog').count(),1);assert.equal(await page.locator('select').count(),0);
  await page.getByLabel('Details (optional)').fill('Updated Client rates');await page.getByRole('button',{name:'Preview',exact:true}).click();
  await page.getByText('The native payment comparison is ready.').waitFor();assert.equal(await page.getByRole('button',{name:'Apply updated rates',exact:true}).isEnabled(),true);
  assert.equal(await page.evaluate(()=>document.querySelector('dialog').scrollWidth>innerWidth),false);
  if(process.env.HEALTHROSTER_UI_ARTIFACT_DIR){mkdirSync(process.env.HEALTHROSTER_UI_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.HEALTHROSTER_UI_ARTIFACT_DIR,'isolated-daily-component-phone.png')});}
  await page.keyboard.press('Escape');assert.equal(await page.getByRole('dialog').count(),0);assert.equal(await page.locator('#more').evaluate(e=>e===document.activeElement),true);
 }finally{await page.close();}
});
test('uncertain acceptance survives reload and retries the same UUID/body; REGISTERED completes without APPLIED/PDF gate',async()=>{
 const page=await browser.newPage();
 try{
  await mount(page,{uncertain:true});await page.getByRole('button',{name:'Preview',exact:true}).click();await page.getByRole('button',{name:'Apply updated rates',exact:true}).click();
  await page.getByRole('button',{name:'Retry same request',exact:true}).waitFor();const original=await page.evaluate(()=>calls.find(c=>c.kind==='accept').body);
  await mount(page,{clear:false});assert.equal(await page.getByRole('button',{name:'Preview',exact:true}).isVisible(),false);
  await page.getByRole('button',{name:'Retry same request',exact:true}).click();await page.getByText('Updated rates applied',{exact:true}).waitFor();
  assert.deepEqual(await page.evaluate(()=>calls.find(c=>c.kind==='accept').body),original);assert.equal(await page.getByText('Pay revision registered',{exact:true}).isVisible(),true);
  assert.equal(await page.getByText('Invoice drafts being prepared',{exact:true}).isVisible(),true);assert.equal(await page.getByRole('button',{name:'Apply updated rates',exact:true}).isVisible(),false);
  assert.equal(await page.evaluate(()=>sessionStorage.length),0);assert.equal(await page.evaluate(()=>calls.filter(c=>c.kind==='accept').length),1);
 }finally{await page.close();}
});
test('native blockers and overlong changed Details invalidate Confirm without changing rates or exposing restricted summary',async()=>{
 const page=await browser.newPage();
 try{
  await mount(page,{blocked:true,redacted:true});await page.getByRole('button',{name:'Preview',exact:true}).click();await page.getByText('DAILY RATE USED RATE MISSING',{exact:true}).waitFor();
  assert.equal(await page.getByRole('button',{name:'Apply updated rates',exact:true}).isVisible(),false);assert.equal(await page.getByText('Restricted for your current role.').count(),2);
  await mount(page);await page.getByRole('button',{name:'Preview',exact:true}).click();await page.getByRole('button',{name:'Apply updated rates',exact:true}).waitFor();
  await page.getByLabel('Details (optional)').fill('x'.repeat(1970));assert.equal(await page.getByRole('button',{name:'Apply updated rates',exact:true}).isVisible(),false);assert.equal(await page.getByRole('button',{name:'Preview',exact:true}).isDisabled(),true);
  assert.equal(await page.evaluate(()=>calls.filter(c=>c.kind==='accept').length),0);
 }finally{await page.close();}
});
test('uncommitted GET does not discard the saved Accept; stale native reply requires a fresh preview',async()=>{
 const page=await browser.newPage();
 try{
  await mount(page,{uncertain:true});await page.getByRole('button',{name:'Preview',exact:true}).click();await page.getByRole('button',{name:'Apply updated rates',exact:true}).click();await page.getByRole('button',{name:'Check saved result',exact:true}).waitFor();
  await page.evaluate(()=>{nativeResult={...nativeResult,state:'REVIEWED',placement:{...nativeResult.placement,status:'NOT_COMMITTED',placement_revision:'0',receipt_id:null}};});
  await page.getByRole('button',{name:'Check saved result',exact:true}).click();await page.getByText('Saved correction result',{exact:true}).waitFor();assert.equal(await page.evaluate(()=>sessionStorage.length),1);
  await page.evaluate(()=>{adapter.accept=async body=>{calls.push({kind:'accept',body});throw {code:'REVIEW_STALE'};};});
  await page.getByRole('button',{name:'Retry same request',exact:true}).click();await page.getByText('The Timesheet or its rates changed. Preview the current values again.').waitFor();
  assert.equal(await page.evaluate(()=>sessionStorage.length),0);assert.equal(await page.getByRole('button',{name:'Preview',exact:true}).isVisible(),true);
 }finally{await page.close();}
});
test('Office HTTP adapter sends only the specified browser commands through the existing authenticated transport',async()=>{
 const page=await browser.newPage();
 try{
  await mount(page);await page.addScriptTag({path:path.join(root,'forward/office/js/daily-rate-correction-http-adapter.js')});
  const seen=await page.evaluate(async({tid,actor,version,sha})=>{
   const requests=[],api=CloudTMSDailyRateCorrectionHttpV1.create({timesheetId:tid,actorUserId:actor,runAction:async()=>{},request:async(path,init)=>{
    requests.push({path,method:init.method,body:init.body?JSON.parse(init.body):null,signal:init.signal instanceof AbortSignal});
    return new Response(JSON.stringify(path.endsWith('/preview')?nativeReview:nativeResult),{headers:{'content-type':'application/json'}});
   }});
   await api.preview({version,request_id:crypto.randomUUID(),timesheet_id:tid});
   await api.accept({version,request_id:crypto.randomUUID(),operation_id:nativeReview.operation_id,review_id:nativeReview.review_id,review_digest:sha,expected_state_revision:'1'});
   await api.get(nativeReview.operation_id);return requests;
  },{tid:id(1),actor:id(20),version,sha});
  assert.equal(seen.length,3);assert.equal(seen[2].method,'GET');assert.equal(seen[2].body,null);assert.ok(seen.every(c=>c.signal));
  assert.ok(seen.filter(c=>c.body).every(c=>!('actor_user_id' in c.body)&&!('calculation_snapshot' in c.body)&&!('to_contract_id' in c.body)));
 }finally{await page.close();}
});
