import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url));
const source=fs.readFileSync(path.join(dir,'forward/js/timesheet-correction-office-context.js'),'utf8');
const recipe=JSON.parse(fs.readFileSync(path.join(dir,'CURRENT_OFFICE_READ_FULL_REVERSE.json'),'utf8'));
const original='81000000-0000-4000-8000-000000000001',replacement='81000000-0000-4000-8000-000000000002';
const actor='81000000-0000-4000-8000-000000000003',requestId='81000000-0000-4000-8000-000000000004';
const request=(patch={})=>({request_id:requestId,correction_id:`awc:${original}:${requestId}`,origin:'SIGNED_TIMESHEET_CORRECTION',
  state:'WAITING',state_revision:1,root_timesheet_id:original,reversed_timesheet_id:original,replacement_timesheet_id:replacement,...patch});
const context=(patch={})=>({ok:true,contract_version:'TIMESHEET_CORRECTION_OFFICE_CONTEXT_V1',view:'PAY',
  target:{timesheet_id:original,timesheet_version:1},create_descriptor:null,request:request(),...patch});
const deferred=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject};};
class Element {
  constructor(tag){this.tagName=tag;this.children=[];this.isConnected=true;this.listeners=new Map();this.attributes={};this.textContent='';}
  setAttribute(key,value){this.attributes[key]=value;}
  appendChild(child){child.parent=this;this.children.push(child);return child;}
  replaceChildren(){for(const child of this.children)child.parent=null;this.children=[];}
  remove(){if(this.parent)this.parent.children=this.parent.children.filter(child=>child!==this);this.parent=null;this.isConnected=false;}
  addEventListener(name,fn){this.listeners.set(name,fn);}
  click(){return this.listeners.get('click')?.();}
  get text(){return this.textContent+this.children.map(child=>child.text).join(' ');}
}
function setup(){
  const document={createElement:tag=>new Element(tag)};
  const ctx=vm.createContext({document,Promise,WeakMap,Set,Map,Number,Error,console,fetch(){throw Error('UNIT_ONLY_NO_NETWORK');}});
  vm.runInContext(source,ctx);return {api:ctx.CloudTmsCorrectionOfficeContext,document,ctx,host:new Element('main')};
}
const options=(s,patch={})=>({document:s.document,host:s.host,timesheetId:original,version:1,view:'NATIVE',
  callerCurrent:()=>true,readContext:async()=>context(),openReplacement:()=>{},...patch});
test('native PAY context renders waiting request and real P0 link without invoice/readiness/finance',async()=>{
  const s=setup(),opened=[];const mount=s.api.mount(options(s,{openReplacement:id=>opened.push(id)}));await mount.ready;
  assert.match(s.host.text,/Correction waiting/);const button=s.host.children[0].children.find(child=>child.tagName==='button');button.click();
  assert.deepEqual(opened,[replacement]);assert.doesNotMatch(s.host.text,/invoice|Ready|finance|Authorise|SEND|Return/);
});
test('genuine null fresh-original descriptors remove the panel without inventing an action',async()=>{
  const s=setup();await s.api.mount(options(s,{readContext:async()=>context({request:null})})).ready;
  assert.equal(s.host.children.length,0);
});
test('native ADMIN minimal request strips invoice ID from display output',async()=>{
  const s=setup();const model=s.api.decode(context({view:'ADMIN',request:request({return_invoice_id:requestId})}),original,1,'NATIVE');
  assert.equal(model.requestId,requestId);assert.equal(Object.hasOwn(model,'return_invoice_id'),false);
});
for(const bad of [{},null,[],context({ok:false}),context({view:'UNKNOWN'}),context({target:{timesheet_id:replacement,timesheet_version:1}}),
  context({target:{timesheet_id:original,timesheet_version:2}}),context({prepared:{}}),
  context({request:request({can_send:true})}),context({create_descriptor:request()}),
  context({request:request({return_invoice_id:requestId})}),context({request:request({state:'READY'})})]){
  test(`empty/foreign/unknown/private response ${JSON.stringify(bad)} stays unavailable`,async()=>{
    const s=setup();await s.api.mount(options(s,{readContext:async()=>bad})).ready;
    assert.match(s.host.text,/unavailable/);assert.equal(s.host.children[0].children.some(child=>child.tagName==='button'),false);
  });
}
test('old successful response cannot replace a newer mount on the same overview root',async()=>{
  const s=setup(),old=deferred();const first=s.api.mount(options(s,{readContext:()=>old.promise}));await Promise.resolve();
  const next=s.api.mount(options(s,{readContext:async()=>context({request:request({state:'APPLIED'})})}));await next.ready;
  old.resolve(context());await first.ready;assert.match(s.host.text,/Correction applied/);assert.doesNotMatch(s.host.text,/waiting/);
});
test('old failure cannot overwrite a newer current response',async()=>{
  const s=setup(),old=deferred();const first=s.api.mount(options(s,{readContext:()=>old.promise}));await Promise.resolve();
  await s.api.mount(options(s,{readContext:async()=>context({request:request({state:'APPLIED'})})})).ready;
  old.reject(Error('old'));await first.ready;assert.match(s.host.text,/Correction applied/);assert.doesNotMatch(s.host.text,/unavailable/);
});
test('actor/frame becomes stale while reading: no reply or error details rendered',async()=>{
  const s=setup(),wait=deferred();let current=true;const mount=s.api.mount(options(s,{callerCurrent:()=>current,readContext:()=>wait.promise}));
  await Promise.resolve();current=false;wait.resolve(context());await mount.ready;assert.match(s.host.text,/Loading/);
  assert.equal(s.host.children[0].children.some(child=>child.tagName==='button'),false);
});
test('disconnected modal never adopts a reply',async()=>{
  const s=setup(),wait=deferred();const mount=s.api.mount(options(s,{readContext:()=>wait.promise}));await Promise.resolve();
  s.host.isConnected=false;wait.resolve(context());await mount.ready;assert.match(s.host.text,/Loading/);
});
test('cancelled native request is text only',async()=>{
  const s=setup();await s.api.mount(options(s,{readContext:async()=>context({request:request({state:'CANCELLED'})})})).ready;
  assert.match(s.host.text,/cancelled/);assert.equal(s.host.children[0].children.some(child=>child.tagName==='button'),false);
});
function mainHarness({response=Promise.resolve({ok:true,json:async()=>context()}),user={id:actor,roles:['admin'],email_signature_html:null}}={}){
  const s=setup(),calls=[];const details={timesheet:{timesheet_id:original,version:1}};
  const mc={timesheetDetails:details,openToken:'unit-token',data:{timesheet_id:original},timesheetMeta:{expected_timesheet_id:original}};
  s.ctx.window=s.ctx;s.ctx.window.modalCtx=mc;s.ctx.SESSION={user};s.ctx.mc=mc;s.ctx.mode='view';s.ctx.root=s.host;
  s.ctx.LOGM=false;s.ctx.document=s.document;s.ctx.API=url=>{assert.equal(url,`/api/timesheets/${original}/corrections/context`);return url;};
  s.ctx.authFetch=async url=>{calls.push(url);return response;};
  s.ctx.tsNow=()=>s.ctx.window.modalCtx?.data?.timesheet_id;s.ctx.expectedNow=()=>s.ctx.window.modalCtx?.timesheetMeta?.expected_timesheet_id;
  s.ctx.isActiveTimesheetModalToken=token=>token===s.ctx.window.modalCtx?.openToken;
  s.ctx.window.openTimesheet=id=>calls.push(['open',id]);
  const actualMount=s.api.mount;s.ctx.CloudTmsCorrectionOfficeContext={mount(opts){s.mounted=actualMount(opts);return s.mounted;}};
  vm.runInContext(recipe.changed_current_owners.find(row=>row.path==='js/main.js').hunks[0].after,s.ctx);
  return {...s,calls,mc};
}
test('actual saved main hunk admits actual handleMe roles-array shape through fixed native GET without flags fiction',async()=>{
  const h=mainHarness();await h.mounted.ready;assert.deepEqual(h.calls,[`/api/timesheets/${original}/corrections/context`]);
  assert.match(h.host.text,/Correction waiting/);
});
test('actual main hook empty JSON success is unavailable without green/action',async()=>{
  const h=mainHarness({response:Promise.resolve({ok:true,json:async()=>({})})});await h.mounted.ready;
  assert.match(h.host.text,/unavailable/);assert.doesNotMatch(h.host.text,/Ready|Authorise/);
});
test('actual main actor change after network wait refuses reply',async()=>{
  const wait=deferred(),h=mainHarness({response:wait.promise});await Promise.resolve();h.ctx.SESSION={user:{id:requestId,role:'admin'}};
  wait.resolve({ok:true,json:async()=>context()});await h.mounted.ready;assert.match(h.host.text,/Loading/);
});
test('actual main modal/version rotation after request refuses reply',async()=>{
  const wait=deferred(),h=mainHarness({response:wait.promise});await Promise.resolve();h.ctx.window.modalCtx={...h.mc,
    data:{timesheet_id:replacement},timesheetDetails:{timesheet:{timesheet_id:replacement,version:2}}};
  wait.resolve({ok:true,json:async()=>context()});await h.mounted.ready;assert.match(h.host.text,/Loading/);
});
test('actual main native request scope permission failure gives no action or invoice data',async()=>{
  const h=mainHarness({response:Promise.resolve({ok:false,json:async()=>({error_code:'CORRECTION_ACTOR_NOT_ALLOWED'})})});
  await h.mounted.ready;assert.match(h.host.text,/unavailable/);assert.doesNotMatch(h.host.text,/invoice|Ready|Return/);
});
test('both whole current Office reversals preserve exact f2de including all existing asset parameters',()=>{
  for(const owner of recipe.changed_current_owners){let after=fs.readFileSync(path.join(dir,'forward',owner.path),'utf8');
    for(const h of [...owner.hunks].reverse()){assert.equal(after.split(h.after).length,2);after=after.replace(h.after,()=>h.before);}
    assert.equal(after,fs.readFileSync(path.join(dir,'before',owner.path),'utf8'));
    assert.equal(crypto.createHash('sha256').update(after).digest('hex'),owner.before_sha256);
  }
});

const adminContext=()=>context({view:'ADMIN',request:request({return_invoice_id:'81000000-0000-4000-8000-000000000005'})});
const noStaleRead=h=>{assert.match(h.host.text,/Loading/);assert.doesNotMatch(h.host.text,/Correction waiting|Open replacement/);
  assert.equal(h.host.children[0].children.some(child=>child.tagName==='button'),false);};

test('actual main same-user roles-array replacement during response wait rejects stale ADMIN reply',async()=>{
  const wait=deferred(),h=mainHarness({response:wait.promise});await Promise.resolve();
  h.ctx.SESSION.user.roles=['staff'];wait.resolve({ok:true,json:async()=>adminContext()});
  await h.mounted.ready;noStaleRead(h);assert.equal(h.calls.length,1);
});

test('actual main in-place roles-array mutation during response wait rejects stale ADMIN reply',async()=>{
  const wait=deferred(),h=mainHarness({response:wait.promise});await Promise.resolve();
  h.ctx.SESSION.user.roles.splice(0,1,'staff');wait.resolve({ok:true,json:async()=>adminContext()});
  await h.mounted.ready;noStaleRead(h);assert.equal(h.calls.length,1);
});

test('actual main session replacement with same actor and same roles rejects the previous response',async()=>{
  const wait=deferred(),h=mainHarness({response:wait.promise});await Promise.resolve();
  h.ctx.SESSION={user:{id:actor,roles:['admin'],email_signature_html:null}};
  wait.resolve({ok:true,json:async()=>adminContext()});await h.mounted.ready;noStaleRead(h);
});

test('actual main replacement link rechecks same-user roles after the context has rendered',async()=>{
  const h=mainHarness({response:Promise.resolve({ok:true,json:async()=>adminContext()})});await h.mounted.ready;
  const button=h.host.children[0].children.find(child=>child.tagName==='button');assert(button);
  h.ctx.SESSION.user.roles[0]='staff';button.click();assert.equal(h.calls.some(call=>Array.isArray(call)&&call[0]==='open'),false);
});

test('actual main roles mutation during JSON wait cannot render an earlier native ADMIN response',async()=>{
  const jsonWait=deferred(),h=mainHarness({response:Promise.resolve({ok:true,json:()=>jsonWait.promise})});
  await Promise.resolve();await Promise.resolve();await Promise.resolve();h.ctx.SESSION.user.roles.push('staff');
  jsonWait.resolve(adminContext());await h.mounted.ready;noStaleRead(h);
});

test('native admitted read still supports a legacy signed session with no current roles or payment flags',async()=>{
  const h=mainHarness({user:{id:actor,role:'admin'}});await h.mounted.ready;
  assert.match(h.host.text,/Correction waiting/);assert.equal(h.calls.length,1);
});
