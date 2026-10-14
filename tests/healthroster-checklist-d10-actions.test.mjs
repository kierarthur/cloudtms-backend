import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {d10OfficePins,d10ReviewEdits,applyD10ReviewEdits,currentOpenReviewOwner,forwardOpenReviewOwner} from '../docs/healthroster-build/proposals/checklist-native-20261014/build-d10-review-packet.mjs';
const directory=new URL('../docs/healthroster-build/proposals/checklist-native-20261014/',import.meta.url);
const helper=fs.readFileSync(new URL('office-d10-review-action.jsinc',directory),'utf8');
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,H='a'.repeat(64),A='b'.repeat(64),B='c'.repeat(64);
const actor=id(1),scope={source_group_id:id(2),client_id:id(3),source_cycle_id:id(4),week_ending:'2026-10-11'},importId=id(5);
const frame=()=>({contract:'WEEKLY_SOURCE_CHECK_ONLY_D10_REVIEW_ACTION_V1',actor_user_id:actor,permission_view:'ADMIN',scope,
  parent_group_key:`d10g_${H}`,item_id:`timesheet:${id(6)}`,proof_hash:H,action_bindings:[A,B].map((action_id,index)=>({
    owner:'public.import_review_actions_page_v1',actor_user_id:actor,scope,import_id:importId,preview_generation:2,state_version:3,
    preview_fingerprint:H,action_id,action_kind:index?'EMAIL_REMINDER':'EMAIL_ISSUE',evidence_fingerprint:H}))});
const header=actions=>({ok:true,import:{id:importId},state:{last_opened_by_user_id:actor,preview_generation:2,state_version:3,preview_fingerprint:H,status:'IN_REVIEW'},
  actions,actions_next_cursor:null});
const native=action=>({action_id:action.action_id,kind:action.action_kind,evidence_fingerprint:action.evidence_fingerprint});
function context(){const ctx=vm.createContext({});vm.runInContext(helper,ctx);return ctx;}
test('native D10 review descriptor is literal parent/action proof under Admin census, with no new send or GET-side D10 write',()=>{
  const source=fs.readFileSync(new URL('../supabase/repeatable/healthroster_wp_u3_source_proposals/wp_u3_check_only_query_groups.sqlinc',import.meta.url),'utf8');
  const descriptor=source.slice(source.indexOf('review_action:='),source.indexOf('group_row:=',source.indexOf('review_action:=')));
  for(const field of ["'CHECK_ONLY_D10_REVIEW'","'actor_user_id',actor.id","'scope',scope","'parent_group_key',parent_key",
    "'proof_hash',parent_proof","'action_bindings',action_bindings"])assert.ok(descriptor.includes(field),field);
  assert.ok(source.includes("'children',children,'actions',jsonb_build_array(review_action)"));
  assert.ok(source.indexOf("if view='ADMIN' then")<source.indexOf('review_action:='));
  assert.doesNotMatch(source,/perform public\.import_review_|public\.import_review_get_v1\(|timesheet_query_email_enqueue_v1\(|REMIND_CANDIDATE|ASK_CANDIDATES/);
  const enqueue=fs.readFileSync(new URL('../supabase/repeatable/13102026_1205_healthroster_issued_signed_query_v1.sql',import.meta.url),'utf8');
  for(const proof of ['v_operation.committed_at_utc is null','TIMESHEET_QUERY_SELECTED_ACTION_SET_MISMATCH','post_commit_email_action_ids',"d.action_kind not in ('EMAIL_ISSUE','EMAIL_REMINDER')"])
    assert.ok(enqueue.includes(proof),proof);
});
test('D10 navigation validates actor, exact scope, native scalar evidence/CAS and one existing import action set',()=>{
  const ctx=context();assert.ok(ctx.normaliseCheckOnlyD10ReviewAction(frame(),actor));
  for(const mutate of [value=>{value.permission_view='PAY_READONLY';},value=>{value.actor_user_id=id(30);},
    value=>{value.parent_group_key=[`d10g_${H}`];},value=>{value.proof_hash=[H];},value=>{value.action_bindings[0].evidence_fingerprint=[H];},
    value=>{value.action_bindings[0].preview_fingerprint=[H];},value=>{value.action_bindings[0].action_id=[A];},
    value=>{value.action_bindings[0].owner='another.writer';},value=>{value.action_bindings[0].scope={...scope,client_id:id(30)};},
    value=>{value.action_bindings[1].import_id=id(30);},value=>{value.action_bindings[1].preview_generation=3;},
    value=>{value.action_bindings[1].action_id=A;},value=>{value.action_bindings[0].action_kind='REMIND_CANDIDATE';},
    value=>{value.extra='unproved';}]){
    const invalid=structuredClone(frame());mutate(invalid);assert.equal(ctx.normaliseCheckOnlyD10ReviewAction(invalid,actor),null);
  }
});
test('fresh native D10 header/actions must match all bound actions across actual returned cursors before review opens',async()=>{
  const ctx=context(),action=frame(),first=header([native(action.action_bindings[0])]);first.actions_next_cursor=A;let calls=0;
  assert.ok(await ctx.assertCheckOnlyD10ReviewFresh(action,actor,importId,first,async cursor=>{
    calls++;assert.equal(cursor,A);return header([native(action.action_bindings[1])]);
  },()=>true));assert.equal(calls,1);
  for(const mutate of [value=>{value.state.preview_fingerprint='changed';},value=>{value.state.last_opened_by_user_id=id(30);},
    value=>{value.state.status='ABANDONED';},value=>{value.import.id=id(30);},value=>{value.actions[0].kind='ADVISORY';},
    value=>{value.actions[0].evidence_fingerprint=[H];},value=>{value.actions_next_cursor=null;}]){
    const invalid=structuredClone(first);mutate(invalid);
    await assert.rejects(()=>ctx.assertCheckOnlyD10ReviewFresh(action,actor,importId,invalid,async()=>{throw Error('Must not open or send');},()=>true));
  }
  await assert.rejects(()=>ctx.assertCheckOnlyD10ReviewFresh(action,actor,importId,first,async()=>header([]),()=>false));
});
test('current complete Office files retain exact native bodies under full reverse and compile with bound navigation',()=>{
  for(const pin of d10OfficePins){
    const saved=fs.readFileSync(pin.path);assert.equal(crypto.createHash('sha256').update(saved).digest('hex'),pin.saved_sha256);
    const current=saved.toString('utf8').replaceAll('\r\n','\n'),forward=applyD10ReviewEdits(current,pin.path);
    assert.equal(applyD10ReviewEdits(forward,pin.path,'reverse'),current);new vm.Script(forward);
  }
  assert.equal(d10ReviewEdits().length,11);
  const changes=d10ReviewEdits().map(edit=>edit.after).join('\n');
  assert.ok(changes.includes('skipAutoRecheck: true'));assert.ok(changes.includes('assertCheckOnlyD10ReviewFresh'));
  assert.doesNotMatch(changes,/method: 'PUT'|\/apply\b|\/selections\b|\.dirty\.set|REMIND_CANDIDATE|ASK_CANDIDATES/);
  assert.equal((forwardOpenReviewOwner().match(/method: 'POST'/g)||[]).length,(currentOpenReviewOwner().match(/method: 'POST'/g)||[]).length);
});

function deferred(){let resolve;const promise=new Promise(done=>{resolve=done;});return {promise,resolve};}
function wholeReview({stage='scope',bound=true,original=false,openReviewSource=null}={}){
  const entered=deferred(),pending=deferred(),calls={header:0,scope:0,page:0,show:0,install:0,poll:0,return:0};
  const oldState={review:null,reviewEpoch:0},parent={_token:'real-parent'},child={_token:'real-review'};
  const root={__modalStack:[parent],setTimeout:callback=>{calls.timer=callback;return 1;},clearTimeout(){}};
  root.showModal=(_title,_tabs,renderTab,_save,_hasId,_return,options)=>{
    calls.show++;Object.assign(child,{kind:options.kind,renderTab,_ctxRef:root.modalCtx});root.__modalStack.push(child);
    calls.render=()=>renderTab('main');calls.rendered=calls.render();
  };
  const initialHeader=header(frame().action_bindings.map(native));
  const dependencies={state:oldState,global:root,SESSION:{user:{id:actor,role:'admin',is_active:true,roles:['admin']}},
    ACTIVE_VIEWS:['PENDING','READY','EMAIL','NO_ACTION'],PAGE_SIZES:[25,50,75,100],
    cancelDocumentPreparationPoll(){},recoveryFor:()=>null,isSingleClientHealthRoster:()=>false,
    fetchReviewHeader:async()=>{calls.header++;if(stage==='header'){entered.resolve();await pending.promise;}return initialHeader;},
    fetchScope:async()=>{calls.scope++;if(stage==='scope'){entered.resolve();await pending.promise;}return {native_scope:true};},
    fetchActionPage:async()=>{calls.page++;if(stage==='page'){entered.resolve();await pending.promise;}return {items:[{status:'TIMESHEET_EVIDENCE_PREPARING'}]};},
    request:async()=>{throw Error('No native refresh or extra action page permitted in this fixture');},
    renderReview:()=>{calls.renderReview=(calls.renderReview||0)+1;return '<div>Native review content</div>';},
    installReviewReturn:()=>{calls.install++;},scheduleDocumentPreparationPoll:()=>{calls.poll++;},
    pageNeedsDocumentPreparationPoll:()=>true,currentImportScreenIsVisible:()=>true,
    IMPORT_SCREEN_KINDS:new Set(['imports-v1','import-coverage-v1','import-review-v1','import-review-confirm-v1']),document:{getElementById:()=>null},
    notify:()=>{calls.notify=(calls.notify||0)+1;}
  };
  const ctx=vm.createContext(dependencies);vm.runInContext(helper+'\n'+(openReviewSource||(original?currentOpenReviewOwner():forwardOpenReviewOwner())),ctx);
  const current=fs.readFileSync(d10OfficePins[1].path,'utf8').replaceAll('\r\n','\n');
  const composed=original?current:applyD10ReviewEdits(current,d10OfficePins[1].path);
  const screens=composed.slice(composed.indexOf('  function currentImportFrame() {'),composed.indexOf('  function cancelDocumentPreparationPoll() {'));
  vm.runInContext(screens,ctx);
  const install=composed.slice(composed.indexOf('  function installReviewReturn(review) {'),composed.indexOf('  async function leaveReview() {')).trim();
  vm.runInContext(install,ctx);const nativeInstall=ctx.installReviewReturn;
  ctx.installReviewReturn=review=>{calls.install++;nativeInstall(review);};
  return {ctx,dependencies,root,parent,child,state:oldState,calls,entered,pending,
    run:()=>ctx.openReview(importId,bound?{checkOnlyD10Action:frame(),skipAutoRecheck:true,returnTo:()=>{calls.return++;}}:{skipAutoRecheck:true})};
}
test('complete actual bound openReview cancels actor/user/session/permission replacement at header, scope and page await before state/render/poll',async()=>{
  const mutations=[ctx=>vm.runInContext(`SESSION.user = {id:'${id(30)}',role:'admin',is_active:true};`,ctx),
    ctx=>vm.runInContext('SESSION = {user: SESSION.user};',ctx),ctx=>vm.runInContext('SESSION.user = {...SESSION.user};',ctx),
    ctx=>vm.runInContext("SESSION.user.role = 'pay_readonly';",ctx),ctx=>vm.runInContext("SESSION.user.roles.push('pay_readonly');",ctx)];
  for(const stage of ['header','scope','page'])for(const mutate of mutations){
    const fixture=wholeReview({stage}),run=fixture.run();await fixture.entered.promise;mutate(fixture.ctx);fixture.pending.resolve();await run;
    assert.equal(fixture.state.review,null,stage);assert.equal(fixture.calls.show,0,stage);assert.equal(fixture.calls.install,0,stage);assert.equal(fixture.calls.poll,0,stage);
    if(stage==='scope')assert.equal(fixture.calls.page,0);
  }
});
test('complete unchanged bound review publishes once; ordinary unbound continuation matches actual predecessor',async()=>{
  const positive=wholeReview(),run=positive.run();await positive.entered.promise;positive.pending.resolve();await run;
  assert.equal(positive.calls.show,1);assert.equal(positive.calls.install,1);assert.equal(positive.calls.poll,1);
  assert.equal(positive.calls.renderReview,1);assert.equal(positive.calls.rendered,'<div>Native review content</div>');
  assert.equal(positive.state.review.header.state.last_opened_by_user_id,actor);assert.equal(positive.state.review.scope.native_scope,true);
  vm.runInContext(`SESSION.user = {id:'${id(30)}'};`,positive.ctx);assert.equal(positive.calls.render(),'');
  for(const original of [true,false]){
    const ordinary=wholeReview({bound:false,original}),running=ordinary.run();await ordinary.entered.promise;
    vm.runInContext(`SESSION.user = {id:'${id(30)}'};`,ordinary.ctx);ordinary.pending.resolve();await running;
    assert.equal(ordinary.calls.show,1);assert.equal(ordinary.calls.install,1);assert.equal(ordinary.calls.poll,1);
    assert.equal(ordinary.state.review.header.state.last_opened_by_user_id,actor);
    assert.deepEqual(JSON.parse(JSON.stringify(ordinary.state.review.scope)),{native_scope:true});
  }
});
function actualModalHost(fixture,beforeHost=()=>{}){
  const mainPath=d10OfficePins[1].path.replace('import-review-v1.js','main.js'),saved=fs.readFileSync(mainPath);
  assert.equal(crypto.createHash('sha256').update(saved).digest('hex'),'d86f2ef841f53243997c7f5bb73b913a4a04a2c563378ab47e58d1a5ee82c847');
  const main=saved.toString('utf8').replaceAll('\r\n','\n'),start=main.indexOf('function showModal(title, tabs, renderTab, onSave, hasId, onReturn, options) {');
  const host=main.slice(start,main.indexOf('function applyTimesheetLifecyclePatchToModal(',start));
  assert.ok(host.includes('stack().push(frame);'));assert.ok(host.includes('byId(\'modalBody\').innerHTML = this.renderTab(k, merged)'));
  const nodes=new Map();class Element {
    constructor(){this.style={};this.dataset={};this.classList={toggle(){},add(){},remove(){},contains:()=>false};this.innerHTML='';}
    setAttribute(){}removeAttribute(){}getAttribute(){return null;}querySelector(){return null;}querySelectorAll(){return [];}addEventListener(){}removeEventListener(){}appendChild(){}
  }
  const document={getElementById:id=>{if(id==='irv1Review')return null;if(!nodes.has(id))nodes.set(id,new Element());return nodes.get(id);},
    createElement:()=>new Element(),querySelector:()=>null,querySelectorAll:()=>[],addEventListener(){},removeEventListener(){},body:{children:[]},activeElement:null};
  Object.assign(fixture.ctx,{window:fixture.root,document,HTMLElement:Element,byId:id=>document.getElementById(id),
    qs:()=>null,qsa:()=>[],console:{log(){},warn(){},error(){},groupCollapsed(){},groupEnd(){}}});
  Object.assign(fixture.root,{__LOG_MODAL:false,addEventListener(){},removeEventListener(){}});
  vm.runInContext(host,fixture.ctx);const native=fixture.ctx.showModal;
  fixture.root.showModal=(...args)=>{fixture.calls.show++;beforeHost();return native(...args);};
  return {nodes,host};
}
test('complete native showModal/setTab push-before-render reproduces preserved FRAME1 and renders only the exact bound child in successor',async()=>{
  const predecessor=execFileSync('git',['show','8c1727a77f8840527afe8266ec928abb988df2ab:docs/healthroster-build/proposals/checklist-native-20261014/office-open-review.forward.jsinc'],{encoding:'utf8'});
  for(const old of [true,false]){
    const fixture=wholeReview({openReviewSource:old?predecessor:null}),host=actualModalHost(fixture),run=fixture.run();
    await fixture.entered.promise;fixture.pending.resolve();await run;await new Promise(resolve=>setImmediate(resolve));
    assert.equal(fixture.calls.renderReview||0,old?0:1);assert.equal(host.nodes.get('modalBody').innerHTML,old?'':'<div>Native review content</div>');
    const child=fixture.root.__modalStack.at(-1);assert.equal(fixture.state.review.returnOwner.reviewFrame,child);
    assert.equal(fixture.calls.install,1);
    if(!old){
      await fixture.ctx.openReview(importId,{checkOnlyD10Action:frame(),skipAutoRecheck:true,preserveLocal:true});
      assert.equal(fixture.calls.renderReview,2);assert.equal(fixture.root.__modalStack.at(-1),child);assert.equal(fixture.calls.show,1);
      fixture.root.__modalStack.push({_token:'foreign',kind:'import-review-v1',renderTab:child.renderTab,_ctxRef:child._ctxRef});
      assert.equal(child.renderTab('main'),'');assert.equal(fixture.calls.renderReview,2);
    }
  }
  const ordinary=wholeReview({bound:false,original:true}),ordinaryHost=actualModalHost(ordinary),ordinaryRun=ordinary.run();
  await ordinary.entered.promise;ordinary.pending.resolve();await ordinaryRun;await new Promise(resolve=>setImmediate(resolve));
  assert.equal(ordinary.calls.renderReview,1);assert.equal(ordinaryHost.nodes.get('modalBody').innerHTML,'<div>Native review content</div>');
  for(const mutation of ['foreign','session','token']){
    const fixture=wholeReview();actualModalHost(fixture,()=>{
      if(mutation==='foreign')fixture.root.__modalStack.push({_token:'foreign',kind:'import-review-v1',renderTab:()=>'<div>Foreign</div>'});
      if(mutation==='session')vm.runInContext('SESSION = {user:SESSION.user};',fixture.ctx);
      if(mutation==='token')fixture.parent._token='changed';
    });
    const run=fixture.run();await fixture.entered.promise;fixture.pending.resolve();await run;await new Promise(resolve=>setImmediate(resolve));
    assert.equal(fixture.calls.renderReview||0,0,mutation);assert.equal(fixture.calls.install,0,mutation);assert.equal(fixture.calls.poll,0,mutation);
    assert.equal(fixture.state.review.returnOwner.reviewFrame,null,mutation);
  }
});
test('actual composed poll and return-owner continuations refuse changed session and retain ordinary native modal ownership',async()=>{
  const saved=fs.readFileSync(d10OfficePins[1].path,'utf8').replaceAll('\r\n','\n'),composed=applyD10ReviewEdits(saved,d10OfficePins[1].path);
  const poll=composed.slice(composed.indexOf('  function scheduleDocumentPreparationPoll(review) {'),composed.indexOf('  function formatDate(value) {')).trim();
  const install=composed.slice(composed.indexOf('  function installReviewReturn(review) {'),composed.indexOf('  async function leaveReview() {')).trim();
  for(const replaceSession of [false,true]){
    const fixture=wholeReview(),run=fixture.run();await fixture.entered.promise;fixture.pending.resolve();await run;
    vm.runInContext(poll+'\n'+install,fixture.ctx);
    fixture.ctx.scheduleDocumentPreparationPoll(fixture.state.review);fixture.ctx.installReviewReturn(fixture.state.review);
    if(replaceSession)vm.runInContext('SESSION = {user:SESSION.user};',fixture.ctx);
    if(replaceSession){await fixture.calls.timer();assert.equal(fixture.calls.header,1);assert.equal(fixture.calls.show,1);}
    fixture.child._onDismiss();fixture.root.__modalStack=[fixture.parent];await Promise.resolve();await Promise.resolve();
    assert.equal(fixture.calls.return,replaceSession?0:1);
  }
  for(const replaceSession of [false,true]){
    const fixture=wholeReview(),run=fixture.run();await fixture.entered.promise;fixture.pending.resolve();await run;
    const callbackEntered=deferred(),callbackPending=deferred();
    fixture.state.review.returnOwner.callback=async()=>{callbackEntered.resolve();await callbackPending.promise;throw Error('native refresh refused');};
    fixture.child._onDismiss();fixture.root.__modalStack=[fixture.parent];await callbackEntered.promise;
    if(replaceSession)vm.runInContext('SESSION = {user:SESSION.user};',fixture.ctx);
    callbackPending.resolve();await new Promise(resolve=>setImmediate(resolve));
    assert.equal(fixture.calls.notify||0,replaceSession?0:1);
  }
});
test('actual Office action handler opens existing native D10 review only, with current actor return and no command dispatch',async()=>{
  const saved=fs.readFileSync(d10OfficePins[0].path,'utf8').replaceAll('\r\n','\n');
  const source=applyD10ReviewEdits(saved,d10OfficePins[0].path);
  const start=source.indexOf('  function handleAction(detailValue) {'),end=source.indexOf('  return Object.freeze({',start);
  assert.ok(start>=0&&end>start);const body=source.slice(start,end).trim();
  const calls=[],session={user:{id:actor}},workspace={refresh:tab=>{calls.push({refresh:tab});}};
  const root={CloudTmsImportReviewV1:{openReview:(id,options)=>{calls.push({id,options});return options;}}};
  const handler=new Function('root','SESSION','asObject','asText','workspaceApi','openDetail',helper+'\nreturn ('+body+');')(
    root,session,value=>value&&typeof value==='object'?value:{},value=>String(value||''),()=>workspace,()=>({blocked:true}));
  const options=await handler({label:'View details',kind:'CHECK_ONLY_D10_REVIEW',enabled:true,payload:frame()});
  assert.equal(calls[0].id,importId);assert.equal(options.view,'EMAIL');assert.equal(options.skipAutoRecheck,true);
  options.returnTo();assert.deepEqual(calls[1],{refresh:'queries'});session.user.id=id(30);options.returnTo();assert.equal(calls.length,2);
  assert.deepEqual(handler({label:'View details',kind:'CHECK_ONLY_D10_REVIEW',enabled:true,payload:frame()}),{blocked:true});
});
