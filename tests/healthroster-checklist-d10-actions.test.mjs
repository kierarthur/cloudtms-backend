import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';import crypto from 'node:crypto';
import {d10OfficePins,d10ReviewEdits,applyD10ReviewEdits} from '../docs/healthroster-build/proposals/checklist-native-20261014/build-d10-review-packet.mjs';
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
test('current complete Office files retain exact native bodies under four-hunk reverse and compile with bound navigation',()=>{
  for(const pin of d10OfficePins){
    const saved=fs.readFileSync(pin.path);assert.equal(crypto.createHash('sha256').update(saved).digest('hex'),pin.saved_sha256);
    const current=saved.toString('utf8').replaceAll('\r\n','\n'),forward=applyD10ReviewEdits(current,pin.path);
    assert.equal(applyD10ReviewEdits(forward,pin.path,'reverse'),current);new vm.Script(forward);
  }
  assert.equal(d10ReviewEdits().length,4);
  const changes=d10ReviewEdits().map(edit=>edit.after).join('\n');
  assert.ok(changes.includes('skipAutoRecheck: true'));assert.ok(changes.includes('assertCheckOnlyD10ReviewFresh'));
  assert.doesNotMatch(changes,/method: 'POST'|method: 'PUT'|\/apply\b|\/selections\b|\.dirty\.set|REMIND_CANDIDATE|ASK_CANDIDATES/);
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
