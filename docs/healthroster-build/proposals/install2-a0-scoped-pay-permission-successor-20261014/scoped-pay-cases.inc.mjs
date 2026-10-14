// Actual source helper, router, factory and orchestrator; finite synthetic native
// output variants are UNIT_ONLY witnesses to source order, never native evidence.
const payUser = { ...user, role: 'operator', payment_authoriser: true, payment_golden_key: false };
const noMutation = h => !h.calls.some(c => ['producer','timesheet_correction_authorise_atomic_v1',
  'timesheet_authorise_bulk_atomic','timesheet_unauthorise_bulk_atomic','timesheet_correction_send_v1',
  'timesheet_correction_cancel_atomic_v1','timesheet_correction_return_to_draft_v1'].includes(c[0]));
const probeIds = h => h.calls.filter(c => c[0] === 'weekly_source_office_authorise_scope_v1').map(c => c[1].p_request.timesheet_id);
const firstCall = (h, name) => h.calls.findIndex(c => c[0] === name);

for (const [name, actor] of [['payment authoriser', payUser], ['golden key', { ...payUser, payment_authoriser:false, payment_golden_key:true }]]) {
 test(`${name} genuine Source request probes actual P0 then original before producer/native AUTHORISE`, async () => {
  const h=harness({ users:[actor] });
  const response=await h.actual.handleTimesheetCorrectionOfficeRoute(env,request('authorise',action()),'authorise');
  assert.equal(response.status,200,JSON.stringify(await response.clone().json()));
  assert.deepEqual(probeIds(h),[I.p0,I.p0,I.original]);
  assert.ok(firstCall(h,'timesheet_correction_request_authority_v1')<firstCall(h,'weekly_source_office_authorise_scope_v1'));
  assert.ok(firstCall(h,'weekly_source_office_authorise_scope_v1')<firstCall(h,'timesheet_correction_prepare_v1'));
  assert.ok(h.calls.findLastIndex(c=>c[0]==='weekly_source_office_authorise_scope_v1')<firstCall(h,'producer'));
  assert.ok(firstCall(h,'producer')<firstCall(h,'timesheet_correction_authorise_atomic_v1'));
  assert.ok(!probeIds(h).includes(I.r0),'Reserved future R0 has no physical scope witness');
  assert.ok(h.calls.filter(c=>c[0]==='requireUser').every(c=>c[1].length===0));
  assert.ok(h.calls.filter(c=>c[0]==='weekly_source_office_authorise_scope_v1').every(c=>c[1].p_request.actor_user_id===I.actor && c[2].timeoutMs===12000));
 });
}
test('SEND/CANCEL/RETURN remain admin management even with current pay flags',async()=>{
 for(const [suffix,method,payload] of [['send','send',send],['cancel','cancel',action()],['return-to-draft','returnToDraft',action(2)]]) {
  const h=harness({users:[payUser]});assert.equal((await h.actual.handleTimesheetCorrectionOfficeRoute(env,request(suffix,payload),method)).status,403);
  assert.equal(h.calls[0][0],'requireUser');assert.deepEqual(h.calls[0][1],['admin']);assert.ok(noMutation(h));assert.equal(probeIds(h).length,0);
 }
});
test('inactive/unflagged/string flags/foreign auth tenant cannot reach Source request or prepare',async()=>{
 for(const actor of [{...payUser,is_active:false},{...payUser,payment_authoriser:false},{...payUser,payment_authoriser:'true'},
  {...payUser,tenant_id:env.MYTMS_OFFICE_AGENCY_ID},{...payUser,tenant_id:id(91)}]) {
  const h=harness({users:[actor]});assert.equal((await h.actual.handleTimesheetCorrectionOfficeRoute(env,request('authorise',action()),'authorise')).status,403);
  assert.ok(noMutation(h));assert.equal(firstCall(h,'timesheet_correction_request_authority_v1'),-1);
 }
});
test('ordinary or Daily nonapplicable P0 scope denies current pay actor before PREPARE',async()=>{
 const h=harness({users:[payUser],scope:()=>({contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:false})});
 const response=await h.actual.handleTimesheetCorrectionOfficeRoute(env,request('authorise',action()),'authorise');
 assert.equal(response.status,403);assert.equal((await response.json()).error_code,'CORRECTION_SOURCE_SCOPE_NOT_ALLOWED');
 assert.ok(noMutation(h));assert.equal(firstCall(h,'timesheet_correction_prepare_v1'),-1);
});
test('foreign/nonapplicable original cannot be hidden behind a Source-qualified P0',async()=>{
 const h=harness({users:[payUser],scope:id=>({contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:id!==I.original})});
 assert.equal((await h.actual.handleTimesheetCorrectionOfficeRoute(env,request('authorise',action()),'authorise')).status,403);
 assert.deepEqual(probeIds(h),[I.p0,I.p0,I.original]);assert.ok(noMutation(h));
});
test('malformed scope, PGRST202 and unknown scope remain retryable503 rather than ordinary fallback',async()=>{
 for(const opts of [{scope:()=>({})},{scope:()=>({contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:true,extra:true})},
  {scope:()=>({contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:'true'})},{scopeError:{code:'PGRST202'}},{scopeError:new Error('UNIT_ONLY unknown')}]) {
  const h=harness({users:[payUser],...opts});const response=await h.actual.handleTimesheetCorrectionOfficeRoute(env,request('authorise',action()),'authorise');
  assert.equal(response.status,503);assert.equal((await response.json()).error_code,'WEEKLY_SOURCE_AUTHORISE_ROUTING_UNAVAILABLE');
  assert.ok(noMutation(h));assert.equal(firstCall(h,'timesheet_correction_prepare_v1'),-1);
 }
});
test('actual Source check-in-progress409 survives exact current helper without prepare or economics',async()=>{
 const h=harness({users:[payUser],scopeError:{json:{message:'SOURCE_CHECK_IN_PROGRESS'}}});
 const response=await h.actual.handleTimesheetCorrectionOfficeRoute(env,request('authorise',action()),'authorise');
 assert.equal(response.status,409);assert.equal((await response.json()).error_code,'SOURCE_CHECK_IN_PROGRESS');assert.ok(noMutation(h));
});
test('actual not-found Source scope cannot admit missing P0 or become ordinary fallback',async()=>{
 const h=harness({users:[payUser],scopeError:{json:{message:'WEEKLY_SOURCE_TIMESHEET_NOT_FOUND'}}});
 assert.equal((await h.actual.handleTimesheetCorrectionOfficeRoute(env,request('authorise',action()),'authorise')).status,403);assert.ok(noMutation(h));
});
test('lost flags or changed actor on fresh recheck denies before request authority or producer',async()=>{
 for(const changed of [{...payUser,payment_authoriser:false},{...payUser,id:id(92)}]) {
  const h=harness({users:[payUser,changed]});assert.equal((await h.actual.handleTimesheetCorrectionOfficeRoute(env,request('authorise',action()),'authorise')).status,403);
  assert.ok(noMutation(h));assert.equal(firstCall(h,'timesheet_correction_request_authority_v1'),-1);
 }
});
test('current Source ordinary selected lifecycle scopes real member before native bulk while management remains closed',async()=>{
 for(const action of ['AUTHORISE','UNAUTHORISE']) {
  const h=harness({users:[payUser],projection:null});const response=await h.actual.handleTimesheetCorrectionOfficeRoute(env,
   request('lifecycle',{action,items:[{timesheet_id:I.original}]}),'runLifecycle');
  assert.equal(response.status,200,JSON.stringify(await response.clone().json()));
  assert.deepEqual(probeIds(h),[I.original,I.original]);
  assert.ok(firstCall(h,'weekly_source_office_authorise_scope_v1')<firstCall(h,'timesheet_correction_request_projection_v1'));
  assert.ok(h.calls.findLastIndex(c=>c[0]==='weekly_source_office_authorise_scope_v1')<firstCall(h,action==='AUTHORISE'?'timesheet_authorise_bulk_atomic':'timesheet_unauthorise_bulk_atomic'));
 }
});
test('any nonapplicable selected current row refuses full resolution before pair/prepare/mutation',async()=>{
 const h=harness({users:[payUser],projection:null,scope:ts=>({contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:ts!==I.original})});
 const response=await h.actual.handleTimesheetCorrectionOfficeRoute(env,request('lifecycle',{
  action:'AUTHORISE',items:[{timesheet_id:I.p0},{timesheet_id:I.original}]}),'runLifecycle');
 assert.equal(response.status,403);assert.equal(firstCall(h,'timesheet_correction_request_projection_v1'),-1);assert.ok(noMutation(h));
});
function appliedPayHarness(options={}) {
 return harness({users:[payUser],physical:ts=>({correction_id:I.correction,correction_kind:ts===I.p0?'WHOLE_TIMESHEET_REPLACEMENT':'WHOLE_TIMESHEET_REVERSAL'}),
  ...options,rpc:async(name,args)=>{
   if(name==='timesheet_correction_request_projection_v1')return {schema_version:1,request_id:I.request,root_timesheet_id:I.root,
    reversed_timesheet_id:I.original,replacement_timesheet_id:I.p0,candidate_id:I.candidate,correction_id:I.correction,state:'APPLIED',state_revision:2};
   if(name==='timesheet_correction_request_authority_v1')return {contract_version:'TIMESHEET_CORRECTION_REQUEST_AUTHORITY_V1',request_id:I.request,state:'APPLIED',state_revision:'2',replacement_timesheet_id:I.p0,return_invoice_id:null};
   if(name==='timesheet_correction_prepare_v1')return appliedFixture({replayed:true});
   if(name==='timesheet_correction_pair_transition_v1')return {ok:true,valid:true,action_ready:true,action:args.p_action,
    correction_shape:'WHOLE_REVERSAL_REPLACEMENT',request_id:I.request,root_timesheet_id:I.root,correction_id:I.correction,
    chain_fingerprint:H,expected_member_count:2,transition_items:[{timesheet_id:I.r0,expected_timesheet_id:I.r0,expected_version:1},
     {timesheet_id:I.p0,expected_timesheet_id:I.p0,expected_version:3}],...options.transition};
   if(['timesheet_authorise_bulk_atomic','timesheet_unauthorise_bulk_atomic'].includes(name))return {ok:true,all_success:true,failure_count:0,
    results:args.p_items.map(i=>({success:true,current_timesheet_id:i.timesheet_id}))};
   assert.fail(`Unexpected UNIT_ONLY owner ${name}`);
  }});
}
test('APPLIED scoped payment lifecycle probes real expanded R0 and P0 before unchanged complete native bulk',async()=>{
 for(const action of ['AUTHORISE','UNAUTHORISE']) {
  const h=appliedPayHarness();const response=await h.actual.handleTimesheetCorrectionOfficeRoute(env,
   request('lifecycle',{action,items:[{timesheet_id:I.p0}]}),'runLifecycle');
  assert.equal(response.status,200,JSON.stringify(await response.clone().json()));
  assert.deepEqual(probeIds(h),[I.p0,I.p0,I.r0,I.p0,I.r0,I.p0]);
  assert.ok(h.calls.findLastIndex(c=>c[0]==='weekly_source_office_authorise_scope_v1')<firstCall(h,action==='AUTHORISE'?'timesheet_authorise_bulk_atomic':'timesheet_unauthorise_bulk_atomic'));
 }
});
test('nonapplicable or unknown native-expanded R0 refuses before any financial mutation',async()=>{
 for(const scope of [ts=>({contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:ts!==I.r0}),ts=>ts===I.r0?{}:{contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:true}]) {
  const h=appliedPayHarness({scope});const response=await h.actual.handleTimesheetCorrectionOfficeRoute(env,
   request('lifecycle',{action:'AUTHORISE',items:[{timesheet_id:I.p0}]}),'runLifecycle');
  assert.ok([403,503].includes(response.status));assert.ok(noMutation(h));assert.ok(probeIds(h).includes(I.r0));
 }
});
test('legacy existing pair caller probes each actual preview member before mutation and denies foreign member',async()=>{
 const from=source.indexOf('async function runCorrectionPairLifecycleIfApplicable('),to=source.indexOf('// DY-08:',from);
 const pair=source.slice(from,to);assert.ok(pair.includes('await assertTimesheetCorrectionSourceSubjects(env, req, user'));
 const h=harness();const scopeBlock=wiring.slice(0,wiring.indexOf('function createTimesheetCorrectionOfficeOrchestrator('));
 for(const allowed of [true,false]) {
  const log=[];const f=Function('owners',`const { sbRpc, assertTimesheetCorrectionSourceSubjects, nowIso, weeklySourceUnauthorisePairPreflight }=owners;\n${pair}\nreturn runCorrectionPairLifecycleIfApplicable;`)({
   sbRpc:async(_,name,args)=>{log.push(name);return name==='timesheet_correction_pair_lifecycle_preview_v1'?{correction_pair_count:1,valid:true,
    groups:[{action_ready:true,members:[{timesheet_id:I.r0},{timesheet_id:I.p0}]}]}:{ok:true};},
   assertTimesheetCorrectionSourceSubjects:Function('owners',`const { weeklySourceScopedLifecycleActor }=owners;\n${scopeBlock}\nreturn assertTimesheetCorrectionSourceSubjects;`)({
    weeklySourceScopedLifecycleActor:async(_,__,id,actor)=>{log.push(`scope:${id}`);return (await weeklySourceAuthoriseRouting(async()=>({contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:allowed||id!==I.r0}),id,actor.id)).bound?{ok:true}:{ok:false,response:new Response('{}',{status:401})};}
   }),nowIso:()=> '2026-10-14T12:00:00Z',weeklySourceUnauthorisePairPreflight:async()=>({ordinary:true})
  });
  if(allowed)await f(env,'AUTHORISE',I.p0,I.p0,'unit-signature',{confirm_pair_lifecycle:true},payUser,request('authorise',action()));
  else await assert.rejects(f(env,'AUTHORISE',I.p0,I.p0,'unit-signature',{confirm_pair_lifecycle:true},payUser,request('authorise',action())),/CORRECTION_SOURCE_SCOPE_NOT_ALLOWED/);
  assert.ok(log.includes(`scope:${I.r0}`));assert.equal(log.includes('timesheet_authorise_bulk_atomic'),allowed);
 }
});
test('both actual ordinary pair catch successors preserve503 unknown source scope,403 ordinary denial and409 source hold',()=>{
 const reverse=JSON.parse(fs.readFileSync(new URL('./CURRENT_SCOPED_PAY_FULL_REVERSE.json',import.meta.url)));
 const hunks=reverse.files.find(f=>f.destination==='broker/src/index.js').hunks.filter(h=>h.name.includes('pair-catch'));
 assert.equal(hunks.length,2);
 for(const h of hunks) {
  const start=h.replacement.indexOf('    if ('),end=h.replacement.indexOf('\n    ',start+8);
  // The complete source conditional/return has two lines; use the actual bytes.
  const conditional=h.replacement.slice(start,h.replacement.indexOf('\n    ',end+1));
  const run=Function('error','withJson',`${conditional}\nreturn null;`);
  for(const [code,status] of [['WEEKLY_SOURCE_AUTHORISE_ROUTING_UNAVAILABLE',503],['CORRECTION_SOURCE_SCOPE_NOT_ALLOWED',403],['SOURCE_CHECK_IN_PROGRESS',409]]) {
   const response=run({code,status},(status,body)=>({status,body}));assert.equal(response.status,status);assert.equal(response.body.error_code,code);assert.equal(response.body.retryable,status===503);
  }
  assert.equal(run({code:'UNIT_ONLY unknown',status:503},()=>assert.fail('Unrelated catch changed')),null);
 }
});
