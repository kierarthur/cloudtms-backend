// Proposed scripts/healthroster-f5-native-setup.mjs. Call only from the existing
// protected root owner runner. No connection, transaction, commit or credential
// is opened here. The parent owns permission, transaction and rollback policy.
import {randomUUID} from 'node:crypto';
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const fail=code=>{throw Error(code);};
const literal=s=>"'"+String(s).replaceAll("'","''")+"'";
const values=Object.freeze({checking_profile_kind:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',invoice_sender:'CLIENT_SELF_BILL',
 invoice_amount_basis:'SOURCE_AMOUNT',source_amount_basis:'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY',
 invoice_membership:'FINALISED_IN_CYCLE_WINDOW',line_vat_method:'LINE_GROSS_MINUS_NET_FROM_SOURCE',rate_difference_policy:'FLAG_ALLOW_IGNORE'});
export function f5NativeSetupDiscoverySql({actorUserId,sourceGroupId,sourceCycleId}){
 if(![actorUserId,sourceGroupId,sourceCycleId].every(id=>uuid.test(id)))fail('F5_NATIVE_IDENTITIES_REQUIRED');
 return `with native as (
 select g.*,c.id cycle_id,c.scope_client_id,c.finalisation_week_ending,cl.cli_ref
 from public.weekly_source_groups g join public.weekly_source_cycles c on c.source_group_id=g.id
 join public.clients cl on cl.id=c.scope_client_id
 where g.id=${literal(sourceGroupId)}::uuid and c.id=${literal(sourceCycleId)}::uuid
 and g.environment='TEST' and g.active and g.source_family='ROSTER' and g.code ~ '^WSQ_TEST_[A-Z0-9_]+$'
 and c.finalisation_week_ending<(clock_timestamp() at time zone 'Europe/London')::date
 and c.cutoff_at_utc<clock_timestamp() and c.current_complete_upload_id is null and c.current_projection_publication_id is null
 and c.current_final_revision_id is null and c.state not in('FINALISING','FINALISED','CORRECTION_IN_PROGRESS')
 and private.weekly_source_test_fixture_reference_v1('CLIENT',cl.id,cl.cli_ref)
 and not exists(select 1 from public.weekly_source_uploads u where u.source_cycle_id=c.id)
 and (select count(*) from public.weekly_source_group_clients m where m.source_group_id=g.id
  and c.finalisation_week_ending between m.valid_from and coalesce(m.valid_to,'infinity'::date))=1
 and exists(select 1 from public.weekly_source_group_clients m where m.source_group_id=g.id and m.client_id=cl.id
  and c.finalisation_week_ending between m.valid_from and coalesce(m.valid_to,'infinity'::date))
 and private.weekly_source_pay_actor_allowed_v1(${literal(actorUserId)}::uuid)
), contract_scope as (
 select k.id,k.candidate_id,p.tms_ref from public.contracts k join native n on n.scope_client_id=k.client_id
 join public.candidates p on p.id=k.candidate_id
 where daterange(k.start_date,k.end_date,'[]') && daterange(n.finalisation_week_ending-6,n.finalisation_week_ending,'[]')
), assignment as (
 select a.*,to_jsonb(a) native_row,v.values_sha256,v.values_json from public.weekly_source_group_rule_assignments a
 join native n on n.id=a.source_group_id and n.finalisation_week_ending between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
 join public.weekly_source_rule_set_versions v on v.id=a.rule_set_version_id
), future_assignment as (
 select a.*,to_jsonb(a) native_row,v.values_sha256,v.values_json from public.weekly_source_group_rule_assignments a
 join native n on n.id=a.source_group_id join public.weekly_source_rule_set_versions v on v.id=a.rule_set_version_id
 where a.effective_from_week_ending>=(clock_timestamp() at time zone 'Europe/London')::date
), release as (select r.* from private.cloudtms_database_releases r order by started_at_utc desc,release_id desc limit 1)
select jsonb_build_object('execution',private.weekly_source_protected_qualification_release_v1(),
 'controls',(select evidence_json->'weekly_source_off_rule_test_controls' from release),
 'expires_at_utc',to_char((clock_timestamp()+interval '1 hour') at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
 'agency_id',n.agency_id,'group_version',n.version::text,'client_id',n.scope_client_id,'group_code',n.code,'week_ending',n.finalisation_week_ending,
 'future_week_ending',((clock_timestamp() at time zone 'Europe/London')::date+14+(7-extract(dow from (clock_timestamp() at time zone 'Europe/London')::date+14)::integer)%7),
 'future_assignment_count',(select count(*) from future_assignment),'future_assignment',(select to_jsonb(a) from future_assignment a),
 'future_assignment_sha256',(select encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFF_RULE_ASSIGNMENT_V1',a.native_row),'hex') from future_assignment a),
 'assignment_count',(select count(*) from assignment),'assignment',(select to_jsonb(a) from assignment a),
 'assignment_version',(select a.version::text from assignment a),
 'assignment_sha256',(select encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFF_RULE_ASSIGNMENT_V1',
   a.native_row),'hex') from assignment a),
 'fixture_manifest',jsonb_build_object('group_code',n.code,'client_ids',jsonb_build_array(n.scope_client_id),
  'contract_ids',(select jsonb_agg(id order by id) from contract_scope),
  'candidate_ids',(select jsonb_agg(candidate_id order by candidate_id) from (select distinct candidate_id from contract_scope) p),
  'client_refs',jsonb_build_array(jsonb_build_object('client_id',n.scope_client_id,'cli_ref',n.cli_ref)),
  'candidate_refs',(select jsonb_agg(jsonb_build_object('candidate_id',candidate_id,'tms_ref',tms_ref) order by candidate_id)
   from(select distinct candidate_id,tms_ref from contract_scope) p)))::text from native n where not exists(select 1 from contract_scope k
 where not private.weekly_source_test_fixture_reference_v1('CANDIDATE',k.candidate_id,k.tms_ref));`;
}
export async function prepareF5NativeSetup({session,actorUserId,sourceGroupId,sourceCycleId,buildAuthorization,turnBackendPid}){
 if(typeof session?.execute!=='function'||typeof buildAuthorization!=='function')fail('F5_EXISTING_PROTECTED_OWNER_REQUIRED');
 const sql=f5NativeSetupDiscoverySql({actorUserId,sourceGroupId,sourceCycleId});
 await session.execute('select private.weekly_source_protected_qualification_owner_v1();');
 await session.execute('select private._weekly_source_settings_require_admin_v1('+literal(actorUserId)+'::uuid);');
 const discover=async()=>{
  const raw=(await session.execute(sql)).trim();
  if(!raw)fail('F5_GENUINE_FIXTURE_PARTICIPANTS_REQUIRED');
  const state=JSON.parse(raw);
  if(state.execution?.environment!=='TEST'||state.execution?.database!=='cloudtms_test_clone'
   ||!state.fixture_manifest?.contract_ids?.length||!state.fixture_manifest?.candidate_ids?.length
   ||state.fixture_manifest.candidate_refs.some(row=>!/^CCR-[0-9]+$/.test(row.tms_ref))
   ||state.assignment_count>1||state.future_assignment_count>1||!/^\d{4}-\d{2}-\d{2}$/.test(state.week_ending))fail('F5_GENUINE_FIXTURE_PARTICIPANTS_REQUIRED');
  return state;
 };
 let state=await discover();
 if(!state.controls)fail('F5_GENUINE_NATIVE_CONTROLS_REQUIRED');
 const base={actor_user_id:actorUserId,agency_id:state.agency_id,environment:'TEST',source_group_id:sourceGroupId};
 const rpc=async(name,request)=>JSON.parse(await session.execute('select public.'+name+'('+literal(JSON.stringify(request))+'::jsonb)::text;'));
 if(state.assignment_count===0){
  if(!Number.isSafeInteger(turnBackendPid)||turnBackendPid<1)fail('F5_ACTUAL_SEPARATE_TURN_LOCK_PID_REQUIRED');
  const groupVersion=()=>{const n=Number(state.group_version);if(!Number.isSafeInteger(n)||n<1)fail('F5_NATIVE_GROUP_VERSION_UNREPRESENTABLE');return n;};
  if(state.future_assignment_count===0){
   const saved=await rpc('weekly_source_rule_set_version_save_atomic_v1',{...base,request_id:randomUUID(),
    rule_set:{id:null,code:'WSQ_TEST_F5_MONEY_'+sourceGroupId.replaceAll('-','').toUpperCase(),display_name:'F5 native Source money fixture',expected_version:0},values});
   await rpc('weekly_source_group_rule_assign_atomic_v1',{...base,request_id:randomUUID(),rule_set_version_id:saved.rule_set_version_id,
    effective_from_week_ending:state.future_week_ending,expected_group_version:groupVersion()});
   state=await discover();
  }
  const reference=state.future_assignment;
  if(state.future_assignment_count!==1||reference.feature_enabled!==false||Object.entries(values).some(([k,v])=>reference.values_json?.[k]!==v)
   ||!/^[a-f0-9]{64}$/.test(state.future_assignment_sha256))fail('F5_GENUINE_DISABLED_FUTURE_REFERENCE_REQUIRED');
  const request={schema_version:'WEEKLY_SOURCE_HISTORICAL_FIXTURE_ASSIGNMENT_REQUEST_V1',expected_execution:state.execution,turn_backend_pid:turnBackendPid,
   source_cycle_id:sourceCycleId,future_reference_assignment_id:reference.id,expected_future_assignment_sha256:state.future_assignment_sha256,
   fixture_manifest:state.fixture_manifest,native_request:{...base,rule_set_version_id:reference.rule_set_version_id,effective_from_week_ending:state.week_ending,
    effective_to_week_ending:state.week_ending,expected_group_version:groupVersion(),request_id:randomUUID()},reason:'Genuine disabled past open-cycle F5 synthetic regression setup'};
  await session.execute('select private.weekly_source_historical_fixture_rule_assign_v1('+literal(actorUserId)+'::uuid,'+literal(JSON.stringify(request))+'::jsonb)::text;');
  state=await discover(); // Actual native historical audit, receipt, clocks/hash.
 }
 const assignment=state.assignment;
 if(state.assignment_count!==1||assignment.feature_enabled!==false
  ||Object.entries(values).some(([key,value])=>assignment.values_json?.[key]!==value))fail('F5_EXISTING_OFF_ASSIGNMENT_REQUIRED');
 const controls=state.controls;
 if(!controls)fail('F5_GENUINE_NATIVE_CONTROLS_REQUIRED');
 const input={authorization_id:randomUUID(),scope_id:randomUUID(),agency_id:state.agency_id,source_group_id:sourceGroupId,
  source_cycle_id:sourceCycleId,setup_actor_user_id:actorUserId,journey_actor_user_id:actorUserId,purpose:'SOURCE_FIXTURE_SETUP',
  week_ending:state.week_ending,expires_at_utc:state.expires_at_utc,fixture_namespace:'WSQ_TEST_',fixture_manifest:state.fixture_manifest,
  assignment_id:assignment.id,assignment_version:state.assignment_version,assignment_sha256:state.assignment_sha256,
  rule_set_version_id:assignment.rule_set_version_id,rule_values_sha256:assignment.values_sha256,original_source_cas:null,
  allowed_operations:['weekly_source_upload_stage_begin_atomic_v1','weekly_source_upload_stage_rows_atomic_v1',
   'weekly_source_upload_seal_atomic_v1','weekly_source_invoice_hold_release_atomic_v1'].sort(),
  banking_install_receipt_sha256:controls.banking_install_receipt_sha256,security_receipt_sha256:controls.security_receipt_sha256,
  source_effects_review_sha256:controls.source_effects_review_sha256};
 const context={environment:'TEST',expectedDatabase:'cloudtms_test_clone',mode:'UPGRADE',releaseId:state.execution.release_id,
  gitCommit:state.execution.source_commit,expectedHash:state.execution.contract_sha256};
 // The existing builder validates real controls. Output is not authority until
 // committed and recorded by the existing protected post-VERIFIED writer.
 const authorization=buildAuthorization(context,input,controls);
 return {input,controls,authorization,execution:state.execution,qualified:false,
  next_owner:'existing committed receipt bundle + recordPostVerifiedEvidence; then mandatory F5 rollback gate'};
}
