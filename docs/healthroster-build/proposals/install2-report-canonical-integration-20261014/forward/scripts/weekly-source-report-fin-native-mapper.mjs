// Fixed native mapper, not a callback-selected qualifier. Import opens no
// connection. Only the root's existing owner rollback session executes queries.
import {exact,sha256} from './weekly-source-protected-release-qualification.mjs';
import {reportFinNativeSeal} from './weekly-source-report-fin-native-seal.mjs';
import {validateNativeFinLockObservation,nativeOrdinaryRequestSha256} from './weekly-source-report-fin-native-lock-observation.mjs';
const fail=code=>{throw Error(code);},hex=/^[0-9a-f]{64}$/,uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b),q=v=>"'"+String(v).replaceAll("'","''")+"'";
const decode=bytes=>{if(typeof bytes!=='string'||Buffer.byteLength(bytes)>256*1024)fail('REPORT_NATIVE_MAPPER_BYTES_INVALID');return JSON.parse(bytes);};
export const reportFinNativeOwnerSignatures=Object.freeze([
 'private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text)',
 'private.weekly_source_report_fin_dispatch_v1(jsonb)',
 'public.weekly_source_group_rule_feature_set_atomic_v1(jsonb)',
 'public.weekly_source_invoice_admit_atomic_v1(jsonb)',
 'private.weekly_source_invoice_allocation_assert_v1(uuid)',
 'private.weekly_source_invoice_issue_validate_v1(uuid)',
 'private._invoice_presentation_snapshot_batch(jsonb,timestamp with time zone)',
 'private.weekly_source_self_bill_report_pay_authority_v1(uuid)',
 'private.weekly_source_self_bill_report_pay_witness_v1(uuid)',
 'private.weekly_source_self_bill_report_prior_binding_v1(uuid)',
 'private.weekly_source_lock_family_rows_v1(uuid[],uuid)',
 'private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text)',
 'private.weekly_source_candidate_serial_gate_v1(uuid,text,uuid,text)',
 'private.weekly_source_resolve_root_identity_v1(uuid)',
 'private.weekly_source_effective_inventory_v1(uuid)',
 'private.weekly_source_protected_final_source_context_v1(uuid,uuid,uuid)',
 'private.weekly_source_actual_timesheet_submission_v1(uuid)',
 'private.weekly_source_protected_wait_witness_v1(uuid,uuid)',
 'private.weekly_source_protected_wait_receipt_v1(uuid,uuid)',
 'private.bpay_next_source_origin_state_v1(uuid,uuid)',
 'public.complete_local_v1(jsonb)',
 'public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)',
 'public._pay_workbench_candidate_serial_try_gate(uuid,uuid,text,jsonb,text)',
 'private._weekly_source_effective_policy_v1(uuid,uuid,date)',
 'private.weekly_source_rule_qualification_context_v1(uuid,uuid)',
 'private.weekly_source_bulk_source_cas_v1(uuid,uuid[])',
 'private.weekly_source_bulk_source_facts_v1(uuid,uuid[])',
 'private.weekly_source_protected_qualification_release_v1()',
 'public._audit_insert(text,text,text,jsonb,jsonb,text,uuid)'
]);
export const nativeGateSlots=Object.freeze(['native_candidate_i1','ordinary_nhsp','origin_authority','no_external_effects']);
export const nativeCandidateJobTypes=Object.freeze(['WORKBENCH_CANDIDATE_FIRST_AUTHORISATION','WORKBENCH_CANDIDATE_FIRST_AUTHORISATION_WITHDRAWAL',
 'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION','WORKBENCH_CANDIDATE_PENDING_ENTITLEMENT_RELEASE']);
export const nativeEffectRelations=Object.freeze(['public.banking_pay_operation_provider_attempts','public.banking_pay_operation_settlement_scope',
 'public.pay_bank_transfers','public.weekly_message_provider_attempts','public.mail_outbox','public.comms_outbox',
 'public.hr_issue_email_deliveries','public.hr_issue_email_delivery_items','public.hr_issue_emails']);
const witnessKeys=['bound_pay_snapshot_line_id','bound_pay_final_revision_id','bound_pay_projection_receipt_id',
 'published_timesheet_financial_id','invoice_timesheet_id','candidate_id','client_id','contract_id','work_event_id'];
export const reportPayAuthorityKeys=Object.freeze([...witnessKeys,'schema_version','binding_id','bound_row_resolution_id','bound_export_projection_publication_id',
 'invoice_ready','native_pay_position','unavailable_reason','native_projection_receipt_hash','native_pay_run_id','native_pay_task_id',
 'approved_inventory_basis','qualified_wait','current_publication_receipt','next_application','actual_submission',
 'prepared_pay_ex_vat','prepared_amount_known','prepared_amount_unavailable_reason','active_pay_ex_vat','active_amount_scope',
 'withheld_pay_ex_vat','withheld_amount_known','authority_sha256']);
const integer=v=>typeof v==='string'&&/^(0|-?[1-9][0-9]{0,14})$/.test(v);
// Case predicates are measured economic/provenance predicates. No label is
// permitted to turn a positive native movement into a negative test case.
export function deriveReportCaseGrain(caseId,basis,financial){
 const lines=financial.movements.filter(m=>m.source_line_kind==='SELF_BILL_REPORT_LINE'),negative=[];
 for(const row of basis.rows){const m=lines.find(value=>value.self_bill_report_row_id===row.id);if(!m)fail('REPORT_NATIVE_CASE_MEMBERSHIP_INVALID');
  if(!Number.isSafeInteger(row.source_shift_charge_pence)||!Number.isSafeInteger(row.source_vat_pence)||!Number.isSafeInteger(row.source_gross_pence))fail('REPORT_NATIVE_CASE_ECONOMICS_INVALID');
  if(row.source_shift_charge_pence<0){if(!['REVERSAL','STANDALONE_NEGATIVE'].includes(m.movement_role))fail('REPORT_NATIVE_NEGATIVE_ROLE_INVALID');negative.push({row,m});}
  else if(m.movement_role!=='POSITIVE')fail('REPORT_NATIVE_POSITIVE_ROLE_INVALID');
 }
 if(caseId==='BOUND_NEGATIVE'){
  const found=negative.filter(({m})=>m.movement_role==='REVERSAL'&&uuid.test(m.prior_movement_id)
   &&m.source_facts_json?.prior_report_movement_id===m.prior_movement_id
   &&['candidate_id','contract_id','work_event_id','invoice_timesheet_id'].every(k=>uuid.test(m[k]))
   &&(m.source_facts_json.published_timesheet_financial_id===null||uuid.test(m.source_facts_json.published_timesheet_financial_id))
   &&['bound_pay_snapshot_line_id','bound_pay_final_revision_id','bound_pay_projection_receipt_id',
      'report_line_binding_id','bound_row_resolution_id','bound_export_projection_publication_id'].every(k=>uuid.test(m.source_facts_json[k])));
  if(!found.length)fail('REPORT_NATIVE_BOUND_NEGATIVE_GRAIN_UNPROVED');
 }
 if(caseId==='STANDALONE_NEGATIVE'){
  const found=negative.filter(({row,m})=>m.movement_role==='STANDALONE_NEGATIVE'&&m.prior_movement_id===null
   &&['candidate_id','contract_id','work_event_id','invoice_timesheet_id'].every(k=>m[k]===null)
   &&['prior_report_movement_id','report_line_binding_id','bound_row_resolution_id','bound_export_projection_publication_id'].every(k=>m.source_facts_json[k]===null)
   &&!basis.bindings.some(b=>b.report_row_id===row.id));
  if(!found.length)fail('REPORT_NATIVE_STANDALONE_NEGATIVE_GRAIN_UNPROVED');
 }
 if(!integer(financial.rounding_pence))fail('REPORT_NATIVE_CASE_ROUNDING_UNPROVED');
 const rounding=BigInt(financial.rounding_pence),sign={D_NEGATIVE:-1,D_POSITIVE:1,D_ZERO:0}[caseId];
 if(sign!==undefined&&(rounding===0n?0:rounding<0n?-1:1)!==sign)fail('REPORT_NATIVE_CASE_ROUNDING_SIGN_MISMATCH');
 return {negative_row_ids:negative.map(({row})=>row.id),rounding_pence:financial.rounding_pence};
}
export function assertReportPayAuthority(raw,baseBytes,binding){
 exact(raw,reportPayAuthorityKeys,'REPORT_NATIVE_PAY_AUTHORITY_SHAPE_INVALID');
 const base=decode(baseBytes),{authority_sha256:ignored,...value}=raw;
 if(!same(base,value)||raw.schema_version!=='WEEKLY_SOURCE_REPORT_PAY_AUTHORITY_V1'||raw.binding_id!==binding.id
  ||raw.bound_row_resolution_id!==binding.bound_row_resolution_id||raw.bound_export_projection_publication_id!==binding.export_projection_publication_id
  ||raw.work_event_id!==binding.work_event_id||raw.invoice_ready!==true||raw.unavailable_reason!==null
  ||!hex.test(raw.authority_sha256)||sha256('WEEKLY_SOURCE_REPORT_PAY_AUTHORITY_V1\x1f'+baseBytes)!==raw.authority_sha256
  ||raw.active_amount_scope!=='ROOT_FAMILY'||typeof raw.prepared_amount_known!=='boolean'||typeof raw.withheld_amount_known!=='boolean')
  fail('REPORT_NATIVE_PAY_AUTHORITY_UNPROVED');
 const waiting=[];
 for(const [known,amount,name] of [['prepared_amount_known','prepared_pay_ex_vat','native_prepared_amount'],['withheld_amount_known','withheld_pay_ex_vat','native_withheld_amount']]){
  if(raw[known]!==true){if(raw[amount]!==null)fail('REPORT_NATIVE_UNKNOWN_AMOUNT_CHANGED');waiting.push(name);}
  else if(typeof raw[amount]!=='string'||!/^-?[0-9]+(?:\.[0-9]+)?$/.test(raw[amount]))fail('REPORT_NATIVE_AMOUNT_INVALID');
 }
 return {waiting};
}
export function validateNegativeProvenance(entries,context){
 const negative=context.basis.rows.filter(row=>row.source_shift_charge_pence<0);
 if(!Array.isArray(entries)||entries.length!==negative.length||new Set(entries.map(e=>e.report_row_id)).size!==negative.length)fail('REPORT_NATIVE_NEGATIVE_PRIOR_CENSUS_INVALID');
 for(const e of entries){exact(e,['report_row_id','prior_bytes','prior_movement','prior_binding','prior_upload_row','prior_report','prior_revision',
   'pay_snapshot','pay_revision','pay_receipt','pay_financial_identity'],'REPORT_NATIVE_NEGATIVE_PRIOR_ENTRY_INVALID');
  const row=negative.find(row=>row.id===e.report_row_id),movement=context.financial.movements.find(m=>m.self_bill_report_row_id===e.report_row_id);
  if(!row||!movement)fail('REPORT_NATIVE_NEGATIVE_PRIOR_UNBOUND');
  if(e.prior_bytes===null){
   if(movement.movement_role!=='STANDALONE_NEGATIVE'||movement.prior_movement_id!==null
    ||['prior_movement','prior_binding','prior_upload_row','prior_report','prior_revision','pay_snapshot','pay_revision','pay_receipt'].some(k=>e[k]!==null)
    ||['candidate_id','contract_id','work_event_id','invoice_timesheet_id'].some(k=>movement[k]!==null))fail('REPORT_NATIVE_STANDALONE_PRIOR_CHANGED');
   const identity=e.pay_financial_identity;exact(identity,['id','timesheet_id','client_id','candidate_id'],'REPORT_NATIVE_PRIOR_FINANCIAL_IDENTITY_INVALID');
   if(Object.values(identity).some(v=>v!==null))fail('REPORT_NATIVE_STANDALONE_FINANCIAL_ORIGIN_INVENTED');continue;
  }
  const selected=decode(e.prior_bytes),prior=e.prior_movement,b=e.prior_binding,original=e.prior_upload_row,report=e.prior_report,revision=e.prior_revision,
   facts=prior?.source_facts_json,paySnapshot=e.pay_snapshot,payRevision=e.pay_revision,receipt=e.pay_receipt;
  exact(selected,['prior_movement_id','row_id','resolution_id','publication_id','work_event_id'],'REPORT_NATIVE_PRIOR_SELECTOR_INVALID');
  if(movement.movement_role!=='REVERSAL'||movement.prior_movement_id!==selected.prior_movement_id||prior?.id!==selected.prior_movement_id
   ||prior.source_line_kind!=='SELF_BILL_REPORT_LINE'||prior.source_profile_kind!=='SELF_BILL_REPORT'||prior.movement_role!=='POSITIVE'
   ||prior.actual_client_id!==context.basis.scope.client_id||prior.self_bill_report_row_id!==original?.id
   ||original.external_source_key!==row.external_source_key||original.source_shift_charge_pence<=0
   ||facts?.report_line_binding_id!==b?.id||b.report_row_id!==original.id||b.self_bill_report_id!==report?.id
   ||b.export_upload_row_id!==selected.row_id||b.bound_row_resolution_id!==selected.resolution_id||b.export_projection_publication_id!==selected.publication_id
   ||b.work_event_id!==selected.work_event_id||prior.work_event_id!==b.work_event_id||revision?.id!==prior.final_revision_id
   ||revision.state!=='CURRENT'||revision.authority_scope_kind!=='SELF_BILL_REPORT_SCOPE'||revision.report_scope_id!==facts.report_scope_id
   ||report.source_group_id!==context.basis.scope.source_group_id||report.client_id!==context.basis.scope.client_id
   ||typeof report.invoice_date!=='string'||typeof context.basis.report.invoice_date!=='string'||report.invoice_date>=context.basis.report.invoice_date
   ||['candidate_id','contract_id','work_event_id','invoice_timesheet_id'].some(k=>!uuid.test(prior[k])||movement[k]!==prior[k])
   ||!context.basis.authorization.fixture_manifest.candidate_ids.includes(prior.candidate_id)
   ||!context.basis.authorization.fixture_manifest.contract_ids.includes(prior.contract_id))fail('REPORT_NATIVE_BOUND_PRIOR_PROVENANCE_INVALID');
  if(paySnapshot?.id!==facts.bound_pay_snapshot_line_id||payRevision?.id!==facts.bound_pay_final_revision_id||receipt?.id!==facts.bound_pay_projection_receipt_id
   ||paySnapshot.final_revision_id!==payRevision.id||paySnapshot.work_event_id!==prior.work_event_id||paySnapshot.client_id!==prior.actual_client_id
   ||paySnapshot.contract_id!==prior.contract_id||paySnapshot.candidate_id!==prior.candidate_id
   ||receipt.final_revision_id!==payRevision.id||receipt.root_timesheet_id!==prior.invoice_timesheet_id||receipt.client_id!==prior.actual_client_id
   ||receipt.published_timesheet_financial_id!==facts.published_timesheet_financial_id)fail('REPORT_NATIVE_BOUND_PRIOR_PAY_PROVENANCE_INVALID');
  for(const key of ['bound_pay_snapshot_line_id','bound_pay_final_revision_id','bound_pay_projection_receipt_id','published_timesheet_financial_id'])
   if(movement.source_facts_json[key]!==facts[key])fail('REPORT_NATIVE_REVERSAL_PAY_WITNESS_CHANGED');
  const identity=e.pay_financial_identity;exact(identity,['id','timesheet_id','client_id','candidate_id'],'REPORT_NATIVE_PRIOR_FINANCIAL_IDENTITY_INVALID');
  if(facts.published_timesheet_financial_id===null){if(Object.values(identity).some(v=>v!==null))fail('REPORT_NATIVE_PRIOR_FINANCIAL_ORIGIN_INVENTED');}
  else if(identity.id!==facts.published_timesheet_financial_id||identity.timesheet_id!==prior.invoice_timesheet_id||identity.client_id!==prior.actual_client_id||identity.candidate_id!==prior.candidate_id)
   fail('REPORT_NATIVE_PRIOR_FINANCIAL_IDENTITY_CHANGED');
 }
 return true;
}
function mount(execution){
 if(reportFinNativeSeal===null)return null;
 exact(reportFinNativeSeal,['schema_version','contract_sha256','managed_manifest_sha256','installed_catalogue_jsonb_sha256','owners','hooks','effect_relations','mapper_query_sha256','tool_source_sha256'], 'REPORT_NATIVE_SEAL_INVALID');
 if(reportFinNativeSeal.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_NATIVE_SEAL_V1'
  ||!['contract_sha256','managed_manifest_sha256','installed_catalogue_jsonb_sha256'].every(k=>hex.test(reportFinNativeSeal[k])&&reportFinNativeSeal[k]===execution[k])
  ||!Array.isArray(reportFinNativeSeal.owners)||reportFinNativeSeal.owners.length!==reportFinNativeOwnerSignatures.length
  ||new Set(reportFinNativeSeal.owners.map(o=>o.signature)).size!==reportFinNativeOwnerSignatures.length
  ||reportFinNativeSeal.owners.some(o=>{exact(o,['signature','definition_sha256','anon_execute','authenticated_execute'],'REPORT_NATIVE_SEAL_INVALID');return !reportFinNativeOwnerSignatures.includes(o.signature)||!hex.test(o.definition_sha256)
    ||typeof o.anon_execute!=='boolean'||typeof o.authenticated_execute!=='boolean';})
  ||!Array.isArray(reportFinNativeSeal.hooks)||!reportFinNativeSeal.hooks.length||reportFinNativeSeal.hooks.length>1024
  ||!same(reportFinNativeSeal.effect_relations,nativeEffectRelations)
  ||reportFinNativeSeal.mapper_query_sha256!==sha256(reportNativeMapperQueries.toString())||!hex.test(reportFinNativeSeal.tool_source_sha256))
  fail('REPORT_NATIVE_SEAL_INVALID');
 return reportFinNativeSeal;
}
// Query strings and their semantic decoders are compiled source. The root seal
// pins this query-family digest plus literal native definitions and hook census.
export function reportNativeMapperQueries(nativeRequest,authorization){
 if(!uuid.test(nativeRequest.actor_user_id)||!uuid.test(nativeRequest.source_cycle_id)||!uuid.test(nativeRequest.report_scope_id)
  ||!authorization?.fixture_manifest?.contract_ids?.length||authorization.fixture_manifest.contract_ids.some(v=>!uuid.test(v)))fail('REPORT_NATIVE_MAPPER_SCOPE_INVALID');
 const r=nativeRequest,contracts=authorization.fixture_manifest.contract_ids.map(v=>q(v)+'::uuid').join(','),ownerValues=reportFinNativeOwnerSignatures.map(v=>'('+q(v)+')').join(',');
 return {
  session:`select jsonb_build_object('database',current_database(),'backend_pid',pg_backend_pid()::text,'transaction_id',pg_current_xact_id_if_assigned()::text,
   'session_user',session_user,'current_user',current_user,'is_owner',session_user=(select tableowner from pg_tables where schemaname='private' and tablename='cloudtms_database_release_executions'),
   'actor', (select jsonb_build_object('id',id,'is_active',is_active,'role',role) from public.tms_users where id=${q(r.actor_user_id)}::uuid),
   'managed_shared_lock',exists(select 1 from pg_locks where pid=pg_backend_pid() and locktype='advisory' and mode='ShareLock' and granted
     and classid=((hashtextextended('cloudtms_database_release_admission_v1',0)>>32)&4294967295)::oid
     and objid=(hashtextextended('cloudtms_database_release_admission_v1',0)&4294967295)::oid and objsubid=1))::text;`,
  owners:`select coalesce(jsonb_agg(jsonb_build_object('signature',requested.signature,'definition_sha256',encode(extensions.digest(pg_get_functiondef(proc.oid),'sha256'),'hex'),
   'anon_execute',has_function_privilege('anon',proc.oid,'EXECUTE'),'authenticated_execute',has_function_privilege('authenticated',proc.oid,'EXECUTE')) order by requested.signature),'[]'::jsonb)::text
   from (values ${ownerValues})requested(signature) left join pg_proc proc on proc.oid=to_regprocedure(requested.signature);`,
  roots:`select coalesce(jsonb_agg(jsonb_build_object('timesheet_id',t.timesheet_id,'candidate_id',c.candidate_id,'contract_id',t.contract_id,
   'booking_id',t.booking_id,'identity',private.weekly_source_resolve_root_identity_v1(t.timesheet_id)) order by t.timesheet_id),'[]'::jsonb)::text
   from public.timesheets t join public.contracts c on c.id=t.contract_id where t.contract_id=any(array[${contracts}]::uuid[]) and t.is_current
    and not t.is_adjustment and t.revoked_at is null and t.archived_at_utc is null;`,
  empty_set:`select private.weekly_source_lock_family_rows_v1(array[]::uuid[],null)::text;`,
  pay_authorities:`select coalesce(jsonb_agg(jsonb_build_object('binding',to_jsonb(b),'authority_bytes',a.value::text,
    'authority_base_bytes',(a.value-'authority_sha256')::text,'pay_witness_bytes',private.weekly_source_self_bill_report_pay_witness_v1(b.id)::text) order by b.id),'[]'::jsonb)::text
   from public.weekly_source_self_bill_report_line_bindings b cross join lateral (select private.weekly_source_self_bill_report_pay_authority_v1(b.id) value) a
   where b.self_bill_report_id=(select self_bill_report_id from public.weekly_source_report_scopes where id=${q(r.report_scope_id)}::uuid) and b.state='CURRENT';`,
  priors:`select coalesce(jsonb_agg(jsonb_build_object('report_row_id',row.id,'prior_bytes',prior.value::text,
    'prior_movement',to_jsonb(m),'prior_binding',to_jsonb(b),'prior_upload_row',to_jsonb(original),'prior_report',to_jsonb(report),'prior_revision',to_jsonb(revision),
    'pay_snapshot',to_jsonb(snapshot),'pay_revision',to_jsonb(pay_revision),'pay_receipt',to_jsonb(receipt),
    'pay_financial_identity',jsonb_build_object('id',financial.id,'timesheet_id',financial.timesheet_id,'client_id',financial.client_id,'candidate_id',financial.candidate_id)) order by row.source_row_ordinal),'[]'::jsonb)::text
   from public.weekly_source_upload_rows row cross join lateral (select private.weekly_source_self_bill_report_prior_binding_v1(row.id) value) prior
   left join public.weekly_source_billing_movements m on m.id=(prior.value->>'prior_movement_id')::uuid
   left join public.weekly_source_self_bill_report_line_bindings b on b.id=(m.source_facts_json->>'report_line_binding_id')::uuid
   left join public.weekly_source_upload_rows original on original.id=m.self_bill_report_row_id
   left join public.weekly_source_report_scopes s on s.id=(m.source_facts_json->>'report_scope_id')::uuid
   left join public.weekly_source_self_bill_reports report on report.id=s.self_bill_report_id
   left join public.weekly_source_final_revisions revision on revision.id=m.final_revision_id
   left join public.weekly_source_final_snapshot_lines snapshot on snapshot.id=(m.source_facts_json->>'bound_pay_snapshot_line_id')::uuid
   left join public.weekly_source_final_revisions pay_revision on pay_revision.id=(m.source_facts_json->>'bound_pay_final_revision_id')::uuid
   left join public.weekly_source_ordinary_pay_projection_receipts receipt on receipt.id=(m.source_facts_json->>'bound_pay_projection_receipt_id')::uuid
   left join public.timesheets_financials financial on financial.id=(m.source_facts_json->>'published_timesheet_financial_id')::uuid
   where row.upload_id=${q(r.upload_id)}::uuid and row.source_shift_charge_pence<0;`,
  inventories:`select coalesce(jsonb_agg(jsonb_build_object('root_timesheet_id',t.timesheet_id,'inventory_bytes',private.weekly_source_effective_inventory_v1(t.timesheet_id)::text) order by t.timesheet_id),'[]'::jsonb)::text
   from public.timesheets t where t.contract_id=any(array[${contracts}]::uuid[]) and t.is_current and not t.is_adjustment and t.revoked_at is null and t.archived_at_utc is null;`,
  next_control:`select jsonb_build_object('active_owner',active_owner)::text from private.bpay_next_module_control where id=1;`,
  ordinary_rows:`select coalesce(jsonb_agg(jsonb_build_object('root_timesheet_id',t.timesheet_id,'contract_id',t.contract_id,'client_id',c.client_id,'candidate_id',c.candidate_id,
   'source_mode',private._weekly_source_effective_policy_v1(c.client_id,c.id,t.week_ending_date)->>'c1_source_mode',
   'inventory_bytes',private.weekly_source_effective_inventory_v1(t.timesheet_id)::text,
   'actual_submission_bytes',private.weekly_source_actual_timesheet_submission_v1(t.timesheet_id)::text,
   'financial_rows_sha256',encode(private.weekly_source_sha256_jsonb_v1('REPORT_NATIVE_ORDINARY_FINANCIAL_ROWS_V1',
    (select coalesce(jsonb_agg(to_jsonb(f) order by f.id),'[]'::jsonb) from public.timesheets_financials f where f.timesheet_id=t.timesheet_id)),'hex')) order by t.timesheet_id),'[]'::jsonb)::text
   from public.timesheets t join public.contracts c on c.id=t.contract_id where t.contract_id=any(array[${contracts}]::uuid[]) and t.is_current
    and not t.is_adjustment and t.revoked_at is null and t.archived_at_utc is null;`,
  hooks:`select coalesce(jsonb_agg(jsonb_build_object('schema',namespace.nspname,'table',relation.relname,'name',trigger.tgname,'enabled',trigger.tgenabled::text,
    'definition_sha256',encode(extensions.digest(pg_get_triggerdef(trigger.oid,true),'sha256'),'hex'),
    'function_definition_sha256',encode(extensions.digest(pg_get_functiondef(trigger.tgfoid),'sha256'),'hex')) order by namespace.nspname,relation.relname,trigger.tgname),'[]'::jsonb)::text
    from pg_trigger trigger join pg_class relation on relation.oid=trigger.tgrelid join pg_namespace namespace on namespace.oid=relation.relnamespace
    where not trigger.tgisinternal and namespace.nspname in ('public','private');`,
  effects:'select jsonb_build_array('+nativeEffectRelations.map(table=>`jsonb_build_object('relation',${q(table)},'row_count',(select count(*)::text from ${table}),
    'rows_sha256',encode(private.weekly_source_sha256_jsonb_v1('REPORT_NATIVE_NO_EXTERNAL_EFFECTS_V1',
     (select coalesce(jsonb_agg(to_jsonb(row) order by to_jsonb(row)::text),'[]'::jsonb) from ${table} row)),'hex'))`).join(',')+')::text;'
 };
}
function result(name,sql,bytes){decode(bytes);return {query_id:name,query_sha256:sha256(sql),result_bytes:bytes,result_sha256:sha256(bytes)};}
const beforeNames=['session','owners','hooks','roots','empty_set','pay_authorities','priors','inventories','next_control','ordinary_rows','effects'];
export function reportNativeI1ProbeSql(candidateId,rootIds,jobType){
 if(!uuid.test(candidateId)||!Array.isArray(rootIds)||!rootIds.length||rootIds.some(id=>!uuid.test(id))||new Set(rootIds).size!==rootIds.length
  ||!nativeCandidateJobTypes.includes(jobType))fail('REPORT_NATIVE_I1_PROBE_REQUEST_INVALID');
 return 'select private.weekly_source_lock_and_resolve_families_v1('+q(candidateId)+'::uuid,array['+rootIds.map(id=>q(id)+'::uuid').join(',')
  +']::uuid[],'+q(jobType)+',null::uuid,\'REPORT_NATIVE_I1_RETAINED_HELPER_PROBE\')::text;';
}
async function captureI1Probes(session,roots){
 if(!Array.isArray(roots)||roots.length>10000)fail('REPORT_NATIVE_ORIGINAL_ROOT_SET_INVALID');const probes=[];
 if(!roots.length)return probes;
 // Probe the genuine I1 helper in a separate native savepoint. Its actual four
 // job gates/booking locks are released before Report FIN, which has no I1 arm.
 await session.execute('savepoint report_native_i1_probe;');
 try{for(const candidateId of [...new Set(roots.map(r=>r.candidate_id))].sort()){
  const requested=roots.filter(r=>r.candidate_id===candidateId).map(r=>r.timesheet_id).sort();
  for(const jobType of nativeCandidateJobTypes){const sql=reportNativeI1ProbeSql(candidateId,requested,jobType),bytes=await session.execute(sql);
   probes.push({candidate_id:candidateId,requested_timesheet_ids:requested,job_type:jobType,
    request_sha256:sha256(sql),result_bytes:bytes,result_sha256:sha256(bytes)});}
 }}finally{await session.execute('rollback to savepoint report_native_i1_probe; release savepoint report_native_i1_probe;');}
 return probes;
}
export async function captureReportNativeGateBefore({session,nativeRequest,basis,expectedExecution}){
 if(!mount(expectedExecution))return null;
 const queries=reportNativeMapperQueries(nativeRequest,basis.authorization),records=[];
 for(const name of beforeNames)records.push(result(name,queries[name],await session.execute(queries[name])));
 const roots=decode(records.find(r=>r.query_id==='roots').result_bytes),i1Probes=await captureI1Probes(session,roots);
 return {schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_MAPPER_BEFORE_V1',records,i1_probes:i1Probes};
}
// Native regression receipt capture is a concrete table query, not arbitrary
// expected/observed objects. Root records actual journeys through its normal
// bounded recorder9/13. Its actual compiled slot/output ABI is mounted in the
// next seal step; until then the collector cannot make I1/origin/network claims.
export async function captureReportNativeGateAfter({session,nativeRequest,basis,expectedExecution,before,nativeRegressionFrames}){
 if(!mount(expectedExecution))return null;
 const queries=reportNativeMapperQueries(nativeRequest,basis.authorization),after=result('session',queries.session,await session.execute(queries.session)),
  ordinaryAfter=result('ordinary_rows',queries.ordinary_rows,await session.execute(queries.ordinary_rows)),effectsAfter=result('effects',queries.effects,await session.execute(queries.effects));
 if(nativeRegressionFrames!==undefined)exact(nativeRegressionFrames,['ordinary_fin_journeys','tool_network_census'],'REPORT_NATIVE_REGRESSION_FRAME_SHAPE_INVALID');
 return {native_candidate_i1:{schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_I1_MAPPER_V1',before,after},
  ordinary_nhsp:{schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_ORDINARY_MAPPER_V1',before_record:before.records.find(r=>r.query_id==='ordinary_rows'),after_record:ordinaryAfter,
   native_fin_journeys:nativeRegressionFrames?.ordinary_fin_journeys??null},
  origin_authority:{schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_ORIGIN_MAPPER_V1',before},
  no_external_effects:{schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_EFFECT_MAPPER_V1',before_record:before.records.find(r=>r.query_id==='effects'),after_record:effectsAfter,
   tool_network_census:nativeRegressionFrames?.tool_network_census??null}};
}
function boundRecord(record,name,context){
 exact(record,['query_id','query_sha256','result_bytes','result_sha256'],'REPORT_NATIVE_MAPPER_RECORD_INVALID');
 const sql=reportNativeMapperQueries(context.native_request,context.basis.authorization)[name];
 if(record.query_id!==name||record.query_sha256!==sha256(sql)||record.result_sha256!==sha256(record.result_bytes))fail('REPORT_NATIVE_MAPPER_RECORD_UNBOUND');
 return decode(record.result_bytes);
}
export function validateNativeOrdinaryFinJourneys(journeys,context){
 if(!Array.isArray(journeys)||journeys.length!==2||new Set(journeys.map(j=>j.source_mode)).size!==2)fail('REPORT_NATIVE_ORDINARY_JOURNEY_CENSUS_INVALID');
 for(const j of journeys){exact(j,['schema_version','source_mode','owner_signature','owner_definition_sha256','argument_bytes','native_result_bytes',
   'source_capture_bytes','pay_capture_bytes','native_tool_receipt_bytes','native_lock_observation'],'REPORT_NATIVE_ORDINARY_JOURNEY_INVALID');
  if(j.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_ORDINARY_NATIVE_JOURNEY_V1'||!['HEALTHROSTER_WEEKLY','NHSP_WEEKLY'].includes(j.source_mode)
   ||j.owner_signature!==reportFinNativeOwnerSignatures[0]||!hex.test(j.owner_definition_sha256))fail('REPORT_NATIVE_ORDINARY_JOURNEY_OWNER_INVALID');
  const mounted=reportFinNativeSeal?.owners.find(o=>o.signature===j.owner_signature);
  if(mounted&&j.owner_definition_sha256!==mounted.definition_sha256)fail('REPORT_NATIVE_ORDINARY_JOURNEY_OWNER_CHANGED');
  const args=decode(j.argument_bytes);exact(args,['p_request','p_correction_session_id','p_prepare_only','p_report_vat_rounding_variant'],'REPORT_NATIVE_ORDINARY_FIN_ARGUMENTS_INVALID');
  const r=args.p_request;
  if(!r||r.actor_user_id!==context.native_request.actor_user_id||!uuid.test(r.source_cycle_id)||!uuid.test(r.upload_id)
   ||!uuid.test(r.projection_publication_id)||!['CYCLE','NHSP_REPORT_SCOPE'].includes(r.authority_scope_kind)
   ||args.p_correction_session_id!==null||args.p_prepare_only!==false||args.p_report_vat_rounding_variant!=='A'
   ||!Number.isSafeInteger(r.expected_authority_scope_version)||r.expected_authority_scope_version<1
   ||!['expected_row_manifest_hash','expected_comparison_manifest_hash','expected_issue_set_hash'].every(k=>hex.test(r[k])))fail('REPORT_NATIVE_ORDINARY_FIN_ARGUMENTS_INVALID');
  nativeOrdinaryRequestSha256(r);
  const lock=validateNativeFinLockObservation(j.native_lock_observation,{execution:context.execution,actor_user_id:r.actor_user_id,
   backend_pid:context.backend_pid,transaction_id:context.transaction_id,native_result_bytes:j.native_result_bytes,native_argument_bytes:j.argument_bytes});
  const native=decode(j.native_result_bytes),source=decode(j.source_capture_bytes),pay=decode(j.pay_capture_bytes);
  if(native.ok!==true||native.status!=='FINALISED'||native.invoice_only===true||!uuid.test(native.final_revision_id)||native.source_cycle_id!==r.source_cycle_id)fail('REPORT_NATIVE_ORDINARY_FIN_RESULT_INVALID');
  exact(source,['source_cycle_id','source_group_id','upload_id','source_mode','revision','rows'],'REPORT_NATIVE_ORDINARY_SOURCE_INVALID');
  if(source.source_cycle_id!==r.source_cycle_id||source.source_group_id!==lock.command.authorization.source_group_id||source.upload_id!==r.upload_id||source.source_mode!==j.source_mode
   ||source.revision?.id!==native.final_revision_id||source.revision.source_cycle_id!==r.source_cycle_id||source.revision.upload_id!==r.upload_id
   ||source.revision.state!=='CURRENT'||source.revision.authority_scope_kind!==r.authority_scope_kind
   ||!Array.isArray(source.rows)||!source.rows.length||source.rows.length>10000||new Set(source.rows.map(row=>row.id)).size!==source.rows.length
   ||source.rows.some(row=>!uuid.test(row.id)||row.upload_id!==r.upload_id||row.row_finalisation_state==='SOURCE_REPORT_LINE'))fail('REPORT_NATIVE_ORDINARY_SOURCE_INVALID');
  exact(pay,['run','tasks','receipts','financials'],'REPORT_NATIVE_ORDINARY_PAY_INVALID');
  if(pay.run?.final_revision_id!==native.final_revision_id||pay.run.source_cycle_id!==r.source_cycle_id||pay.run.state!=='COMPLETE'
   ||!uuid.test(pay.run.id)||!Array.isArray(pay.tasks)||!pay.tasks.length||pay.tasks.length!==pay.run.task_count
   ||!Array.isArray(pay.receipts)||pay.receipts.length!==pay.tasks.length||!Array.isArray(pay.financials)
   ||new Set(pay.tasks.map(t=>t.id)).size!==pay.tasks.length||new Set(pay.receipts.map(t=>t.id)).size!==pay.receipts.length
   ||pay.tasks.some(t=>!uuid.test(t.id)||t.run_id!==pay.run.id||!uuid.test(t.root_timesheet_id)||!pay.receipts.some(receipt=>receipt.id===t.projection_receipt_id
     &&receipt.final_revision_id===native.final_revision_id&&receipt.root_timesheet_id===t.root_timesheet_id&&receipt.outcome===t.state)))fail('REPORT_NATIVE_ORDINARY_PAY_INVALID');
  validateNativeToolReceipt(decode(j.native_tool_receipt_bytes),context,{required_result_sha256:sha256(j.native_result_bytes),required_request_sha256:sha256(j.argument_bytes)});
 }
 return true;
}
function validateOrdinarySlot(slot,context,waiting){
 if(slot===null){waiting.push('ordinary_nhsp');return;}
 exact(slot,['schema_version','before_record','after_record','native_fin_journeys'],'REPORT_NATIVE_ORDINARY_MAPPER_INVALID');
 if(slot.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_NATIVE_ORDINARY_MAPPER_V1')fail('REPORT_NATIVE_ORDINARY_MAPPER_INVALID');
 const before=boundRecord(slot.before_record,'ordinary_rows',context),after=boundRecord(slot.after_record,'ordinary_rows',context);
 if(slot.before_record.result_bytes!==slot.after_record.result_bytes||!same(before,after))fail('REPORT_NATIVE_ORDINARY_STATE_CHANGED');
 if(!Array.isArray(before)||before.length>10000||new Set(before.map(row=>row.root_timesheet_id)).size!==before.length)fail('REPORT_NATIVE_ORDINARY_CENSUS_INVALID');
 const modes=new Set();for(const row of before){exact(row,['root_timesheet_id','contract_id','client_id','candidate_id','source_mode','inventory_bytes','actual_submission_bytes','financial_rows_sha256'],'REPORT_NATIVE_ORDINARY_ROW_INVALID');
  if(!uuid.test(row.root_timesheet_id)||!context.basis.authorization.fixture_manifest.contract_ids.includes(row.contract_id)
   ||!context.basis.authorization.fixture_manifest.candidate_ids.includes(row.candidate_id)||!context.basis.authorization.fixture_manifest.client_ids.includes(row.client_id)
   ||!hex.test(row.financial_rows_sha256))fail('REPORT_NATIVE_ORDINARY_FOREIGN_FIXTURE');
  const inventory=decode(row.inventory_bytes);if(inventory.ok!==true||!['HEAD','TSFIN'].includes(inventory.authority)||!Array.isArray(inventory.components)
   ||inventory.component_count!==inventory.components.length||!hex.test(inventory.inventory_digest))fail('REPORT_NATIVE_ORDINARY_INVENTORY_INVALID');
  if(inventory.approval_basis?.coverage_complete!==true)waiting.push('native_ordinary_approved_inventory');
  modes.add(row.source_mode);
 }
 for(const mode of ['HEALTHROSTER_WEEKLY','NHSP_WEEKLY'])if(!modes.has(mode))waiting.push('native_ordinary_fixture_'+mode);
 if(slot.native_fin_journeys===null)waiting.push('native_actual_ordinary_nhsp_fin_pay_journeys');else validateNativeOrdinaryFinJourneys(slot.native_fin_journeys,context);
}
export function validateNextOrigin(n){
 exact(n,['ok','code','work_id','owner_epoch','current_revision_id','applied_revision_id','source_event_id','source_head_id','financial_snapshot_id',
  'physical_root_id','physical_root_version','revision_seal','source_inventory_digest','application_state','command_id','agency_sequence','financial_view_revision'],'REPORT_NATIVE_NEXT_ORIGIN_INVALID');
 if(!['APPLIED','PENDING','UNAVAILABLE','INCOMPATIBLE'].includes(n.application_state)||typeof n.ok!=='boolean'
  ||!['OK','UNAVAILABLE','INCOMPATIBLE'].includes(n.code)||['APPLIED','PENDING'].includes(n.application_state)!==(n.ok===true))fail('REPORT_NATIVE_NEXT_ORIGIN_INVALID');
 if(n.application_state!=='APPLIED')return {qualified:false,waiting:['native_next_application_'+n.application_state.toLowerCase()]};
 if(n.ok!==true||n.code!=='OK'||!uuid.test(n.work_id)||!uuid.test(n.current_revision_id)||!uuid.test(n.applied_revision_id)
  ||!uuid.test(n.command_id)||!uuid.test(n.physical_root_id)||!hex.test(n.revision_seal)||!hex.test(n.source_inventory_digest)
  ||n.physical_root_version===null||n.owner_epoch===null||n.agency_sequence===null||n.financial_view_revision===null
  ||n.source_head_id===null&&n.financial_snapshot_id===null)fail('REPORT_NATIVE_NEXT_ORIGIN_UNPROVED');
 return {qualified:true,waiting:[]};
}
export function validateNativeToolReceipt(receipt,context,required={}){
 exact(receipt,['schema_version','execution','tool_id','tool_source_sha256','actor_user_id','database','backend_pid','transaction_id',
  'request_sha256','result_sha256','native_calls','network_calls','receipt_base_bytes','receipt_sha256'],'REPORT_NATIVE_TOOL_RECEIPT_INVALID');
 if(receipt.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_ACTUAL_NATIVE_TOOL_RECEIPT_V1'||receipt.tool_id!=='CLOUDTMS_REPORT_FIN_OWNER_ROLLBACK_SESSION_V1'
  ||!same(receipt.execution,context.execution)||receipt.actor_user_id!==context.native_request.actor_user_id||receipt.database!=='cloudtms_test_clone'
  ||receipt.backend_pid!==context.backend_pid||receipt.transaction_id!==context.transaction_id||!hex.test(receipt.tool_source_sha256)
  ||reportFinNativeSeal!==null&&receipt.tool_source_sha256!==reportFinNativeSeal.tool_source_sha256
  ||!hex.test(receipt.request_sha256)||!hex.test(receipt.result_sha256)||!Array.isArray(receipt.native_calls)||!receipt.native_calls.length
  ||receipt.native_calls.length>1000||!Array.isArray(receipt.network_calls)||receipt.network_calls.length!==0
  ||required.required_result_sha256&&receipt.result_sha256!==required.required_result_sha256
  ||required.required_request_sha256&&receipt.request_sha256!==required.required_request_sha256)fail('REPORT_NATIVE_TOOL_RECEIPT_UNPROVED');
 const {receipt_base_bytes:ignoredBase,receipt_sha256:ignoredHash,...body}=receipt;
 if(!same(decode(receipt.receipt_base_bytes),body)||sha256('WEEKLY_SOURCE_REPORT_FIN_ACTUAL_NATIVE_TOOL_RECEIPT_V1\x1f'+receipt.receipt_base_bytes)!==receipt.receipt_sha256)fail('REPORT_NATIVE_TOOL_RECEIPT_HASH_CHANGED');
 for(const [index,call]of receipt.native_calls.entries()){
  exact(call,['sequence','kind','owner_signature','request_sha256','result_sha256'],'REPORT_NATIVE_TOOL_CALL_INVALID');
  if(call.sequence!==index+1||call.kind!=='PG_NATIVE_OWNER'||!reportFinNativeOwnerSignatures.includes(call.owner_signature)
   ||!hex.test(call.request_sha256)||!hex.test(call.result_sha256))fail('REPORT_NATIVE_TOOL_CALL_UNPROVED');
 }
 return true;
}
function validateEffectSlot(slot,context,waiting){
 if(slot===null){waiting.push('no_external_effects');return;}
 exact(slot,['schema_version','before_record','after_record','tool_network_census'],'REPORT_NATIVE_EFFECT_MAPPER_INVALID');
 if(slot.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_NATIVE_EFFECT_MAPPER_V1')fail('REPORT_NATIVE_EFFECT_MAPPER_INVALID');
 const before=boundRecord(slot.before_record,'effects',context),after=boundRecord(slot.after_record,'effects',context);
 if(!Array.isArray(before)||!Array.isArray(after)||before.length!==nativeEffectRelations.length||after.length!==before.length
  ||slot.before_record.result_bytes!==slot.after_record.result_bytes)fail('REPORT_NATIVE_EXTERNAL_EFFECT_OBSERVED');
 for(const [index,row]of before.entries()){exact(row,['relation','row_count','rows_sha256'],'REPORT_NATIVE_EFFECT_ROW_INVALID');
  if(row.relation!==nativeEffectRelations[index]||typeof row.row_count!=='string'||!/^(0|[1-9][0-9]*)$/.test(row.row_count)||!hex.test(row.rows_sha256))fail('REPORT_NATIVE_EFFECT_CENSUS_INVALID');}
 if(slot.tool_network_census===null)waiting.push('native_actual_tool_network_census');else validateNativeToolReceipt(slot.tool_network_census,context);
}
export function validateReportNativeGateFacts(raw,context){
 const seal=mount(context.execution);
 if(!seal)return {waiting:['native_mapper_compiled_contract_mount','native_candidate_i1','ordinary_nhsp','origin_authority','no_external_effects']};
 if(raw===null)return {waiting:[...nativeGateSlots]};
 exact(raw,nativeGateSlots,'REPORT_NATIVE_GATE_FACTS_INVALID');const waiting=[];
 if(raw.native_candidate_i1===null)waiting.push('native_candidate_i1');else{
  const frame=raw.native_candidate_i1;exact(frame,['schema_version','before','after'],'REPORT_NATIVE_I1_MAPPER_INVALID');
  if(frame.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_NATIVE_I1_MAPPER_V1')fail('REPORT_NATIVE_I1_MAPPER_INVALID');
  exact(frame.before,['schema_version','records','i1_probes'],'REPORT_NATIVE_MAPPER_BEFORE_INVALID');
  if(frame.before.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_NATIVE_MAPPER_BEFORE_V1'||!Array.isArray(frame.before.records)||frame.before.records.length!==beforeNames.length)fail('REPORT_NATIVE_MAPPER_BEFORE_INVALID');
  const queries=reportNativeMapperQueries(context.native_request,context.basis.authorization),seen=new Set(),decoded={};
  for(const record of frame.before.records){exact(record,['query_id','query_sha256','result_bytes','result_sha256'],'REPORT_NATIVE_MAPPER_RECORD_INVALID');
   if(seen.has(record.query_id)||!beforeNames.includes(record.query_id)||record.query_sha256!==sha256(queries[record.query_id])||record.result_sha256!==sha256(record.result_bytes))fail('REPORT_NATIVE_MAPPER_RECORD_UNBOUND');
   seen.add(record.query_id);decoded[record.query_id]=decode(record.result_bytes);
  }
  const session=decoded.session;
  exact(session,['database','backend_pid','transaction_id','session_user','current_user','is_owner','actor','managed_shared_lock'],'REPORT_NATIVE_MAPPER_SESSION_INVALID');
  if(session.database!=='cloudtms_test_clone'||session.backend_pid!==context.backend_pid||session.transaction_id!==context.transaction_id
   ||session.is_owner!==true||session.current_user!==session.session_user||session.managed_shared_lock!==true
   ||session.actor?.id!==context.native_request.actor_user_id||session.actor?.is_active!==true||String(session.actor?.role).toLowerCase()!=='admin')fail('REPORT_NATIVE_MAPPER_SESSION_INVALID');
  if(!Array.isArray(decoded.owners)||decoded.owners.length!==seal.owners.length||new Set(decoded.owners.map(o=>o.signature)).size!==seal.owners.length
   ||decoded.owners.some(o=>!seal.owners.some(p=>p.signature===o.signature&&p.definition_sha256===o.definition_sha256&&p.anon_execute===o.anon_execute&&p.authenticated_execute===o.authenticated_execute)))fail('REPORT_NATIVE_MAPPER_OWNER_CHANGED');
  if(!Array.isArray(decoded.hooks)||decoded.hooks.length!==seal.hooks.length||!same(decoded.hooks,seal.hooks))fail('REPORT_NATIVE_CURRENT_HOOK_CENSUS_CHANGED');
  exact(decoded.empty_set,['ok','code','retryable','reason'],'REPORT_NATIVE_I1_EMPTY_SET_INVALID');
  if(decoded.empty_set.ok!==false||decoded.empty_set.code!=='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'||decoded.empty_set.retryable!==false||decoded.empty_set.reason!=='NO_REQUESTED_ROOTS')fail('REPORT_NATIVE_I1_EMPTY_SET_INVALID');
  if(!Array.isArray(decoded.pay_authorities)||decoded.pay_authorities.length!==context.basis.bindings.filter(b=>b.state==='CURRENT').length)fail('REPORT_NATIVE_MAPPER_BINDING_CENSUS_CHANGED');
  for(const entry of decoded.pay_authorities){exact(entry,['binding','authority_bytes','authority_base_bytes','pay_witness_bytes'],'REPORT_NATIVE_PAY_ENTRY_INVALID');
   const binding=context.basis.bindings.find(b=>b.id===entry.binding.id);if(!binding||!same(binding,entry.binding))fail('REPORT_NATIVE_MAPPER_BINDING_CHANGED');
   const authority=decode(entry.authority_bytes),qualified=assertReportPayAuthority(authority,entry.authority_base_bytes,binding);waiting.push(...qualified.waiting);
   const witness=decode(entry.pay_witness_bytes);exact(witness,witnessKeys,'REPORT_NATIVE_PAY_WITNESS_INVALID');
   if(witnessKeys.some(k=>witness[k]!==authority[k]))fail('REPORT_NATIVE_PAY_WITNESS_CHANGED');
  }
  validateNegativeProvenance(decoded.priors,context);
  if(!Array.isArray(decoded.roots)||new Set(decoded.roots.map(r=>r.timesheet_id)).size!==decoded.roots.length
    ||decoded.roots.some(r=>!uuid.test(r.timesheet_id)||!context.basis.authorization.fixture_manifest.contract_ids.includes(r.contract_id)
     ||!context.basis.authorization.fixture_manifest.candidate_ids.includes(r.candidate_id)||r.identity?.ok!==true||r.identity.canonical_timesheet_id!==r.timesheet_id))fail('REPORT_NATIVE_ORIGINAL_ROOT_SET_INVALID');
  const candidates=[...new Set(decoded.roots.map(r=>r.candidate_id))].sort(),probes=frame.before.i1_probes;
  if(!Array.isArray(probes)||probes.length!==candidates.length*nativeCandidateJobTypes.length)fail('REPORT_NATIVE_I1_PROBE_CENSUS_INVALID');
  if(!probes.length)waiting.push('native_i1_actual_nonempty_original_roots');
  const keys=new Set();for(const probe of probes){exact(probe,['candidate_id','requested_timesheet_ids','job_type','request_sha256','result_bytes','result_sha256'],'REPORT_NATIVE_I1_PROBE_INVALID');
   const roots=decoded.roots.filter(r=>r.candidate_id===probe.candidate_id).map(r=>r.timesheet_id).sort(),key=probe.candidate_id+'|'+probe.job_type;
   if(keys.has(key)||!same(roots,probe.requested_timesheet_ids)||probe.request_sha256!==sha256(reportNativeI1ProbeSql(probe.candidate_id,roots,probe.job_type))
     ||probe.result_sha256!==sha256(probe.result_bytes))fail('REPORT_NATIVE_I1_PROBE_UNBOUND');keys.add(key);
   const native=decode(probe.result_bytes);exact(native,['ok','gate','families'],'REPORT_NATIVE_I1_PROBE_RESULT_INVALID');
   if(native.ok!==true||native.gate!=='GRANTED'||!Array.isArray(native.families)||native.families.length!==roots.length
    ||new Set(native.families.map(f=>f.requested_timesheet_id)).size!==roots.length||native.families.some(f=>{
     const original=decoded.roots.find(r=>r.timesheet_id===f.requested_timesheet_id);return !original||f.canonical_timesheet_id!==original.identity.canonical_timesheet_id
      ||f.family_booking_id!==original.identity.family_booking_id||!same(f.member_timesheet_ids,original.identity.member_timesheet_ids)||f.family_is_current!==true;
    }))fail('REPORT_NATIVE_I1_PROBE_RESULT_INVALID');
  }
  const after=frame.after;exact(after,['query_id','query_sha256','result_bytes','result_sha256'],'REPORT_NATIVE_MAPPER_RECORD_INVALID');
  if(after.query_id!=='session'||after.query_sha256!==sha256(queries.session)||after.result_sha256!==sha256(after.result_bytes)||!same(decode(after.result_bytes),session))fail('REPORT_NATIVE_MAPPER_SESSION_CHANGED');
 }
 validateOrdinarySlot(raw.ordinary_nhsp,context,waiting);
 validateEffectSlot(raw.no_external_effects,context,waiting);
 if(raw.origin_authority===null)waiting.push('origin_authority');else{
  exact(raw.origin_authority,['schema_version','before'],'REPORT_NATIVE_ORIGIN_MAPPER_INVALID');
  if(raw.origin_authority.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_NATIVE_ORIGIN_MAPPER_V1'||!same(raw.origin_authority.before,raw.native_candidate_i1?.before))fail('REPORT_NATIVE_ORIGIN_MAPPER_INVALID');
  const records=raw.origin_authority.before.records,entry=records.find(r=>r.query_id==='pay_authorities'),authorities=decode(entry.result_bytes).map(r=>decode(r.authority_bytes));
  for(const [gate,positions]of [['prepared',['PREPARED_FOR_AUTHORISATION']],['c1',['RETAINED_C1_PUBLISHED']],
    ['withheld',['PROPOSED','TARGET_MANAGED_SUPPRESSED']],['headless',['LOCAL_FIRST_AUTHORISED','DIRECT_NEXT_PUBLISHED']]])
   if(!authorities.some(a=>positions.includes(a.native_pay_position)))waiting.push('native_'+gate+'_origin_witness');
  if(context.case_id==='NEXT_ACTIVE'){
   const control=decode(records.find(r=>r.query_id==='next_control').result_bytes);
   if(control.active_owner!=='NEXT')waiting.push('native_next_active_owner');
   const next=authorities.filter(a=>a.native_pay_position==='DIRECT_NEXT_PUBLISHED').map(a=>a.next_application);
   if(!next.length)waiting.push('native_actual_next_applied_origin');for(const n of next)waiting.push(...validateNextOrigin(n).waiting);
  }
 }
 return {waiting:[...new Set(waiting)]};
}
