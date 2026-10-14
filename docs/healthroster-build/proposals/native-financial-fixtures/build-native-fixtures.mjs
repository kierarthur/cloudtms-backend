import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { FIXTURE_CONTRACT_RATES, NATIVE_CASES, nativeCase } from './native-casebook.mjs';

// Emits bounded source-only recipes. This program has no DB or network client.
const sourceRoot=path.resolve(process.argv[2]);
const baseOut=path.dirname(fileURLToPath(import.meta.url));
const multi=process.argv[3]==='MULTI_ROW';
if(process.argv[3]&&!multi)throw Error('NFX_UNKNOWN_GENERATOR_VARIANT');
const out=multi?path.join(baseOut,'multirow-lineage-fixture'):baseOut;
fs.mkdirSync(out,{recursive:true});
const {deriveDeterministicUuid,deriveExternalSourceKey,ID_NAMESPACE}=await import(pathToFileURL(path.join(sourceRoot,'tests/weekly-source/harness/deterministic-identities.mjs')));
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const lit=x=>`'${String(x).replaceAll("'","''")}'`;
const json=x=>`${lit(JSON.stringify(x))}::jsonb`;
const cid=multi?'WS-NFX-MULTI-HR-20261013':'WS-NFX-NULL-HR-20261013';
const c=nativeCase(cid),rowCount=c.rowCount??1;
const {payPence,chargePence,vatPence,grossPence}=c.expected;
const ids=Object.fromEntries(['actor','client','candidate','contract'].map(role=>[role,deriveDeterministicUuid(cid,role)]));
// Stable public source key, derived by the established identity namespace.
const sourceKey=deriveExternalSourceKey(cid,'shift',0,{prefix:'NFX'});
const sourceKeys=Array.from({length:rowCount},(_,i)=>deriveExternalSourceKey(cid,'shift',i,{prefix:'NFX'}));
const ruleRequests=Object.fromEntries(['rule-save','rule-assign'].map(role=>[role,deriveDeterministicUuid(cid,role)]));
const {validateRuleSetValues}=await import(pathToFileURL(path.join(sourceRoot,'broker/src/weekly-source/rule-set-validation.js')));
const disabledMoneyValues=validateRuleSetValues({checking_profile_kind:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',invoice_amount_basis:'SOURCE_AMOUNT',source_amount_basis:'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY',line_vat_method:'LINE_GROSS_MINUS_NET_FROM_SOURCE',rate_difference_policy:'FLAG_ALLOW_IGNORE'},{sourceFamily:'ROSTER',cutoffWeekday:3,cutoffLocalTime:'23:59'});
const code=multi?'NFX_MULTI_HR_20261013':'NFX_NULL_HR_20261013';
const email=multi?'nfx-multi-hr-20261013@example.test':'nfx-null-hr-20261013@example.test';
const clientName=multi?'NFX October Multi Client':'NFX October Client';
const candidateName=multi?'NFX October Multi Nurse':'NFX October Nurse';
const tmsRef=multi?'NFX-101002':'NFX-101001';
const state=`(select * from pg_temp.nfx_state)`;
const files=[];
function write(name,body){
 const scoped=body.replaceAll('NFX October Client',()=>clientName).replaceAll('NFX October Nurse',()=>candidateName).replaceAll('NFX-101001',()=>tmsRef).replaceAll('NFX_MONEY_OFF_20261013',()=>multi?'NFX_MULTI_MONEY_OFF_20261013':'NFX_MONEY_OFF_20261013');
 const s=`-- Source proposal only. Root executes inside its one agency TEST rollback transaction.\n-- No nested transaction, clock override, queue drain, payment/provider/email call.\n${scoped.trim()}\n`;
 if(Buffer.byteLength(s)>2_000_000)throw Error('ARTHUR_BYTES_EXCEEDED');
 fs.writeFileSync(path.join(out,name),s);files.push({file:name,sha256:sha(s),bytes:Buffer.byteLength(s),statement_count_upper_bound:(s.match(/;/g)||[]).length});
}
write('01_current_null_setup.sqlinc',`
do $nfx_preflight$ begin
 if current_database()<>'cloudtms_test_clone' then raise exception 'NFX_WRONG_DATABASE';end if;
 if (transaction_timestamp() at time zone 'Europe/London')::date<>date '2026-10-13' then raise exception 'NFX_CURRENT_CASE_DATE_CHANGED';end if;
 if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1) then raise exception 'NFX_LEGACY_1_REQUIRED';end if;
 if nullif(current_setting('cloudtms.nfx.agency_id',true),'') is null or nullif(current_setting('cloudtms.nfx.agency_anchor_group_id',true),'') is null then raise exception 'NFX_REVIEWED_AGENCY_MANIFEST_REQUIRED';end if;
 if not exists(select 1 from public.weekly_source_groups where id=current_setting('cloudtms.nfx.agency_anchor_group_id')::uuid and agency_id=current_setting('cloudtms.nfx.agency_id')::uuid and environment='TEST') then raise exception 'NFX_AGENCY_ANCHOR_MISMATCH';end if;
 if exists(select 1 from public.tms_users where id=${lit(ids.actor)}::uuid or email=${lit(email)})
 or exists(select 1 from public.clients where id=${lit(ids.client)}::uuid or name='NFX October Client')
 or exists(select 1 from public.candidates where id=${lit(ids.candidate)}::uuid or tms_ref='NFX-101001' or display_name='NFX October Nurse')
 or exists(select 1 from public.contracts where id=${lit(ids.contract)}::uuid)
 or exists(select 1 from public.weekly_source_groups where code=${lit(code)})
 or exists(select 1 from public.weekly_source_upload_rows where external_source_key in (${sourceKeys.map(lit).join(',')})) then raise exception 'NFX_NAMESPACE_NOT_VACANT';end if;
end;$nfx_preflight$;
select set_config('request.jwt.claim.role','service_role',true);
create temporary table nfx_state(case_id text primary key,actor_id uuid,client_id uuid,candidate_id uuid,contract_id uuid,group_id uuid,cycle_id uuid,upload_id uuid,publication_id uuid,revision_id uuid,manifest_id uuid,invoice_id uuid,pay_run jsonb,pay_start jsonb,model jsonb) on commit drop;
insert into pg_temp.nfx_state(case_id,actor_id,client_id,candidate_id,contract_id) values(${lit(cid)},${lit(ids.actor)}::uuid,${lit(ids.client)}::uuid,${lit(ids.candidate)}::uuid,${lit(ids.contract)}::uuid);
-- Identity/configuration fixtures only. All source/finance descendants are native-produced below.
insert into public.tms_users(id,email,role,is_active,password_hash,payment_authoriser,payment_golden_key) values(${lit(ids.actor)}::uuid,${lit(email)},'admin',true,'NFX_NOT_A_LOGIN',true,false);
do $nfx_client$ declare result jsonb;begin
 result:=public.client_create_with_settings_v1(${lit(ids.client)}::uuid,
 ${json({name:'NFX October Client',vat_chargeable:true,payment_terms_days:30,invoice_address:'TEST fixture address',ts_queries_email:'nfx-manager@example.test'})},${lit(ids.actor)}::uuid,
 ${json({timezone_id:'Europe/London',day_start:'06:00',day_end:'20:00',night_start:'20:00',night_end:'06:00',sat_start:'00:00',sat_end:'00:00',sun_start:'00:00',sun_end:'00:00',bh_start:'00:00',bh_end:'00:00',bh_list:[],vat_rate_pct:20,holiday_pay_pct:0,erni_pct:0,hr_validation_required:true,autoprocess_hr:true,requires_hr:true,no_timesheet_required:true,self_bill_no_invoices_sent:true,is_nhsp:false,hr_attach_to_invoice:false,ts_attach_to_invoice:false,healthroster_import_auto_authorise:false,pay_reference_required:false,invoice_reference_required:false,reference_number_required_to_issue_invoice:false,candidate_expenses_require_separate_timesheet:true,candidate_expense_invoice_email:'nfx-expense@example.test',opt_in_email:false,opt_in_sms:false,opt_in_whatsapp:false})});
 if result->>'ok' is distinct from 'true' then raise exception 'NFX_NATIVE_CLIENT_CREATE_FAILED';end if;
end;$nfx_client$;
insert into public.candidates(id,tms_ref,display_name,pay_method,active,nhsp_hr_name_aliases,opt_in_email,opt_in_sms,opt_in_whatsapp) values(${lit(ids.candidate)}::uuid,'NFX-101001','NFX October Nurse','PAYE',true,'["nfx october nurse"]'::jsonb,false,false,false);
insert into public.contracts(id,candidate_id,client_id,role,band,start_date,end_date,pay_method_snapshot,rates_json,weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr,require_reference_to_pay,require_reference_to_invoice,hr_attach_to_invoice,ts_attach_to_invoice,healthroster_import_auto_authorise_override,overrideclientsettings) values(${lit(ids.contract)}::uuid,${lit(ids.candidate)}::uuid,${lit(ids.client)}::uuid,'NFX nurse','5',date '2026-10-01',date '2026-10-31','PAYE',${json(FIXTURE_CONTRACT_RATES)},'HEALTHROSTER',true,true,true,true,false,false,false,false,false,false);
do $nfx_setup$ declare result jsonb;shape jsonb;g uuid;c uuid;resolved jsonb;begin
 result:=public.weekly_source_source_group_save_atomic_v1(jsonb_build_object('actor_user_id',${lit(ids.actor)},'agency_id',current_setting('cloudtms.nfx.agency_id'),'environment','TEST','source_group',${json({id:null,expected_version:null,code,display_name:'NFX current NULL HealthRoster',source_family:'ROSTER',cutoff_weekday:3,cutoff_local_time:'23:59',nhsp_report_heading_name:null,active:true})}));
 if result->>'ok' is distinct from 'true' then raise exception 'NFX_GROUP_NATIVE_SAVE_FAILED';end if;
 g:=(result->>'saved_source_group_id')::uuid;
 shape:=public.weekly_source_client_settings_get_v1(jsonb_build_object('actor_user_id',${lit(ids.actor)},'agency_id',current_setting('cloudtms.nfx.agency_id'),'environment','TEST','client_id',${lit(ids.client)},'effective_date',current_date));
 if shape->>'eligible' is distinct from 'true' then raise exception 'NFX_ACTUAL_CONTRACT_SCOPE_NOT_ELIGIBLE';end if;
 result:=public.weekly_source_client_settings_save_atomic_v1(jsonb_build_object('actor_user_id',${lit(ids.actor)},'agency_id',current_setting('cloudtms.nfx.agency_id'),'environment','TEST','client_id',${lit(ids.client)},'expected_settings_version',shape->>'settings_version','settings',(shape->'settings')||jsonb_build_object('source_group_id',g,'effective_from',current_date,'authority_mode','SOURCE_AUTHORITY','document_mode','IMPORT_ONLY','self_bill_enabled',true,'candidate_queries_enabled',false,'manager_queries_enabled',false,'completed_pack_copy_enabled',false)));
 if result->>'ok' is distinct from 'true' then raise exception 'NFX_CLIENT_NATIVE_SAVE_FAILED';end if;
 select id into strict c from public.weekly_source_cycles where source_group_id=g and scope_client_id is null and state='OPEN' and finalisation_week_ending=date '2026-10-11';
 resolved:=public.weekly_source_client_cycle_resolve_atomic_v1(jsonb_build_object('actor_user_id',${lit(ids.actor)},'source_cycle_id',c,'client_id',${lit(ids.client)}));
 if resolved->>'ok' is distinct from 'true' then raise exception 'NFX_NATIVE_CLIENT_CYCLE_FAILED';end if;
 c:=(resolved->>'source_cycle_id')::uuid;
 if private.weekly_source_effective_rule_set_v1(${lit(ids.client)}::uuid,c) is not null then raise exception 'NFX_NULL_RULE_CONTROL_CHANGED';end if;
 update pg_temp.nfx_state set group_id=g,cycle_id=c;
end;$nfx_setup$;
`);
function envelope(phase,operation,extra=''){
 return `select jsonb_build_object('contract','NFX_NATIVE_CONTEXT_V1','case_id',s.case_id,'phase',${lit(phase)},'actor_user_id',s.actor_id,'fixture_client_id',s.client_id,'fixture_candidate_id',s.candidate_id,'fixture_candidate_name',${lit(candidateName)},'fixture_contract_id',s.contract_id,'publication_id',s.publication_id,'external_source_key',${lit(sourceKey)},'external_source_keys',${json(sourceKeys)},'database_observed_at_utc',transaction_timestamp(),'context',public.weekly_source_upload_context_v1(jsonb_build_object('operation',${lit(operation)},'actor_user_id',s.actor_id,'source_group_id',s.group_id,'source_cycle_id',s.cycle_id,'client_id',s.client_id,'upload_id',${operation==='DISCOVER_SCOPE'?'null':'s.upload_id'}))${extra}) from pg_temp.nfx_state s;`;
}
write('02_actual_stage_context.sqlinc',envelope('STAGE','DISCOVER_SCOPE'));
write('03_actual_projection_context.sqlinc',envelope('PROJECTION','BUILD_PROJECTION'));
write('04_prepare_and_fin.sqlinc',`
do $nfx_fin$ declare s pg_temp.nfx_state%rowtype;ctx jsonb;prepared jsonb;fin jsonb;begin
 select * into strict s from pg_temp.nfx_state;
 ctx:=public.weekly_source_upload_context_v1(jsonb_build_object('operation','BUILD_PROJECTION','actor_user_id',s.actor_id,'source_group_id',s.group_id,'source_cycle_id',s.cycle_id,'client_id',s.client_id,'upload_id',s.upload_id));
 if ctx->>'projection_state'<>'CURRENT' or (ctx->>'projection_publication_id')::uuid is distinct from s.publication_id then raise exception 'NFX_ACTUAL_PUBLICATION_NOT_CURRENT';end if;
 prepared:=public.weekly_source_import_prepare_atomic_v1(jsonb_build_object('actor_user_id',s.actor_id,'upload_id',s.upload_id,'projection_publication_id',s.publication_id,'expected_authority_scope_version',ctx->'authority_scope_version','expected_row_manifest_hash',ctx->>'row_manifest_hash'));
 if prepared->>'ok' is distinct from 'true' then raise exception 'NFX_NATIVE_PREPARE_FAILED';end if;
 fin:=public.weekly_source_finalise_atomic_v1(jsonb_build_object('actor_user_id',s.actor_id,'source_cycle_id',s.cycle_id,'authority_scope_kind','CYCLE','report_scope_id',null,'upload_id',s.upload_id,'projection_publication_id',s.publication_id,'expected_authority_scope_version',ctx->'authority_scope_version','expected_row_manifest_hash',ctx->>'row_manifest_hash','expected_comparison_manifest_hash',ctx->>'comparison_manifest_hash','expected_issue_set_hash',ctx->>'issue_set_hash','exclude_unfinalised_acknowledged',false));
 if fin->>'ok' is distinct from 'true' or fin->>'status'<>'FINALISED' then raise exception 'NFX_NATIVE_FIN_FAILED';end if;
 update pg_temp.nfx_state set revision_id=(fin->>'final_revision_id')::uuid;
 select * into strict s from pg_temp.nfx_state;
 if not exists(select 1 from public.weekly_source_final_revisions where id=s.revision_id and source_cycle_id=s.cycle_id and upload_id=s.upload_id and rule_set_version_id is null) then raise exception 'NFX_NULL_RULE_REVISION_PROVENANCE';end if;
 if (select count(*) from public.weekly_source_billing_movements where final_revision_id=s.revision_id)<>${rowCount} or exists(select 1 from public.weekly_source_billing_movements where final_revision_id=s.revision_id and (actual_client_id<>s.client_id or candidate_id<>s.candidate_id or contract_id<>s.contract_id or total_pay_ex_vat<>${payPence/100/rowCount} or invoice_presentation_charge_pence<>${chargePence/rowCount} or vat_amount<>${vatPence/100/rowCount} or total_inc_vat<>${grossPence/100/rowCount} or movement_role<>'POSITIVE')) then raise exception 'NFX_NATIVE_FIN_EXACT_MONEY_FAILED';end if;
 if (select count(distinct invoice_timesheet_id) from public.weekly_source_billing_movements where final_revision_id=s.revision_id)<>1 or (select count(*) from public.contract_weeks where contract_id=s.contract_id)<>1 or (select count(*) from public.timesheets where contract_id=s.contract_id)<>1 then raise exception 'NFX_NATIVE_CREATED_ROOT_NOT_EXACT';end if;
 update pg_temp.nfx_state set manifest_id=(select id from public.weekly_source_client_manifests where final_revision_id=s.revision_id and client_id=s.client_id);
end;$nfx_fin$;
do $nfx_pay_open$ declare s pg_temp.nfx_state%rowtype;r jsonb;begin
 select * into strict s from pg_temp.nfx_state;
 r:=public.weekly_source_finalisation_pay_open_atomic_v1(jsonb_build_object('schema_version','WEEKLY_SOURCE_FINALISATION_PAY_OPEN_V1','actor_user_id',s.actor_id,'final_revision_id',s.revision_id));
 if r->>'ok' is distinct from 'true' or jsonb_array_length(r->'tasks')<>1 then raise exception 'NFX_NATIVE_ONE_PAY_TASK_REQUIRED';end if;
 update pg_temp.nfx_state set pay_run=r;
end;$nfx_pay_open$;
`);
write('05_actual_pay_snapshot_context.sqlinc',`
select jsonb_build_object('contract','NFX_NATIVE_CONTEXT_V1','case_id',s.case_id,'phase','PAY_SNAPSHOT','actor_user_id',s.actor_id,'fixture_client_id',s.client_id,'fixture_candidate_id',s.candidate_id,'fixture_contract_id',s.contract_id,'context','{}'::jsonb,'root_context',s.pay_run#>'{tasks,0,root_context}','final_revision_id',s.revision_id,'native_read_responses',jsonb_build_object('tsfin_load_context_batch',(select jsonb_agg(to_jsonb(t)) from public.tsfin_load_context_batch(array[(s.pay_run#>>'{tasks,0,root_timesheet_id}')::uuid]) t),'tsfin_load_weekly_context_batch',(select jsonb_agg(to_jsonb(t)) from public.tsfin_load_weekly_context_batch(array[(s.pay_run#>>'{tasks,0,root_timesheet_id}')::uuid]) t))) from pg_temp.nfx_state s;
`);
// Bridge PAY_APPLY consumes the actual pure calculator output, never a literal finance result.
write('07_invoice_admit_issue_model.sqlinc',`
do $nfx_invoice$ declare s pg_temp.nfx_state%rowtype;admitted jsonb;i uuid;issued record;m record;verdict jsonb;begin
 select * into strict s from pg_temp.nfx_state;
 admitted:=public.weekly_source_invoice_admit_atomic_v1(jsonb_build_object('actor_user_id',s.actor_id,'client_manifest_id',s.manifest_id,'expected_manifest_hash',(select encode(manifest_hash,'hex') from public.weekly_source_client_manifests where id=s.manifest_id)));
 if admitted->>'ok' is distinct from 'true' or admitted->>'status'<>'ADMITTED' or jsonb_array_length(admitted->'invoice_ids')<>1 then raise exception 'NFX_NATIVE_INVOICE_ADMISSION_FAILED';end if;
 i:=(admitted#>>'{invoice_ids,0}')::uuid;
 perform private.weekly_source_invoice_allocation_assert_v1(i);
 verdict:=private.weekly_source_invoice_issue_validate_v1(i);
 if verdict->>'is_source_invoice' is distinct from 'true' then raise exception 'NFX_INVOICE_SOURCE_IDENTITY_LOST';end if;
 if not exists(select 1 from public.invoices where id=i and subtotal_ex_vat=${chargePence/100} and vat_amount=${vatPence/100} and total_inc_vat=${grossPence/100}) then raise exception 'NFX_NATIVE_INVOICE_MONEY_FAILED';end if;
 -- DRAFT_PREVIEW is the actual native purpose literal. No wall-clock argument is supplied.
 select * into strict m from private._invoice_presentation_snapshot_batch(jsonb_build_array(jsonb_build_object('request_key','NFX_NATIVE_PREVIEW','entity_type','INVOICE','entity_id',i,'purpose','DRAFT_PREVIEW','template_version','invoice-professional-v2')));
 if m.valid is distinct from true then raise exception 'NFX_NATIVE_MODEL_HELD_%',m.error_code;end if;
 update pg_temp.nfx_state set invoice_id=i,model=m.presentation_model;
 select * into strict issued from public.invoice_issue_one(i,s.actor_id);
 if issued.status<>'ISSUED' then raise exception 'NFX_NATIVE_ISSUE_HELD_%',issued.on_hold_reason;end if;
 select * into strict m from private._invoice_presentation_snapshot_batch(jsonb_build_array((select jsonb_build_object('request_key','NFX_NATIVE_FINAL_ISSUE','entity_type','INVOICE','entity_id',id,'purpose','FINAL_ISSUE','template_version','invoice-professional-v2','issue_at_utc',issued_at_utc,'tax_point_utc',coalesce(nullif(header_snapshot_json->>'tax_point_utc','')::timestamptz,issued_at_utc),'due_at_utc',due_at_utc) from public.invoices where id=i)));
 if m.valid is distinct from true then raise exception 'NFX_NATIVE_FINAL_ISSUE_MODEL_HELD_%',m.error_code;end if;
 -- Native model readiness is distinct from actual PDF/R2 document rendering.
end;$nfx_invoice$;
`);
write('08_null_pay_and_invoice_assertions.sqlinc',`
do $nfx_exact$ declare s pg_temp.nfx_state%rowtype;begin
 select * into strict s from pg_temp.nfx_state;
 if (select count(*) from public.weekly_source_ordinary_pay_projection_receipts where final_revision_id=s.revision_id)<>1 then raise exception 'NFX_NATIVE_PAY_RECEIPT_NOT_EXACT';end if;
 if exists(select 1 from public.weekly_source_billing_movements where final_revision_id=s.revision_id and source_line_kind='INVOICE_VAT_ROUNDING') then raise exception 'NFX_NULL_PATH_NEW_ROUND_LINE';end if;
 if exists(select 1 from public.weekly_source_upload_rows where upload_id=s.upload_id and (source_gross_pence is not null or source_vat_pence is not null or finalised_at_utc is not null)) then raise exception 'NFX_NULL_PATH_SOURCE_FIELDS_CHANGED';end if;
 if exists(select 1 from public.weekly_source_final_snapshot_lines where final_revision_id=s.revision_id and invoice_economics_sha256 is not null) then raise exception 'NFX_NULL_PATH_INVOICE_HASH_CHANGED';end if;
 if private.weekly_source_effective_rule_set_v1(s.client_id,s.cycle_id) is not null then raise exception 'NFX_NULL_PATH_RULES_CHANGED';end if;
 if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1) then raise exception 'NFX_MODULE_CHANGED';end if;
end;$nfx_exact$;
set constraints all immediate;
`);
write('09_fresh_namespace_absence.sqlinc',`
-- Execute on a NEW read-only connection only AFTER root's unconditional outer ROLLBACK.
select not exists(select 1 from public.tms_users where id=${lit(ids.actor)}::uuid or email=${lit(email)})
 and not exists(select 1 from public.clients where id=${lit(ids.client)}::uuid or name='NFX October Client')
 and not exists(select 1 from public.candidates where id=${lit(ids.candidate)}::uuid or tms_ref='NFX-101001' or display_name='NFX October Nurse')
 and not exists(select 1 from public.contracts where id=${lit(ids.contract)}::uuid)
 and not exists(select 1 from public.weekly_source_groups where code=${lit(code)})
 and not exists(select 1 from public.weekly_source_upload_rows where external_source_key in (${sourceKeys.map(lit).join(',')}))
 and not exists(select 1 from public.weekly_source_group_clients where client_id=${lit(ids.client)}::uuid)
 and not exists(select 1 from public.weekly_source_client_policies where client_id=${lit(ids.client)}::uuid)
 and not exists(select 1 from public.weekly_source_client_manifests where client_id=${lit(ids.client)}::uuid)
 and not exists(select 1 from public.weekly_source_billing_movements where actual_client_id=${lit(ids.client)}::uuid or candidate_id=${lit(ids.candidate)}::uuid or contract_id=${lit(ids.contract)}::uuid)
 and not exists(select 1 from public.contract_weeks where contract_id=${lit(ids.contract)}::uuid)
 and not exists(select 1 from public.timesheets where contract_id=${lit(ids.contract)}::uuid)
 and not exists(select 1 from public.weekly_source_rule_sets where created_by_user_id=${lit(ids.actor)}::uuid or code='NFX_MONEY_OFF_20261013')
 and not exists(select 1 from public.weekly_source_rule_set_versions where created_by_user_id=${lit(ids.actor)}::uuid)
 and not exists(select 1 from public.weekly_source_group_rule_assignments where created_by_user_id=${lit(ids.actor)}::uuid)
 and exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1) as all_fixture_roots_and_descendants_absent_legacy_1;
`);
write('10_current_rule_save_assign_off.sqlinc',`
-- Run before phase 02. This proves configuration owners/OFF semantics, not enabled money admission.
create temporary table nfx_rule_state(saved jsonb,assigned jsonb,before_policy jsonb) on commit drop;
do $nfx_rules_off$ declare s pg_temp.nfx_state%rowtype;v_saved jsonb;v_assigned jsonb;before_policy jsonb;begin
 select * into strict s from pg_temp.nfx_state;
 if exists(select 1 from public.weekly_source_rule_sets where code='NFX_MONEY_OFF_20261013')
 or exists(select 1 from public.weekly_source_rule_set_versions where save_request_id=${lit(ruleRequests['rule-save'])}::uuid)
 or exists(select 1 from public.weekly_source_group_rule_assignments where assignment_request_id=${lit(ruleRequests['rule-assign'])}::uuid) then raise exception 'NFX_RULE_NAMESPACE_NOT_VACANT';end if;
 before_policy:=private._weekly_source_effective_policy_v1(s.client_id,s.contract_id,date '2026-10-06');
 v_saved:=public.weekly_source_rule_set_version_save_atomic_v1(jsonb_build_object('actor_user_id',s.actor_id,'agency_id',current_setting('cloudtms.nfx.agency_id'),'environment','TEST','source_group_id',s.group_id,'request_id',${lit(ruleRequests['rule-save'])},'rule_set',jsonb_build_object('id',null,'code','NFX_MONEY_OFF_20261013','display_name','NFX disabled source money','expected_version',0),'values',${json(disabledMoneyValues)}));
 if v_saved->>'contract'<>'WEEKLY_SOURCE_RULE_SET_SAVED_V1' then raise exception 'NFX_NATIVE_RULE_SAVE_FAILED';end if;
 v_assigned:=public.weekly_source_group_rule_assign_atomic_v1(jsonb_build_object('actor_user_id',s.actor_id,'agency_id',current_setting('cloudtms.nfx.agency_id'),'environment','TEST','source_group_id',s.group_id,'request_id',${lit(ruleRequests['rule-assign'])},'rule_set_version_id',v_saved->>'rule_set_version_id','effective_from_week_ending','2026-10-18','effective_to_week_ending',null,'expected_group_version',(select version from public.weekly_source_groups where id=s.group_id)));
 if v_assigned->>'contract'<>'WEEKLY_SOURCE_RULE_ASSIGNED_V1' or not exists(select 1 from public.weekly_source_group_rule_assignments where id=(v_assigned->>'assignment_id')::uuid and feature_enabled=false and effective_from_week_ending=date '2026-10-18') then raise exception 'NFX_NATIVE_ASSIGNMENT_NOT_OFF';end if;
 if private.weekly_source_effective_rule_set_v1(s.client_id,s.cycle_id) is not null
 or private._weekly_source_effective_policy_v1(s.client_id,s.contract_id,date '2026-10-06') is distinct from before_policy then raise exception 'NFX_DISABLED_RULE_POLICY_DRIFT';end if;
 insert into pg_temp.nfx_rule_state values(v_saved,v_assigned,before_policy);
end;$nfx_rules_off$;
`);
write('11_current_null_and_missing_negatives.sqlinc',`
-- Run after native FIN. These are actual NULL-rule Source and absent-root negatives.
-- They do not claim an ordinary signed-document or NHSP positive journey.
do $nfx_negative$ declare s pg_temp.nfx_state%rowtype;root uuid;absent uuid:=${lit(deriveDeterministicUuid(cid,'absent-timesheet'))}::uuid;v jsonb;begin
 select * into strict s from pg_temp.nfx_state;
 select min(invoice_timesheet_id::text)::uuid into root from public.weekly_source_billing_movements where final_revision_id=s.revision_id having count(distinct invoice_timesheet_id)=1;
 if root is null then raise exception 'NFX_NATIVE_ONE_ROOT_REQUIRED';end if;
 if private.weekly_source_signed_check_only_v1(root) is distinct from false or private.weekly_source_agency_invoice_check_only_v1(root) is distinct from false then raise exception 'NFX_NULL_SELF_BILL_SCOPE_BROADENED';end if;
 if exists(select 1 from public.timesheets where timesheet_id=absent) then raise exception 'NFX_ABSENT_UUID_NOT_VACANT';end if;
 if private.weekly_source_signed_check_only_v1(absent) is distinct from false or private.weekly_source_agency_invoice_check_only_v1(absent) is distinct from false then raise exception 'NFX_MISSING_SCOPE_DID_NOT_FAIL_CLOSED';end if;
 v:=private.weekly_source_invoice_issue_validate_v1(absent);
 if v->>'is_source_invoice' is distinct from 'false' then raise exception 'NFX_MISSING_INVOICE_SCOPE_DID_NOT_FAIL_CLOSED';end if;
end;$nfx_negative$;
`);
const inventory={contract:'NFX_NATIVE_FINANCIAL_FIXTURE_MANIFEST_V1',source_only:true,hosted_acceptance:false,identity_namespace:ID_NAMESPACE,fixture_identities:ids,rule_request_identities:ruleRequests,source_key:sourceKey,unique_code:code,unique_email:email,fixture_contract_rates:FIXTURE_CONTRACT_RATES,root_supplied_scope:['cloudtms.nfx.agency_id','cloudtms.nfx.agency_anchor_group_id'],no_seeded_business_results:['contract_weeks','timesheets','weekly_source_uploads','weekly_source_upload_rows','weekly_source_projection_publications','weekly_source_row_resolutions','weekly_source_final_revisions','weekly_source_final_snapshot_lines','weekly_source_billing_movements','invoices','invoice_lines','timesheet_financials','manager_signatures','qualification_receipts'],batch_files:files,cases:NATIVE_CASES,proof_gates:{fresh_before_manifest:'Root hashes actual ledger/catalogue and records namespace vacancy before any fixture DML.',fresh_after_manifest:'Root repeats exact same fresh ledger/catalogue queries and this package 09 after unconditional rollback.',pdf:'HELD until unchanged renderer is run with actual source/support bytes in authorized TEST R2 and the true FINAL_ISSUE snapshot. No document queue drain or fake R2 locator.',enabled_rule_money_report:'HELD until genuine upload timestamp satisfies F1 floor and F8 gate; setup/assignment/activation must use current exact VERIFIED release and existing controlled owner.',variant_B:'HELD operational choice; Report native internal variant can be qualified separately without exposed selector or manual movement insertion.',signed_documents:'HELD genuine Candidate/manager signature flow; no manager evidence seeded.'}};
fs.writeFileSync(path.join(out,'NATIVE_FIXTURE_MANIFEST.json'),JSON.stringify(inventory,null,2)+'\n');
console.log(JSON.stringify({files:files.length,total_bytes:files.reduce((a,f)=>a+f.bytes,0),database_calls:0,network_calls:0,hosted_acceptance:false}));
