import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
const inputs=[];
const excluded="('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION')";
function read(file) { const raw=fs.readFileSync(file,'utf8'); inputs.push({file,sha256:crypto.createHash('sha256').update(raw).digest('hex')}); return raw.replaceAll('\r\n','\n'); }
function get(file,name) { const source=read(file), start=source.indexOf('create or replace function '+name+'('); if(start<0) throw new Error(name+' missing'); const end=source.indexOf('$function$;',source.indexOf('as $function$',start))+11; return source.slice(start,end); }
function replace(source,before,after,n=1) {const actual=source.split(before).length-1;if(actual!==n)throw new Error(`Expected ${n} anchors, found ${actual}: ${before.slice(0,90)}`);return source.replaceAll(before,after);}
function signature(body) {const m=/create or replace function ([\w.]+)\(([\s\S]*?)\) returns/.exec(body);if(!m)throw new Error('signature');const args=m[2].trim()?m[2].split(',').map(a=>a.trim().split(/\s+/)[1]):[];return `${m[1]}(${args.join(',')})`;}
function put(file,functions) {fs.writeFileSync(file,'-- HealthRoster v2 F5/F6 complete controlling successors; adjacent source owners only.\n\\set ON_ERROR_STOP on\nbegin;\n\n'+functions.join('\n\n')+'\n\n'+functions.map(b=>`alter function ${signature(b)} owner to postgres;\nrevoke all on function ${signature(b)} from public,anon,authenticated,service_role;`+(signature(b).startsWith('public.')?`\ngrant execute on function ${signature(b)} to service_role;`:'' )).join('\n')+"\nnotify pgrst,'reload schema';\ncommit;\n");}
const contextFile='../healthroster-v2-date-guards-build-13102026/supabase/repeatable/13102026_1404_weekly_source_upload_coverage_context_v1.sql';
let context=get(contextFile,'public.weekly_source_upload_context_v1');
context=replace(context,"'client_name',v_client_name,'nhsp_report_heading_name',v_group.nhsp_report_heading_name,","'client_name',v_client_name,'nhsp_report_heading_name',v_group.nhsp_report_heading_name,\n      'effective_rule_set',private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id),");
context=replace(context,"'settings_authority',private._contract_settings_effective_core_v1(","'effective_rule_set',private.weekly_source_effective_rule_set_v1(v_row_client_id,v_cycle.id),\n          'settings_authority',private._contract_settings_effective_core_v1(");
context=replace(context,"'source_shift_charge_pence',v_source_row.source_shift_charge_pence,","'source_shift_charge_pence',v_source_row.source_shift_charge_pence,\n      'source_gross_pence',v_source_row.source_gross_pence,'source_vat_pence',v_source_row.source_vat_pence,\n      'finalised_at_utc',v_source_row.finalised_at_utc,");
const publicationFile='supabase/repeatable/15092026_1534_weekly_source_upload_publication_v1.sql';
let stage=get(publicationFile,'public.weekly_source_upload_stage_rows_atomic_v1');
stage=replace(stage,"'source_shift_charge_pence','source_money_parse_state',","'source_shift_charge_pence','source_money_parse_state','source_gross_pence','source_vat_pence','finalised_at_utc',");
stage=replace(stage,"'bounded_raw_columns_json',v_row.bounded_raw_columns_json\n    );","'bounded_raw_columns_json',v_row.bounded_raw_columns_json\n    )||case when v_row.source_gross_pence is not null or v_row.source_vat_pence is not null or v_row.finalised_at_utc is not null\n      then jsonb_build_object('source_gross_pence',v_row.source_gross_pence,'source_vat_pence',v_row.source_vat_pence,'finalised_at_utc',v_row.finalised_at_utc) else '{}'::jsonb end;");
stage=replace(stage,'actual_net_minutes,row_finalisation_state,finalised_by,role_band_source,','actual_net_minutes,row_finalisation_state,finalised_by,role_band_source,source_gross_pence,source_vat_pence,finalised_at_utc,');
stage=replace(stage,'v_row.row_finalisation_state,v_row.finalised_by,v_row.role_band_source,','v_row.row_finalisation_state,v_row.finalised_by,v_row.role_band_source,v_row.source_gross_pence,v_row.source_vat_pence,v_row.finalised_at_utc,');
stage=replace(stage,"v_money.money_field_kind not in ('COMMISSION','TOTAL_COST','FMC','BOTTOM_TOTAL_COST')","v_money.money_field_kind not in ('COMMISSION','TOTAL_COST','FMC','BOTTOM_TOTAL_COST','SOURCE_NET','SOURCE_GROSS','SOURCE_VAT')");
let seal=get(publicationFile,'private.weekly_source_upload_seal_core_v1');
const oldElse=`  else
    if exists(select 1 from public.weekly_source_money_cell_evidence where upload_id=v_upload.id)
       or exists(
         select 1 from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (source_row.source_money_parse_state<>'NOT_APPLICABLE'
                or source_row.source_commission_pence is not null
                or source_row.source_total_cost_pence is not null
                or source_row.source_shift_charge_pence is not null)
       ) then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_NOT_ALLOWED' using errcode='55000';
    end if;
  end if;`;
seal=replace(seal,oldElse,`  elsif (private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id)#>>'{values,invoice_sender}')='CLIENT_SELF_BILL'
    and (private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id)#>>'{values,invoice_amount_basis}')='SOURCE_AMOUNT'
    and (private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id)#>>'{values,source_amount_basis}')='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY' then
    if v_profile.profile_code not in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1')
      or exists(select 1 from public.weekly_source_upload_rows s where s.upload_id=v_upload.id and (
        s.source_commission_pence is not null or s.source_total_cost_pence is not null
        or (s.row_finalisation_state='SOURCE_WORKED' and (s.source_money_parse_state<>'VALID'
          or s.source_qualification_profile_version is distinct from 'SOURCE_INVOICE_MONEY_PENCE_V1'
          or s.source_shift_charge_pence is null or s.source_gross_pence is null or s.source_vat_pence is null
          or s.source_shift_charge_pence<0 or s.source_gross_pence<0 or s.source_shift_charge_pence+s.source_vat_pence<>s.source_gross_pence
          or s.finalised_at_utc is null or nullif(btrim(s.finalised_by),'') is null
          or (select count(*) from public.weekly_source_money_cell_evidence e where e.upload_id=s.upload_id and e.source_row_ordinal=s.source_row_ordinal
            and e.money_field_kind in ('SOURCE_NET','SOURCE_GROSS') and e.parse_state='VALID' and not e.formula_present and e.source_file_sha256=v_upload.content_sha256
            and e.parsed_pence=case e.money_field_kind when 'SOURCE_NET' then s.source_shift_charge_pence else s.source_gross_pence end)<>2))
        or (s.row_finalisation_state<>'SOURCE_WORKED' and (s.source_money_parse_state<>'NOT_APPLICABLE' or s.source_shift_charge_pence is not null
          or s.source_gross_pence is not null or s.source_vat_pence is not null or s.finalised_at_utc is not null))))
      or exists(select 1 from public.weekly_source_money_cell_evidence e where e.upload_id=v_upload.id
        and e.money_field_kind not in ('SOURCE_NET','SOURCE_GROSS')) then
      raise exception 'WEEKLY_SOURCE_INVOICE_MONEY_EVIDENCE_INVALID' using errcode='55000'; end if;
  else
    if exists(select 1 from public.weekly_source_money_cell_evidence where upload_id=v_upload.id)
       or exists(select 1 from public.weekly_source_upload_rows source_row where source_row.upload_id=v_upload.id
         and (source_row.source_money_parse_state<>'NOT_APPLICABLE' or source_row.source_commission_pence is not null
           or source_row.source_total_cost_pence is not null or source_row.source_shift_charge_pence is not null
           or source_row.source_gross_pence is not null or source_row.source_vat_pence is not null)) then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_NOT_ALLOWED' using errcode='55000'; end if;
  end if;`);
let projection=get('supabase/repeatable/07102026_2215_weekly_source_same_file_recheck_recovery_v1.sql','public.weekly_source_projection_rows_apply_atomic_v1');
projection=replace(projection,'  v_effective_policy jsonb;','  v_effective_policy jsonb;\n  v_invoice_rules jsonb;\n  v_source_amount boolean;');
projection=replace(projection,'    v_effective_policy:=null;','    v_effective_policy:=null;\n    v_invoice_rules:=null; v_source_amount:=false;');
projection=replace(projection,"      v_source_authority:=coalesce(v_effective_policy->>'authority_mode','')='SOURCE_AUTHORITY';","      v_invoice_rules:=private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id)->'values';\n      v_source_amount:=v_invoice_rules is not null and v_invoice_rules->>'invoice_sender'='CLIENT_SELF_BILL' and v_invoice_rules->>'invoice_amount_basis'='SOURCE_AMOUNT' and v_invoice_rules->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY';\n      v_source_authority:=coalesce(v_effective_policy->>'authority_mode','')='SOURCE_AUTHORITY';");
projection=replace(projection,"    v_charge:=v_row_json->'charge_check';","    v_charge:=v_row_json->'charge_check';\n    if v_source_amount and v_calculation_fingerprint is not null and jsonb_typeof(v_charge) is distinct from 'object' then\n      raise exception 'WEEKLY_SOURCE_INVOICE_SOURCE_PRICE_MISSING' using errcode='55000'; end if;");
projection=replace(projection,"if v_profile.profile_code not in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1')\n         or v_mapping_state<>'RESOLVED' then","if (v_profile.profile_code not in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1') and not coalesce(v_source_amount,false))\n         or v_mapping_state<>'RESOLVED' then");
projection=replace(projection,"      v_charge_expected_result:=case\n        when v_source_row.source_commission_pence is null",`      if v_source_amount then
        if v_source_row.source_money_parse_state<>'VALID' or v_charge_source_pence is null or v_charge_source_pence<0 or v_charge_calculated_pence is null or v_charge_calculated_pence<=0 then
          raise exception 'WEEKLY_SOURCE_INVOICE_SOURCE_PRICE_INVALID' using errcode='55000'; end if;
        v_charge_expected_result:=case when v_charge_difference_pence=0 then 'EXACT' when abs(v_charge_difference_pence)=1 then 'SOURCE_ROUNDING_EQUIVALENT' else 'SOURCE_AMOUNT_DIFFERS' end;
        v_charge_expected_sign_kind:='POSITIVE';
      else
      v_charge_expected_result:=case
        when v_source_row.source_commission_pence is null`);
projection=replace(projection,"        else 'MISMATCH' end;\n      v_charge_claimed_result", "        else 'MISMATCH' end;\n      end if;\n      v_charge_claimed_result");
projection=replace(projection,"      if v_charge_claimed_result<>'UNVERIFIABLE'\n         and v_charge_claimed_result is distinct from v_charge_expected_result then","      if (v_source_amount or v_charge_claimed_result<>'UNVERIFIABLE')\n         and v_charge_claimed_result is distinct from v_charge_expected_result then");
projection=replace(projection,"        'NHSP_TWO_COMPONENT_PENCE_V1',v_charge_claimed_result,","        case when v_source_amount then 'SOURCE_INVOICE_MONEY_PENCE_V1' else 'NHSP_TWO_COMPONENT_PENCE_V1' end,v_charge_claimed_result,");
let accept=get('supabase/repeatable/15092026_1534_weekly_source_charge_acceptance_v1.sql','public.weekly_source_charge_accept_atomic_v1');
accept=replace(accept,'  v_actor uuid;','  v_actor uuid;\n  v_invoice_rules jsonb;\n  v_roster_source_amount boolean:=false;');
accept=replace(accept,"     or v_publication.authority_scope_kind<>'NHSP_REPORT_SCOPE' then","     or v_publication.authority_scope_kind not in ('NHSP_REPORT_SCOPE','CYCLE') then");
accept=replace(accept,"  if v_upload.state<>'CURRENT' or v_profile.profile_code<>'NHSP_FINAL_BACKING_V1'",`  v_invoice_rules:=private.weekly_source_effective_rule_set_v1(nullif(v_upload.file_metadata_json->>'client_id','')::uuid,v_cycle_id)->'values';
  v_roster_source_amount:=v_publication.authority_scope_kind='CYCLE' and v_invoice_rules is not null and v_invoice_rules->>'invoice_sender'='CLIENT_SELF_BILL'
    and v_invoice_rules->>'invoice_amount_basis'='SOURCE_AMOUNT' and v_invoice_rules->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'
    and v_profile.final_authority_kind='HEALTHROSTER_ACTUAL_ROWS';
  if v_upload.state<>'CURRENT' or (not coalesce(v_roster_source_amount,false) and v_profile.profile_code<>'NHSP_FINAL_BACKING_V1')`);
accept=replace(accept,'  v_policy_fingerprint:=private.weekly_source_charge_acceptance_policy_fingerprint_v1();','  v_policy_fingerprint:=case when v_roster_source_amount then private.weekly_source_source_amount_acceptance_policy_fingerprint_v1() else private.weekly_source_charge_acceptance_policy_fingerprint_v1() end;');
accept=accept.replaceAll("charge.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE')","((not v_roster_source_amount and charge.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE')) or (v_roster_source_amount and charge.comparison_result='SOURCE_AMOUNT_DIFFERS'))");
accept=accept.replaceAll("charge.phase_severity='PROVISIONAL_WARNING'","charge.phase_severity=case when v_roster_source_amount then 'INFORMATION' else 'PROVISIONAL_WARNING' end");
accept=replace(accept,"    'NHSP_RATE_WARNING_SELECTION_V1',","    case when v_roster_source_amount then 'SOURCE_AMOUNT_RATE_WARNING_SELECTION_V1' else 'NHSP_RATE_WARNING_SELECTION_V1' end,");
accept=replace(accept,"       or v_check.comparison_result not in ('MISMATCH','ZERO_SOURCE_CHARGE')\n       or v_check.phase_severity<>'PROVISIONAL_WARNING'",`       or (not v_roster_source_amount and v_check.comparison_result not in ('MISMATCH','ZERO_SOURCE_CHARGE'))
       or (v_roster_source_amount and (v_check.comparison_result<>'SOURCE_AMOUNT_DIFFERS' or v_check.comparison_profile_version<>'SOURCE_INVOICE_MONEY_PENCE_V1'))
       or v_check.phase_severity<>case when v_roster_source_amount then 'INFORMATION' else 'PROVISIONAL_WARNING' end`);
put('supabase/repeatable/13102026_1432_weekly_source_invoice_money_publication_v1.sql',[context,stage,seal,projection,accept]);

const finFile='supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql';
let current=get(finFile,'private.weekly_source_finalisation_insert_current_movement_v1');
current=replace(current,'  v_pay_vector jsonb;','  v_invoice_rules jsonb;\n  v_invoice_amounts jsonb;\n  v_source_amount boolean:=false;\n  v_source_facts jsonb:=p_source_facts_json;\n  v_pay_vector jsonb;');
current=replace(current,'  if v_economic.row_resolution_id is distinct from v_resolution.id',`  v_invoice_rules:=private.weekly_source_frozen_rule_values_v1(p_final_revision_id);
  v_source_amount:=v_invoice_rules is not null and v_invoice_rules->>'invoice_sender'='CLIENT_SELF_BILL' and v_invoice_rules->>'invoice_amount_basis'='SOURCE_AMOUNT' and v_invoice_rules->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY';
  if v_economic.row_resolution_id is distinct from v_resolution.id`);
current=replace(current,`       p_source_validation_charge_pence is not null
       or p_invoice_presentation_charge_pence is distinct from v_economic.calculated_charge_pence
       or p_price_check_result<>'NOT_APPLICABLE'
       or p_price_check_fingerprint is not null
       or p_charge_acceptance_id is not null`, `       (not v_source_amount and (p_source_validation_charge_pence is not null
         or p_invoice_presentation_charge_pence is distinct from v_economic.calculated_charge_pence
         or p_price_check_result<>'NOT_APPLICABLE' or p_price_check_fingerprint is not null or p_charge_acceptance_id is not null))
       or (v_source_amount and (p_source_validation_charge_pence is distinct from v_source_row.source_shift_charge_pence
         or p_invoice_presentation_charge_pence is distinct from v_source_row.source_shift_charge_pence
         or p_price_check_result not in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','SOURCE_AMOUNT_DIFFERS','ACCEPTED_DISPARITY')
         or p_price_check_fingerprint is distinct from v_economic.calculation_fingerprint
         or ((p_price_check_result='ACCEPTED_DISPARITY') is distinct from (p_charge_acceptance_id is not null))))`);
current=replace(current,"  v_pay_vector:=private.weekly_source_finalisation_vector_v1(v_economic.id,'PAY',1::smallint);",`  if p_source_profile_kind<>'NHSP_TRUST_BACKING_REPORT' and v_invoice_rules is not null and v_invoice_rules->>'invoice_sender'='CLIENT_SELF_BILL' then
    v_invoice_amounts:=private.weekly_source_invoice_line_amounts_v1(v_resolution.id,p_final_revision_id);
    if (v_invoice_amounts->>'net_pence')::bigint is distinct from p_invoice_presentation_charge_pence
      or v_invoice_amounts->>'price_check_result' is distinct from p_price_check_result
      or nullif(v_invoice_amounts->>'charge_acceptance_id','')::uuid is distinct from p_charge_acceptance_id then raise exception 'WEEKLY_SOURCE_INVOICE_MONEY_NOT_REDERIVED' using errcode='55000'; end if;
    v_source_facts:=v_source_facts||jsonb_build_object('rule_set_version_id',v_invoice_amounts->'rule_set_version_id',
      'rule_set_values_sha256',(select version.values_sha256 from public.weekly_source_rule_set_versions version where version.id=(v_invoice_amounts->>'rule_set_version_id')::uuid),
      'invoice_hold',v_invoice_amounts->'invoice_hold','hold_release_id',v_invoice_amounts->'hold_release_id');
  end if;
  v_pay_vector:=private.weekly_source_finalisation_vector_v1(v_economic.id,'PAY',1::smallint);`);
current=replace(current,'  v_total:=(p_invoice_presentation_charge_pence::numeric/100)+v_vat;',`  v_total:=(p_invoice_presentation_charge_pence::numeric/100)+v_vat;
  if v_invoice_amounts is not null then v_vat:=(v_invoice_amounts->>'vat_pence')::numeric/100; v_total:=(v_invoice_amounts->>'gross_pence')::numeric/100; end if;`);
current=replace(current,"'source_line_kind',p_source_line_kind,'source_facts',p_source_facts_json,","'source_line_kind',p_source_line_kind,'source_facts',v_source_facts,");
current=replace(current,'p_source_profile_kind,p_source_line_kind,p_source_facts_json,v_pay_vector,v_charge_vector,','p_source_profile_kind,p_source_line_kind,v_source_facts,v_pay_vector,v_charge_vector,');
let reversal=get(finFile,'private.weekly_source_finalisation_insert_reversal_v1');
reversal=replace(reversal,'  v_pay_vector jsonb;','  v_prior_invoice jsonb;\n  v_prior_vat numeric;\n  v_prior_total numeric;\n  v_pay_vector jsonb;');
reversal=replace(reversal,'  v_pay_vector:=private.weekly_source_finalisation_negate_vector_v1(v_prior.canonical_pay_vector_json);',`  v_prior_invoice:=private.weekly_source_invoice_movement_money_v1(v_prior.id);
  v_prior_vat:=(v_prior_invoice->>'vat_pence')::numeric/100; v_prior_total:=(v_prior_invoice->>'gross_pence')::numeric/100;
  v_pay_vector:=private.weekly_source_finalisation_negate_vector_v1(v_prior.canonical_pay_vector_json);`);
reversal=reversal.replaceAll('-v_prior.vat_amount','-v_prior_vat').replaceAll('-v_prior.total_inc_vat','-v_prior_total');
let expense=get(finFile,'private.weekly_source_finalisation_materialise_expense_invoice_v1');
let expenseAssert=get('supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql','private.weekly_source_ordinary_projection_expense_movement_assert_v1');
for (const key of ['current','reversal','expense','expenseAssert']) {
  let body=({current,reversal,expense,expenseAssert})[key];
  body=body.replace(/private\.weekly_source_sha256_jsonb_v1\(\n\s*'WEEKLY_SOURCE_BILLING_MOVEMENT_V1',\n/g,'private.weekly_source_billing_movement_hash_v1(\n');
  if(key==='current')current=body;if(key==='reversal')reversal=body;if(key==='expense')expense=body;if(key==='expenseAssert')expenseAssert=body;
}
let engine=get('../healthroster-v2-f2-build-13102026/supabase/repeatable/13102026_1210_weekly_source_finalisation_unchanged_values_v1.sql','private.weekly_source_finalise_engine_v1');
engine=replace(engine,'  v_effective_policy jsonb;','  v_rule_set jsonb;\n  v_rule_values jsonb;\n  v_rule_version uuid;\n  v_invoice_amounts jsonb;\n  v_invoice_correction jsonb;\n  v_effective_policy jsonb;');
engine=replace(engine,'  select * into v_cycle\n  from public.weekly_source_cycles where id=v_cycle_id for update;',`  -- The same group row serializes dated assignment changes before freeze.
  perform 1 from public.weekly_source_groups where id=(select source_group_id from public.weekly_source_cycles where id=v_cycle_id) for update;
  select * into v_cycle
  from public.weekly_source_cycles where id=v_cycle_id for update;`);
engine=replace(engine,'  from public.weekly_source_groups where id=v_cycle.source_group_id;','  from public.weekly_source_groups where id=v_cycle.source_group_id;');
engine=replace(engine,'  v_source_profile_kind:=v_profile.final_authority_kind;',"  v_rule_set:=private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id);\n  v_rule_values:=v_rule_set->'values'; v_rule_version:=nullif(v_rule_set->>'rule_set_version_id','')::uuid;\n  v_source_profile_kind:=v_profile.final_authority_kind;");
engine=replace(engine,'  if not private.weekly_source_import_is_prepared_v1(v_upload.id) then','  perform private.weekly_source_final_upload_assert_v1(v_upload.id);\n  if not private.weekly_source_import_is_prepared_v1(v_upload.id) then');
const selection=`(v_rule_values is null or v_rule_values->>'invoice_sender'<>'CLIENT_SELF_BILL' or v_rule_values->>'self_bill_source'<>'SAME_FILE'
          or coalesce((private.weekly_source_shift_on_self_bill_v1(resolution.work_event_id,v_cycle.id)->>'s25_pay_selected')::boolean,false))`;
engine=replace(engine,"    where source_row.upload_id=v_upload.id\n      and (v_source_profile_kind='NHSP_TRUST_BACKING_REPORT'",`    where source_row.upload_id=v_upload.id
      and ${selection}
      and (v_source_profile_kind='NHSP_TRUST_BACKING_REPORT'`);
engine=replace(engine,"      where source_row.upload_id=v_upload.id\n        and (source_row.row_finalisation_state='SOURCE_WORKED'",`      where source_row.upload_id=v_upload.id
        and ${selection}
        and (source_row.row_finalisation_state='SOURCE_WORKED'`);
engine=replace(engine,"        where prior_line.client_id=v_client_id\n          and (",`        where prior_line.client_id=v_client_id
          and not (v_rule_values is not null and v_rule_values->>'invoice_sender'='CLIENT_SELF_BILL' and v_rule_values->>'self_bill_source'='SAME_FILE'
            and coalesce((private.weekly_source_shift_on_self_bill_v1(prior_line.work_event_id,v_cycle.id)->>'bound_to_finalised_export')::boolean,false)
            and not coalesce((private.weekly_source_shift_on_self_bill_v1(prior_line.work_event_id,v_cycle.id)->>'s25_pay_selected')::boolean,false))
          and (`);
engine=replace(engine,"      'policy_fingerprint',pg_catalog.encode(v_policy_fingerprint,'hex')\n    )\n  );",`      'policy_fingerprint',pg_catalog.encode(v_policy_fingerprint,'hex')
    )||case when v_rule_version is null then '{}'::jsonb else jsonb_build_object(
      'rule_set_version_id',v_rule_version,'rule_set_values_sha256',v_rule_set->>'values_sha256') end
  );`);
engine=replace(engine,'    finalised_at_utc,manifest_hash,policy_fingerprint,state\n  ) values (','    finalised_at_utc,manifest_hash,policy_fingerprint,state,rule_set_version_id\n  ) values (');
engine=replace(engine,"    case when p_prepare_only then 'PREPARED' else 'CURRENT' end\n  ) returning id into v_revision_id;","    case when p_prepare_only then 'PREPARED' else 'CURRENT' end,v_rule_version\n  ) returning id into v_revision_id;");
engine=replace(engine,'        null -- SOURCE_AMOUNT wire waits for the exact F5 released-money producer.',`        case when v_rule_values is not null and v_rule_values->>'invoice_sender'='CLIENT_SELF_BILL' and v_rule_values->>'invoice_amount_basis'='SOURCE_AMOUNT' and v_rule_values->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'
          then private.weekly_source_invoice_economics_fingerprint_v1(jsonb_build_object('source_shift_charge_pence',v_row.source_shift_charge_pence,
            'source_gross_pence',v_row.source_gross_pence,'source_vat_pence',v_row.source_vat_pence),v_rule_values->>'line_vat_method',
            case when v_economic.invoice_vat_chargeable then v_economic.invoice_vat_rate_pct else 0 end) else null end`);
engine=replace(engine,"      if v_outcome='NO_CHANGE' then\n        v_unchanged_count:=v_unchanged_count+1;",`      if v_outcome='NO_CHANGE' then
        v_unchanged_count:=v_unchanged_count+1;
        v_invoice_correction:=private.weekly_source_invoice_correction_insert_v1(v_transition_id,v_transition.work_event_id,v_revision_id);
        v_movement_count:=v_movement_count+coalesce((v_invoice_correction->>'movement_count')::integer,0);`);
engine=replace(engine,"            and movement.movement_role in ('POSITIVE','REPLACEMENT')","            and movement.movement_role in ('POSITIVE','REPLACEMENT')\n            and movement.source_line_kind not in "+excluded);
engine=replace(engine,'          v_movement_count:=v_movement_count+1;\n        else\n          v_correction_unit_id:=null;',`          v_movement_count:=v_movement_count+1;
          if private.weekly_source_invoice_correction_unwind_v1(v_transition_id,v_transition.work_event_id,v_revision_id) is not null then v_movement_count:=v_movement_count+1; end if;
        else
          v_correction_unit_id:=null;`);
engine=replace(engine,'          v_movement_id:=private.weekly_source_finalisation_insert_current_movement_v1(\n            v_transition_id,null',`          v_invoice_amounts:=private.weekly_source_invoice_line_amounts_v1(v_row.resolution_id,v_revision_id);
          v_movement_id:=private.weekly_source_finalisation_insert_current_movement_v1(
            v_transition_id,null`);
engine=replace(engine,"            v_row.resolution_id,v_economic.id,v_timesheet_id,null,\n            v_economic.calculated_charge_pence,'NOT_APPLICABLE',null,",`            v_row.resolution_id,v_economic.id,v_timesheet_id,case when (v_invoice_amounts->>'source_amount')::boolean then (v_invoice_amounts->>'net_pence')::bigint else null end,
            (v_invoice_amounts->>'net_pence')::bigint,v_invoice_amounts->>'price_check_result',case when (v_invoice_amounts->>'source_amount')::boolean then v_economic.calculation_fingerprint else null end,`);
engine=replace(engine,'            null,\n            pg_catalog.jsonb_build_object(\n              \'schema_version\',\'WEEKLY_SOURCE_CURRENT_FACTS_V1\'',"            nullif(v_invoice_amounts->>'charge_acceptance_id','')::uuid,\n            pg_catalog.jsonb_build_object(\n              'schema_version','WEEKLY_SOURCE_CURRENT_FACTS_V1'");
engine=replace(engine,"        ),'[]'::jsonb)\n      )\n    ) into v_client_manifest_hash", "        ),'[]'::jsonb)\n      )||case when v_rule_version is null then '{}'::jsonb else jsonb_build_object('rule_set_version_id',v_rule_version,'rule_set_values_sha256',v_rule_set->>'values_sha256') end\n    ) into v_client_manifest_hash");
engine=replace(engine,"        v_invoice_correction:=private.weekly_source_invoice_correction_insert_v1(v_transition_id,v_transition.work_event_id,v_revision_id);\n        v_movement_count:=v_movement_count+coalesce((v_invoice_correction->>'movement_count')::integer,0);", 
"        if v_transition.new_snapshot_line_id is not null and v_transition.previous_snapshot_line_id is not null and exists(select 1 from public.weekly_source_final_snapshot_lines current_line join public.weekly_source_final_snapshot_lines prior_line on prior_line.id=v_transition.previous_snapshot_line_id where current_line.id=v_transition.new_snapshot_line_id and current_line.external_event_identity is distinct from prior_line.external_event_identity) then\n          if private.weekly_source_rebill_movement_insert_v1(v_transition_id,v_revision_id) is not null then v_movement_count:=v_movement_count+1; end if;\n        else\n          v_invoice_correction:=private.weekly_source_invoice_correction_insert_v1(v_transition_id,v_transition.work_event_id,v_revision_id);\n          v_movement_count:=v_movement_count+coalesce((v_invoice_correction->>'movement_count')::integer,0);\n        end if;");
// G1: discover/validate without business-row locks, retain the native complete
// I-1 set once, then group -> cycle and exact locked discovery/root rechecks.
// These transforms do not replace any Banking/native inventory owner.
engine=replace(engine,'  v_finalisation_lock_result jsonb;',`  v_finalisation_lock_result jsonb;
  v_lock_phase integer;
  v_lock_discovery jsonb;
  v_lock_observed jsonb;
  v_finalisation_lock_context boolean:=false;`);
const scopeStart=engine.indexOf('  -- The same group row serializes dated assignment changes before freeze.');
const scopeEnd=engine.indexOf('  -- One transaction at a time may decide the final order',scopeStart);
const familyStart=engine.indexOf('  -- Review F2.  Finalisation calls the lineage ensure owner');
const familyEnd=engine.indexOf('  -- The source Timesheet owner is the only component',familyStart);
if([scopeStart,scopeEnd,familyStart,familyEnd].some(n=>n<0))throw new Error('G1 complete owner anchors missing');
let familyBlock=engine.slice(familyStart,familyEnd);
const legacyRecheckStart=familyBlock.indexOf('  -- Review G2.');
const legacyRecheckEnd=familyBlock.indexOf('  -- Publish the locked set',legacyRecheckStart);
if(legacyRecheckStart<0||legacyRecheckEnd<0)throw new Error('G1 retained root recheck missing');
let legacyRecheck=familyBlock.slice(legacyRecheckStart,legacyRecheckEnd);
// Stabilise set representation; the membership and physical lock owner stay exact.
familyBlock=familyBlock.replaceAll('array_agg(distinct contract_week.timesheet_id)','array_agg(distinct contract_week.timesheet_id order by contract_week.timesheet_id)');
legacyRecheck=legacyRecheck.replaceAll('array_agg(distinct contract_week.timesheet_id)','array_agg(distinct contract_week.timesheet_id order by contract_week.timesheet_id)');
const priorContext=`  v_prior_finalisation_lock_operation:=coalesce(pg_catalog.current_setting('cloudtms.weekly_source_finalisation_lock_operation',true),'');
  v_prior_finalisation_root_set:=coalesce(pg_catalog.current_setting('cloudtms.weekly_source_finalisation_root_set',true),'');`;
familyBlock=replace(familyBlock,priorContext,'');
familyBlock=replace(familyBlock,"  select active_owner='NEXT' into v_next_source_hooks",priorContext+"\n  v_finalisation_lock_context:=true;\n  select active_owner='NEXT' into v_next_source_hooks");
familyBlock=replace(familyBlock,"coalesce(pg_catalog.array_to_string(v_finalisation_root_ids,','),'')","coalesce(pg_catalog.array_to_string(v_finalisation_root_ids,','),'EMPTY_LOCKED_SET')");
let phase=engine.slice(scopeStart,scopeEnd);
phase=replace(phase,`  -- The same group row serializes dated assignment changes before freeze.
  perform 1 from public.weekly_source_groups where id=(select source_group_id from public.weekly_source_cycles where id=v_cycle_id) for update;
  select * into v_cycle
  from public.weekly_source_cycles where id=v_cycle_id for update;`,`  if v_lock_phase=2 then
    -- Use the discovered exact group, never a fresh group chosen after I-1.
    select * into v_group from public.weekly_source_groups
    where id=(v_lock_discovery#>>'{group,id}')::uuid for update;
    if not found or to_jsonb(v_group) is distinct from v_lock_discovery->'group' then
      raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
    select * into v_cycle from public.weekly_source_cycles where id=v_cycle_id for update;
    if not found or to_jsonb(v_cycle) is distinct from v_lock_discovery->'cycle' then
      raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
    select * into v_cycle from public.weekly_source_cycles where id=v_cycle_id;
  end if;`);
function lockedRead(body,original,plain,locked,key,variable){return replace(body,original,`  if v_lock_phase=2 then
${locked}
    if not found or to_jsonb(${variable}) is distinct from v_lock_discovery->'${key}' then
      raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
${plain}
  end if;`);}
phase=lockedRead(phase,`    select * into v_scope
    from public.weekly_source_report_scopes where id=v_report_scope_id for update;`,
'    select * into v_scope from public.weekly_source_report_scopes where id=v_report_scope_id;',
'    select * into v_scope from public.weekly_source_report_scopes where id=v_report_scope_id for update;','scope','v_scope');
phase=lockedRead(phase,`  select * into v_upload
  from public.weekly_source_uploads where id=v_upload_id for share;`,
'    select * into v_upload from public.weekly_source_uploads where id=v_upload_id;',
'    select * into v_upload from public.weekly_source_uploads where id=v_upload_id for share;','upload','v_upload');
phase=lockedRead(phase,`  select * into strict v_publication
  from public.weekly_source_projection_publications where id=v_publication_id for share;`,
'    select * into strict v_publication from public.weekly_source_projection_publications where id=v_publication_id;',
'    select * into v_publication from public.weekly_source_projection_publications where id=v_publication_id for share;','publication','v_publication');
phase=lockedRead(phase,`    select * into v_correction
    from public.weekly_final_source_correction_sessions
    where id=p_correction_session_id for share;`,
'    select * into v_correction from public.weekly_final_source_correction_sessions where id=p_correction_session_id;',
'    select * into v_correction from public.weekly_final_source_correction_sessions where id=p_correction_session_id for share;','correction','v_correction');
phase=replace(phase,`  from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;`,`  from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;
  if v_lock_phase=2 and to_jsonb(v_profile) is distinct from v_lock_discovery->'profile' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;`);
// The first phase executes all the original admission/profile/actor/client and
// coverage guards, including unconditional F8 upload assertion, without row locks.
// The second repeats them under the exact discovered scope. Immutable source
// hashes plus actual resolution rows also fence changed inventory selectors.
const discovery=`  v_lock_observed:=jsonb_build_object(
    'group',to_jsonb(v_group),'cycle',to_jsonb(v_cycle),'scope',to_jsonb(v_scope),
    'upload',to_jsonb(v_upload),'publication',to_jsonb(v_publication),
    'profile',to_jsonb(v_profile),'correction',to_jsonb(v_correction),
    'client_id',v_client_id,'generation',v_generation,'coverage_start',v_coverage_start,
    'coverage_end',v_coverage_end,'source_profile_kind',v_source_profile_kind,
    'rule_set',v_rule_set,'resolutions',coalesce((select jsonb_agg(to_jsonb(r) order by r.id)
      from public.weekly_source_row_resolutions r join public.weekly_source_upload_rows s on s.id=r.upload_row_id
      where s.upload_id=v_upload.id and r.generation=v_generation),'[]'::jsonb));
  if v_lock_phase=1 then
    v_lock_discovery:=v_lock_observed;
${familyBlock}
  else
    if v_lock_observed is distinct from v_lock_discovery then
      raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
    if v_finalisation_lock_operation is not null then
      perform private.bpay_next_source_current_lock_recheck_v1(v_finalisation_lock_operation);
    else
${legacyRecheck}
    end if;
  end if;
`;
engine=engine.slice(0,scopeStart)+`  -- G1 plain scope discovery -> native complete I-1 -> group -> cycle.
  -- Repeat the same guards under locks; changed source/rules/roots retry before any mutation.
  for v_lock_phase in 1..2 loop
`+phase+discovery+'  end loop;\n\n'+engine.slice(scopeEnd,familyStart)+engine.slice(familyEnd);
const restoreContext=`    perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_lock_operation',v_prior_finalisation_lock_operation,true);
    perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_root_set',v_prior_finalisation_root_set,true);`;
engine=replace(engine,`      return pg_catalog.jsonb_build_object(
        'ok',true,'status','FINALISED','idempotent',true,`,`      -- Native operation is transaction-local, including exact immutable replay.
      if v_finalisation_lock_operation is not null then
        perform private.bpay_next_source_current_lock_release_v1(v_finalisation_lock_operation);
      end if;
      if v_finalisation_lock_context then
${restoreContext}
      end if;
      return pg_catalog.jsonb_build_object(
        'ok',true,'status','FINALISED','idempotent',true,`);
engine=replace(engine,`    perform private.bpay_next_source_current_lock_release_v1(v_finalisation_lock_operation);
${restoreContext}
  end if;`,`    perform private.bpay_next_source_current_lock_release_v1(v_finalisation_lock_operation);
  end if;
  if v_finalisation_lock_context then
${restoreContext}
  end if;`);
engine=replace(engine,`    if v_finalisation_lock_operation is not null then
      perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_lock_operation',v_prior_finalisation_lock_operation,true);`,`    if v_finalisation_lock_context then
      perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_lock_operation',v_prior_finalisation_lock_operation,true);`,2);
// This complete engine is intentionally handed to the root integrator. The
// shared selected-row/report arm and tested rounding identity are composed there.
// Standalone lineage is a service owner too: it cannot rely on FIN already
// holding I-1. Retain its native family owner before its first Source row lock.
let lineage=get('supabase/repeatable/15092026_1534_weekly_source_timesheet_lineage_v1.sql','public.weekly_source_timesheet_lineage_ensure_atomic_v1');
lineage=replace(lineage,'  v_lock_result jsonb;',`  v_lock_result jsonb;
  v_lineage_lock_phase integer;
  v_lineage_discovery jsonb;
  v_lineage_observed jsonb;`);
const lineageStart=lineage.indexOf('  -- Lock in the same outer-to-inner order as finalisation.');
const lineageEnd=lineage.indexOf('  -- A missing base Contract Week has no row to lock',lineageStart);
if(lineageStart<0||lineageEnd<0)throw new Error('G1 standalone lineage anchors missing');
let lineagePhase=lineage.slice(lineageStart,lineageEnd);
lineagePhase=lineagePhase.slice(lineagePhase.indexOf('  select * into v_cycle'));
for(const [variable,key] of [['v_cycle','cycle'],['v_scope','scope'],['v_upload','upload'],['v_publication','publication'],['v_correction','correction'],['v_resolution','resolution']]){
  const start=lineagePhase.indexOf(`  select * into ${variable}\n`);
  if(start<0)throw new Error('G1 lineage missing '+variable);
  const end=lineagePhase.indexOf(';',start)+1;
  const locked=lineagePhase.slice(start,end);
  if(!/for (update|share);$/.test(locked))throw new Error('G1 lineage lock missing '+variable);
  const plain=locked.replace(/\n\s*for (update|share);$/,';');
  lineagePhase=replace(lineagePhase,locked,`  if v_lineage_lock_phase=2 then
${locked}
    if not found or to_jsonb(${variable}) is distinct from v_lineage_discovery->'${key}' then
      raise exception 'WEEKLY_SOURCE_LINEAGE_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
${plain}
  end if;`);
}
lineagePhase=replace(lineagePhase,'  if v_known_root_timesheet_id is not null then\n    v_lock_result:=private.weekly_source_lock_family_rows_v1(',
'  if v_known_root_timesheet_id is not null and v_lineage_lock_phase=1 then\n    v_lock_result:=private.weekly_source_lock_family_rows_v1(');
lineagePhase=replace(lineagePhase,`  -- Review G2, the finalisation side of the same rule.`,`  if v_lineage_lock_phase=2 and v_known_root_timesheet_id is distinct from
      nullif(v_lineage_discovery->>'root_timesheet_id','')::uuid then
    raise exception 'WEEKLY_SOURCE_BASE_WEEK_ROOT_CHANGED_DURING_LOCK' using errcode='40001'; end if;

  -- Review G2, the finalisation side of the same rule.`);
lineagePhase=replace(lineagePhase,`     and not (v_known_root_timesheet_id::text
              =any(pg_catalog.string_to_array(v_finalisation_root_set,','))) then`,`     and (v_finalisation_root_set='EMPTY_LOCKED_SET'
       or not (v_known_root_timesheet_id::text
              =any(pg_catalog.string_to_array(v_finalisation_root_set,',')))) then`);
lineagePhase+=`  v_lineage_observed:=jsonb_build_object('cycle',to_jsonb(v_cycle),'group',to_jsonb(v_group),
    'scope',to_jsonb(v_scope),'upload',to_jsonb(v_upload),'publication',to_jsonb(v_publication),
    'correction',to_jsonb(v_correction),'resolution',to_jsonb(v_resolution),'source_row',to_jsonb(v_source_row),
    'profile',to_jsonb(v_profile),'event',to_jsonb(v_event),'contract',to_jsonb(v_contract),
    'policy',v_policy,'expense_policy',to_jsonb(v_expense_policy),
    'week_ending_date',v_week_ending_date,'root_timesheet_id',v_known_root_timesheet_id);
  if v_lineage_lock_phase=1 then v_lineage_discovery:=v_lineage_observed;
  elsif v_lineage_observed is distinct from v_lineage_discovery then
    raise exception 'WEEKLY_SOURCE_LINEAGE_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
`;
lineage=lineage.slice(0,lineageStart)+`  -- G1 standalone and inherited FIN paths share the same physical I-1 owner.
  -- Phase1 is plain key/scope admission plus its unchanged family call. Phase2
  -- locks discovered group -> cycle and revalidates every selector before DML.
  for v_lineage_lock_phase in 1..2 loop
    if v_lineage_lock_phase=2 then
      select * into v_group from public.weekly_source_groups
      where id=(v_lineage_discovery#>>'{group,id}')::uuid for share;
      if not found or to_jsonb(v_group) is distinct from v_lineage_discovery->'group' then
        raise exception 'WEEKLY_SOURCE_LINEAGE_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
    end if;
`+lineagePhase+'  end loop;\n\n'+lineage.slice(lineageEnd);
put('supabase/repeatable/13102026_1433_weekly_source_invoice_money_finalisation_v1.sql',[current,reversal,expense,expenseAssert,engine,lineage]);

const admissionFile='supabase/repeatable/15092026_1534_weekly_source_invoice_admission_v1.sql';
let admit=get(admissionFile,'public.weekly_source_invoice_admit_atomic_v1');
admit=replace(admit,'  v_is_expense boolean;','  v_is_expense boolean;\n  v_is_rounding boolean;\n  v_release_id uuid;\n  v_release_authority jsonb;\n  v_release_member uuid;\n  v_release_money jsonb;\n  v_identity_movement public.weekly_source_billing_movements%rowtype;');
admit=replace(admit,"  select * into strict v_client from public.clients where id=v_manifest.client_id for share;",`  if exists(select 1 from public.weekly_source_manifest_movements mm join public.weekly_source_billing_movements m on m.id=mm.billing_movement_id
    where mm.client_manifest_id=v_manifest.id and m.source_facts_json->>'invoice_hold'='VAT_FIT'
      and not exists(select 1 from public.weekly_source_invoice_hold_releases receipt where receipt.upload_row_id::text=m.source_facts_json->>'upload_row_id' and receipt.hold_kind='VAT_FIT')) then
    raise exception 'WEEKLY_SOURCE_INVOICE_VAT_FIT_HELD' using errcode='55000'; end if;
  select * into strict v_client from public.clients where id=v_manifest.client_id for share;`);
admit=replace(admit,"        then 'NHSP_TWO_COMPONENT_PENCE_V1' else null end,","        then 'NHSP_TWO_COMPONENT_PENCE_V1' when private.weekly_source_invoice_source_amount_movement_valid_v1(v_movement.id) then 'SOURCE_INVOICE_MONEY_PENCE_V1' else null end,");
admit=replace(admit,'    select * into strict v_contract from public.contracts',`    v_is_rounding:=v_movement.source_line_kind='INVOICE_VAT_ROUNDING';
    v_release_id:=null; v_release_authority:=null; v_release_member:=null;
    if not (v_is_rounding and v_movement.candidate_id is null) then
    select * into strict v_contract from public.contracts`);
admit=replace(admit,'    v_client_display:=coalesce(nullif(pg_catalog.btrim(v_client.name),\'\'),v_client.id::text);',`    else v_contract:=null; v_candidate:=null; v_candidate_display:=null; end if;
    v_client_display:=coalesce(nullif(pg_catalog.btrim(v_client.name),''),v_client.id::text);`);
admit=replace(admit,'    if v_is_expense then\n      select source_row.* into strict v_source_row',`    if v_is_rounding then
      v_work_date:=null; v_start:=null; v_end:=null; v_break:=null; v_reference:=null;
      if v_movement.prior_movement_id is not null then
        select * into strict v_identity_movement from public.weekly_source_billing_movements where id=v_movement.prior_movement_id;
        select snapshot.* into strict v_snapshot from public.weekly_source_state_transitions transition_row
          join public.weekly_source_final_snapshot_lines snapshot on snapshot.id=coalesce(transition_row.new_snapshot_line_id,transition_row.previous_snapshot_line_id)
          where transition_row.id=v_identity_movement.transition_id;
        v_work_date:=v_snapshot.work_date; v_start:=v_snapshot.start_at_local; v_end:=v_snapshot.end_at_local;
        v_break:=v_snapshot.break_minutes; v_reference:=v_snapshot.external_event_identity;
      end if;
    elsif v_is_expense then
      select source_row.* into strict v_source_row`);
const oldSource=`      private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_INVOICE_PRESENTATION_SOURCE_V1',
        pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'movement_id',movement.id,
          'movement_economic_hash',pg_catalog.encode(movement.movement_economic_hash,'hex')
        ) order by movement.id)
      )`;
admit=replace(admit,oldSource,'      private.weekly_source_invoice_presentation_source_hash_v1(v_ids,null)');
admit=replace(admit,'    if v_is_expense then\n      v_hours_day:=0;',`    select receipt.id,m.id into v_release_id,v_release_member from public.weekly_source_billing_movements m
      join public.weekly_source_invoice_hold_releases receipt on receipt.upload_row_id::text=m.source_facts_json->>'upload_row_id' and receipt.hold_kind='VAT_FIT'
      where m.id=any(v_ids) and m.movement_role in ('POSITIVE','REPLACEMENT')
        and m.source_line_kind in ('SOURCE_ORDINARY','SOURCE_REPLACEMENT','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION');
    if found then
      v_release_money:=private.weekly_source_invoice_movement_money_v1(v_release_member,v_release_id);
      v_release_authority:=v_release_money->'release_authority';
      select v_vat-m.vat_amount+(v_release_money->>'vat_pence')::numeric/100,
        v_total-m.total_inc_vat+(v_release_money->>'gross_pence')::numeric/100 into v_vat,v_total
        from public.weekly_source_billing_movements m where m.id=v_release_member;
      v_source_hash:=private.weekly_source_invoice_presentation_source_hash_v1(v_ids,v_release_id);
    end if;
    if v_is_expense then
      v_hours_day:=0;`);
admit=replace(admit,"    v_line_kind:=case\n      when v_is_expense",`    v_line_kind:=case
      when v_movement.source_line_kind in ('INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION') then v_movement.source_line_kind
      when v_is_expense`);
admit=replace(admit,"    v_origin_kind:=case\n      when v_is_expense",`    v_origin_kind:=case
      when v_is_rounding then 'INVOICE_VAT_ROUNDING'
      when v_is_expense`);
admit=replace(admit,"    v_description:=case when v_is_expense then 'Source expense'",`    v_description:=case when v_is_rounding then 'VAT rounding (matches the client''s self-bill)'
      when v_movement.source_line_kind='SOURCE_INVOICE_CORRECTION' then 'Source invoice correction'
      when v_is_expense then 'Source expense'`);
admit=replace(admit,"        'source_hash',pg_catalog.encode(v_source_hash,'hex')\n      )","        'source_hash',pg_catalog.encode(v_source_hash,'hex')\n      )||case when v_release_authority is null then '{}'::jsonb else jsonb_build_object('invoice_hold_release_authority',v_release_authority) end");
admit=replace(admit,'      amount_authority,presentation_hash,companion_presentation_line_id','      amount_authority,presentation_hash,invoice_hold_release_id,companion_presentation_line_id');
admit=replace(admit,"      case when v_movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT' or v_is_expense",`      case when v_movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT' or v_is_expense
        or private.weekly_source_invoice_source_amount_movement_valid_v1(v_movement.id)`);
admit=replace(admit,'      v_presentation_hash,\n      -- 24 section 12','      v_presentation_hash,v_release_id,\n      -- 24 section 12');
admit=replace(admit,"        'client_manifest_id',v_manifest.id,'movement_ids',pg_catalog.to_jsonb(v_ids)\n      ),",`        'client_manifest_id',v_manifest.id,'movement_ids',pg_catalog.to_jsonb(v_ids)
      )||case when v_release_authority is null then '{}'::jsonb else jsonb_build_object('invoice_hold_release_authority',v_release_authority) end,`);

let allocation=get(admissionFile,'private.weekly_source_invoice_allocation_assert_v1');
allocation=replace(allocation,"        or invoice_line.total_inc_vat is distinct from presentation.total_inc_vat",`        or invoice_line.total_inc_vat is distinct from presentation.total_inc_vat
        or (presentation.invoice_hold_release_id is not null and (presentation.source_hash is distinct from
          private.weekly_source_invoice_presentation_source_hash_v1((select array_agg(b.billing_movement_id order by b.billing_movement_id)
            from public.weekly_source_invoice_line_bindings b where b.presentation_line_id=presentation.id and b.state='CURRENT'),presentation.invoice_hold_release_id)
          or invoice_line.meta_json->'invoice_hold_release_authority' is distinct from private.weekly_source_invoice_presentation_release_authority_v1(
            (select array_agg(b.billing_movement_id order by b.billing_movement_id) from public.weekly_source_invoice_line_bindings b
              where b.presentation_line_id=presentation.id and b.state='CURRENT'),presentation.invoice_hold_release_id)))`);
let move=get(admissionFile,'public.weekly_source_invoice_move_atomic_v1');
move=replace(move,"    v_source.client_id,v_event.work_date\n  );",`    v_source.client_id,v_event.work_date
  );
  if exists(select 1 from public.weekly_source_invoice_line_bindings binding join public.weekly_source_billing_movements movement on movement.id=binding.billing_movement_id
    where binding.invoice_id in (v_source_invoice_id,v_destination_invoice_id) and binding.state='CURRENT'
      and private.weekly_source_frozen_rule_values_v1(movement.final_revision_id)->>'invoice_sender'='CLIENT_SELF_BILL'
      and private.weekly_source_frozen_rule_values_v1(movement.final_revision_id)->>'invoice_vat_method'<>'SUM_OF_LINES') then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_VAT_MEMBERSHIP_LOCKED' using errcode='55000'; end if;`);
let issue=get('supabase/repeatable/02092026_1833_weekly_source_invoice_issue_validator_v1.sql','private.weekly_source_invoice_issue_validate_v1');
const oldRebuilt=`          private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_INVOICE_PRESENTATION_SOURCE_V1',
            pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
              'movement_id',movement.id,
              'movement_economic_hash',
                pg_catalog.encode(movement.movement_economic_hash,'hex')
            ) order by movement.id)
          ) rebuilt`;
issue=replace(issue,oldRebuilt,'          private.weekly_source_invoice_presentation_source_hash_v1(array_agg(movement.id order by movement.id),presentation.invoice_hold_release_id) rebuilt');
issue=replace(issue,'        group by presentation.id,presentation.source_hash','        group by presentation.id,presentation.source_hash,presentation.invoice_hold_release_id');
issue=replace(issue,"          or (presentation.origin_kind not in ('NHSP_PHYSICAL_ROW','SOURCE_FIXED_EXPENSE')\n            and presentation.amount_authority<>'CLOUDTMS_CALCULATION')",`          or (presentation.origin_kind not in ('NHSP_PHYSICAL_ROW','SOURCE_FIXED_EXPENSE')
            and not (private.weekly_source_invoice_source_amount_movement_valid_v1(movement.id)
              and presentation.amount_authority='VALIDATED_SOURCE_PENCE')
            and presentation.amount_authority<>'CLOUDTMS_CALCULATION')`);
issue=replace(issue,"          or (presentation.origin_kind not in ('NHSP_PHYSICAL_ROW','SOURCE_FIXED_EXPENSE')", "          or (presentation.amount_authority='VALIDATED_SOURCE_PENCE' and movement.source_line_kind in ('SOURCE_ORDINARY','SOURCE_REPLACEMENT','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION') and private.weekly_source_invoice_source_amount_movement_valid_v1(movement.id) is distinct from true)\n          or (presentation.origin_kind not in ('NHSP_PHYSICAL_ROW','SOURCE_FIXED_EXPENSE')");
issue=replace(issue,'          pg_catalog.sum(movement.vat_amount) movement_vat,','          pg_catalog.sum((private.weekly_source_invoice_movement_money_v1(movement.id)->>\'vat_pence\')::numeric/100) movement_vat,');
issue=replace(issue,'          pg_catalog.sum(movement.total_inc_vat) movement_total,','          pg_catalog.sum((private.weekly_source_invoice_movement_money_v1(movement.id)->>\'gross_pence\')::numeric/100) movement_total,');
issue=replace(issue,'          invoice_line.timesheet_id is null\n          or invoice_line.timesheet_id is distinct from movement.invoice_timesheet_id',`          (invoice_line.timesheet_id is null and not (movement.invoice_timesheet_id is null and (movement.source_line_kind='INVOICE_VAT_ROUNDING'
            or (movement.source_line_kind='SELF_BILL_REPORT_LINE' and movement.movement_role='STANDALONE_NEGATIVE'))))
          or invoice_line.timesheet_id is distinct from movement.invoice_timesheet_id`);
put('supabase/repeatable/13102026_1434_weekly_source_invoice_money_admission_v1.sql',[allocation,admit,move,issue]);

const payGuard = alias => alias+".source_profile_kind<>'SELF_BILL_REPORT' and "+alias+".source_line_kind not in "+excluded;
function excludeReads(body, aliases) {
  for (const alias of aliases) body=body.replaceAll('where '+alias+'.','where '+payGuard(alias)+'\n      and '+alias+'.');
  return body;
}
const payFile='supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql';
let ordinary=excludeReads(get(payFile,'public.weekly_source_ordinary_pay_projection_apply_atomic_v1'),['movement','predecessor_movement']);
let units=excludeReads(get(payFile,'private.weekly_source_ordinary_projection_source_units_v1'),['movement','prior_movement']);
let active=get(payFile,'private.weekly_source_ordinary_projection_active_movements_v1');
active=replace(active,"      and movement.source_line_kind<>'SOURCE_FIXED_EXPENSE'","      and movement.source_line_kind<>'SOURCE_FIXED_EXPENSE'\n      and "+payGuard('movement'));
active=excludeReads(active,['prior']);
let witness=excludeReads(get('supabase/repeatable/05102026_0645_weekly_source_selected_source_witness_v1.sql','private.weekly_source_selected_source_witness_v1'),['m']);
let protectedContext=excludeReads(get('supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql','private.weekly_source_protected_final_source_context_v1'),['movement','live_movement']);
const payOrchestration='supabase/repeatable/15092026_2336_weekly_source_finalisation_pay_orchestration_v1.sql';
let payContext=excludeReads(get(payOrchestration,'private.weekly_source_finalisation_pay_context_v1'),['movement']);
let payOpen=excludeReads(get(payOrchestration,'public.weekly_source_finalisation_pay_open_atomic_v1'),['movement']);
let correctContext=excludeReads(get('supabase/repeatable/15092026_1534_weekly_source_correct_final_source_v1.sql','private.weekly_source_correct_final_prepared_context_v1'),['movement']);
let payFinish=excludeReads(get(payOrchestration,'public.weekly_source_finalisation_pay_task_finish_atomic_v1'),['movement']);
let payRecover=excludeReads(get(payOrchestration,'public.weekly_source_finalisation_pay_task_recover_atomic_v1'),['movement']);
let proposal=excludeReads(get('supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql','private.weekly_source_office_proposal_revision_v1'),['movement_row']);
put('supabase/repeatable/13102026_1435_weekly_source_invoice_only_pay_isolation_v1.sql',[ordinary,units,active,witness,protectedContext,payContext,payOpen,correctContext,payFinish,payRecover,proposal]);

fs.writeFileSync('.codex-tmp/F5_SOURCE_INPUTS.json',JSON.stringify(inputs,null,2)+'\n');
