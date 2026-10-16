import { writeFileSync } from 'node:fs';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { SOURCE_NATIVE_SNAPSHOT_KEYS } from '../../../../broker/src/weekly-source/source-native-snapshot.mjs';
const base = path.dirname(fileURLToPath(import.meta.url));
const literal = values => `array[${values.map(v => `'${v}'`).join(',')}]`;
const buckets = ['day','night','sat','sun','bh'];
const amounts = ['total_hours',...buckets.map(k => `hours_${k}`),'additional_pay_ex_vat','additional_charge_ex_vat','additional_margin_ex_vat',
  'total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat','expenses_pay_ex_vat','expenses_charge_ex_vat','mileage_units','mileage_pay_ex_vat','mileage_charge_ex_vat'];
const rates = buckets.flatMap(k => [`pay_${k}`,`charge_${k}`]);
const segmentKeys = ['segment_id','date','start','end','overnight','start_utc','end_utc','break_mins','ref_num','breaks',
  'hours_day','hours_night','hours_sat','hours_sun','hours_bh','pay_amount','charge_amount','is_reversal','exclude_from_pay'];
const sql = `-- Pure structural/current-input validation of the actual native calculator
-- snapshot. Does not calculate financial values or author a policy/receipt hash.
create or replace function private.weekly_source_pay_decision_native_snapshot_assert_v1(p_snapshot jsonb,p_facts jsonb)
returns void language plpgsql immutable security definer set search_path=pg_catalog,private,public as $function$
declare required constant text[]:=${literal(SOURCE_NATIVE_SNAPSHOT_KEYS)};
 policy jsonb;breakdown jsonb;segment jsonb;item jsonb;identity jsonb;event text;seen text[]:='{}';
 keys text[];next_route boolean;preview jsonb;bucket text;
begin
 policy:=case when p_facts#>'{protected_current_placement,moved}'='true'::jsonb
  then p_facts#>'{current_entitlement,timesheet,settings_authority_json,values}' else p_facts->'policy' end;
 policy:=policy-'resolved_at_utc';next_route:=p_facts->'next_source_segments' is distinct from 'null'::jsonb;
 if not private.weekly_exceptional_json_keys_exact_v1(p_snapshot,required||array['hr_crosscheck_status','hr_crosscheck_issues','external_source_rows_json'])
  or not(p_snapshot ?& required) or jsonb_typeof(policy) is distinct from 'object'
  or p_snapshot->'timesheet_id' is distinct from p_facts#>'{current_entitlement,timesheet,timesheet_id}'
  or p_snapshot->'timesheet_version' is distinct from p_facts#>'{current_entitlement,timesheet,version}'
  or p_snapshot->'candidate_id' is distinct from p_facts->'candidate_id' or p_snapshot->'client_id' is distinct from p_facts->'client_id'
  or p_snapshot->'policy_snapshot_json' is distinct from policy
  or p_snapshot->'rate_source_refs_json' is distinct from jsonb_build_object('mode','CONTRACT_RATES_JSON',
    'contract_id',p_facts#>'{current_entitlement,contract_record,id}')
  or jsonb_typeof(p_snapshot->'basis') is distinct from 'string' or char_length(p_snapshot->>'basis')<1
  or p_snapshot->>'candidate_assignment' is distinct from 'ASSIGNED'
  or coalesce(p_snapshot->>'processing_status','') not in('PENDING_AUTH','READY_FOR_INVOICE')
  or jsonb_typeof(p_snapshot->'additional_units_json') is distinct from 'object'
  or exists(select 1 from unnest(${literal(amounts)}) k where jsonb_typeof(p_snapshot->k) is distinct from 'number')
  or exists(select 1 from unnest(${literal(rates)}) k where jsonb_typeof(p_snapshot->k) not in('number','null'))
  or exists(select 1 from unnest(array['role','band','pay_method','expenses_description','expenses_evidence_r2_key','mileage_evidence_r2_key']) k
    where jsonb_typeof(p_snapshot->k) not in('string','null'))
  or exists(select 1 from unnest(array['expenses_evidence_manifest','mileage_evidence_manifest']) k
    where jsonb_typeof(p_snapshot->k) not in('object','array','null'))
  or exists(select 1 from unnest(array['mileage_pay_rate','mileage_charge_rate']) k where jsonb_typeof(p_snapshot->k) not in('number','string','null')
    or (jsonb_typeof(p_snapshot->k)='string' and (p_snapshot->>k) !~ '^-?(0|[1-9][0-9]*)(\\.[0-9]+)?$'))
  or (p_snapshot ? 'hr_crosscheck_status' and jsonb_typeof(p_snapshot->'hr_crosscheck_status') not in('string','null'))
  or (p_snapshot ? 'external_source_rows_json' and jsonb_typeof(p_snapshot->'external_source_rows_json') is distinct from 'object') then
  raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
 if p_snapshot ? 'hr_crosscheck_issues' and p_snapshot->'hr_crosscheck_issues'<>'null'::jsonb then
  if jsonb_typeof(p_snapshot->'hr_crosscheck_issues') is distinct from 'array' then
   raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
  if exists(select 1 from jsonb_array_elements(p_snapshot->'hr_crosscheck_issues') x where jsonb_typeof(x) is distinct from 'string') then
   raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
 end if;
 breakdown:=p_snapshot->'invoice_breakdown_json';
 if not private.weekly_exceptional_json_keys_exact_v1(breakdown,array['mode','segments','totals','additional'])
  or not(breakdown ?& array['mode','segments','totals']) or breakdown->>'mode' is distinct from 'SEGMENTS'
  or jsonb_typeof(breakdown->'segments') is distinct from 'array'
  or not private.weekly_exceptional_json_keys_exact_v1(breakdown->'totals',array['total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat'])
  or not((breakdown->'totals') ?& array['total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat'])
  or exists(select 1 from unnest(array['total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat']) k
    where jsonb_typeof(breakdown->'totals'->k) is distinct from 'number' or breakdown->'totals'->k is distinct from p_snapshot->k) then
  raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
 if breakdown ? 'additional' then
  item:=breakdown->'additional';
  if not private.weekly_exceptional_json_keys_exact_v1(item,array['units','pay_ex_vat','charge_ex_vat','margin_ex_vat'])
   or not(item ?& array['units','pay_ex_vat','charge_ex_vat','margin_ex_vat']) or item->'units' is distinct from p_snapshot->'additional_units_json'
   or exists(select 1 from unnest(array['pay_ex_vat','charge_ex_vat','margin_ex_vat']) k
    where jsonb_typeof(item->k) is distinct from 'number' or item->k is distinct from p_snapshot->('additional_'||k)) then
   raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
 end if;
 for segment in select value from jsonb_array_elements(breakdown->'segments') loop
  keys:=${literal(segmentKeys)}||case when next_route then array['work_event_id','protected_target_state'] else array['weekly_protected_component_identity'] end;
  if not private.weekly_exceptional_json_keys_exact_v1(segment,keys||array['invoice_target_week_start','invoice_locked_invoice_id']
    ||case when next_route then array['protected_next_preview'] else '{}'::text[] end) or not(segment ?& keys)
   or jsonb_typeof(segment->'segment_id') is distinct from 'string' or char_length(segment->>'segment_id')<1
   or coalesce(segment->>'date','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
   or coalesce(segment->>'start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' or coalesce(segment->>'end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
   or coalesce(segment->>'start_utc','') !~ '(Z|[+-][0-9]{2}:[0-9]{2})$' or coalesce(segment->>'end_utc','') !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
   or exists(select 1 from unnest(array['overnight','is_reversal','exclude_from_pay']) k where jsonb_typeof(segment->k) is distinct from 'boolean')
   or jsonb_typeof(segment->'break_mins') is distinct from 'number' or coalesce(segment->>'break_mins','') !~ '^(0|[1-9][0-9]*)$'
   or jsonb_typeof(segment->'ref_num') not in('string','null') or jsonb_typeof(segment->'breaks') is distinct from 'array'
   or exists(select 1 from unnest(${literal([...buckets.map(k => `hours_${k}`),'pay_amount','charge_amount'])}) k where jsonb_typeof(segment->k) is distinct from 'number') then
   raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
  perform (segment->>'date')::date;perform (segment->>'start_utc')::timestamptz;perform (segment->>'end_utc')::timestamptz;
  if segment ? 'invoice_target_week_start' then
   if jsonb_typeof(segment->'invoice_target_week_start') is distinct from 'string' or coalesce(segment->>'invoice_target_week_start','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then
    raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
   perform (segment->>'invoice_target_week_start')::date;end if;
  if segment ? 'invoice_locked_invoice_id' then
   if jsonb_typeof(segment->'invoice_locked_invoice_id') is distinct from 'string' or coalesce(segment->>'invoice_locked_invoice_id','') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
   perform (segment->>'invoice_locked_invoice_id')::uuid;end if;
  for item in select value from jsonb_array_elements(segment->'breaks') loop
   if not private.weekly_exceptional_json_keys_exact_v1(item,array['start','end']) or not(item ?& array['start','end'])
    or coalesce(item->>'start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' or coalesce(item->>'end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
    raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
  end loop;
  identity:=segment->'weekly_protected_component_identity';
  event:=case when next_route then segment->>'work_event_id' else identity->>'work_event_id' end;
  if coalesce(event,'') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' or event=any(seen) then
   raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;seen:=array_append(seen,event);
  if not next_route then
   if not private.weekly_exceptional_json_keys_exact_v1(identity,array['schema_version','work_event_id']) or not(identity ?& array['schema_version','work_event_id'])
    or identity->>'schema_version' is distinct from 'WEEKLY_PROTECTED_COMPONENT_IDENTITY_V1' or segment->>'segment_id' is distinct from 'weekly-source-event:'||event then
    raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
  elsif coalesce(segment->>'protected_target_state','') not in('WAIT','SOURCE','ACCEPTED_SOURCE') then
   raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';
  elsif segment->>'protected_target_state'='WAIT' then
   preview:=segment->'protected_next_preview';
   keys:=array['schema_version','calculator_owner','calculator_version','mode','category','sign','bucket_minutes','paid_minutes','break_minutes','hours','rates','charge_rates','pay_ex_vat','charge_ex_vat'];
   if not private.weekly_exceptional_json_keys_exact_v1(preview,keys) or not(preview ?& keys)
    or preview->>'schema_version' is distinct from 'WEEKLY_PROTECTED_NEXT_PREVIEW_V1' or preview->>'calculator_owner' is distinct from 'buildWeeklyScheduleSegmentsSnapshot'
    or preview->>'calculator_version' is distinct from 'WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1' or coalesce(preview->>'mode','') not in('SPLIT_RATE_WINDOWS','WHOLE_SHIFT_START_DAY')
    or (preview->>'mode'='SPLIT_RATE_WINDOWS' and preview->'category' is distinct from 'null'::jsonb)
    or (preview->>'mode'='WHOLE_SHIFT_START_DAY' and coalesce(preview->>'category','') not in('day','night','sat','sun','bh'))
    or preview->'sign' not in('1'::jsonb,'-1'::jsonb)
    or exists(select 1 from unnest(array['paid_minutes','break_minutes']) k where jsonb_typeof(preview->k) is distinct from 'number' or coalesce(preview->>k,'') !~ '^(0|[1-9][0-9]*)$')
    or exists(select 1 from unnest(array['pay_ex_vat','charge_ex_vat']) k where jsonb_typeof(preview->k) is distinct from 'string' or coalesce(preview->>k,'') !~ '^-?(0|[1-9][0-9]*)\\.[0-9]{2}$') then
    raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
   foreach bucket in array array['bucket_minutes','hours','rates','charge_rates'] loop
    if not private.weekly_exceptional_json_keys_exact_v1(preview->bucket,array['day','night','sat','sun','bh'])
     or not((preview->bucket) ?& array['day','night','sat','sun','bh']) then raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
   end loop;
   foreach bucket in array array['day','night','sat','sun','bh'] loop
    if jsonb_typeof(preview->'bucket_minutes'->bucket) is distinct from 'number' or coalesce(preview->'bucket_minutes'->>bucket,'') !~ '^(0|[1-9][0-9]*)$'
     or jsonb_typeof(preview->'hours'->bucket) is distinct from 'string' or coalesce(preview->'hours'->>bucket,'') !~ '^-?(0|[1-9][0-9]*)(\\.[0-9]+)?$'
     or exists(select 1 from unnest(array['rates','charge_rates']) k where jsonb_typeof(preview->k->bucket) not in('string','null')
       or (jsonb_typeof(preview->k->bucket)='string' and (preview->k->>bucket) !~ '^-?(0|[1-9][0-9]*)(\\.[0-9]+)?$')) then
     raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
   end loop;
  elsif segment ? 'protected_next_preview' then raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';end if;
 end loop;
exception when invalid_text_representation or datetime_field_overflow or numeric_value_out_of_range then
 raise exception 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';
end;
$function$;
revoke all on function private.weekly_source_pay_decision_native_snapshot_assert_v1(jsonb,jsonb)
 from public,anon,authenticated,service_role;
`;
writeFileSync(path.join(base,'forward/44_strict_native_snapshot_validation.sqlinc'),sql);
const sha=value=>createHash('sha256').update(value).digest('hex');
const producers=[];
for(const commit of ['a934ee1d460b444939234e812babccb90dc3ddff','459559aead8b07e1013ac7d3086a9d478a985f03']){
 const source=execFileSync('git',['show',`${commit}:broker/src/index.js`],{encoding:'utf8',maxBuffer:24*1024*1024});
 const start=source.indexOf('async function buildWeeklyScheduleSegmentsSnapshot(');
 const end=source.indexOf('\nasync function rebuildFromExistingSegmentsEvidence(',start);
 if(start<0||end<start)throw new Error('Complete actual snapshot producer missing');
 producers.push({commit,source:'broker/src/index.js',complete_calculator_sha256:sha(source.slice(start,end)),
  required_snapshot_keys:SOURCE_NATIVE_SNAPSHOT_KEYS.length});
}
const helpers=['weekly-rate-owner.js','protected-next-preview.mjs'].map(name=>{
 const source=`broker/src/weekly-source/${name}`;
 const text=execFileSync('git',['show',`459559aead8b07e1013ac7d3086a9d478a985f03:${source}`],{encoding:'utf8'});
 return {commit:'459559aead8b07e1013ac7d3086a9d478a985f03',source,sha256:sha(text)};
});
writeFileSync(path.join(base,'NATIVE_SNAPSHOT_VALIDATOR_RECEIPT.json'),JSON.stringify({version:'SOURCE_NATIVE_SNAPSHOT_VALIDATION_SOURCE_V1',
 producers,helpers,calculator_bodies_equal:producers[0].complete_calculator_sha256===producers[1].complete_calculator_sha256,
 output:'forward/44_strict_native_snapshot_validation.sqlinc',sha256:sha(sql),
 current_host:{commit:'459559aead8b07e1013ac7d3086a9d478a985f03',state:'SOURCE_PRESENT',
  option:'retain_protected_next_preview_facts: input.retainNextPreviewFacts===true',qualification:'NATIVE_RUNTIME_REQUIRED_NOT_RUN'},
 policy_witness:{domain:'SOURCE_UNPROTECT_CALCULATION_POLICY_V1',operand:{context_sha256:'opaque actual server SOURCE_UNPROTECT_NATIVE_CONTEXT_V1 hash'},
  scope:'UNINSTALLED_INTERNAL_NEW_SOURCE_DOMAIN_SEMANTIC_CORRECTION',existing_native_policy_and_three_receipt_domains_changed:false,
  numeric_normalization:false,sql_independent_witness_owner:'forward/35_native_preview_quote_registration.sqlinc + forward/22_full_native_payload_preparation.sqlinc'},
 qualification:'SOURCE_ONLY_POSTGRESQL_NATIVE_BANK_FULL_ACCEPTANCE_REQUIRED_NOT_RUN'},null,2)+'\n');
process.stdout.write('Generated strict native snapshot/current policy/rate-reference validator; no calculation or hash authored.\n');
