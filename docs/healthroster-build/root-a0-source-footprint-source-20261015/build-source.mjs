import fs from 'node:fs';import cp from 'node:child_process';import path from 'node:path';import {createHash} from 'node:crypto';
const base='docs/healthroster-build/proposals/a0-source-native-footprint-construction-20261015';
const commit='8291f6290044e21a3210bd04a8db0d9cfd2f81e8';
const source='docs/healthroster-build/proposals/agency-source-selector-precision-forward-20261014/';
const sha=b=>createHash('sha256').update(b).digest('hex');
if(fs.existsSync(base))throw Error('Existing immutable source packet');
const put=(f,b)=>{fs.mkdirSync(path.dirname(base+'/'+f),{recursive:true});fs.writeFileSync(base+'/'+f,b,{flag:'wx'});};
const inputs=[['reference/85_source_dependency_witness.sqlinc',source+'construction/85_complete_source_dependency_witness_with_exact_selector_tuples.sqlinc'],['reference/02_source_facts.sqlinc',source+'reference/02_complete_current_source_facts_with_contract_client.sqlinc']];
const pins=inputs.map(([file,p])=>{const b=cp.execFileSync('git',['cat-file','blob',commit+':'+p],{maxBuffer:4e6});put(file,b);return{file,source_commit:commit,source_path:p,bytes:b.length,sha256:sha(b)};});
put('INPUT_PINS.json',JSON.stringify({source_only:true,pins},null,2)+'\n');
const sql=`-- Exact selected native rows and predicate tuples in the SAME A0 arbiter.
-- Owner-private dependency construction, not a writer admission or authority.
create or replace function private.timesheet_correction_a0_source_footprint_v1(
 p_source_cycle_id uuid,p_client_ids uuid[]
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $source_footprint$
declare witness jsonb; selectors jsonb; tokens jsonb:='[]'::jsonb; item jsonb; relation jsonb;
 field record; identity_key jsonb; value_text text; identity uuid; token jsonb; pair jsonb;
 clients jsonb; coverage jsonb; result jsonb;
begin
 -- One complete native facts read owns currentness and genuine Source CAS.
 witness:=private.weekly_source_agency_source_dependencies_v1(p_source_cycle_id,p_client_ids);
 if witness->>'schema_version' is distinct from 'WEEKLY_SOURCE_AGENCY_SOURCE_DEPENDENCIES_V1'
  or witness->>'source_cycle_id' is distinct from p_source_cycle_id::text
  or jsonb_typeof(witness->'relations') is distinct from 'array'
  or jsonb_typeof(witness->'native_selector_scopes') is distinct from 'object'
  or jsonb_typeof(witness->'source_cas') is distinct from 'object'
  or jsonb_typeof(witness->'client_ids') is distinct from 'array' then
  raise exception 'CORRECTION_A0_SOURCE_WITNESS_INVALID' using errcode='55000';end if;
 selectors:=witness->'native_selector_scopes';clients:=witness->'client_ids';
 -- Only the physical primary identity of each actually selected row is an
 -- object token. Other UUID columns are evidence, not broad parent tokens.
 for field in select * from(values
  ('public.weekly_source_cycles','id'),('public.clients','id'),
  ('public.weekly_source_group_clients','id'),('public.weekly_source_client_policies','id'),
  ('public.client_settings','id'),('public.contracts','id'),
  ('public.weekly_source_contract_policies','id'),('public.weekly_source_group_rule_assignments','id'),
  ('public.weekly_source_rule_set_versions','id'),('public.weekly_source_format_profiles','id'),
  ('public.weekly_source_uploads','id'),('public.weekly_source_projection_publications','id'),
  ('public.weekly_source_upload_rows','id'),('public.weekly_source_row_resolutions','id'),
  ('public.weekly_source_row_economic_snapshots','id'),('public.weekly_source_row_expense_policy_snapshots','id'),
  ('public.weekly_expense_authority_generations','id'),('public.weekly_source_expense_cell_evidence','id'),
  ('public.weekly_source_charge_checks','id'),('public.weekly_source_charge_acceptances','id'),
  ('public.weekly_source_invoice_hold_releases','id'),('public.weekly_source_state_transitions','id'),
  ('public.weekly_source_report_scopes','id'),('public.weekly_source_self_bill_reports','id'),
  ('public.weekly_source_self_bill_report_line_bindings','id'),('public.weekly_work_events','id'),
  ('public.weekly_source_final_revisions','id'),('public.weekly_source_final_snapshot_lines','id'),
  ('public.weekly_source_row_timesheet_lineages','id'),('public.weekly_source_ordinary_pay_projection_receipts','id'),
  ('public.timesheets_financials','id'),('public.timesheets','timesheet_id')
 ) actual(relation_name,primary_column) order by relation_name
 loop
  select value into strict relation from jsonb_array_elements(witness->'relations')
   where value->>'relation'=field.relation_name;
  select value into strict identity_key from jsonb_array_elements(relation->'keys')
   where value->>'column'=field.primary_column and value->>'type'='UUID';
  if jsonb_typeof(identity_key->'values') is distinct from 'array' then
   raise exception 'CORRECTION_A0_SOURCE_WITNESS_INVALID' using errcode='55000';end if;
  select coalesce(jsonb_agg(jsonb_build_object('kind','SOURCE_OBJECT',
   'json',jsonb_build_array(field.relation_name,value::uuid)::text)),'[]'::jsonb) into item
   from jsonb_array_elements_text(identity_key->'values');
  tokens:=tokens||item;
 end loop;
 tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_OBJECT',
  'json',jsonb_build_array('public.weekly_source_groups',(witness->>'source_group_id')::uuid)::text));
 for field in select * from(values
  ('selected_incident_ids','public.weekly_discrepancy_incidents'),
  ('selected_comparison_ids','public.weekly_issue_comparison_revisions'),
  ('selected_billing_movement_ids','public.weekly_source_billing_movements'),
  ('referenced_rule_set_ids','public.weekly_source_rule_sets'),
  ('exact_report_bound_resolution_ids','public.weekly_source_row_resolutions')
 ) extra(witness_field,relation_name)
 loop
  item:=case when field.witness_field='exact_report_bound_resolution_ids'
   then selectors->field.witness_field else witness->field.witness_field end;
  if jsonb_typeof(item) is distinct from 'array' then
   raise exception 'CORRECTION_A0_SOURCE_WITNESS_INVALID' using errcode='55000';end if;
  select coalesce(jsonb_agg(jsonb_build_object('kind','SOURCE_OBJECT',
   'json',jsonb_build_array(field.relation_name,value::uuid)::text)),'[]'::jsonb) into item
   from jsonb_array_elements_text(item);
  tokens:=tokens||item;
 end loop;
 for field in select * from(values
  ('resolution_slots','SOURCE_RESOLUTION_SLOT'),
  ('revision_client_pairs','SOURCE_REVISION_CLIENT'),('revision_event_pairs','SOURCE_REVISION_EVENT')
 ) tuples(selector_field,token_kind)
 loop
  if jsonb_typeof(selectors->field.selector_field) is distinct from 'array' then
   raise exception 'CORRECTION_A0_SOURCE_WITNESS_INVALID' using errcode='55000';end if;
  -- Aggregate the whole tuple population once; do not copy a growing JSONB
  -- array per row. Exact canonical native JSONB text is validated below.
  select coalesce(jsonb_agg(jsonb_build_object('kind',field.token_kind,'json',value::text)),
   '[]'::jsonb) into item from jsonb_array_elements(selectors->field.selector_field);
  tokens:=tokens||item;
 end loop;
 if jsonb_typeof(selectors->'group_client_pairs') is distinct from 'array'
  or jsonb_typeof(selectors->'contract_coverage') is distinct from 'object'
  or selectors->'global_settings_singleton' is distinct from 'true'::jsonb then
  raise exception 'CORRECTION_A0_SOURCE_WITNESS_INVALID' using errcode='55000';end if;
 for pair in select value from jsonb_array_elements(selectors->'group_client_pairs') loop
  tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_GROUP_CLIENT',
   'json',jsonb_build_array((pair->>'source_group_id')::uuid,(pair->>'client_id')::uuid)::text));
 end loop;
 coverage:=jsonb_build_array(selectors#>>'{contract_coverage,coverage_start}',
  selectors#>>'{contract_coverage,coverage_end}');
 for value_text in select jsonb_array_elements_text(clients) loop
  identity:=value_text::uuid;
  if identity is null then raise exception 'CORRECTION_A0_SOURCE_WITNESS_INVALID' using errcode='55000';end if;
  tokens:=tokens||jsonb_build_array(
   jsonb_build_object('kind','SOURCE_CYCLE_CLIENT','json',jsonb_build_array(p_source_cycle_id,identity)::text),
   jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',identity::text,'coverage',coverage));
 end loop;
 tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_GLOBAL_SETTINGS','json','true'));
 -- Whole tuple validation before any owner can retain it. Never sample,
 -- truncate by row count, or manufacture FAMILY_UUID from a Source row.
 for token in select value from jsonb_array_elements(tokens) loop
  perform private.timesheet_correction_a0_source_token_assert_v1(token);
 end loop;
 select coalesce(jsonb_agg(value order by value),'[]'::jsonb) into tokens
  from(select distinct value from jsonb_array_elements(tokens)) complete;
 result:=jsonb_build_object('schema_version','CORRECTION_A0_NATIVE_SOURCE_FOOTPRINT_V1',
  'tokens',tokens,'source_cas',witness->'source_cas','source_witness_sha256',witness->'witness_sha256',
  'family_bookings',witness->'family_bookings','lineage_bookings',witness->'lineage_bookings');
 return result;
exception when no_data_found or too_many_rows or invalid_text_representation then
 raise exception 'CORRECTION_A0_SOURCE_WITNESS_INVALID' using errcode='55000';
end;
$source_footprint$;
alter function private.timesheet_correction_a0_source_footprint_v1(uuid,uuid[]) owner to postgres;
revoke all on function private.timesheet_correction_a0_source_footprint_v1(uuid,uuid[]) from public,anon,authenticated,service_role;
`;
put('construction/01_native_source_footprint.sqlinc',sql);
put('SOURCE_BOUNDARY.md','# Native selected-row and precise-selector footprint construction\n\nSource only, unreviewed and unmounted. One owner-private dependency constructor delegates one complete native Source witness read and preserves its genuine Source CAS. It emits selected primary object identities across the32 literal native arrays, actual incident/comparison/movement/rule-set/report-bound identities, and precise row/generation/Client and revision/Client or revision/event tuples. Selected primary IDs do not become every parent UUID found on a child. Contract coverage, group/Client and cycle/Client predicates remain native typed domains; the global singleton is true. Family bookings are carried as evidence, never relabelled as a family.\n\nThis is dependency construction, not admission. Whole OLD/NEW guards, nested revision and lineage phantom predicates, transitive policy/claim dependencies, existing physical family derivation, actor/Candidate/I1 ordering, full re-derived Source CAS, allocated phase and ACTIVE between-RPC foreign-writer refusal/recovery are still required. Native GiST/dirty/RR, role, registry/verifier/contract and final acceptance gates remain unproved. No database, lock, fixture, deployment, external effect, business-data edit or cleanup.\n');
const artifacts=fs.readdirSync(base,{recursive:true}).filter(f=>fs.statSync(base+'/'+f).isFile()).sort().map(file=>{const b=fs.readFileSync(base+'/'+file);return{file:file.replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};});
put('SOURCE_MANIFEST.json',JSON.stringify({source_only:true,native:false,mount_ready:false,artifacts},null,2)+'\n');console.log(JSON.stringify({artifacts:artifacts.length,sql_sha256:sha(sql)}));
