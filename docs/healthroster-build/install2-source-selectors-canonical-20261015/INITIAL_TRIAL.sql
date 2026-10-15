set local jit=off;
set local pg_show_plans.is_enabled=off;
select current_database() as database,encode(sha256(convert_to(coalesce((select jsonb_agg(jsonb_build_object('oid',p.oid,'definition',pg_get_functiondef(p.oid),'acl',p.proacl,'owner',p.proowner) order by p.oid)::text from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname||'.'||p.proname in('private.weekly_source_agency_source_dependencies_v1','private.timesheet_correction_a0_source_footprint_v1','private.timesheet_correction_a0_source_raw_tokens_v1','private.timesheet_correction_a0_source_revision_tokens_v1','private.timesheet_correction_a0_source_policy_tokens_v1','private.timesheet_correction_a0_source_reference_before_v1','private.timesheet_correction_a0_source_token_assert_v1')),'[]'),'UTF8')),'hex') as routine_fingerprint,(select count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname||'.'||p.proname in('private.weekly_source_agency_source_dependencies_v1','private.timesheet_correction_a0_source_footprint_v1','private.timesheet_correction_a0_source_raw_tokens_v1','private.timesheet_correction_a0_source_revision_tokens_v1','private.timesheet_correction_a0_source_policy_tokens_v1','private.timesheet_correction_a0_source_reference_before_v1','private.timesheet_correction_a0_source_token_assert_v1')) as helper_count;
-- Closed grammar for actual Source dependencies in the SAME proposed A0 arbiter.
-- No caller SQL, fake family identity or monetary permission is introduced.
create or replace function private.timesheet_correction_a0_source_token_assert_v1(p_token jsonb)
returns void language plpgsql immutable security definer set search_path='pg_catalog','pg_temp'
as $source_token$
declare kind text:=p_token->>'kind';identity jsonb; i integer; expected jsonb; span daterange;
begin
 if kind not in('SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT') then
  if p_token ? 'coverage' then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 if kind in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE') then
  if jsonb_typeof(p_token->'coverage') is distinct from 'array' or jsonb_array_length(p_token->'coverage')<>2
   or exists(select 1 from jsonb_array_elements(p_token->'coverage') x where jsonb_typeof(x) not in('string','null')
    or(jsonb_typeof(x)='string' and pg_input_is_valid(x#>>'{}','date') is distinct from true)) then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  span:=daterange((p_token->'coverage'->>0)::date,(p_token->'coverage'->>1)::date,'[]');
  if isempty(span) then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 if p_token ? 'coverage' or jsonb_typeof(p_token->'json') is distinct from 'string' then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 if kind='SOURCE_GLOBAL_SETTINGS' then
  if p_token->>'json' is distinct from 'true' then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 begin identity:=(p_token->>'json')::jsonb;
 exception when invalid_text_representation then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end;
 if jsonb_typeof(identity) is distinct from 'array' or jsonb_array_length(identity)<>(case when kind='SOURCE_RESOLUTION_SLOT' then 3 else 2 end) then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 if kind='SOURCE_RESOLUTION_SLOT' then
  if jsonb_typeof(identity->0) is distinct from 'string' or pg_input_is_valid(identity->>0,'uuid') is distinct from true
   or jsonb_typeof(identity->1) is distinct from 'number' or pg_input_is_valid(identity->>1,'bigint') is distinct from true
   or jsonb_typeof(identity->2) is distinct from 'string' or pg_input_is_valid(identity->>2,'uuid') is distinct from true then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  expected:=jsonb_build_array((identity->>0)::uuid,(identity->>1)::bigint,(identity->>2)::uuid);
 elsif kind='SOURCE_OBJECT' then
  if jsonb_typeof(identity->0) is distinct from 'string' or identity->>0 not in('public.weekly_source_groups','public.weekly_source_cycles','public.weekly_source_uploads','public.weekly_source_upload_rows','public.weekly_source_projection_publications','public.weekly_source_row_resolutions','public.weekly_source_row_economic_snapshots','public.weekly_source_row_expense_policy_snapshots','public.weekly_expense_authority_generations','public.weekly_source_expense_cell_evidence','public.weekly_source_charge_checks','public.weekly_source_charge_acceptances','public.weekly_source_invoice_hold_releases','public.weekly_source_state_transitions','public.weekly_source_report_scopes','public.weekly_source_self_bill_reports','public.weekly_source_self_bill_report_line_bindings','public.weekly_work_events','public.weekly_source_final_revisions','public.weekly_source_final_snapshot_lines','public.weekly_source_row_timesheet_lineages','public.weekly_source_ordinary_pay_projection_receipts','public.weekly_discrepancy_incidents','public.weekly_issue_comparison_revisions','public.weekly_source_billing_movements','public.weekly_source_group_clients','public.weekly_source_client_policies','public.weekly_source_contract_policies','public.weekly_source_group_rule_assignments','public.weekly_source_rule_set_versions','public.weekly_source_rule_sets','public.weekly_source_format_profiles','public.clients','public.client_settings','public.contracts','public.timesheets','public.timesheets_financials')
   or jsonb_typeof(identity->1) is distinct from 'string' or pg_input_is_valid(identity->>1,'uuid') is distinct from true then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  expected:=jsonb_build_array(identity->>0,(identity->>1)::uuid);
 else
  for i in 0..1 loop
   if jsonb_typeof(identity->i) is distinct from 'string' or pg_input_is_valid(identity->>i,'uuid') is distinct from true then
    raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  end loop;
  expected:=jsonb_build_array((identity->>0)::uuid,(identity->>1)::uuid);
 end if;
 if p_token->>'json' is distinct from expected::text then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
end;
$source_token$;
alter function private.timesheet_correction_a0_source_token_assert_v1(jsonb) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_token_assert_v1(jsonb) from public,anon,authenticated,service_role;

-- Canonical exact Source witness/footprint/raw/nested helper composition.
-- No trigger registration or application admission in this module.

-- Finite native dependency projection for the SAME proposed A0 arbiter.
-- This reads actual selected Source parents. It does not retain a token,
-- admit a writer, establish role/lock authority or create a second fence.
create or replace function private.weekly_source_agency_source_dependencies_v1(
 p_source_cycle_id uuid,p_client_ids uuid[]
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
 facts jsonb; result jsonb; parents jsonb; relations jsonb; incidents jsonb; comparisons jsonb;
 billing_movements jsonb; rule_set_ids jsonb; resolution_slots jsonb; revision_client_pairs jsonb;
 revision_event_pairs jsonb; exact_report_bound_resolutions jsonb; policy_dates jsonb; policy_groups jsonb; policy_parents jsonb;
 cycle public.weekly_source_cycles%rowtype; upload public.weekly_source_uploads%rowtype;
 group_id uuid; clients uuid[]; field record; item jsonb; key_name text; value_text text;
 values_json jsonb; coverage_start date; coverage_end date;
begin
 -- The existing Source reader owns current publication, accepted upload,
 -- real Client scope and the exact native Source economic/identity checks.
 facts:=private.weekly_source_bulk_source_facts_v1(p_source_cycle_id,p_client_ids);
 if facts->>'schema_version' is distinct from 'WEEKLY_SOURCE_BULK_SOURCE_FACTS_V1'
  or facts->>'source_cycle_id' is distinct from p_source_cycle_id::text
  or jsonb_typeof(facts->'client_ids') is distinct from 'array'
  or jsonb_typeof(facts->'group') is distinct from 'object' then
  raise exception 'AGENCY_SOURCE_DEPENDENCY_FACTS_UNAVAILABLE' using errcode='55000';end if;
 select coalesce(array_agg(value::uuid order by value::uuid),'{}'::uuid[]) into clients
  from jsonb_array_elements_text(facts->'client_ids') value;
 group_id:=(facts#>>'{group,id}')::uuid;
 select * into strict cycle from public.weekly_source_cycles where id=p_source_cycle_id and source_group_id=group_id;
 select * into strict upload from public.weekly_source_uploads where id=cycle.current_complete_upload_id;
 coverage_start:=coalesce(upload.confirmed_coverage_start_local_date,cycle.finalisation_week_ending-6);
 coverage_end:=coalesce(upload.confirmed_coverage_end_local_date,cycle.finalisation_week_ending);
 if coverage_start is null or coverage_end is null or coverage_start>coverage_end then
  raise exception 'AGENCY_SOURCE_DEPENDENCY_COVERAGE_UNAVAILABLE' using errcode='55000';end if;

 -- Exactly the current bulk reader's selected relation arrays. These preserve
 -- raw NULLs by omitting absent keys; no row is assigned a synthetic parent.
 -- SQL names/columns are literal. There is no caller relation selector or SQL.
 relations:='[]'::jsonb;
 for field in select * from(values
  ('cycles','weekly_source_cycles',array['id','source_group_id','scope_client_id']),
  ('clients','clients',array['id']),
  ('memberships','weekly_source_group_clients',array['id','source_group_id','client_id']),
  ('client_policies','weekly_source_client_policies',array['id','source_group_id','client_id']),
  ('client_settings','client_settings',array['id','client_id']),
  ('contracts','contracts',array['id','client_id','candidate_id']),
  ('contract_policies','weekly_source_contract_policies',array['id','contract_id']),
  ('assignments','weekly_source_group_rule_assignments',array['id','source_group_id','rule_set_version_id']),
  ('rule_versions','weekly_source_rule_set_versions',array['id','rule_set_id']),
  ('profiles','weekly_source_format_profiles',array['id']),
  ('uploads','weekly_source_uploads',array['id','source_cycle_id','report_scope_id','source_format_profile_id','correction_session_id']),
  ('publications','weekly_source_projection_publications',array['id','source_cycle_id','report_scope_id','upload_id','correction_session_id']),
  ('rows','weekly_source_upload_rows',array['id','upload_id']),
  ('resolutions','weekly_source_row_resolutions',array['id','upload_row_id','candidate_id','client_id','contract_id','work_event_id']),
  ('economics','weekly_source_row_economic_snapshots',array['id','row_resolution_id','upload_row_id','work_event_id','candidate_id','client_id','contract_id']),
  ('expense_policy_snapshots','weekly_source_row_expense_policy_snapshots',array['id','row_resolution_id','upload_row_id','work_event_id','candidate_id','client_id','contract_id']),
  ('expense_authorities','weekly_expense_authority_generations',array['id','final_revision_id','work_event_id','contract_id','row_expense_policy_snapshot_id','prior_expense_authority_generation_id']),
  ('expense_cell_evidence','weekly_source_expense_cell_evidence',array['id','upload_id']),
  ('charge_checks','weekly_source_charge_checks',array['id','upload_row_id','row_resolution_id']),
  ('charge_acceptances','weekly_source_charge_acceptances',array['id','upload_row_id','row_resolution_id','charge_check_id','source_upload_id','contract_id']),
  ('invoice_hold_releases','weekly_source_invoice_hold_releases',array['id','upload_row_id']),
  ('transitions','weekly_source_state_transitions',array['id','final_revision_id','finalisation_cycle_id','work_event_id','previous_snapshot_line_id','new_snapshot_line_id']),
  ('report_scopes','weekly_source_report_scopes',array['id','source_cycle_id','source_group_id','client_id','current_complete_upload_id','current_final_revision_id','current_projection_publication_id','self_bill_report_id']),
  ('reports','weekly_source_self_bill_reports',array['id','source_group_id','client_id','source_cycle_id']),
  ('report_bindings','weekly_source_self_bill_report_line_bindings',array['id','report_row_id','export_upload_row_id','bound_row_resolution_id','export_projection_publication_id','work_event_id','self_bill_report_id']),
  ('events','weekly_work_events',array['id','candidate_id','client_id','first_source_group_id','source_format_profile_id']),
  ('revisions','weekly_source_final_revisions',array['id','source_cycle_id','report_scope_id','upload_id','predecessor_revision_id','prior_state_cutoff_revision_id','rule_set_version_id']),
  ('snapshots','weekly_source_final_snapshot_lines',array['id','final_revision_id','upload_row_id','row_resolution_id','charge_check_id','work_event_id','candidate_id','client_id','contract_id']),
  ('lineages','weekly_source_row_timesheet_lineages',array['id','row_resolution_id','source_cycle_id','work_event_id','candidate_id','client_id','contract_id','contract_week_id','timesheet_id']),
  ('pay_receipts','weekly_source_ordinary_pay_projection_receipts',array['id','final_revision_id','source_cycle_id','client_id','root_timesheet_id','published_timesheet_financial_id']),
  ('financial_identities','timesheets_financials',array['id','timesheet_id','client_id','candidate_id']),
  ('root_identities','timesheets',array['timesheet_id','contract_id'])
 ) mapping(facts_field,relation_name,uuid_columns) order by relation_name
 loop
  if jsonb_typeof(facts->field.facts_field) is distinct from 'array' then
   raise exception 'AGENCY_SOURCE_DEPENDENCY_FACTS_UNAVAILABLE' using errcode='55000';end if;
  parents:='[]'::jsonb;
  foreach key_name in array field.uuid_columns loop
   if exists(select 1 from jsonb_array_elements(facts->field.facts_field) row_data
    where jsonb_typeof(row_data) is distinct from 'object'
     or(row_data ? key_name and jsonb_typeof(row_data->key_name) is distinct from 'null'
      and(jsonb_typeof(row_data->key_name) is distinct from 'string'
       or pg_input_is_valid(row_data->>key_name,'uuid') is distinct from true))) then
    raise exception 'AGENCY_SOURCE_DEPENDENCY_RAW_ID_INVALID' using errcode='55000';end if;
   select coalesce(jsonb_agg(to_jsonb(id) order by id),'[]'::jsonb) into values_json from(
    select distinct (row_data->>key_name)::uuid id from jsonb_array_elements(facts->field.facts_field) row_data
     where row_data->>key_name is not null) actual;
   parents:=parents||jsonb_build_array(jsonb_build_object('column',key_name,'type','UUID','values',values_json));
  end loop;
  relations:=relations||jsonb_build_array(jsonb_build_object('relation','public.'||field.relation_name,'keys',parents));
 end loop;

 -- These IDs are used by the native Source selector but deliberately absent
 -- from its financial facts JSON. Select ALL states, before parent lookup.
 select coalesce(jsonb_agg(to_jsonb(id) order by id),'[]'::jsonb) into incidents from(
  select incident.id from public.weekly_discrepancy_incidents incident
   where incident.source_cycle_id=cycle.id and incident.client_id=any(clients)) native;
 select coalesce(jsonb_agg(to_jsonb(id) order by id),'[]'::jsonb) into comparisons from(
  select distinct comparison.id from public.weekly_issue_comparison_revisions comparison
   join public.weekly_discrepancy_incidents incident on incident.current_comparison_revision_id=comparison.id
   where incident.source_cycle_id=cycle.id and incident.client_id=any(clients)) native;
 -- The bulk reader validates these exact movement IDs/economic identities
 -- against the actual receipt/root/financial provenance before returning.
 -- They are a native read dependency even though the reader does not return
 -- whole movement rows. No Report/NONE invoice companion becomes PAY basis.
 select coalesce(jsonb_agg(to_jsonb(id) order by id),'[]'::jsonb) into billing_movements from(
  select distinct (unit->>'movement_id')::uuid id
   from jsonb_array_elements(facts->'pay_receipts') receipt
   cross join lateral jsonb_array_elements(case when jsonb_typeof(receipt->'source_unit_outcomes_json')='array'
    then receipt->'source_unit_outcomes_json' else '[]'::jsonb end) unit) native;
 select coalesce(jsonb_agg(to_jsonb(id) order by id),'[]'::jsonb) into rule_set_ids from(
  select distinct (version->>'rule_set_id')::uuid id from jsonb_array_elements(facts->'rule_versions') version
   where version->>'rule_set_id' is not null) native;
 -- Predicate-shaped phantom scopes come from the SAME real facts read.
 -- Every Source row/publication upload match is retained for every actually
 -- selected Client, including slots with no currently visible resolution.
 -- An existing report-bound resolution is a separate exact object dependency.
 -- No parent-only upload/revision/event/Client scope substitutes for these
 -- conjunctions, and no LIMIT/sample/truncation can claim a complete footprint.
 with actual_rows as (
  select (row_data->>'id')::uuid upload_row_id,(row_data->>'upload_id')::uuid upload_id
   from jsonb_array_elements(facts->'rows') row_data
 ), actual_publications as (
  select (publication->>'upload_id')::uuid upload_id,
   coalesce(publication->>'projection_generation',publication->>'authority_scope_version')::integer generation
   from jsonb_array_elements(facts->'publications') publication
 ), actual_slots as (
  select distinct row_data.upload_row_id,publication.generation,client_id
   from actual_rows row_data join actual_publications publication on publication.upload_id=row_data.upload_id
   cross join unnest(clients) client_id
 )
 select coalesce(jsonb_agg(jsonb_build_array(upload_row_id,generation,client_id)
  order by upload_row_id,generation,client_id),'[]'::jsonb) into resolution_slots from actual_slots;
 select coalesce(jsonb_agg(jsonb_build_array(revision_id,client_id) order by revision_id,client_id),'[]'::jsonb)
  into revision_client_pairs from(
   select distinct (revision->>'id')::uuid revision_id,client_id
    from jsonb_array_elements(facts->'revisions') revision cross join unnest(clients) client_id
  ) native;
 select coalesce(jsonb_agg(jsonb_build_array(revision_id,event_id) order by revision_id,event_id),'[]'::jsonb)
  into revision_event_pairs from(
   select distinct (revision->>'id')::uuid revision_id,(event->>'id')::uuid event_id
    from jsonb_array_elements(facts->'revisions') revision cross join jsonb_array_elements(facts->'events') event
  ) native;
 select coalesce(jsonb_agg(to_jsonb(resolution_id) order by resolution_id),'[]'::jsonb)
  into exact_report_bound_resolutions from(
   select distinct (binding->>'bound_row_resolution_id')::uuid resolution_id
    from jsonb_array_elements(facts->'report_bindings') binding
    where binding->>'bound_row_resolution_id' is not null
  ) native;
 -- The effective-policy reader counts ALL active groups for each actual
 -- Client/work date. A competing group's membership is a real phantom even
 -- when the cycle's own group has not changed. No extra facts/CAS read.
 select coalesce(jsonb_agg(jsonb_build_array(client_id,work_date) order by client_id,work_date),'[]'::jsonb)
  into policy_dates from(select distinct (item->>'client_id')::uuid client_id,(item->>'work_date')::date work_date
   from jsonb_array_elements(facts->'resolved_policies') item) actual;
 select coalesce(jsonb_agg(jsonb_build_array(group_id,client_id) order by group_id,client_id),'[]'::jsonb)
  into policy_groups from(select distinct (item#>>'{policy,source_group_id}')::uuid group_id,(item->>'client_id')::uuid client_id
   from jsonb_array_elements(facts->'resolved_policies') item) actual;
 select coalesce(jsonb_agg(jsonb_build_array(relation_name,id) order by relation_name,id),'[]'::jsonb)
  into policy_parents from(select distinct field.relation_name,(item#>>array['policy',field.policy_key])::uuid id
   from jsonb_array_elements(facts->'resolved_policies') item cross join(values
    ('public.weekly_source_groups','source_group_id'),
    ('public.weekly_source_group_clients','source_group_membership_id'),
    ('public.weekly_source_client_policies','client_policy_id'),
    ('public.weekly_source_contract_policies','contract_policy_id')
   ) field(relation_name,policy_key) where item#>>array['policy',field.policy_key] is not null
   union select distinct 'public.contracts',(item->>'contract_id')::uuid
    from jsonb_array_elements(facts->'resolved_policies') item where item->>'contract_id' is not null) actual;
 -- Group/client and date predicates are retained as typed native selectors,
 -- not incorrectly squeezed into FAMILY_UUID or an existing cycle token.
 result:=jsonb_build_object('schema_version','WEEKLY_SOURCE_AGENCY_SOURCE_DEPENDENCIES_V1',
  'source_cycle_id',cycle.id,'source_group_id',group_id,'client_ids',to_jsonb(clients),
  'source_cas',jsonb_build_object('schema_version','WEEKLY_SOURCE_BULK_SOURCE_CAS_V1','source_cycle_id',cycle.id,
   'client_ids',to_jsonb(clients),'source_cas_sha256',encode(private.weekly_source_sha256_jsonb_v1(
     'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1',facts),'hex')),
  'relations',relations,'selected_incident_ids',incidents,'selected_comparison_ids',comparisons,
  'selected_billing_movement_ids',billing_movements,'referenced_rule_set_ids',rule_set_ids,
  'native_selector_scopes',jsonb_build_object(
   'group_client_pairs',coalesce((select jsonb_agg(jsonb_build_object('source_group_id',group_id,'client_id',id) order by id)
     from unnest(clients) id),'[]'::jsonb),
   'policy_date_pairs',policy_dates,'policy_group_client_pairs',policy_groups,'policy_parent_objects',policy_parents,
   'resolution_slots',resolution_slots,
   'revision_client_pairs',revision_client_pairs,'revision_event_pairs',revision_event_pairs,
   'exact_report_bound_resolution_ids',exact_report_bound_resolutions,
   'contract_coverage',jsonb_build_object('client_ids',to_jsonb(clients),
     'coverage_start',coverage_start,'coverage_end',coverage_end),
   'all_assignments_source_group_id',group_id,'all_client_settings_client_ids',to_jsonb(clients),
   'global_settings_singleton',true),
  'family_bookings',coalesce((select jsonb_agg(jsonb_build_object('timesheet_id',row_data->'timesheet_id','booking_id',row_data->'booking_id')
    order by row_data->>'timesheet_id') from jsonb_array_elements(facts->'root_identities') row_data),'[]'::jsonb),
  'lineage_bookings',coalesce((select jsonb_agg(jsonb_build_object('lineage_id',row_data->'id','family_booking_id',row_data->'family_booking_id')
    order by row_data->>'id') from jsonb_array_elements(facts->'lineages') row_data),'[]'::jsonb));
 if octet_length(result::text)>8388608 then
  raise exception 'AGENCY_SOURCE_DEPENDENCY_SCOPE_TOO_LARGE' using errcode='54000';end if;
 return result||jsonb_build_object('witness_sha256',encode(private.weekly_source_sha256_jsonb_v1(
  'WEEKLY_SOURCE_AGENCY_SOURCE_DEPENDENCIES_V1',result),'hex'));
exception when no_data_found or too_many_rows or invalid_text_representation then
 raise exception 'AGENCY_SOURCE_DEPENDENCY_SCOPE_UNAVAILABLE' using errcode='55000';
end;
$function$;
alter function private.weekly_source_agency_source_dependencies_v1(uuid,uuid[]) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_agency_source_dependencies_v1(uuid,uuid[]) from public,anon,authenticated,service_role;

-- Exact selected native rows and predicate tuples in the SAME A0 arbiter.
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
 -- Precise actual resolved-policy transitive closure, including any genuine
 -- earlier/foreign group selected by the native Client/work-date policy owner.
 for field in select * from(values('policy_parent_objects','SOURCE_OBJECT'),
   ('policy_group_client_pairs','SOURCE_GROUP_CLIENT')) mapping(selector_field,token_kind) loop
  if jsonb_typeof(selectors->field.selector_field) is distinct from 'array' then
   raise exception 'CORRECTION_A0_SOURCE_WITNESS_INVALID' using errcode='55000';end if;
  select coalesce(jsonb_agg(jsonb_build_object('kind',field.token_kind,'json',value::text)),'[]'::jsonb)
   into item from jsonb_array_elements(selectors->field.selector_field);tokens:=tokens||item;
 end loop;
 if jsonb_typeof(selectors->'policy_date_pairs') is distinct from 'array' then
  raise exception 'CORRECTION_A0_SOURCE_WITNESS_INVALID' using errcode='55000';end if;
 select coalesce(jsonb_agg(jsonb_build_object('kind','SOURCE_POLICY_MEMBERSHIP_COVERAGE',
  'uuid',(value->>0)::uuid::text,'coverage',jsonb_build_array((value->>1)::date,(value->>1)::date))),'[]'::jsonb)
  into item from jsonb_array_elements(selectors->'policy_date_pairs');tokens:=tokens||item;
 -- Revisions are selected in the actual Source group by the current cycle,
 -- a selected Report scope, or an existing snapshot of a selected event.
 -- Current-cycle identity is a genuine original SOURCE_CYCLE_UUID domain.
 tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_CYCLE_UUID','uuid',p_source_cycle_id::text));
 for field in select * from(values
  ('public.weekly_source_report_scopes','SOURCE_GROUP_REPORT_REVISION'),
  ('public.weekly_work_events','SOURCE_GROUP_REVISION_EVENT')
 ) revision_scope(relation_name,token_kind)
 loop
  select value into strict relation from jsonb_array_elements(witness->'relations')
   where value->>'relation'=field.relation_name;
  select value into strict identity_key from jsonb_array_elements(relation->'keys')
   where value->>'column'='id' and value->>'type'='UUID';
  select coalesce(jsonb_agg(jsonb_build_object('kind',field.token_kind,
   'json',jsonb_build_array((witness->>'source_group_id')::uuid,value::uuid)::text)),'[]'::jsonb) into item
   from jsonb_array_elements_text(identity_key->'values');
  tokens:=tokens||item;
 end loop;
 -- Native lineage predicate: selected Client AND (selected resolution OR
 -- selected event). Both phantom arms remain distinct typed conjunctions.
 for field in select * from(values
  ('public.weekly_source_row_resolutions','SOURCE_LINEAGE_RESOLUTION_CLIENT'),
  ('public.weekly_work_events','SOURCE_LINEAGE_EVENT_CLIENT')
 ) lineage(relation_name,token_kind)
 loop
  select value into strict relation from jsonb_array_elements(witness->'relations')
   where value->>'relation'=field.relation_name;
  select value into strict identity_key from jsonb_array_elements(relation->'keys')
   where value->>'column'='id' and value->>'type'='UUID';
  select coalesce(jsonb_agg(jsonb_build_object('kind',field.token_kind,
   'json',jsonb_build_array(parent_id::uuid,client_id::uuid)::text)),'[]'::jsonb) into item
   from jsonb_array_elements_text(identity_key->'values') parents(parent_id)
   cross join jsonb_array_elements_text(clients) selected(client_id);
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
alter function private.timesheet_correction_a0_source_footprint_v1(uuid,uuid[]) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_footprint_v1(uuid,uuid[]) from public,anon,authenticated,service_role;

-- Raw immutable selector extraction. No parent/request lookup precedes it.
-- Same proposed A0 arbiter; no business pricing, publication or entitlement.
create or replace function private.timesheet_correction_a0_source_raw_tokens_v1(p_relation text,p_row jsonb)
returns jsonb language plpgsql immutable security definer set search_path to 'pg_catalog','pg_temp'
as $source_raw$
declare tokens jsonb:='[]'::jsonb; identity uuid; primary_key text; token jsonb; pair jsonb;
 parent_relation text; parent_column text; scope_kind text; left_column text; right_column text;
begin
 if jsonb_typeof(p_row) is distinct from 'object' or p_relation is null
  or p_relation not in('public.weekly_source_groups','public.weekly_source_cycles','public.weekly_source_uploads','public.weekly_source_upload_rows','public.weekly_source_projection_publications','public.weekly_source_row_resolutions','public.weekly_source_row_economic_snapshots','public.weekly_source_row_expense_policy_snapshots','public.weekly_expense_authority_generations','public.weekly_source_expense_cell_evidence','public.weekly_source_charge_checks','public.weekly_source_charge_acceptances','public.weekly_source_invoice_hold_releases','public.weekly_source_state_transitions','public.weekly_source_report_scopes','public.weekly_source_self_bill_reports','public.weekly_source_self_bill_report_line_bindings','public.weekly_work_events','public.weekly_source_final_revisions','public.weekly_source_final_snapshot_lines','public.weekly_source_row_timesheet_lineages','public.weekly_source_ordinary_pay_projection_receipts','public.weekly_discrepancy_incidents','public.weekly_issue_comparison_revisions','public.weekly_source_billing_movements','public.weekly_source_group_clients','public.weekly_source_client_policies','public.weekly_source_contract_policies','public.weekly_source_group_rule_assignments','public.weekly_source_rule_set_versions','public.weekly_source_rule_sets','public.weekly_source_format_profiles','public.clients','public.client_settings','public.contracts','public.timesheets','public.timesheets_financials','public.weekly_source_global_settings') then
  raise exception 'CORRECTION_A0_SOURCE_RAW_CONTEXT_INVALID' using errcode='55000';end if;
 if p_relation='public.weekly_source_global_settings' then
  if p_row->'singleton'='true'::jsonb then
   return jsonb_build_array(jsonb_build_object('kind','SOURCE_GLOBAL_SETTINGS','json','true'));
  end if;
  return tokens;
 end if;
 primary_key:=case when p_relation='public.timesheets' then 'timesheet_id' else 'id' end;
 identity:=(p_row->>primary_key)::uuid;
 if identity is null then raise exception 'CORRECTION_A0_SOURCE_RAW_CONTEXT_INVALID' using errcode='55000';end if;
 tokens:=jsonb_build_array(jsonb_build_object('kind','SOURCE_OBJECT',
  'json',jsonb_build_array(p_relation,identity)::text));
 -- These exact parent predicates select ALL child rows; another UUID column
 -- on a child is never automatically a dependency/phantom token.
 case p_relation
 when 'public.weekly_source_upload_rows','public.weekly_source_expense_cell_evidence' then
  parent_relation:='public.weekly_source_uploads';parent_column:='upload_id';
 when 'public.weekly_source_row_economic_snapshots','public.weekly_source_row_expense_policy_snapshots',
   'public.weekly_source_charge_checks','public.weekly_source_charge_acceptances' then
  parent_relation:='public.weekly_source_row_resolutions';parent_column:='row_resolution_id';
 when 'public.weekly_source_invoice_hold_releases' then
  parent_relation:='public.weekly_source_upload_rows';parent_column:='upload_row_id';
 when 'public.weekly_source_contract_policies' then
  parent_relation:='public.contracts';parent_column:='contract_id';
 when 'public.client_settings' then
  parent_relation:='public.clients';parent_column:='client_id';
 when 'public.weekly_source_group_rule_assignments' then
  parent_relation:='public.weekly_source_groups';parent_column:='source_group_id';
 when 'public.weekly_source_self_bill_report_line_bindings' then
  if p_row->>'state'='CURRENT' then
   parent_relation:='public.weekly_source_self_bill_reports';parent_column:='self_bill_report_id';
  end if;
 else null;
 end case;
 if parent_column is not null and p_row->>parent_column is not null then
  tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_OBJECT',
   'json',jsonb_build_array(parent_relation,(p_row->>parent_column)::uuid)::text));
 end if;
 if p_relation='public.weekly_source_group_clients' then
  if p_row->>'client_id' is null or p_row->>'valid_from' is null then
   raise exception 'CORRECTION_A0_SOURCE_RAW_CONTEXT_INVALID' using errcode='55000';end if;
  tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_POLICY_MEMBERSHIP_COVERAGE',
   'uuid',(p_row->>'client_id')::uuid::text,'coverage',jsonb_build_array((p_row->>'valid_from')::date,(p_row->>'valid_to')::date)));
 end if;
 if p_relation='public.weekly_source_final_revisions' and p_row->>'state' in('CURRENT','PREPARED','SUPERSEDED')
  and p_row->>'source_cycle_id' is not null then
  tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_CYCLE_UUID','uuid',(p_row->>'source_cycle_id')::uuid::text));
 end if;
 if p_relation='public.weekly_source_row_resolutions' and p_row->>'upload_row_id' is not null
  and p_row->>'generation' is not null and p_row->>'client_id' is not null then
  -- Never broaden this AND slot to a raw-row/upload/Client object token.
  pair:=jsonb_build_array((p_row->>'upload_row_id')::uuid,(p_row->>'generation')::integer,(p_row->>'client_id')::uuid);
  tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_RESOLUTION_SLOT','json',pair::text));
 end if;
 case p_relation
 when 'public.weekly_source_final_snapshot_lines','public.weekly_source_ordinary_pay_projection_receipts' then
  scope_kind:='SOURCE_REVISION_CLIENT';left_column:='final_revision_id';right_column:='client_id';
 when 'public.weekly_expense_authority_generations','public.weekly_source_state_transitions' then
  scope_kind:='SOURCE_REVISION_EVENT';left_column:='final_revision_id';right_column:='work_event_id';
 when 'public.weekly_source_group_clients','public.weekly_source_client_policies','public.weekly_source_self_bill_reports' then
  scope_kind:='SOURCE_GROUP_CLIENT';left_column:='source_group_id';right_column:='client_id';
 when 'public.weekly_source_report_scopes' then
  if p_row->>'self_bill_report_id' is not null then
   scope_kind:='SOURCE_GROUP_CLIENT';left_column:='source_group_id';right_column:='client_id';
  end if;
 when 'public.weekly_discrepancy_incidents' then
  scope_kind:='SOURCE_CYCLE_CLIENT';left_column:='source_cycle_id';right_column:='client_id';
 else null;
 end case;
 if scope_kind is not null and p_row->>left_column is not null and p_row->>right_column is not null then
  tokens:=tokens||jsonb_build_array(jsonb_build_object('kind',scope_kind,
   'json',jsonb_build_array((p_row->>left_column)::uuid,(p_row->>right_column)::uuid)::text));
 end if;
 if p_relation='public.weekly_source_row_timesheet_lineages' and p_row->>'client_id' is not null then
  if p_row->>'row_resolution_id' is not null then
   tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_LINEAGE_RESOLUTION_CLIENT',
    'json',jsonb_build_array((p_row->>'row_resolution_id')::uuid,(p_row->>'client_id')::uuid)::text));
  end if;
  if p_row->>'work_event_id' is not null then
   tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_LINEAGE_EVENT_CLIENT',
    'json',jsonb_build_array((p_row->>'work_event_id')::uuid,(p_row->>'client_id')::uuid)::text));
  end if;
 end if;
 if p_relation='public.contracts' and p_row->>'client_id' is not null
  and p_row->>'start_date' is not null and p_row->>'end_date' is not null
  and (p_row->>'start_date')::date<=(p_row->>'end_date')::date then
  tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE',
   'uuid',(p_row->>'client_id')::uuid::text,'coverage',jsonb_build_array(p_row->>'start_date',p_row->>'end_date')));
 end if;
 for token in select value from jsonb_array_elements(tokens) loop
  perform private.timesheet_correction_a0_source_token_assert_v1(token);
 end loop;
 select coalesce(jsonb_agg(value order by value),'[]'::jsonb) into tokens
  from(select distinct value from jsonb_array_elements(tokens)) complete;
 return tokens;
exception when invalid_text_representation or datetime_field_overflow then
 raise exception 'CORRECTION_A0_SOURCE_RAW_CONTEXT_INVALID' using errcode='55000';
end;
$source_raw$;
alter function private.timesheet_correction_a0_source_raw_tokens_v1(text,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_raw_tokens_v1(text,jsonb) from public,anon,authenticated,service_role;

-- Raw primary/conjunction acquisition in the trigger has already occurred.
-- This subsequent phase reads the actual parent group. Missing RR parents
-- refuse with a retryable error; they cannot silently erase a dependency.
create or replace function private.timesheet_correction_a0_source_revision_tokens_v1(p_relation text,p_row jsonb)
returns jsonb language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $source_revision$
declare tokens jsonb:='[]'::jsonb; v_group_id uuid; v_state text; item jsonb; token jsonb;
begin
 if p_relation is null then raise exception 'CORRECTION_A0_SOURCE_RAW_CONTEXT_INVALID' using errcode='55000';end if;
 if p_relation not in('public.weekly_source_final_revisions','public.weekly_source_final_snapshot_lines') then return tokens;end if;
 if jsonb_typeof(p_row) is distinct from 'object' then
  raise exception 'CORRECTION_A0_SOURCE_RAW_CONTEXT_INVALID' using errcode='55000';end if;
 if p_relation='public.weekly_source_final_revisions' then
  if coalesce(p_row->>'state','') not in('CURRENT','PREPARED','SUPERSEDED') then return tokens;end if;
  -- The direct actual cycle UUID was acquired in the preceding raw phase.
  select cycle.source_group_id into strict v_group_id from public.weekly_source_cycles cycle
   where cycle.id=(p_row->>'source_cycle_id')::uuid;
  if p_row->>'report_scope_id' is not null then
   tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_GROUP_REPORT_REVISION',
    'json',jsonb_build_array(v_group_id,(p_row->>'report_scope_id')::uuid)::text));
  end if;
  select coalesce(jsonb_agg(jsonb_build_object('kind','SOURCE_GROUP_REVISION_EVENT',
   'json',jsonb_build_array(v_group_id,snapshot.work_event_id)::text)),'[]'::jsonb) into item
   from public.weekly_source_final_snapshot_lines snapshot where snapshot.final_revision_id=(p_row->>'id')::uuid
    and snapshot.work_event_id is not null;
  tokens:=tokens||item;
 else
  select cycle.source_group_id,revision.state into strict v_group_id,v_state
   from public.weekly_source_final_revisions revision join public.weekly_source_cycles cycle on cycle.id=revision.source_cycle_id
   where revision.id=(p_row->>'final_revision_id')::uuid;
  if v_state in('CURRENT','PREPARED','SUPERSEDED') and p_row->>'work_event_id' is not null then
   tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_GROUP_REVISION_EVENT',
    'json',jsonb_build_array(v_group_id,(p_row->>'work_event_id')::uuid)::text));
  end if;
 end if;
 for token in select value from jsonb_array_elements(tokens) loop
  perform private.timesheet_correction_a0_source_token_assert_v1(token);
 end loop;
 select coalesce(jsonb_agg(value order by value),'[]'::jsonb) into tokens
  from(select distinct value from jsonb_array_elements(tokens)) complete;
 return tokens;
exception when no_data_found or too_many_rows then
 raise exception 'CORRECTION_A0_SOURCE_PARENT_SNAPSHOT_UNAVAILABLE' using errcode='40001';
end;
$source_revision$;
alter function private.timesheet_correction_a0_source_revision_tokens_v1(text,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_revision_tokens_v1(text,jsonb) from public,anon,authenticated,service_role;

-- Actual group activation scopes after RAW OLD/NEW acquisition. Membership
-- ranges are extracted without parent lookup, including currently inactive
-- groups: otherwise concurrent activation plus insertion can erase a phantom.
create or replace function private.timesheet_correction_a0_source_policy_tokens_v1(p_relation text,p_row jsonb)
returns jsonb language plpgsql volatile security definer set search_path='pg_catalog','pg_temp'
as $source_policy$
declare tokens jsonb:='[]'::jsonb;token jsonb;
begin
 if p_relation is null then raise exception 'CORRECTION_A0_SOURCE_RAW_CONTEXT_INVALID' using errcode='55000';end if;
 if p_relation<>'public.weekly_source_groups' then return tokens;end if;
 if jsonb_typeof(p_row) is distinct from 'object' then raise exception 'CORRECTION_A0_SOURCE_RAW_CONTEXT_INVALID' using errcode='55000';end if;
 if p_row->'active' is distinct from 'true'::jsonb then return tokens;end if;
 select coalesce(jsonb_agg(jsonb_build_object('kind','SOURCE_POLICY_MEMBERSHIP_COVERAGE','uuid',member.client_id::text,
  'coverage',jsonb_build_array(member.valid_from,member.valid_to))),'[]'::jsonb) into tokens
  from public.weekly_source_group_clients member where member.source_group_id=(p_row->>'id')::uuid;
 for token in select value from jsonb_array_elements(tokens) loop
  perform private.timesheet_correction_a0_source_token_assert_v1(token);end loop;
 select coalesce(jsonb_agg(value order by value),'[]'::jsonb) into tokens
  from(select distinct value from jsonb_array_elements(tokens)) complete;return tokens;
end;
$source_policy$;
alter function private.timesheet_correction_a0_source_policy_tokens_v1(text,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_policy_tokens_v1(text,jsonb) from public,anon,authenticated,service_role;

create or replace function private.timesheet_correction_a0_source_reference_before_v1()
returns trigger language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $source_reference$
declare tokens jsonb:='[]'::jsonb; nested_tokens jsonb:='[]'::jsonb;
begin
 if tg_table_schema<>'public' or tg_op not in('INSERT','UPDATE','DELETE') then
  raise exception 'CORRECTION_A0_SOURCE_RAW_CONTEXT_INVALID' using errcode='55000';end if;
 if tg_op in('UPDATE','DELETE') then
  tokens:=tokens||private.timesheet_correction_a0_source_raw_tokens_v1('public.'||tg_table_name,to_jsonb(old));
 end if;
 if tg_op in('INSERT','UPDATE') then
  tokens:=tokens||private.timesheet_correction_a0_source_raw_tokens_v1('public.'||tg_table_name,to_jsonb(new));
 end if;
 -- Only after the union of actual raw OLD/NEW may native fence inspection
 -- occur. Existing business/immutable/state/financial owners remain intact.
 perform private.timesheet_correction_a0_fence_acquire_v1(tokens,false);
 if tg_op in('UPDATE','DELETE') then
  nested_tokens:=nested_tokens||private.timesheet_correction_a0_source_revision_tokens_v1('public.'||tg_table_name,to_jsonb(old))
   ||private.timesheet_correction_a0_source_policy_tokens_v1('public.'||tg_table_name,to_jsonb(old));
 end if;
 if tg_op in('INSERT','UPDATE') then
  nested_tokens:=nested_tokens||private.timesheet_correction_a0_source_revision_tokens_v1('public.'||tg_table_name,to_jsonb(new))
   ||private.timesheet_correction_a0_source_policy_tokens_v1('public.'||tg_table_name,to_jsonb(new));
 end if;
 perform private.timesheet_correction_a0_fence_acquire_v1(nested_tokens,false);
 if tg_op='DELETE' then return old;end if;return new;
end;
$source_reference$;
alter function private.timesheet_correction_a0_source_reference_before_v1() OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_reference_before_v1() from public,anon,authenticated,service_role;

notify pgrst, 'reload schema';


do $selector_profile$
declare e record;p record;role_name text;
begin
 for e in select * from(values
('private.weekly_source_agency_source_dependencies_v1(uuid,uuid[])','028407bae355cf1c79de10e374b7232d012faf821c4dd6c37464a46cc5cb13dc','s','search_path=pg_catalog, public, private, pg_temp'),
('private.timesheet_correction_a0_source_footprint_v1(uuid,uuid[])','00b9edb762a81b2e1e782ca59339c8a2b00078504da3a48d550e92b5c7abfc73','s','search_path=pg_catalog, public, private, pg_temp'),
('private.timesheet_correction_a0_source_raw_tokens_v1(text,jsonb)','19599b34f465fdeb87f19b49e5af67abd0d533e1b4301b66d142d65d0beb2cb7','i','search_path=pg_catalog, pg_temp'),
('private.timesheet_correction_a0_source_revision_tokens_v1(text,jsonb)','9cc4a7069fc18a6d5ad3bc45f97fefa3174b454553cbff58d9c15143fb94fcfd','v','search_path=pg_catalog, pg_temp'),
('private.timesheet_correction_a0_source_policy_tokens_v1(text,jsonb)','6121e93116dc1ecf9936e6ea32fb30b7bc8123768745761b10e2b7e8f8d9f3ea','v','search_path=pg_catalog, pg_temp'),
('private.timesheet_correction_a0_source_reference_before_v1()','c49c00771146d2f366060bcc7c15e78c6ed17586bbccca5d94d0457614450c2d','v','search_path=pg_catalog, pg_temp')
 ) x(signature,body_sha256,volatility,configuration) loop
  select f.* into p from pg_proc f where f.oid=to_regprocedure(e.signature);
  if not found or p.proowner<>current_user::regrole::oid or not p.prosecdef or p.proleakproof
   or p.provolatile::text<>e.volatility or p.proconfig is distinct from array[e.configuration]
   or encode(sha256(convert_to(p.prosrc,'UTF8')),'hex')<>e.body_sha256 then
    raise exception 'HEALTHROSTER_SOURCE_SELECTOR_BODY_SECURITY_PROFILE_INVALID' using errcode='55000';end if;
  foreach role_name in array array['anon','authenticated','service_role'] loop
   if has_function_privilege(role_name,p.oid,'EXECUTE') then raise exception 'HEALTHROSTER_SOURCE_SELECTOR_PRIVATE_EXECUTE_EXPOSED' using errcode='42501';end if;
  end loop;
 end loop;
end $selector_profile$;
do $actual_raw_selector_cases$
declare ids uuid[]:=array['817995fd-27b4-4f52-bf8e-9337baddddbf'::uuid,'ad4016e3-d1dc-48e2-9d49-9a5b873e8d02'::uuid,'a26fa07c-7767-41ec-8952-d785bda41430'::uuid,'f82dda3a-47a3-4ed2-aefd-ee92157e8a47'::uuid,'724fe408-beb0-4137-b27a-fa465f5708de'::uuid];tokens jsonb;row jsonb;bad jsonb;ok boolean;
begin
 if private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_global_settings','{"singleton":true}') is distinct from '[{"kind":"SOURCE_GLOBAL_SETTINGS","json":"true"}]'::jsonb then raise exception 'TRUE_SINGLETON_TOKEN_FAILED';end if;
 foreach bad in array array['{"singleton":false}'::jsonb,'{"singleton":"true"}'::jsonb,'{}'::jsonb] loop
  if private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_global_settings',bad)<>'[]'::jsonb then raise exception 'NON_BOOLEAN_SINGLETON_NOT_EMPTY';end if;
 end loop;
 row:=jsonb_build_object('id',ids[1],'upload_row_id',ids[2],'generation',3,'client_id',ids[3]);tokens:=private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_row_resolutions',row);
 if jsonb_array_length(tokens)<>2 or not tokens @> jsonb_build_array(jsonb_build_object('kind','SOURCE_RESOLUTION_SLOT','json',jsonb_build_array(ids[2],3,ids[3])::text)) or tokens @> jsonb_build_array(jsonb_build_object('kind','SOURCE_OBJECT','json',jsonb_build_array('public.weekly_source_upload_rows',ids[2])::text)) then raise exception 'RESOLUTION_EXACT_CONJUNCTION_FAILED';end if;
 tokens:=private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_row_resolutions',row||'{"client_id":null}');if jsonb_array_length(tokens)<>1 then raise exception 'NULL_CLIENT_CREATED_SLOT';end if;
 tokens:=private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_row_timesheet_lineages',jsonb_build_object('id',ids[1],'row_resolution_id',ids[2],'client_id',ids[3],'work_event_id',ids[4]));
 if jsonb_array_length(tokens)<>3 or not tokens @> jsonb_build_array(jsonb_build_object('kind','SOURCE_LINEAGE_RESOLUTION_CLIENT','json',jsonb_build_array(ids[2],ids[3])::text),jsonb_build_object('kind','SOURCE_LINEAGE_EVENT_CLIENT','json',jsonb_build_array(ids[4],ids[3])::text)) then raise exception 'LINEAGE_EXACT_CONJUNCTIONS_FAILED';end if;
 tokens:=private.timesheet_correction_a0_source_raw_tokens_v1('public.contracts',jsonb_build_object('id',ids[1],'client_id',ids[3],'start_date','2026-09-01','end_date','2026-09-30'));
 if jsonb_array_length(tokens)<>2 or not tokens @> jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',ids[3]::text,'coverage',jsonb_build_array('2026-09-01','2026-09-30'))) then raise exception 'CONTRACT_NATIVE_RANGE_FAILED';end if;
 tokens:=private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_group_clients',jsonb_build_object('id',ids[1],'source_group_id',ids[2],'client_id',ids[3],'valid_from','2026-09-01','valid_to',null,'active',false));
 if jsonb_array_length(tokens)<>3 or not tokens @> jsonb_build_array(jsonb_build_object('kind','SOURCE_POLICY_MEMBERSHIP_COVERAGE','uuid',ids[3]::text,'coverage',jsonb_build_array('2026-09-01',null))) then raise exception 'MEMBERSHIP_OPEN_RANGE_DOMAIN_FAILED';end if;
 tokens:=private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_final_snapshot_lines',jsonb_build_object('id',ids[1],'final_revision_id',ids[2],'client_id',ids[3]));
 if jsonb_array_length(tokens)<>2 or not tokens @> jsonb_build_array(jsonb_build_object('kind','SOURCE_REVISION_CLIENT','json',jsonb_build_array(ids[2],ids[3])::text)) then raise exception 'SNAPSHOT_AND_GRAIN_FAILED';end if;
 row:=jsonb_build_object('id',ids[1],'state','CURRENT','source_cycle_id',ids[2]);tokens:=private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_final_revisions',row);
 if jsonb_array_length(tokens)<>2 or not tokens @> jsonb_build_array(jsonb_build_object('kind','SOURCE_CYCLE_UUID','uuid',ids[2]::text)) then raise exception 'CURRENT_REVISION_CYCLE_FAILED';end if;
 if jsonb_array_length(private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_final_revisions',row||'{"state":"ARCHIVED"}'))<>1 then raise exception 'EXCLUDED_REVISION_BROADENED';end if;
 row:=jsonb_build_object('id',ids[1],'state','CURRENT','self_bill_report_id',ids[2]);
 if jsonb_array_length(private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_self_bill_report_line_bindings',row))<>2 or jsonb_array_length(private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_self_bill_report_line_bindings',row||'{"state":"STALE"}'))<>1 then raise exception 'REPORT_BINDING_EXACT_STATE_FAILED';end if;
 foreach bad in array array['null'::jsonb,'[]'::jsonb,'{}'::jsonb,jsonb_build_object('id','invalid')] loop
  ok:=false;begin perform private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_rows',bad);exception when sqlstate '55000' then ok:=true;end;if not ok then raise exception 'UNKNOWN_RELATION_OR_SHAPE_NOT_REFUSED';end if;
 end loop;
 ok:=false;begin perform private.timesheet_correction_a0_source_raw_tokens_v1('public.weekly_source_row_resolutions',jsonb_build_object('id',ids[1],'upload_row_id',ids[2],'generation',-1,'client_id',ids[3]));exception when sqlstate '55000' then ok:=true;end;if not ok then raise exception 'NEGATIVE_GENERATION_NOT_REFUSED';end if;
 if exists(select 1 from public.weekly_source_cycles where id=ids[5]) then raise exception 'VACANT_PARENT_UUID_COLLISION';end if;
 ok:=false;begin perform private.timesheet_correction_a0_source_revision_tokens_v1('public.weekly_source_final_revisions',jsonb_build_object('id',ids[1],'state','CURRENT','source_cycle_id',ids[5]));exception when sqlstate '40001' then ok:=true;end;if not ok then raise exception 'MISSING_PARENT_DID_NOT_FAIL_CLOSED';end if;
end $actual_raw_selector_cases$;

select true as six_canonical_helpers_and_raw_selector_cases_passed;
