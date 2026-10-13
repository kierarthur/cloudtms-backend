-- Exact scoped native own-stage exception; normal four facts retain full predicates.
\set ON_ERROR_STOP on
begin;
create or replace function private.weekly_source_bulk_own_stage_event_excluded_v1(
  p_work_event_id uuid,p_source_cycle_id uuid,p_context jsonb
) returns boolean language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare current_context jsonb; matching integer;
begin
  if p_context is null then return false; end if;
  if p_work_event_id is null or p_source_cycle_id is null
    or not private.weekly_exceptional_json_keys_exact_v1(p_context,array['schema_version','source_cycle_id','actor_user_id',
      'bulk_request_key','selection_sha256','source_cas_sha256','client_ids','exclusions'])
    or not(p_context ?& array['schema_version','source_cycle_id','actor_user_id','bulk_request_key','selection_sha256','source_cas_sha256','client_ids','exclusions'])
    or p_context->>'schema_version' is distinct from 'WEEKLY_SOURCE_BULK_OWN_STAGE_CONTEXT_V1'
    or p_context->>'source_cycle_id' is distinct from p_source_cycle_id::text
    or jsonb_typeof(p_context->'client_ids') is distinct from 'array'
    or jsonb_typeof(p_context->'exclusions') is distinct from 'array'
    or jsonb_array_length(p_context->'exclusions')=0
    or coalesce(p_context->>'selection_sha256','') !~ '^[0-9a-f]{64}$'
    or coalesce(p_context->>'source_cas_sha256','') !~ '^[0-9a-f]{64}$'
    or nullif(btrim(p_context->>'bulk_request_key'),'') is null
    or not pg_input_is_valid(coalesce(p_context->>'actor_user_id',''),'uuid') then
    raise exception 'WEEKLY_SOURCE_BULK_OWN_STAGE_UNAVAILABLE' using errcode='55000'; end if;
  -- Rebuild from actual READY/PENDING_C1 requests, generation/vector/proof,
  -- approval/run/family/WAIT/signed claim and current Source CAS owners.
  -- No supplied exclusion, receipt or field is accepted on its own.
  current_context:=private.weekly_source_bulk_own_stage_context_v1(p_source_cycle_id,
    (p_context->>'actor_user_id')::uuid,p_context->>'bulk_request_key',
    p_context->>'selection_sha256',p_context->>'source_cas_sha256');
  if current_context is null or current_context is distinct from p_context then
    raise exception 'WEEKLY_SOURCE_BULK_OWN_STAGE_UNAVAILABLE' using errcode='55000'; end if;
  select count(*) into matching from jsonb_array_elements(current_context->'exclusions') item
    where item->>'work_event_id'=p_work_event_id::text;
  if matching>1 then raise exception 'WEEKLY_SOURCE_BULK_OWN_STAGE_UNAVAILABLE' using errcode='55000'; end if;
  return matching=1;
exception when invalid_text_representation or numeric_value_out_of_range or invalid_parameter_value then
  raise exception 'WEEKLY_SOURCE_BULK_OWN_STAGE_UNAVAILABLE' using errcode='55000';
end;
$function$;

create or replace function private.weekly_source_report_shift_facts_v1(p_work_event_id uuid,p_cycle_or_report uuid,p_own_stage_context jsonb)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare e public.weekly_work_events%rowtype; c public.weekly_source_cycles%rowtype; s public.weekly_source_report_scopes%rowtype;
  v_on boolean:=false; v_bound boolean:=false; v_ready boolean:=false; v_protected boolean:=false; v_wait jsonb;
  v_pay_export_worked boolean:=false; v_own_stage_excluded boolean:=false;
begin
  select * into e from public.weekly_work_events where id=p_work_event_id;
  if not found then raise exception 'WEEKLY_SOURCE_SELF_BILL_EVENT_NOT_FOUND' using errcode='22023'; end if;
  select * into c from public.weekly_source_cycles where id=p_cycle_or_report;
  if not found then
    select * into s from public.weekly_source_report_scopes where id=p_cycle_or_report and self_bill_report_id is not null;
    if not found then raise exception 'WEEKLY_SOURCE_SELF_BILL_SCOPE_NOT_FOUND' using errcode='22023'; end if;
    if s.client_id is distinct from e.client_id then
      raise exception 'WEEKLY_SOURCE_SELF_BILL_SCOPE_MISMATCH' using errcode='22023'; end if;
    select * into strict c from public.weekly_source_cycles where id=s.source_cycle_id;
  end if;
  if e.first_source_group_id is distinct from c.source_group_id
    or (c.scope_client_id is not null and c.scope_client_id is distinct from e.client_id)
    or (s.id is not null and (s.source_group_id is distinct from c.source_group_id
      or not exists(select 1 from public.weekly_source_groups source_group where source_group.id=c.source_group_id
        and source_group.agency_id=s.agency_id and source_group.environment=s.environment))) then
    raise exception 'WEEKLY_SOURCE_SELF_BILL_SCOPE_MISMATCH' using errcode='22023'; end if;
  -- The actual event's client is the F1 helper's first parameter.
  if private.weekly_source_effective_rule_set_v1(e.client_id,c.id)#>>'{values,self_bill_source}' is distinct from 'SEPARATE_FINAL_REPORT' then
    return jsonb_build_object('on_accepted_report',false,'bound_to_finalised_export',false,'invoice_ready',false,'s25_pay_selected',false); end if;
  select coalesce(bool_or(true),false),coalesce(bool_or(b.id is not null and er.row_finalisation_state='SOURCE_WORKED'
      and nullif(btrim(er.finalised_by),'') is not null),false),
    coalesce(bool_or(b.id is not null and er.row_finalisation_state='SOURCE_WORKED' and nullif(btrim(er.finalised_by),'') is not null
      and private.weekly_source_self_bill_report_pay_witness_v1(b.id) is not null),false)
    into v_on,v_bound,v_ready
    from public.weekly_source_report_scopes rs
    join public.weekly_source_projection_publications rp on rp.id=rs.current_projection_publication_id
      and rp.upload_id=rs.current_complete_upload_id and rp.report_scope_id=rs.id and rp.state='CURRENT'
      and rp.authority_scope_kind='SELF_BILL_REPORT_SCOPE' and rp.authority_scope_version=rs.version
    join public.weekly_source_uploads ru on ru.id=rp.upload_id and ru.state='CURRENT'
    join public.weekly_source_upload_rows rr on rr.upload_id=ru.id and rr.row_finalisation_state='SOURCE_REPORT_LINE'
    left join public.weekly_source_self_bill_report_line_bindings b on b.report_row_id=rr.id and b.state='CURRENT'
    left join public.weekly_source_upload_rows er on er.id=b.export_upload_row_id
    where rs.source_group_id=c.source_group_id and rs.client_id=e.client_id
      and (s.id is null or rs.id=s.id) and (b.work_event_id=e.id
        or (e.profile_external_key is not null and rr.external_source_key=e.profile_external_key));
  v_own_stage_excluded:=private.weekly_source_bulk_own_stage_event_excluded_v1(e.id,c.id,p_own_stage_context);
  v_wait:=case when v_own_stage_excluded then null else private.weekly_source_protected_wait_witness_v1(e.id,c.id) end;
  v_protected:=v_wait is not null and v_wait->>'state'='QUALIFIED_WAIT';
  select exists(select 1 from public.weekly_source_projection_publications ep
    join public.weekly_source_uploads eu on eu.id=ep.upload_id and eu.state='CURRENT'
    join public.weekly_source_upload_rows er on er.upload_id=eu.id and er.row_finalisation_state='SOURCE_WORKED'
      and nullif(btrim(er.finalised_by),'') is not null and er.work_date<=c.finalisation_week_ending
    join public.weekly_source_row_resolutions rr on rr.upload_row_id=er.id and rr.mapping_state='RESOLVED'
      and rr.work_event_id=e.id and rr.client_id=e.client_id and rr.generation=coalesce(ep.projection_generation,ep.authority_scope_version)
    where ep.id=c.current_projection_publication_id and ep.upload_id=c.current_complete_upload_id
      and ep.authority_scope_kind='CYCLE' and ep.state='CURRENT') into v_pay_export_worked;
  return jsonb_build_object('on_accepted_report',v_on,'bound_to_finalised_export',v_bound,
    'invoice_ready',v_bound and v_ready,'s25_pay_selected',v_protected or (v_bound and v_pay_export_worked));
end;
$function$;
create or replace function private.weekly_source_report_shift_facts_v1(p_work_event_id uuid,p_cycle_or_report uuid)
returns jsonb language sql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$ select private.weekly_source_report_shift_facts_v1(p_work_event_id,p_cycle_or_report,null::jsonb); $function$;

create or replace function private.weekly_source_shift_on_self_bill_context_v1(p_work_event_id uuid,p_cycle_or_report uuid,p_own_stage_context jsonb)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare cycle public.weekly_source_cycles%rowtype; scope public.weekly_source_report_scopes%rowtype;
  event public.weekly_work_events%rowtype; rules jsonb;
begin
  if p_own_stage_context is null then return private.weekly_source_shift_on_self_bill_v1(p_work_event_id,p_cycle_or_report); end if;
  select * into strict event from public.weekly_work_events where id=p_work_event_id;
  select * into cycle from public.weekly_source_cycles where id=p_cycle_or_report;
  if cycle.id is null then
    select * into strict scope from public.weekly_source_report_scopes where id=p_cycle_or_report;
    select * into strict cycle from public.weekly_source_cycles where id=scope.source_cycle_id;
    if scope.client_id is distinct from event.client_id or scope.source_group_id is distinct from cycle.source_group_id
      or not exists(select 1 from public.weekly_source_groups g where g.id=cycle.source_group_id
        and g.agency_id=scope.agency_id and g.environment=scope.environment) then
      raise exception 'WEEKLY_SOURCE_SELF_BILL_SCOPE_MISMATCH' using errcode='22023'; end if;
  end if;
  if event.first_source_group_id is distinct from cycle.source_group_id
    or (cycle.scope_client_id is not null and cycle.scope_client_id is distinct from event.client_id) then
    raise exception 'WEEKLY_SOURCE_SELF_BILL_SCOPE_MISMATCH' using errcode='22023'; end if;
  perform private.weekly_source_bulk_own_stage_event_excluded_v1(event.id,cycle.id,p_own_stage_context);
  rules:=private.weekly_source_effective_rule_set_v1(event.client_id,cycle.id)->'values';
  if rules->>'invoice_sender'='CLIENT_SELF_BILL' and rules->>'self_bill_source'='SEPARATE_FINAL_REPORT' then
    if scope.id is not null and scope.self_bill_report_id is null then
      raise exception 'WEEKLY_SOURCE_SELF_BILL_REPORT_SCOPE_REQUIRED' using errcode='22023'; end if;
    return private.weekly_source_report_shift_facts_v1(event.id,p_cycle_or_report,p_own_stage_context);
  end if;
  return private.weekly_source_shift_on_self_bill_v1(event.id,p_cycle_or_report);
end;
$function$;

alter function private.weekly_source_bulk_own_stage_event_excluded_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_bulk_own_stage_event_excluded_v1(uuid,uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_report_shift_facts_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_report_shift_facts_v1(uuid,uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_report_shift_facts_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_report_shift_facts_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_shift_on_self_bill_context_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_shift_on_self_bill_context_v1(uuid,uuid,jsonb) from public,anon,authenticated,service_role;
commit;
