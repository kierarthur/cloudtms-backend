-- WP-F2 helpers. Called only within the finaliser's existing authority/locks.
-- Full provenance stays sealed separately from this economic-value comparison.

create or replace function private.weekly_source_authority_subset_v1(p_policy jsonb)
returns jsonb
language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_subset jsonb;
begin
  v_subset:=pg_catalog.jsonb_build_object(
    'authority_mode',p_policy->'authority_mode',
    'self_bill_enabled',p_policy->'self_bill_enabled',
    'source_group_id',p_policy->'source_group_id',
    'self_bill_correction_presentation',p_policy->'self_bill_correction_presentation'
  );
  if pg_catalog.jsonb_typeof(p_policy) is distinct from 'object'
     or pg_catalog.jsonb_typeof(v_subset->'authority_mode') is distinct from 'string'
     or coalesce(v_subset->>'authority_mode','')=''
     or pg_catalog.jsonb_typeof(v_subset->'self_bill_enabled') is distinct from 'boolean'
     or pg_catalog.jsonb_typeof(v_subset->'source_group_id') is distinct from 'string'
     or coalesce(v_subset->>'source_group_id','') !~
       '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or coalesce(v_subset->>'self_bill_correction_presentation','') not in (
       'FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION'
     ) then
    raise exception 'WEEKLY_SOURCE_AUTHORITY_SUBSET_INVALID' using errcode='22023';
  end if;
  return v_subset;
end;
$function$;

create or replace function private.weekly_source_final_value_vector_v1(p_vector jsonb)
returns jsonb
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'row_sign',p_vector->'row_sign','paid_minutes',p_vector->'paid_minutes',
    'break_minutes',p_vector->'break_minutes','total_pence',p_vector->'total_pence',
    'hours',pg_catalog.jsonb_build_object(
      'day',p_vector#>'{hours,day}','night',p_vector#>'{hours,night}',
      'sat',p_vector#>'{hours,sat}','sun',p_vector#>'{hours,sun}','bh',p_vector#>'{hours,bh}'
    ),
    'rates',pg_catalog.jsonb_build_object(
      'day',p_vector#>'{rates,day}','night',p_vector#>'{rates,night}',
      'sat',p_vector#>'{rates,sat}','sun',p_vector#>'{rates,sun}','bh',p_vector#>'{rates,bh}'
    )
  );
$function$;

create or replace function private.weekly_source_final_state_fingerprint_v1(p_state jsonb)
returns bytea
language sql immutable
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_FINAL_VALUE_STATE_V2',
    pg_catalog.jsonb_build_object(
      'work_event_id',p_state->'work_event_id','candidate_id',p_state->'candidate_id',
      'client_id',p_state->'client_id','contract_id',p_state->'contract_id',
      'work_date',p_state->'work_date','start_at_local',p_state->'start_at_local',
      'end_at_local',p_state->'end_at_local','break_minutes',p_state->'break_minutes',
      'actual_net_minutes',p_state->'actual_net_minutes',
      'pay_vector',private.weekly_source_final_value_vector_v1(p_state->'pay_vector'),
      'charge_vector',private.weekly_source_final_value_vector_v1(p_state->'charge_vector'),
      'invoice_vat_rate_pct',p_state->'invoice_vat_rate_pct',
      'invoice_vat_chargeable',p_state->'invoice_vat_chargeable',
      'authority_subset',p_state->'authority_subset'
    )
  );
$function$;

create or replace function private.weekly_source_invoice_economics_fingerprint_v1(
  p_source_money jsonb, p_line_vat_method text, p_vat_rate numeric
) returns bytea
language plpgsql immutable
set search_path to 'private','pg_catalog','pg_temp'
as $function$
declare v_key text; v_money jsonb:='{}'::jsonb; v_amount bigint;
begin
  if pg_catalog.jsonb_typeof(p_source_money) is distinct from 'object'
     or p_line_vat_method not in (
       'ROUND_EACH_LINE_FROM_NET','LINE_GROSS_MINUS_NET_FROM_SOURCE',
       'LINE_VAT_FROM_REPORT','ROUND_TOTAL_FROM_EXACT_NET'
     ) or p_line_vat_method is null or p_vat_rate is null or p_vat_rate<0 or p_vat_rate>100 then
    raise exception 'WEEKLY_SOURCE_INVOICE_ECONOMICS_NOT_READY' using errcode='55000';
  end if;
  foreach v_key in array array['source_shift_charge_pence','source_gross_pence','source_vat_pence']::text[] loop
    if coalesce(p_source_money->>v_key,'') !~ '^[+-]?[0-9]+$' then
      raise exception 'WEEKLY_SOURCE_INVOICE_ECONOMICS_NOT_READY' using errcode='55000';
    end if;
    v_amount:=(p_source_money->>v_key)::bigint;
    if v_amount not between -999999999999 and 999999999999 then
      raise exception 'WEEKLY_SOURCE_INVOICE_ECONOMICS_NOT_READY' using errcode='55000';
    end if;
    v_money:=v_money||pg_catalog.jsonb_build_object(v_key,v_amount);
  end loop;
  return private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_INVOICE_ECONOMICS_V1',
    v_money||pg_catalog.jsonb_build_object('line_vat_method',p_line_vat_method,'vat_rate_pct',p_vat_rate)
  );
exception when invalid_text_representation or numeric_value_out_of_range then
  raise exception 'WEEKLY_SOURCE_INVOICE_ECONOMICS_NOT_READY' using errcode='55000';
end;
$function$;

create or replace function private.weekly_source_snapshot_authority_subset_v1(p_snapshot_id uuid)
returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_snapshot public.weekly_source_final_snapshot_lines%rowtype;
  v_subset jsonb;
  v_origin record;
  v_cutover timestamptz;
  v_presentation text;
  v_movement_count bigint;
  v_presentation_count bigint;
begin
  if p_snapshot_id is null then return null; end if;
  select snapshot.* into v_snapshot
  from public.weekly_source_final_snapshot_lines snapshot where snapshot.id=p_snapshot_id;
  if not found then raise exception 'WEEKLY_SOURCE_FINAL_SNAPSHOT_MISSING' using errcode='55000'; end if;
  if v_snapshot.authority_subset_json is not null then
    v_subset:=private.weekly_source_authority_subset_v1(v_snapshot.authority_subset_json);
    if v_subset is distinct from v_snapshot.authority_subset_json
       or private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_AUTHORITY_SUBSET_V1',v_subset)
          is distinct from v_snapshot.authority_subset_sha256 then
      raise exception 'WEEKLY_SOURCE_AUTHORITY_SUBSET_INVALID' using errcode='55000';
    end if;
    return v_subset;
  end if;
  -- Approved controlled recorded-source history: 109 attempts / five full
  -- engines, including FAILED partial attempts; independent control-flow and
  -- sole-writer census, exact e0 installed prosrc/ledger and immutable guards.
  -- This is not a claim about theoretical unrecorded administrator writes.
  -- A later original FIN closure needs a fresh review before this gate changes.
  if not exists(
    select 1 from private.cloudtms_migration_ledger ledger
    where ledger.path='supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql'
      and ledger.content_sha256='1fc7833c7054cb56da725d15280ff757f3b78e1046b80f12c337020695374c0d'
      and ledger.first_release_id='20260920-weekly-source-plan62-upgrade-dd0a9ce17261'
  ) then return null; end if;
  if not exists(
    select 1 from private.cloudtms_repeatable_ledger ledger
    where ledger.path='supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql'
      and ledger.closure_sha256='240e2460baf9bff3164cc4ed389b6acb5774f8f88ab0deefc70d6f085fae00f1'
  ) then return null; end if;
  select ledger.applied_at_utc into v_cutover
  from private.cloudtms_migration_ledger ledger
  where ledger.path='supabase/migrations/13102026_1210_weekly_source_final_value_snapshot_authority.sql'
    and ledger.content_sha256='0d0564eea33cff211efe8452d3c7d4cf0817aeb21854a967a4f7ad402966a7d6';
  if v_cutover is null or v_snapshot.created_at_utc>=v_cutover then return null; end if;
  select revision.id as revision_id,revision.finalised_at_utc,cycle.source_group_id,
         cycle.finalisation_week_ending
  into v_origin
  from public.weekly_source_final_revisions revision
  join public.weekly_source_cycles cycle on cycle.id=revision.source_cycle_id
  join public.weekly_source_groups source_group on source_group.id=cycle.source_group_id
  join public.weekly_source_uploads upload on upload.id=revision.upload_id
    and upload.source_cycle_id=cycle.id and upload.report_scope_id is null
  join public.weekly_source_format_profiles profile on profile.id=upload.source_format_profile_id
  join public.weekly_source_upload_rows source_row on source_row.id=v_snapshot.upload_row_id
    and source_row.upload_id=upload.id
  join public.weekly_source_row_resolutions resolution on resolution.id=v_snapshot.row_resolution_id
    and resolution.upload_row_id=source_row.id
  where revision.id=v_snapshot.final_revision_id
    and revision.authority_scope_kind='CYCLE' and revision.report_scope_id is null
    and revision.state in ('CURRENT','SUPERSEDED')
    and revision.finalised_at_utc<v_cutover
    and revision.reason in ('INITIAL_FINALISATION','CORRECT_FINAL_SOURCE')
    and (revision.reason='INITIAL_FINALISATION')=(upload.purpose='ORDINARY')
    and source_group.source_family='ROSTER'
    and profile.final_authority_kind=v_snapshot.source_profile_kind
    and ((v_snapshot.source_profile_kind='GENERIC_COMPLETE_SNAPSHOT'
      and profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1')
      or (v_snapshot.source_profile_kind='HEALTHROSTER_ACTUAL_ROWS'
      and profile.profile_code in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1')))
    and v_snapshot.work_date between revision.coverage_start_local_date and revision.coverage_end_local_date
    and resolution.work_event_id=v_snapshot.work_event_id
    and resolution.candidate_id=v_snapshot.candidate_id
    and resolution.client_id=v_snapshot.client_id and resolution.contract_id=v_snapshot.contract_id
    and source_row.work_date=v_snapshot.work_date
    and source_row.start_at_local=v_snapshot.start_at_local and source_row.end_at_local=v_snapshot.end_at_local
    and source_row.break_minutes=v_snapshot.break_minutes and source_row.actual_net_minutes=v_snapshot.actual_net_minutes;
  if not found then return null; end if;
  with eligible as (
    select movement.source_facts_json->>'correction_presentation' as presentation,
      pg_catalog.dense_rank() over(order by movement_cycle.finalisation_week_ending desc,
        movement_revision.finalised_at_utc desc,movement_revision.revision_number desc,movement_revision.id desc) as origin_rank
    from public.weekly_source_billing_movements movement
    join public.weekly_source_final_revisions movement_revision on movement_revision.id=movement.final_revision_id
    join public.weekly_source_cycles movement_cycle on movement_cycle.id=movement.finalisation_cycle_id
      and movement_revision.source_cycle_id=movement_cycle.id
    join public.weekly_source_final_snapshot_lines movement_snapshot
      on movement_snapshot.id::text=movement.source_facts_json->>'snapshot_line_id'
      and movement_snapshot.final_revision_id=movement_revision.id
      and movement_snapshot.work_event_id=v_snapshot.work_event_id
      and movement_snapshot.client_id=v_snapshot.client_id
      and movement_snapshot.contract_id=v_snapshot.contract_id
      and movement_snapshot.created_at_utc<v_cutover
    where movement.work_event_id=v_snapshot.work_event_id
      and movement.actual_client_id=v_snapshot.client_id and movement.candidate_id=v_snapshot.candidate_id
      and movement.contract_id=v_snapshot.contract_id
      and movement.source_profile_kind=v_snapshot.source_profile_kind
      and ((movement.movement_role='POSITIVE' and movement.source_line_kind='SOURCE_ORDINARY')
        or (movement.movement_role='REPLACEMENT' and movement.source_line_kind='SOURCE_REPLACEMENT'))
      and movement.placement_state<>'VOIDED_BY_CORRECT_FINAL'
      and movement_revision.state in ('CURRENT','SUPERSEDED')
      and movement_revision.authority_scope_kind='CYCLE' and movement_revision.report_scope_id is null
      and movement_cycle.source_group_id=v_origin.source_group_id
      and movement_cycle.finalisation_week_ending<=v_origin.finalisation_week_ending
      and movement_revision.finalised_at_utc<=v_origin.finalised_at_utc
      and movement.created_at_utc<=v_origin.finalised_at_utc
  )
  select pg_catalog.count(*),pg_catalog.count(presentation),pg_catalog.min(presentation)
  into v_movement_count,v_presentation_count,v_presentation
  from eligible where origin_rank=1;
  if v_movement_count<>1 or v_presentation_count<>1 or v_presentation not in (
    'FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION'
  ) then return null; end if;
  return pg_catalog.jsonb_build_object(
    'authority_mode','SOURCE_AUTHORITY','self_bill_enabled',true,
    'source_group_id',v_origin.source_group_id,'self_bill_correction_presentation',v_presentation
  );
end;
$function$;

create or replace function private.weekly_source_snapshot_value_fingerprint_v1(
  p_snapshot_id uuid, p_authority_subset jsonb
) returns bytea
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_snapshot public.weekly_source_final_snapshot_lines%rowtype;
  v_economic public.weekly_source_row_economic_snapshots%rowtype;
begin
  if p_snapshot_id is null then return null; end if;
  select snapshot.* into strict v_snapshot
  from public.weekly_source_final_snapshot_lines snapshot where snapshot.id=p_snapshot_id;
  -- Immutable row_resolution_id is unique on the stored economic snapshot.
  -- VAT comes from that exact original snapshot, never today's settings.
  select economic.* into strict v_economic
  from public.weekly_source_row_economic_snapshots economic
  where economic.row_resolution_id=v_snapshot.row_resolution_id;
  return private.weekly_source_final_state_fingerprint_v1(pg_catalog.jsonb_build_object(
    'work_event_id',v_snapshot.work_event_id,'candidate_id',v_snapshot.candidate_id,
    'client_id',v_snapshot.client_id,'contract_id',v_snapshot.contract_id,
    'work_date',v_snapshot.work_date,'start_at_local',v_snapshot.start_at_local,
    'end_at_local',v_snapshot.end_at_local,'break_minutes',v_snapshot.break_minutes,
    'actual_net_minutes',v_snapshot.actual_net_minutes,
    'pay_vector',v_snapshot.pay_vector_json,'charge_vector',v_snapshot.charge_vector_json,
    'invoice_vat_rate_pct',v_economic.invoice_vat_rate_pct,
    'invoice_vat_chargeable',v_economic.invoice_vat_chargeable,
    'authority_subset',p_authority_subset
  ));
end;
$function$;

alter function private.weekly_source_authority_subset_v1(jsonb) owner to postgres;
alter function private.weekly_source_final_value_vector_v1(jsonb) owner to postgres;
alter function private.weekly_source_final_state_fingerprint_v1(jsonb) owner to postgres;
alter function private.weekly_source_invoice_economics_fingerprint_v1(jsonb,text,numeric) owner to postgres;
alter function private.weekly_source_snapshot_authority_subset_v1(uuid) owner to postgres;
alter function private.weekly_source_snapshot_value_fingerprint_v1(uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_authority_subset_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_final_value_vector_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_final_state_fingerprint_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_economics_fingerprint_v1(jsonb,text,numeric) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_snapshot_authority_subset_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_snapshot_value_fingerprint_v1(uuid,jsonb) from public,anon,authenticated,service_role;
