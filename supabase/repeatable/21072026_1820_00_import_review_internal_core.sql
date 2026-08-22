-- Owner-only core used by the import-review RPC surface.
-- Public RPCs do not orchestrate other public RPCs; shared work is kept here.

create or replace function public._import_review_overlap_preflight_core_v2(
  p_import_id uuid,
  p_source_system public.hr_source_enum,
  p_source_route text,
  p_coverage_start_date date,
  p_coverage_end_date date,
  p_scope_clients jsonb default '[]'::jsonb
)
returns jsonb
language sql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
  with current_clients as (
    select nullif(client.value->>'client_id','')::uuid as client_id,
           nullif(btrim(client.value->>'source_client_key'),'') as source_client_key
    from jsonb_array_elements(case when jsonb_typeof(coalesce(p_scope_clients,'[]'::jsonb))='array'
      then coalesce(p_scope_clients,'[]'::jsonb) else '[]'::jsonb end) client(value)
  ), active_reviews as (
    select s.import_id,s.status,s.state_version,s.updated_at_utc,
           hi.filename,hi.coverage_start_date,hi.coverage_end_date,
           coalesce(hi.import_scope,hi.source_system::text) as source_route
    from public.import_review_states s
    join public.hr_imports hi on hi.id=s.import_id
    where s.import_id<>p_import_id
      and s.status in ('STAGED','IN_REVIEW','BLOCKED','READY','APPLYING')
      and hi.source_system=p_source_system
      and upper(coalesce(hi.import_scope,hi.source_system::text))=upper(coalesce(p_source_route,p_source_system::text))
      and daterange(hi.coverage_start_date,hi.coverage_end_date,'[]')
        && daterange(p_coverage_start_date,p_coverage_end_date,'[]')
  ), matched_healthroster as (
    select distinct on (active.import_id)
      active.*,
      case when current.client_id is not null and current.client_id=other_client.client_id
        then 'RESOLVED_CLIENT' else 'SOURCE_CLIENT_KEY' end as overlap_reason,
      coalesce(current.client_id,other_client.client_id) as client_id,
      coalesce(current.source_client_key,other_client.source_client_key) as source_client_key
    from active_reviews active
    join lateral (
      select sc.client_id,sc.source_client_key
      from public.import_review_scope_clients sc
      where sc.import_id=active.import_id
      union all
      select hi.client_id,'client:'||hi.client_id::text
      from public.hr_imports hi
      where hi.id=active.import_id and hi.client_id is not null
        and not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=active.import_id)
    ) other_client on true
    join current_clients current on
      (current.client_id is not null and other_client.client_id=current.client_id)
      or (current.source_client_key is not null and other_client.source_client_key=current.source_client_key)
    where p_source_system<>'NHSP'::public.hr_source_enum
    order by active.import_id,
      case when current.client_id is not null and current.client_id=other_client.client_id then 0 else 1 end
  ), matched as (
    -- NHSP is a single cross-client feed, so any unfinished NHSP review for
    -- an overlapping period conflicts. HealthRoster feeds are client-owned
    -- and conflict only for the same client and the same Weekly/Daily route.
    select active.*,
      'NHSP_PERIOD'::text as overlap_reason,
      null::uuid as client_id,
      null::text as source_client_key
    from active_reviews active
    where p_source_system='NHSP'::public.hr_source_enum
    union all
    select * from matched_healthroster
  ), bounded as (
    select * from matched order by updated_at_utc desc,import_id desc limit 20
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'import_id',import_id,'status',status,'state_version',state_version,'filename',filename,
    'source_route',source_route,'coverage_start_date',coverage_start_date,'coverage_end_date',coverage_end_date,
    'overlap_reason',overlap_reason,'client_id',client_id,'source_client_key',source_client_key,
    'updated_at_utc',updated_at_utc
  ) order by updated_at_utc desc,import_id desc),'[]'::jsonb)
  from bounded
$function$;

revoke all on function public._import_review_overlap_preflight_core_v2(uuid,public.hr_source_enum,text,date,date,jsonb)
  from public,anon,authenticated,service_role;

create or replace function public._import_review_hash_v1(p_value text)
returns text
language sql
immutable
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
  select encode(extensions.digest(convert_to(coalesce(p_value,''),'UTF8'),'sha256'::text),'hex')
$function$;

-- One server-owned authority decision for staging, review and final apply.
-- Import eligibility deliberately uses the latest settings row effective now;
-- the evidence date remains part of the fingerprint and contract-date checks,
-- but does not resurrect historical import settings.
create or replace function public._import_review_effective_authority_core_v1(
  p_source_route text,
  p_contract_id uuid,
  p_client_id uuid,
  p_evidence_date date default null
)
returns table (
  route_eligible boolean,
  validation_eligible boolean,
  import_authoritative boolean,
  authority_mode text,
  authority_basis text,
  effective_is_nhsp boolean,
  effective_autoprocess_hr boolean,
  effective_requires_hr boolean,
  effective_no_timesheet_required boolean,
  settings_as_of_date date,
  client_settings_id uuid,
  client_settings_effective_from date,
  client_settings_updated_at timestamptz,
  contract_updated_at timestamptz,
  authority_fingerprint text
)
language sql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
  with context as (
    select upper(btrim(coalesce(p_source_route,''))) route,
      (statement_timestamp() at time zone 'Europe/London')::date today_london
  ), contract_row as (
    select c.* from public.contracts c where c.id=p_contract_id
  ), current_setting as (
    select cs.*
    from public.client_settings cs,context x
    where cs.client_id=coalesce((select c.client_id from contract_row c),p_client_id)
      and (cs.effective_from is null or cs.effective_from<=x.today_london)
    order by cs.effective_from desc nulls last,cs.updated_at desc nulls last,cs.id desc
    limit 1
  ), effective as (
    select x.route,x.today_london,c.id contract_id,c.updated_at contract_updated_at,
      cs.id client_settings_id,cs.effective_from client_settings_effective_from,
      cs.updated_at client_settings_updated_at,
      case when coalesce(c.overrideclientsettings,false) and c.is_nhsp is not null then c.is_nhsp
        else coalesce(cs.is_nhsp,false) end is_nhsp,
      case when coalesce(c.overrideclientsettings,false) and c.autoprocess_hr is not null then c.autoprocess_hr
        else coalesce(cs.autoprocess_hr,false) end autoprocess_hr,
      case when coalesce(c.overrideclientsettings,false) and c.requires_hr is not null then c.requires_hr
        else coalesce(cs.requires_hr,false) end requires_hr,
      case when coalesce(c.overrideclientsettings,false) and c.no_timesheet_required is not null then c.no_timesheet_required
        else coalesce(cs.no_timesheet_required,false) end no_timesheet_required,
      case when coalesce(c.overrideclientsettings,false) and (
        c.is_nhsp is not null or c.autoprocess_hr is not null or c.requires_hr is not null
        or c.no_timesheet_required is not null) then 'CONTRACT_OVERRIDE' else 'CLIENT_SETTINGS_CURRENT' end basis
    from context x
    left join contract_row c on true
    left join current_setting cs on true
  ), decision as (
    select e.*,
      case when e.route='NHSP' then e.is_nhsp
        when e.route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY','HR_DAILY','HEALTHROSTER_DAILY')
          then e.autoprocess_hr else false end route_ok,
      case when e.route in ('HR_DAILY','HEALTHROSTER_DAILY') then e.autoprocess_hr
        when e.route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY')
          then e.autoprocess_hr and not e.no_timesheet_required
        else false end validation_ok,
      case when e.route='NHSP' then e.is_nhsp
        when e.route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY')
          then e.autoprocess_hr and e.no_timesheet_required
        else false end authoritative
    from effective e
  )
  select d.route_ok,d.validation_ok,d.authoritative,
    case when not d.route_ok then 'OUT_OF_SCOPE'
      when d.authoritative then 'AUTHORITATIVE'
      when d.validation_ok then 'VALIDATION_ONLY' else 'OUT_OF_SCOPE' end,
    d.basis,d.is_nhsp,d.autoprocess_hr,d.requires_hr,d.no_timesheet_required,
    d.today_london,d.client_settings_id,d.client_settings_effective_from,d.client_settings_updated_at,
    d.contract_updated_at,
    public._import_review_hash_v1(concat_ws('|','import-authority-v1',d.route,p_contract_id,
      coalesce((select c.client_id from contract_row c),p_client_id),p_evidence_date,d.today_london,
      d.client_settings_id,d.client_settings_effective_from,d.client_settings_updated_at,
      d.contract_updated_at,d.is_nhsp,d.autoprocess_hr,d.requires_hr,d.no_timesheet_required,
      d.route_ok,d.validation_ok,d.authoritative,d.basis))
  from decision d
$function$;

revoke all on function public._import_review_effective_authority_core_v1(text,uuid,uuid,date)
  from public,anon,authenticated,service_role;

create or replace function public._import_review_assert_actor_v1(p_actor_user_id uuid)
returns void
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
begin
  if p_actor_user_id is null or not exists (
    select 1 from public.tms_users u where u.id=p_actor_user_id and coalesce(u.is_active,false)
  ) then
    raise exception 'IMPORT_REVIEW_ACTOR_INVALID' using errcode='42501';
  end if;
end
$function$;

create or replace function public._import_review_ready_action_ids_core_v1(p_import_id uuid)
returns table(action_id text)
language plpgsql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
begin
  return query
  with selected_units as (
    select distinct d.candidate_id,d.client_id
    from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.selectable and d.selected
      and not d.blocking and d.candidate_id is not null and d.client_id is not null
      and not exists (
        select 1 from public.import_review_action_outcomes o where o.action_id=d.action_id
      )
      and not (d.action_kind='NO_ACTION' and exists (
        select 1 from public.import_review_action_outcomes o
        where o.import_id=d.import_id and o.source_identity=d.source_identity
      ))
  ), eligible_units as (
    select u.candidate_id,u.client_id
    from selected_units u
    where not exists (
      select 1 from public.import_review_decisions b
      where b.import_id=p_import_id and b.is_current and b.blocking
        -- A blocker only owns a resolved candidate/client unit when both
        -- identities are known.  An unresolved source worker or source client
        -- must remain pending without freezing unrelated ready candidates in
        -- the same Trust/import.
        and b.candidate_id=u.candidate_id
        and b.client_id=u.client_id
    )
  )
  select d.action_id
  from public.import_review_decisions d
  join eligible_units u on u.candidate_id=d.candidate_id and u.client_id=d.client_id
  where d.import_id=p_import_id and d.is_current and d.selectable and d.selected and not d.blocking
    and not exists (
      select 1 from public.import_review_action_outcomes o where o.action_id=d.action_id
    )
    and not (d.action_kind='NO_ACTION' and exists (
      select 1 from public.import_review_action_outcomes o
      where o.import_id=d.import_id and o.source_identity=d.source_identity
    ))
  order by d.action_id;
end
$function$;

create or replace function public._import_review_apply_envelope_core_v1(p_import_id uuid)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_state public.import_review_states%rowtype;
  v_import public.hr_imports%rowtype;
  v_selected_ids text[];
  v_invalidation_ids text[];
  v_correction_units jsonb;
  v_reconciliation_units jsonb;
begin
  select * into v_state from public.import_review_states where import_id=p_import_id;
  select * into v_import from public.hr_imports where id=p_import_id;
  if v_state.import_id is null or v_import.id is null then
    raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002';
  end if;
  select coalesce(array_agg(r.action_id order by r.action_id),array[]::text[])
  into v_selected_ids from public._import_review_ready_action_ids_core_v1(p_import_id) r;
  select coalesce(array_agg(d.action_id order by d.action_id),array[]::text[])
  into v_invalidation_ids from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.action_id=any(v_selected_ids)
    and d.action_kind='INVALIDATE_REFERENCE';
  select coalesce(jsonb_agg(jsonb_build_object(
    'action_id',d.action_id,'root_timesheet_id',d.timesheet_id,'source_row_key',d.source_identity,
    'correction_action',case when d.action_kind='APPLY_AMENDMENT' then 'CHANGED_HOURS' else 'CANCELLATION' end,
    'correction_shape',case when d.action_kind='APPLY_AMENDMENT' then 'REVERSAL_REPLACEMENT' else 'REVERSAL_ONLY' end
  ) order by d.action_id),'[]'::jsonb)
  into v_correction_units
  from public.import_review_decisions d
  cross join lateral (
    select public._import_review_timesheet_protection_core_v1(d.timesheet_id) as protection
  ) pr
  where d.import_id=p_import_id and d.is_current and d.action_id=any(v_selected_ids)
    and d.action_kind in ('APPLY_AMENDMENT','APPLY_CANCELLATION') and d.timesheet_id is not null
    and (coalesce((pr.protection->>'paid')::boolean,false)
      or coalesce((pr.protection->>'invoice_locked')::boolean,false))
    and coalesce((select a.import_authoritative
      from public._import_review_effective_authority_core_v1(
        case when v_import.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end,
        d.contract_id,d.client_id,coalesce(d.summary_json->>'work_date',d.summary_json->>'week_ending_date')::date) a),false);
  select coalesce(jsonb_agg(unit_json order by action_id),'[]'::jsonb)
  into v_reconciliation_units
  from (
    select d.action_id,
      jsonb_build_object(
        'schema_version','IMPORT_AUTHORITATIVE_RECONCILIATION_V1',
        'action_id',d.action_id,'source_identity',d.source_identity,
        'source_system',case when v_import.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HEALTHROSTER' end,
        'source_shift_id',d.summary_json->>'existing_shift_id',
        'hr_row_id',d.hr_row_id,
        'authoritative_import_id',p_import_id,
        'source_timesheet_id',d.timesheet_id,
        'candidate_id',d.candidate_id,'client_id',d.client_id,'contract_id',d.contract_id,
        'week_ending_date',d.summary_json->>'week_ending_date',
        'invoice_stream',d.summary_json->>'invoice_stream',
        'source_scope_fingerprint',d.summary_json->>'source_scope_fingerprint',
        'route',d.summary_json->>'amendment_route',
        'reconciliation_mode',d.summary_json->>'reconciliation_mode',
        'B_effective_invoice_ids',coalesce(d.summary_json->'effective_invoice_ids','[]'::jsonb),
        'B_effective_invoice_line_ids',coalesce(d.summary_json->'effective_invoice_line_ids','[]'::jsonb),
        'B_hours',d.summary_json->'B_hours','B_financials',d.summary_json->'B_financials',
        'B_standard_schedule_json',coalesce(d.summary_json->'B_standard_schedule_json','[]'::jsonb),
        'B_invoice_fingerprint',d.summary_json->>'effective_invoice_fingerprint',
        'M_active_member_ids',coalesce(d.summary_json->'active_mutable_member_ids','[]'::jsonb),
        'M_missing_roles',coalesce(d.summary_json->'physically_missing_mutable_roles',d.summary_json->'missing_mutable_roles','[]'::jsonb),
        'M_hours',d.summary_json->'M_hours','M_fingerprint',d.summary_json->>'mutable_generation_fingerprint',
        'A_schedule_json',d.summary_json->'A_schedule_json','A_hours',d.summary_json->'A_hours',
        'A_evidence_fingerprint',d.summary_json->>'authoritative_evidence_fingerprint',
        'archived_timesheet_ids',coalesce(d.summary_json->'archived_history_timesheet_ids',d.summary_json->'archived_timesheet_ids','[]'::jsonb),
        'archived_history_roles',coalesce(d.summary_json->'archived_history_roles','[]'::jsonb),
        'historical_missing_timesheet_ids',coalesce(d.summary_json->'historical_missing_timesheet_ids','[]'::jsonb),
        'reviewed_existing_correction_id',coalesce(d.summary_json->>'reviewed_existing_correction_id',d.summary_json->>'correction_id'),
        'reviewed_existing_member_ids',coalesce(d.summary_json->'active_mutable_member_ids','[]'::jsonb),
        'repair_identity_mode',d.summary_json->>'repair_identity_mode',
        'reversal_repair_required',coalesce((d.summary_json->>'reversal_repair_required')::boolean,false),
        'replacement_repair_required',coalesce((d.summary_json->>'replacement_repair_required')::boolean,false),
        'expected_roles',case when d.summary_json->>'amendment_route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
          then jsonb_build_array('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT') else '[]'::jsonb end,
        'parent_timesheet_id',coalesce(
          nullif(d.summary_json->>'active_mutable_parent_timesheet_id','')::uuid,
          d.timesheet_id
        ),
        'review_policy_basis_kind',d.summary_json->>'review_policy_basis_kind',
        'review_policy_basis_fingerprint',d.summary_json->>'review_policy_basis_fingerprint',
        'intended_authorisation_action',d.summary_json->>'intended_authorisation_action',
        'financial_validation_mode',d.summary_json->>'financial_validation_mode',
        'reconciliation_fingerprint',d.summary_json->>'reconciliation_fingerprint',
        'unit_fingerprint',public._import_review_hash_v1(concat_ws('|','unit-v2',d.action_id,d.source_identity,
          d.summary_json->>'existing_shift_id',d.summary_json->>'amendment_route',d.summary_json->>'reconciliation_mode',
          d.summary_json->>'reconciliation_fingerprint',d.summary_json->>'review_policy_basis_kind',
          d.summary_json->>'review_policy_basis_fingerprint',d.evidence_fingerprint))
      ) unit_json
    from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.selected and d.selectable
      and d.action_id=any(v_selected_ids) and d.action_kind='APPLY_AMENDMENT'
      and upper(coalesce(d.summary_json->>'authority_mode',''))='AUTHORITATIVE'
      and coalesce((d.summary_json->>'is_daily')::boolean,false)=false
  ) frozen;
  if exists (
    select 1 from jsonb_array_elements(v_reconciliation_units) u
    where nullif(u->>'action_id','') is null or nullif(u->>'source_identity','') is null
      or nullif(u->>'route','') is null or nullif(u->>'reconciliation_fingerprint','') is null
      or jsonb_typeof(u->'A_schedule_json')<>'array' or jsonb_typeof(u->'A_hours')<>'object'
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
  end if;
  return jsonb_build_object(
    'schema_version','IMPORT_REVIEW_APPLY_V1','import_id',p_import_id,
    'selected_action_ids',to_jsonb(v_selected_ids),'coverage_fingerprint',v_import.coverage_fingerprint,
    'preview_fingerprint',v_state.preview_fingerprint,
    'reference_invalidation_action_ids',to_jsonb(v_invalidation_ids),
    'correction_units',v_correction_units,
    'reconciliation_units',v_reconciliation_units,
    'batch_scope_units',coalesce((select jsonb_agg(jsonb_build_object(
      'candidate_id',u.candidate_id,'client_id',u.client_id) order by u.candidate_id,u.client_id)
      from (select distinct d.candidate_id,d.client_id from public.import_review_decisions d
        where d.import_id=p_import_id and d.action_id=any(v_selected_ids)) u),'[]'::jsonb),
    'deferred_action_count',(select count(*) from public.import_review_decisions d
      where d.import_id=p_import_id and d.is_current and d.selectable and not d.selected));
end
$function$;

create or replace function public._import_review_validate_ui_state_v1(p_ui_state jsonb)
returns jsonb
language plpgsql
immutable
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v jsonb:=coalesce(p_ui_state,'{}'::jsonb);
  v_key text;
begin
  if jsonb_typeof(v)<>'object' or pg_column_size(v)>65536 then
    raise exception 'IMPORT_REVIEW_UI_STATE_INVALID' using errcode='22023';
  end if;
  for v_key in select jsonb_object_keys(v) loop
    if v_key not in ('expanded_candidates','expanded_clients','expanded_weeks','expanded_shifts',
                     'active_section','scroll_anchor','show_no_action','show_automatic',
                     'page_number','page_size','sort_by','sort_direction') then
      raise exception 'IMPORT_REVIEW_UI_STATE_KEY_NOT_ALLOWED' using errcode='22023',detail=v_key;
    end if;
  end loop;
  -- Validate presentation state structurally.  Scanning the entire JSON text
  -- treated the legitimate active section EMAILS as authority and made a
  -- checkbox save fail.  Expansion tokens are now opaque, typed and bounded;
  -- no recipient, action, timesheet or financial identity can be persisted.
  if exists (
    select 1
    from jsonb_each(v) e
    where e.key in ('expanded_candidates','expanded_clients','expanded_weeks','expanded_shifts')
      and jsonb_typeof(e.value)<>'array'
  ) then
    raise exception 'IMPORT_REVIEW_UI_STATE_EXPANSION_INVALID' using errcode='22023';
  end if;
  if exists (
    select 1 from jsonb_each(v) e
    where jsonb_array_length(case when jsonb_typeof(e.value)='array' then e.value else '[]'::jsonb end)>500
  ) then raise exception 'IMPORT_REVIEW_UI_STATE_ARRAY_LIMIT_EXCEEDED' using errcode='22023'; end if;
  if exists (
    select 1
    from jsonb_each(v) e
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(e.value)='array' then e.value else '[]'::jsonb end
    ) a(value)
    where e.key in ('expanded_candidates','expanded_clients','expanded_weeks','expanded_shifts')
      and (
        jsonb_typeof(a.value)<>'string'
        or length(a.value#>>'{}')>96
        or (a.value#>>'{}') !~ '^(candidate|client|week|shift):u-[0-9a-f]{16}(-[0-9a-f]{16})?$'
      )
  ) then
    raise exception 'IMPORT_REVIEW_UI_STATE_CONTAINS_AUTHORITY' using errcode='22023';
  end if;
  if v?'active_section' and (
    jsonb_typeof(v->'active_section')<>'string'
    or upper(v->>'active_section') not in ('PENDING','READY','EMAIL','NO_ACTION')
  ) then raise exception 'IMPORT_REVIEW_UI_STATE_SECTION_INVALID' using errcode='22023'; end if;
  if v?'scroll_anchor' and jsonb_typeof(v->'scroll_anchor')<>'null' then
    raise exception 'IMPORT_REVIEW_UI_STATE_SCROLL_ANCHOR_INVALID' using errcode='22023'; end if;
  if v?'show_no_action' and jsonb_typeof(v->'show_no_action')<>'boolean' then
    raise exception 'IMPORT_REVIEW_UI_STATE_FLAG_INVALID' using errcode='22023'; end if;
  if v?'show_automatic' and jsonb_typeof(v->'show_automatic')<>'boolean' then
    raise exception 'IMPORT_REVIEW_UI_STATE_FLAG_INVALID' using errcode='22023'; end if;
  if v?'page_number' and (jsonb_typeof(v->'page_number')<>'number' or (v->>'page_number')!~'^\d+$'
      or (v->>'page_number')::integer not between 1 and 100) then
    raise exception 'IMPORT_REVIEW_UI_STATE_PAGE_INVALID' using errcode='22023'; end if;
  if v?'page_size' and (jsonb_typeof(v->'page_size')<>'number' or (v->>'page_size')!~'^\d+$'
      or (v->>'page_size')::integer not in (25,50,75,100)) then
    raise exception 'IMPORT_REVIEW_UI_STATE_PAGE_SIZE_INVALID' using errcode='22023'; end if;
  if v?'sort_by' and upper(v->>'sort_by') not in ('CANDIDATE','CLIENT','WEEK_ENDING','WORK_DATE','ACTION','STATUS') then
    raise exception 'IMPORT_REVIEW_UI_STATE_SORT_INVALID' using errcode='22023'; end if;
  if v?'sort_direction' and upper(v->>'sort_direction') not in ('ASC','DESC') then
    raise exception 'IMPORT_REVIEW_UI_STATE_SORT_DIRECTION_INVALID' using errcode='22023'; end if;
  return v;
end
$function$;

create or replace function public._import_review_timesheet_protection_core_v1(p_timesheet_id uuid)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_active_draft boolean:=false;
  v_paid boolean:=false;
  v_invoice_locked boolean:=false;
  v_processing_status text;
  v_correction_root_id uuid;
  v_correction_family_ids uuid[];
begin
  if p_timesheet_id is null then
    return jsonb_build_object('active_pay_draft',false,'paid',false,'invoice_locked',false,'protected',false);
  end if;

  with recursive correction_ancestry as (
    select
      t.timesheet_id,
      t.parent_timesheet_id,
      array[t.timesheet_id]::uuid[] as visited_ids,
      0 as depth
    from public.timesheets t
    where t.timesheet_id=p_timesheet_id
    union all
    select
      parent_timesheet.timesheet_id,
      parent_timesheet.parent_timesheet_id,
      correction_ancestry.visited_ids||parent_timesheet.timesheet_id,
      correction_ancestry.depth+1
    from correction_ancestry
    join public.timesheets parent_timesheet
      on parent_timesheet.timesheet_id=correction_ancestry.parent_timesheet_id
    where correction_ancestry.depth<64
      and not parent_timesheet.timesheet_id=any(correction_ancestry.visited_ids)
  )
  select
    coalesce(array_agg(correction_ancestry.timesheet_id order by correction_ancestry.depth),array[p_timesheet_id]::uuid[]),
    (array_agg(correction_ancestry.timesheet_id order by correction_ancestry.depth desc))[1]
  into v_correction_family_ids,v_correction_root_id
  from correction_ancestry;
  v_correction_family_ids:=coalesce(v_correction_family_ids,array[p_timesheet_id]::uuid[]);
  v_correction_root_id:=coalesce(v_correction_root_id,p_timesheet_id);

  select coalesce(tf.paid_at_utc is not null,false),
         coalesce(tf.locked_by_invoice_id is not null,false),
         tf.processing_status::text
  into v_paid,v_invoice_locked,v_processing_status
  from public.timesheets_financials tf
  where tf.timesheet_id=p_timesheet_id and tf.is_current=true
  order by tf.updated_at desc nulls last
  limit 1;

  -- Banking Pay settlement is recorded in frozen batch artifacts and the
  -- canonical pay-state cache; it does not rewrite the legacy TSFIN paid marker.
  -- Treat a settled, non-voided batch item anywhere in the correction family
  -- as paid evidence so imports cannot amend a CSV/provider-settled replacement
  -- in place. Frozen delta items are intentionally rooted at the original
  -- timesheet, not at the replacement member that contributed the delta.
  v_paid := coalesce(v_paid,false) or exists (
    select 1
    from public.pay_batch_items settled_item
    join public.pay_batch_candidates settled_candidate
      on settled_candidate.id=settled_item.pay_batch_candidate_id
    where (
        settled_item.timesheet_id=any(v_correction_family_ids)
        or settled_item.frozen_component_snapshot_json->>'correction_root_id'=any(v_correction_family_ids::text[])
        or settled_item.frozen_resolution_payload_json->>'correction_root_id'=any(v_correction_family_ids::text[])
      )
      and coalesce(settled_item.is_voided,false)=false
      and (
        upper(btrim(coalesce(settled_candidate.settlement_status,'')))='SETTLED'
        or settled_candidate.settled_at_utc is not null
      )
  );

  -- Once any invoice line exists, the timesheet must not be amended in place.
  -- This applies equally to draft, unissued, issued and paid invoices: every
  -- subsequent import-authoritative change must use the reversal route.
  v_invoice_locked := coalesce(v_invoice_locked,false) or exists (
    select 1
    from public.invoice_lines il
    where il.timesheet_id=p_timesheet_id
  );

  v_active_draft := exists (
    select 1
    from (
      select pb.id
      from public.pay_batch_items pbi
      join public.pay_batch_candidates pbc on pbc.id=pbi.pay_batch_candidate_id
      join public.pay_batches pb on pb.id=pbc.pay_batch_id
      where pbi.timesheet_id=p_timesheet_id and not coalesce(pbi.is_voided,false)
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_batch_timesheet_snapshots pts
      join public.pay_batches pb on pb.id=pts.pay_batch_id
      where pts.timesheet_id=p_timesheet_id
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_payment_correction_items pci
      join public.pay_batches pb on pb.id=pci.pay_batch_id
      where pci.timesheet_id=p_timesheet_id and upper(btrim(coalesce(pci.status,'')))='APPLIED'
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.timesheet_payment_overrides tpo
      join public.pay_batches pb on pb.id=tpo.consumed_by_pay_batch_id
      where tpo.timesheet_id=p_timesheet_id and tpo.cleared_at_utc is null
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_advances pa
      join public.pay_advance_reservations par on par.finance_case_id=pa.id
      join public.pay_batches pb on pb.id=par.pay_batch_id
      where pa.linked_timesheet_id=p_timesheet_id
        and upper(btrim(coalesce(par.status,''))) in ('RESERVED','COMMITTED')
        and par.released_at_utc is null and par.settled_at_utc is null
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_finance_case_components pfc
      join public.pay_advance_reservations par on par.finance_component_id=pfc.id
      join public.pay_batches pb on pb.id=par.pay_batch_id
      where pfc.linked_timesheet_id=p_timesheet_id
        and upper(btrim(coalesce(par.status,''))) in ('RESERVED','COMMITTED')
        and par.released_at_utc is null and par.settled_at_utc is null
        and public._pay_batch_status_is_active_reservation(pb.status)
      limit 1
    ) blockers
  );

  return jsonb_build_object(
    'active_pay_draft',v_active_draft,
    'paid',coalesce(v_paid,false),
    'invoice_locked',coalesce(v_invoice_locked,false),
    'processing_status',v_processing_status,
    'protected',v_active_draft or coalesce(v_paid,false) or coalesce(v_invoice_locked,false)
  );
end
$function$;

create or replace function public._import_review_query_evidence_core_v1(p_timesheet_id uuid)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_ts public.timesheets%rowtype;
  v_doc public.invoice_document_versions%rowtype;
  v_asset public.invoice_document_assets%rowtype;
  v_protection jsonb;
  v_has_hours boolean:=false;
  v_source_complete boolean:=false;
  v_document_ready boolean:=false;
  v_is_signed_qr boolean:=false;
  v_reason_code text;
  v_fingerprint text;
begin
  if p_timesheet_id is null then
    return jsonb_build_object(
      'timesheet_id',null,'source_complete',false,'document_ready',false,
      'preparation_required',false,'reason_code','TIMESHEET_EVIDENCE_INCOMPLETE',
      'evidence_fingerprint',public._import_review_hash_v1('query-evidence-v1|null')
    );
  end if;

  select * into v_ts
  from public.timesheets t
  where t.timesheet_id=p_timesheet_id
  limit 1;

  if not found or not coalesce(v_ts.is_current,false) or v_ts.archived_at_utc is not null then
    return jsonb_build_object(
      'timesheet_id',p_timesheet_id,'source_complete',false,'document_ready',false,
      'preparation_required',false,'reason_code','TIMESHEET_EVIDENCE_INCOMPLETE',
      'evidence_fingerprint',public._import_review_hash_v1(concat_ws('|','query-evidence-v1',p_timesheet_id,'MISSING_CURRENT_SOURCE'))
    );
  end if;

  v_protection:=public._import_review_timesheet_protection_core_v1(p_timesheet_id);
  v_has_hours:=case
    when upper(coalesce(v_ts.sheet_scope::text,''))='WEEKLY' then
      jsonb_typeof(coalesce(v_ts.actual_schedule_json,'[]'::jsonb))='array'
      and jsonb_array_length(coalesce(v_ts.actual_schedule_json,'[]'::jsonb))>0
      and not exists (
        select 1 from jsonb_array_elements(coalesce(v_ts.actual_schedule_json,'[]'::jsonb)) day(value)
        where nullif(btrim(coalesce(day.value->>'start',day.value->>'start_time',day.value->>'worked_start','')),'') is null
           or nullif(btrim(coalesce(day.value->>'end',day.value->>'end_time',day.value->>'worked_end','')),'') is null
      )
    else v_ts.worked_start_iso is not null and v_ts.worked_end_iso is not null
  end;

  -- Any QR lifecycle marker makes this a QR timesheet.  It is complete only
  -- when the complete signed-QR contract below is satisfied; a pending or
  -- unsigned QR PDF must never fall through as an ordinary manual upload.
  v_is_signed_qr:=v_ts.qr_status is not null
    or v_ts.qr_scanned_at is not null
    or nullif(btrim(coalesce(v_ts.qr_signed_hash,'')),'') is not null
    or v_ts.qr_signed_at_utc is not null;

  if upper(coalesce(v_ts.submission_mode::text,''))='ELECTRONIC' then
    v_source_complete:=v_has_hours
      and nullif(btrim(coalesce(v_ts.r2_nurse_key,'')),'') is not null
      and nullif(btrim(coalesce(v_ts.img_sha256_nurse,'')),'') is not null
      and nullif(btrim(coalesce(v_ts.r2_auth_key,'')),'') is not null
      and nullif(btrim(coalesce(v_ts.img_sha256_auth,'')),'') is not null
      and nullif(btrim(coalesce(v_ts.auth_name,'')),'') is not null
      and v_ts.authorised_at_server is not null;
  elsif upper(coalesce(v_ts.submission_mode::text,''))='MANUAL' and v_is_signed_qr then
    v_source_complete:=v_has_hours
      and upper(coalesce(v_ts.qr_status::text,''))='USED'
      and v_ts.qr_scanned_at is not null
      and nullif(btrim(coalesce(v_ts.qr_signed_hash,'')),'') is not null
      and v_ts.qr_signed_at_utc is not null
      and nullif(btrim(coalesce(v_ts.manual_pdf_r2_key,'')),'') is not null;
  elsif upper(coalesce(v_ts.submission_mode::text,''))='MANUAL' then
    if v_ts.manual_document_asset_id is not null then
      select * into v_asset from public.invoice_document_assets a
      where a.id=v_ts.manual_document_asset_id limit 1;
    end if;
    v_source_complete:=v_has_hours and (
      nullif(btrim(coalesce(v_ts.manual_pdf_r2_key,'')),'') is not null
      or (
        v_asset.id is not null
        and upper(coalesce(v_asset.status,''))='READY'
        and nullif(btrim(coalesce(v_asset.normalised_r2_key,v_asset.original_r2_key,'')),'') is not null
      )
    );
  end if;

  if v_ts.current_document_version_id is not null then
    select * into v_doc from public.invoice_document_versions d
    where d.id=v_ts.current_document_version_id limit 1;
  end if;
  v_document_ready:=v_source_complete
    and v_doc.id is not null
    and v_doc.entity_type='TIMESHEET'
    and v_doc.entity_id=v_ts.timesheet_id
    and v_doc.purpose='TIMESHEET'
    and v_doc.source_revision=v_ts.document_revision::text
    and v_doc.status='READY'
    and v_doc.superseded_at_utc is null
    and nullif(btrim(coalesce(v_doc.r2_key,'')),'') is not null
    and coalesce(v_doc.sha256,'')~'^[0-9a-f]{64}$'
    and coalesce(v_doc.size_bytes,0)>0
    and coalesce(v_doc.page_count,0)>0
    and v_doc.ready_at_utc is not null
    and v_doc.verified_at_utc is not null;

  v_reason_code:=case
    when coalesce((v_protection->>'invoice_locked')::boolean,false) then 'TIMESHEET_PRESENT_BUT_INVOICED'
    when not v_source_complete then 'TIMESHEET_EVIDENCE_INCOMPLETE'
    when not v_document_ready then 'TIMESHEET_EVIDENCE_PREPARING'
  end;
  v_fingerprint:=public._import_review_hash_v1(concat_ws('|','query-evidence-v1',v_ts.timesheet_id,
    v_ts.document_revision,v_ts.document_state,v_ts.current_document_version_id,
    v_ts.submission_mode::text,v_ts.sheet_scope::text,v_ts.updated_at,
    v_source_complete,v_document_ready,v_reason_code,
    coalesce(v_doc.id::text,''),coalesce(v_doc.source_revision,''),coalesce(v_doc.sha256,''),
    coalesce(v_doc.size_bytes::text,''),coalesce(v_doc.page_count::text,''),
    coalesce((v_protection->>'invoice_locked')::boolean,false)));

  return jsonb_strip_nulls(jsonb_build_object(
    'timesheet_id',v_ts.timesheet_id,
    'source_complete',v_source_complete,
    'document_ready',v_document_ready,
    'preparation_required',v_source_complete and not v_document_ready
      and not coalesce((v_protection->>'invoice_locked')::boolean,false),
    'reason_code',v_reason_code,
    'evidence_fingerprint',v_fingerprint,
    'document_revision',v_ts.document_revision,
    'document_state',v_ts.document_state,
    'document_version_id',case when v_document_ready then v_doc.id end,
    'attachment_r2_key',case when v_document_ready then v_doc.r2_key end,
    'attachment_sha256',case when v_document_ready then v_doc.sha256 end,
    'attachment_size_bytes',case when v_document_ready then v_doc.size_bytes end,
    'attachment_page_count',case when v_document_ready then v_doc.page_count end,
    'attachment_filename',case when v_document_ready then 'timesheet-'||v_ts.timesheet_id::text||'.pdf' end,
    'submission_mode',v_ts.submission_mode::text,
    'sheet_scope',v_ts.sheet_scope::text,
    'signed_qr',v_is_signed_qr,
    'protection',v_protection
  ));
end
$function$;

create or replace function public._import_review_timesheet_has_calculated_expenses_core_v1(
  p_timesheet_id uuid
)
returns boolean
language sql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
  select p_timesheet_id is not null and exists (
    select 1
    from public.timesheets_financials tf
    where tf.timesheet_id=p_timesheet_id
      and tf.is_current=true
      and (
        coalesce(tf.expenses_pay_ex_vat,0)<>0
        or coalesce(tf.expenses_charge_ex_vat,0)<>0
        or coalesce(tf.mileage_pay_ex_vat,0)<>0
        or coalesce(tf.mileage_charge_ex_vat,0)<>0
        or coalesce(tf.travel_pay_ex_vat,0)<>0
        or coalesce(tf.travel_charge_ex_vat,0)<>0
        or coalesce(tf.accommodation_pay_ex_vat,0)<>0
        or coalesce(tf.accommodation_charge_ex_vat,0)<>0
        or coalesce(tf.other_pay_ex_vat,0)<>0
        or coalesce(tf.other_charge_ex_vat,0)<>0
      )
  )
$function$;

create or replace function public._import_review_auto_authorise_targets_core_v1(
  p_timesheet_ids uuid[],
  p_source_system public.hr_source_enum,
  p_validation_context boolean default false
)
returns uuid[]
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_source text:=upper(btrim(coalesce(p_source_system::text,'')));
  v_result uuid[]:=array[]::uuid[];
begin
  if coalesce(cardinality(p_timesheet_ids),0)>100 then
    raise exception 'IMPORT_REVIEW_AUTO_AUTHORISE_SCOPE_TOO_LARGE' using errcode='54000';
  end if;
  if v_source not in ('NHSP','HEALTHROSTER','HEALTHROSTER_DAILY') then
    raise exception 'IMPORT_REVIEW_AUTO_AUTHORISE_SOURCE_INVALID' using errcode='22023';
  end if;

  select coalesce(array_agg(target.timesheet_id order by target.timesheet_id),array[]::uuid[])
  into v_result
  from (
    select distinct t.timesheet_id
    from unnest(coalesce(p_timesheet_ids,array[]::uuid[])) requested(timesheet_id)
    join public.timesheets t
      on t.timesheet_id=requested.timesheet_id
     and t.is_current=true
     and t.revoked_at is null
    join public.contracts c on c.id=t.contract_id
    cross join lateral (
      select public._import_review_timesheet_protection_core_v1(t.timesheet_id) as value
    ) protection
    cross join lateral (
      select public.import_auto_authorise_policy_resolve_v1(
        p_source_system,c.client_id,c.id,coalesce(p_validation_context,false)
      ) as value
    ) policy
    where t.authorised_at_server is null
      -- Import correction reversals/replacements are mandatory-authorisation
      -- targets owned by the transactional route, not policy-controlled new
      -- timesheets.  Keep the configuration helper strictly ordinary-only.
      and not (coalesce(t.is_adjustment,false) and t.correction_id is not null)
      and not exists (
        select 1 from public.timesheets_financials tf
        where tf.timesheet_id=t.timesheet_id and tf.is_current=true
          and tf.authorised_at_utc is not null
      )
      and not exists (
        select 1 from public.contract_weeks cw
        where cw.timesheet_id=t.timesheet_id and upper(coalesce(cw.status::text,''))='AUTHORISED'
      )
      and coalesce((protection.value->>'paid')::boolean,false)=false
      and coalesce((protection.value->>'invoice_locked')::boolean,false)=false
      and coalesce((protection.value->>'active_pay_draft')::boolean,false)=false
      and public._import_review_timesheet_has_calculated_expenses_core_v1(t.timesheet_id)=false
      and coalesce((policy.value->>'effective_value')::boolean,false)=true
  ) target;
  return v_result;
end
$function$;

alter function public._import_review_timesheet_has_calculated_expenses_core_v1(uuid) owner to postgres;
revoke all on function public._import_review_timesheet_has_calculated_expenses_core_v1(uuid) from public,anon,authenticated,service_role;

create or replace function public._import_review_effective_invoice_balance_core_v1(
  p_import_id uuid,
  p_source_items jsonb,
  p_max_sources integer default 100,
  p_max_invoice_lines_per_source integer default 512,
  p_max_audit_rows_per_source integer default 256,
  p_max_operations_per_source integer default 128
)
returns table(source_identity text,balance_json jsonb)
language plpgsql
security invoker
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_item jsonb;
  v_source_identity text;
  v_source_system text;
  v_external_row_key text;
  v_invoice_stream text;
  v_source_shift_id uuid;
  v_source_timesheet_id uuid;
  v_hr_row_id uuid;
  v_candidate_id uuid;
  v_client_id uuid;
  v_contract_id uuid;
  v_week_ending_date date;
  v_authoritative_import_id uuid;
  v_a_schedule jsonb;
  v_a_hours jsonb;
  v_a_fingerprint text;
  v_scope_fingerprint text;
  v_hist_ids uuid[]:=array[]::uuid[];
  v_audit_ids uuid[]:=array[]::uuid[];
  v_archived_ids uuid[]:=array[]::uuid[];
  v_active_ids uuid[]:=array[]::uuid[];
  v_missing_ids uuid[]:=array[]::uuid[];
  v_import_ids uuid[]:=array[]::uuid[];
  v_operation_ids uuid[]:=array[]::uuid[];
  v_effective_invoice_ids uuid[]:=array[]::uuid[];
  v_effective_line_ids uuid[]:=array[]::uuid[];
  v_issued_invoice_ids uuid[]:=array[]::uuid[];
  v_issued_line_ids uuid[]:=array[]::uuid[];
  v_pending_invoice_ids uuid[]:=array[]::uuid[];
  v_pending_line_ids uuid[]:=array[]::uuid[];
  v_credit_line_ids uuid[]:=array[]::uuid[];
  v_line_count integer:=0;
  v_audit_count integer:=0;
  v_operation_count integer:=0;
  v_operation_evidence jsonb:='[]'::jsonb;
  v_operation_member_ids uuid[]:=array[]::uuid[];
  v_member_supersession_map jsonb:='[]'::jsonb;
  v_member_supersession_conflict boolean:=false;
  v_operation_evidence_conflict boolean:=false;
  v_operation_in_progress boolean:=false;
  v_transition_operation_id uuid:=case
    when coalesce(current_setting('cloudtms.import_reconciliation_operation_id',true),'')
      ~*'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
      then current_setting('cloudtms.import_reconciliation_operation_id',true)::uuid
    else null end;
  v_member_role_map jsonb:='[]'::jsonb;
  v_member_role_conflict boolean:=false;
  v_effective_component_count integer:=0;
  v_issued_component_count integer:=0;
  v_pending_component_count integer:=0;
  v_b_day numeric:=0;
  v_b_night numeric:=0;
  v_b_sat numeric:=0;
  v_b_sun numeric:=0;
  v_b_bh numeric:=0;
  v_b_pay numeric:=0;
  v_b_charge numeric:=0;
  v_b_margin numeric:=0;
  v_issued_day numeric:=0;
  v_issued_night numeric:=0;
  v_issued_sat numeric:=0;
  v_issued_sun numeric:=0;
  v_issued_bh numeric:=0;
  v_issued_pay numeric:=0;
  v_issued_charge numeric:=0;
  v_issued_margin numeric:=0;
  v_pending_day numeric:=0;
  v_pending_night numeric:=0;
  v_pending_sat numeric:=0;
  v_pending_sun numeric:=0;
  v_pending_bh numeric:=0;
  v_pending_pay numeric:=0;
  v_pending_charge numeric:=0;
  v_pending_margin numeric:=0;
  v_component_day numeric:=0;
  v_component_night numeric:=0;
  v_component_sat numeric:=0;
  v_component_sun numeric:=0;
  v_component_bh numeric:=0;
  v_component_pay numeric:=0;
  v_component_charge numeric:=0;
  v_component_margin numeric:=0;
  v_b_schedule jsonb:='[]'::jsonb;
  v_candidate_schedule jsonb:='[]'::jsonb;
  v_candidate_hours jsonb:='{}'::jsonb;
  v_schedule_candidates jsonb:='[]'::jsonb;
  v_candidate_policy_fingerprint text;
  v_b_policy_fingerprint text;
  v_terminal_generation_id text;
  v_terminal_positive_timesheet_id uuid;
  v_terminal_positive_member_count integer:=0;
  v_terminal_frozen_candidate jsonb;
  v_terminal_frozen_candidate_count integer:=0;
  v_terminal_frozen_schedule_variant_count integer:=0;
  v_terminal_frozen_policy_variant_count integer:=0;
  v_terminal_operation jsonb;
  v_terminal_operation_schedule jsonb:='[]'::jsonb;
  v_terminal_operation_hours jsonb:='{}'::jsonb;
  v_terminal_operation_policy_fingerprint text;
  v_terminal_operation_id uuid;
  v_terminal_policy_snapshot jsonb:='{}'::jsonb;
  v_terminal_policy_snapshot_fingerprint text;
  v_terminal_derived_day numeric:=0;
  v_terminal_derived_night numeric:=0;
  v_terminal_derived_sat numeric:=0;
  v_terminal_derived_sun numeric:=0;
  v_terminal_derived_bh numeric:=0;
  v_terminal_derived_total numeric:=0;
  v_terminal_frozen_matches_b boolean:=false;
  v_terminal_operation_matches_b boolean:=false;
  v_terminal_schedule_authority_conflict boolean:=false;
  v_terminal_policy_authority_conflict boolean:=false;
  v_b_standard_schedule_authority text:='NONE';
  v_b_standard_schedule_authority_timesheet_id uuid;
  v_b_standard_schedule_authority_correction_id text;
  v_b_standard_schedule_authority_operation_id uuid;
  v_b_standard_schedule_authority_policy_fingerprint text;
  v_b_standard_schedule_authority_fingerprint text;
  v_b_standard_schedule_authority_diagnostic text;
  v_effective_fingerprint text;
  v_line_evidence jsonb:='[]'::jsonb;
  v_ignored_nonhours_line_ids uuid[]:=array[]::uuid[];
  v_generation_role_evidence jsonb:='[]'::jsonb;
  v_fully_invoiced_generation_ids text[]:=array[]::text[];
  v_partial_generation_ids text[]:=array[]::text[];
  v_mutable_generation_ids text[]:=array[]::text[];
  v_archived_history_roles jsonb:='[]'::jsonb;
  v_role_evidence_conflicts jsonb:='[]'::jsonb;
  v_role_evidence_fingerprint text;
  v_repair_identity_mode text;
  v_reversal_repair_required boolean:=false;
  v_replacement_repair_required boolean:=false;
  v_line record;
  v_original_line public.invoice_lines%rowtype;
  v_original_invoice public.invoices%rowtype;
  v_tf public.timesheets_financials%rowtype;
  v_original_tf public.timesheets_financials%rowtype;
  v_seg jsonb;
  v_original_seg jsonb;
  v_line_type text;
  v_original_line_type text;
  v_is_weekly_hours boolean:=false;
  v_is_separable_nonhours boolean:=false;
  v_original_line_id uuid;
  v_seg_count integer:=0;
  v_matching_seg_count integer:=0;
  v_original_seg_count integer:=0;
  v_original_matching_seg_count integer:=0;
  v_single_source boolean:=false;
  v_line_scope_proven boolean:=false;
  v_operation_member_scope_proven boolean:=false;
  v_component_timesheet_id uuid;
  v_component_correction_id text;
  v_component_correction_kind text;
  v_component_economic_state text;
  v_scope_unprovable boolean:=false;
  v_credit_ambiguous boolean:=false;
  v_stream_conflict boolean:=false;
  v_archived_invoice_conflict boolean:=false;
  v_partial_invoice_state boolean:=false;
  v_active_invoice_activity boolean:=false;
  v_role_partial_invoice_state boolean:=false;
  v_role_active_invoice_activity boolean:=false;
  v_role_scope_unprovable boolean:=false;
  v_paid_mutable_state boolean:=false;
  v_mutable_correction_id text;
  v_mutable_member_ids uuid[]:=array[]::uuid[];
  v_mutable_missing_roles text[]:=array[]::text[];
  v_mutable_fingerprint text;
  v_mutable_parent_id uuid;
  v_m_day numeric:=0;
  v_m_night numeric:=0;
  v_m_sat numeric:=0;
  v_m_sun numeric:=0;
  v_m_bh numeric:=0;
  v_m_pay numeric:=0;
  v_m_charge numeric:=0;
  v_m_margin numeric:=0;
  v_m_financials_complete boolean:=true;
  v_b_standard_representable boolean:=false;
  v_b_hours_zero boolean:=false;
  v_b_money_zero boolean:=false;
  v_effective_zero boolean:=false;
  v_current_source_safe boolean:=false;
  v_current_source_safety_reason text;
  v_current_source_count integer:=0;
  v_current_source_invoice_lined boolean:=false;
  v_current_source_paid boolean:=false;
  v_current_source_unlocked boolean:=false;
  v_current_source_fresh boolean:=false;
  v_current_source_segment_unlocked boolean:=false;
  v_current_source_contract_week_safe boolean:=false;
  v_current_source_invoice_operation_clear boolean:=false;
  v_source_protection jsonb:='{}'::jsonb;
  v_financial_position_requires_amendment boolean:=false;
  v_blocking_code text;
  v_reconciliation_fingerprint text;
  v_uuid_re constant text:='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
begin
  if p_import_id is null or jsonb_typeof(coalesce(p_source_items,'null'::jsonb))<>'array' then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_INPUT_INVALID' using errcode='22023';
  end if;
  if p_max_sources not between 1 and 100
     or p_max_invoice_lines_per_source not between 1 and 512
     or p_max_audit_rows_per_source not between 1 and 256
     or p_max_operations_per_source not between 1 and 128 then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_BOUND_INVALID' using errcode='22023';
  end if;
  if jsonb_array_length(p_source_items)>p_max_sources then
    raise exception 'IMPORT_REVIEW_SOURCE_LIMIT_EXCEEDED' using errcode='54000';
  end if;
  if exists (
    select 1 from jsonb_array_elements(p_source_items) s(value)
    group by nullif(btrim(s.value->>'source_identity'),'') having count(*)>1
  ) then
    raise exception 'IMPORT_REVIEW_SOURCE_IDENTITY_DUPLICATE' using errcode='22023';
  end if;

  for v_item in select s.value from jsonb_array_elements(p_source_items) s(value)
  loop
    if jsonb_typeof(v_item)<>'object' then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023';
    end if;
    v_source_identity:=nullif(btrim(v_item->>'source_identity'),'');
    v_source_system:=upper(btrim(coalesce(v_item->>'source_system','')));
    v_external_row_key:=nullif(btrim(v_item->>'external_row_key'),'');
    v_invoice_stream:=upper(btrim(coalesce(v_item->>'invoice_stream','')));
    if v_source_identity is null or v_source_system not in ('NHSP','HEALTHROSTER')
       or v_external_row_key is null or v_invoice_stream not in ('NORMAL','SELF_BILL')
       or coalesce(v_item->>'source_shift_id','')!~*v_uuid_re
       or coalesce(v_item->>'hr_row_id','')!~*v_uuid_re
       or coalesce(v_item->>'source_timesheet_id','')!~*v_uuid_re
       or coalesce(v_item->>'candidate_id','')!~*v_uuid_re
       or coalesce(v_item->>'client_id','')!~*v_uuid_re
       or coalesce(v_item->>'contract_id','')!~*v_uuid_re
       or coalesce(v_item->>'authoritative_import_id','')!~*v_uuid_re
       or coalesce(v_item->>'week_ending_date','')!~'^\d{4}-\d{2}-\d{2}$'
       or jsonb_typeof(v_item->'authoritative_schedule_json')<>'array'
       or jsonb_array_length(v_item->'authoritative_schedule_json')<>1
       or jsonb_typeof(v_item->'authoritative_hours')<>'object' then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=coalesce(v_source_identity,'missing source_identity');
    end if;
    v_source_shift_id:=(v_item->>'source_shift_id')::uuid;
    v_hr_row_id:=(v_item->>'hr_row_id')::uuid;
    v_source_timesheet_id:=(v_item->>'source_timesheet_id')::uuid;
    v_candidate_id:=(v_item->>'candidate_id')::uuid;
    v_client_id:=(v_item->>'client_id')::uuid;
    v_contract_id:=(v_item->>'contract_id')::uuid;
    v_authoritative_import_id:=(v_item->>'authoritative_import_id')::uuid;
    v_week_ending_date:=(v_item->>'week_ending_date')::date;
    v_a_schedule:=v_item->'authoritative_schedule_json';
    v_a_hours:=jsonb_build_object(
      'hours_day',coalesce((v_item#>>'{authoritative_hours,hours_day}')::numeric,0),
      'hours_night',coalesce((v_item#>>'{authoritative_hours,hours_night}')::numeric,0),
      'hours_sat',coalesce((v_item#>>'{authoritative_hours,hours_sat}')::numeric,0),
      'hours_sun',coalesce((v_item#>>'{authoritative_hours,hours_sun}')::numeric,0),
      'hours_bh',coalesce((v_item#>>'{authoritative_hours,hours_bh}')::numeric,0),
      'total_hours',coalesce((v_item#>>'{authoritative_hours,total_hours}')::numeric,0)
    );
    v_a_fingerprint:=encode(digest(convert_to(concat_ws('|','A-v1',v_source_identity,v_authoritative_import_id,v_a_schedule::text,v_a_hours::text),'UTF8'),'sha256'),'hex');
    v_scope_fingerprint:=encode(digest(convert_to(concat_ws('|','source-scope-v1',v_source_identity,v_source_system,v_source_shift_id,v_external_row_key,v_source_timesheet_id,v_candidate_id,v_client_id,v_contract_id,v_week_ending_date,v_invoice_stream),'UTF8'),'sha256'),'hex');

    perform 1 from public.hr_rows r
    where r.id=v_hr_row_id and r.import_id=p_import_id and r.external_row_key=v_external_row_key;
    if not found then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=v_source_identity;
    end if;
    perform 1 from public.nhsp_shifts s
    where s.id=v_source_shift_id and s.external_row_key=v_external_row_key
      and upper(s.source_system::text)=v_source_system
      and s.candidate_id=v_candidate_id and s.client_id=v_client_id
      and s.contract_id=v_contract_id and s.week_ending_date=v_week_ending_date;
    if not found then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=v_source_identity;
    end if;

    select count(*)::integer into v_audit_count
    from public.audit_events ae
    where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
      and (ae.after_json->>'shift_id'=v_source_shift_id::text
        or ae.after_json->>'external_row_key'=v_external_row_key);
    if v_audit_count>p_max_audit_rows_per_source then
      raise exception 'IMPORT_REVIEW_AUDIT_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    select coalesce(array_agg(distinct candidate_id order by candidate_id),array[]::uuid[])
    into v_audit_ids
    from (
      select candidate_id
      from public.audit_events ae
      cross join lateral unnest(array[
        case when ae.object_type='timesheets' then ae.object_id_text end,
        ae.after_json->>'timesheet_id',
        ae.after_json->>'reversal_timesheet_id',
        ae.after_json->>'replacement_timesheet_id',
        ae.after_json->>'counterpart_timesheet_id'
      ]) raw(candidate_text)
      cross join lateral (select case when raw.candidate_text~*v_uuid_re then raw.candidate_text::uuid end candidate_id) parsed
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text
          or ae.after_json->>'external_row_key'=v_external_row_key)
        and parsed.candidate_id is not null
    ) candidates;

    select coalesce(array_agg(distinct import_id order by import_id),array[]::uuid[])
    into v_import_ids
    from (
      select p_import_id import_id
      union all select v_authoritative_import_id
      union all select s.latest_import_id from public.nhsp_shifts s where s.id=v_source_shift_id
      union all
      select case when raw.import_text~*v_uuid_re then raw.import_text::uuid end
      from public.audit_events ae
      cross join lateral unnest(array[
        ae.after_json->>'import_id',ae.after_json->>'trigger_import_id',ae.after_json->>'evidence_import_id'
      ]) raw(import_text)
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text
          or ae.after_json->>'external_row_key'=v_external_row_key)
    ) imports where import_id is not null;
    -- A later authoritative import replaces latest_import_id, so historical
    -- operation identity is discovered both through known imports and through
    -- the immutable decision/outcome link for this exact source shift.
    select coalesce(array_agg(distinct operation_id order by operation_id),array[]::uuid[])
    into v_operation_ids
    from (
      select op.id operation_id
      from public.import_apply_operations op
      where op.import_id=any(v_import_ids)
      union all
      select outcome.operation_id
      from public.import_review_decisions decision
      join public.import_review_action_outcomes outcome on outcome.action_id=decision.action_id
      where decision.shift_id=v_source_shift_id and outcome.shift_id=v_source_shift_id
        and decision.source_identity=v_source_identity and outcome.source_identity=v_source_identity
        and decision.candidate_id=v_candidate_id and outcome.candidate_id=v_candidate_id
        and decision.client_id=v_client_id and outcome.client_id=v_client_id
        and decision.contract_id is not distinct from v_contract_id
        and outcome.contract_id is not distinct from v_contract_id
        and decision.action_kind='APPLY_AMENDMENT' and outcome.action_kind='APPLY_AMENDMENT'
    ) operation_candidates
    where operation_id is not null;
    v_operation_count:=cardinality(v_operation_ids);
    if v_operation_count>p_max_operations_per_source then
      raise exception 'IMPORT_REVIEW_OPERATION_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    -- A completed reconciliation operation is durable identity evidence, not
    -- economic evidence.  Validate every request/applied/policy triple before
    -- any invoice-line scope is built so physically deleted members remain
    -- discoverable without allowing an operation to contribute money twice.
    with matching_requests as (
      select op.id operation_id,op.state::text operation_state,op.committed_at_utc,op.finalised_at_utc,
        op.response_json,request_unit
      from public.import_apply_operations op
      cross join lateral jsonb_array_elements(coalesce(op.response_json#>'{request_envelope,reconciliation_units}','[]'::jsonb)) request_unit
      where op.id=any(v_operation_ids)
        and (request_unit->>'source_identity'=v_source_identity
          or request_unit->>'source_shift_id'=v_source_shift_id::text)
    ), triples as (
      select mr.*,
        (select count(*) from jsonb_array_elements(coalesce(mr.response_json#>'{request_envelope,reconciliation_units}','[]'::jsonb)) candidate
          where candidate->>'action_id'=mr.request_unit->>'action_id'
            and (candidate->>'source_identity'=v_source_identity or candidate->>'source_shift_id'=v_source_shift_id::text)) request_count,
        applied_match.applied_unit,applied_match.applied_count,
        policy_match.policy_unit,policy_match.policy_count,
        outcome_match.action_outcome,outcome_match.outcome_count,
        mr.response_json->'correction_operation_contract' operation_contract
      from matching_requests mr
      left join lateral (
        select min(applied::text)::jsonb applied_unit,count(*)::integer applied_count
        from jsonb_array_elements(coalesce(mr.response_json->'reconciliation_units','[]'::jsonb)) applied
        where applied->>'action_id'=mr.request_unit->>'action_id'
      ) applied_match on true
      left join lateral (
        select min(policy::text)::jsonb policy_unit,count(*)::integer policy_count
        from jsonb_array_elements(coalesce(mr.response_json#>'{correction_operation_contract,correction_units}','[]'::jsonb)) policy
        where policy->>'action_id'=mr.request_unit->>'action_id'
      ) policy_match on true
      left join lateral (
        select min(to_jsonb(outcome)::text)::jsonb action_outcome,count(*)::integer outcome_count
        from public.import_review_action_outcomes outcome
        where outcome.operation_id=mr.operation_id
          and outcome.action_id=mr.request_unit->>'action_id'
      ) outcome_match on true
    ), evaluated as (
      select t.*,
        case when t.operation_state='COMPLETE'
          and t.committed_at_utc is not null and t.finalised_at_utc is not null
          and t.request_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
          and t.request_count=1 and t.applied_count=1 and t.policy_count=1 and t.outcome_count=1
          and t.request_unit->>'action_id'=t.applied_unit->>'action_id'
          and t.request_unit->>'action_id'=t.policy_unit->>'action_id'
          and t.request_unit->>'source_identity'=v_source_identity
          and t.applied_unit->>'source_identity'=v_source_identity
          and t.policy_unit->>'source_row_key'=v_source_identity
          and t.request_unit->>'source_system'=v_source_system
          and t.applied_unit->>'source_system'=v_source_system
          and t.request_unit->>'source_shift_id'=v_source_shift_id::text
          and t.applied_unit->>'source_shift_id'=v_source_shift_id::text
          and t.policy_unit->>'source_shift_id'=v_source_shift_id::text
          and t.request_unit->>'source_timesheet_id'=v_source_timesheet_id::text
          and t.policy_unit->>'root_timesheet_id'=v_source_timesheet_id::text
          and t.request_unit->>'candidate_id'=v_candidate_id::text
          and t.request_unit->>'client_id'=v_client_id::text
          and t.request_unit->>'contract_id'=v_contract_id::text
          and t.request_unit->>'week_ending_date'=v_week_ending_date::text
          and t.request_unit->>'invoice_stream'=v_invoice_stream
          and t.request_unit->>'source_scope_fingerprint'=v_scope_fingerprint
          and t.action_outcome->>'action_kind'='APPLY_AMENDMENT'
          and t.action_outcome->>'source_identity'=v_source_identity
          and t.action_outcome->>'shift_id'=v_source_shift_id::text
          and t.action_outcome->>'candidate_id'=v_candidate_id::text
          and t.action_outcome->>'client_id'=v_client_id::text
          and t.action_outcome->>'contract_id'=v_contract_id::text
          and nullif(t.action_outcome->>'evidence_fingerprint','') is not null
          and nullif(t.request_unit->>'unit_fingerprint','') is not null
          and t.request_unit->>'unit_fingerprint'=encode(digest(convert_to(concat_ws('|','unit-v2',
            t.request_unit->>'action_id',t.request_unit->>'source_identity',t.request_unit->>'source_shift_id',
            t.request_unit->>'route',t.request_unit->>'reconciliation_mode',
            t.request_unit->>'reconciliation_fingerprint',t.request_unit->>'review_policy_basis_kind',
            t.request_unit->>'review_policy_basis_fingerprint',t.action_outcome->>'evidence_fingerprint'),'UTF8'),'sha256'),'hex')
          and t.applied_unit->>'reviewed_unit_fingerprint'=t.request_unit->>'unit_fingerprint'
          and t.applied_unit->>'reconciliation_fingerprint'=t.request_unit->>'reconciliation_fingerprint'
          and case
            when t.request_unit->>'route'='CREATE_REVERSAL_REPLACEMENT' then
              coalesce(t.request_unit->>'repair_identity_mode','') in ('','CREATE_NEW_GENERATION')
              and t.applied_unit->>'repair_identity_mode'='CREATE_NEW_GENERATION'
              and nullif(t.request_unit->>'reviewed_existing_correction_id','') is null
            when t.request_unit->>'route'='AMEND_EXISTING_REPLACEMENT' then
              t.request_unit->>'repair_identity_mode' in ('RETAIN_EXISTING_CORRECTION_ID','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED')
              and t.applied_unit->>'repair_identity_mode'=t.request_unit->>'repair_identity_mode'
              and nullif(t.request_unit->>'reviewed_existing_correction_id','') is not null
              and case
                when t.request_unit->>'repair_identity_mode'='RETAIN_EXISTING_CORRECTION_ID' then
                  t.applied_unit->>'correction_id'=t.request_unit->>'reviewed_existing_correction_id'
                else
                  t.request_unit->>'reviewed_existing_correction_id'<>t.applied_unit->>'correction_id'
                  and jsonb_typeof(t.request_unit->'reviewed_existing_member_ids')='array'
                  and jsonb_array_length(t.request_unit->'reviewed_existing_member_ids') between 1 and 2
                  and exists(select 1 from jsonb_array_elements_text(t.request_unit->'reviewed_existing_member_ids') reviewed(member_id)
                    where reviewed.member_id in (t.applied_unit->>'reversal_timesheet_id',t.applied_unit->>'replacement_timesheet_id'))
              end
            else false
          end
          and nullif(t.applied_unit->>'correction_id','') is not null
          and coalesce(t.applied_unit->>'reversal_timesheet_id','')~*v_uuid_re
          and coalesce(t.applied_unit->>'replacement_timesheet_id','')~*v_uuid_re
          and coalesce(t.applied_unit->>'parent_timesheet_id','')~*v_uuid_re
          and t.applied_unit->>'reversal_timesheet_id'<>t.applied_unit->>'replacement_timesheet_id'
          and jsonb_typeof(t.applied_unit->'applied_member_ids')='array'
          and jsonb_array_length(t.applied_unit->'applied_member_ids')=2
          and not exists(select 1 from jsonb_array_elements_text(t.applied_unit->'applied_member_ids') member(value)
            where member.value!~*v_uuid_re)
          and t.applied_unit->'applied_member_ids' @> jsonb_build_array(t.applied_unit->>'reversal_timesheet_id')
          and t.applied_unit->'applied_member_ids' @> jsonb_build_array(t.applied_unit->>'replacement_timesheet_id')
          and jsonb_typeof(t.policy_unit->'policy_envelope')='object'
          and nullif(t.policy_unit->>'policy_envelope_fingerprint','') is not null
          and t.policy_unit#>>'{policy_envelope,envelope_fingerprint}'=t.policy_unit->>'policy_envelope_fingerprint'
          and encode(digest(convert_to(((t.policy_unit->'policy_envelope')-'envelope_fingerprint'::text)::text,'UTF8'),'sha256'),'hex')
            =t.policy_unit->>'policy_envelope_fingerprint'
          and jsonb_typeof(t.operation_contract)='object'
          and nullif(t.operation_contract->>'operation_contract_fingerprint','') is not null
          and encode(digest(convert_to((t.operation_contract-'operation_contract_fingerprint'::text)::text,'UTF8'),'sha256'),'hex')
            =t.operation_contract->>'operation_contract_fingerprint'
          and case when coalesce(t.applied_unit->>'reversal_timesheet_id','')~*v_uuid_re
              and coalesce(t.applied_unit->>'replacement_timesheet_id','')~*v_uuid_re
              and coalesce(t.applied_unit->>'parent_timesheet_id','')~*v_uuid_re
            then t.applied_unit->>'applied_result_fingerprint'=encode(digest(convert_to(jsonb_build_object(
              'correction_id',t.applied_unit->>'correction_id',
              'reversal_timesheet_id',(t.applied_unit->>'reversal_timesheet_id')::uuid,
              'replacement_timesheet_id',(t.applied_unit->>'replacement_timesheet_id')::uuid,
              'M_active_member_ids',t.applied_unit->'applied_member_ids',
              'applied_member_ids',t.applied_unit->'applied_member_ids',
              'parent_timesheet_id',(t.applied_unit->>'parent_timesheet_id')::uuid,
              'repair_identity_mode',t.applied_unit->>'repair_identity_mode',
              'reviewed_unit_fingerprint',t.applied_unit->>'reviewed_unit_fingerprint',
              'reconciliation_fingerprint',t.applied_unit->>'reconciliation_fingerprint')::text,'UTF8'),'sha256'),'hex')
            else false end
          then true else false end valid_historical_authority
      from triples t
    )
    select
      coalesce(jsonb_agg(jsonb_build_object(
        'operation_id',e.operation_id,'action_id',e.request_unit->>'action_id',
        'evidence_at',e.finalised_at_utc,'source_identity',e.request_unit->>'source_identity',
        'source_shift_id',e.request_unit->>'source_shift_id','source_timesheet_id',e.request_unit->>'source_timesheet_id',
        'correction_id',e.applied_unit->>'correction_id',
        'reversal_timesheet_id',e.applied_unit->>'reversal_timesheet_id',
        'replacement_timesheet_id',e.applied_unit->>'replacement_timesheet_id',
        'applied_member_ids',e.applied_unit->'applied_member_ids',
        'parent_timesheet_id',e.applied_unit->>'parent_timesheet_id',
        'route',e.request_unit->>'route',
        'invoice_stream',e.request_unit->>'invoice_stream',
        'source_scope_fingerprint',e.request_unit->>'source_scope_fingerprint',
        'reviewed_existing_correction_id',e.request_unit->>'reviewed_existing_correction_id',
        'reviewed_existing_member_ids',coalesce(e.request_unit->'reviewed_existing_member_ids','[]'::jsonb),
        'request_repair_identity_mode',e.request_unit->>'repair_identity_mode',
        'repair_identity_mode',e.applied_unit->>'repair_identity_mode',
        'reviewed_unit_fingerprint',e.request_unit->>'unit_fingerprint',
        'action_evidence_fingerprint',e.action_outcome->>'evidence_fingerprint',
        'reconciliation_fingerprint',e.request_unit->>'reconciliation_fingerprint',
        'B_standard_schedule_json',coalesce(e.request_unit->'B_standard_schedule_json','[]'::jsonb),
        'B_hours',coalesce(e.request_unit->'B_hours','{}'::jsonb),
        'A_schedule_json',coalesce(e.request_unit->'A_schedule_json','[]'::jsonb),
        'A_hours',coalesce(e.request_unit->'A_hours','{}'::jsonb),
        'policy_envelope',e.policy_unit->'policy_envelope',
        'policy_envelope_fingerprint',e.policy_unit->>'policy_envelope_fingerprint'
      ) order by e.finalised_at_utc,e.operation_id,e.request_unit->>'action_id')
        filter(where e.valid_historical_authority),'[]'::jsonb),
      coalesce(bool_or(e.operation_state='COMPLETE'
        and e.request_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
        and not e.valid_historical_authority),false),
      coalesce(bool_or(e.operation_state in ('SOURCE_COMMITTED_TSFIN_PENDING','FINANCIALISED_PENDING_FINALISATION')
        and e.committed_at_utc is not null
        and e.operation_id is distinct from v_transition_operation_id),false)
    into v_operation_evidence,v_operation_evidence_conflict,v_operation_in_progress
    from evaluated e;

    -- A surviving member can be re-keyed more than once as successive archived
    -- siblings occupy prior correction identities.  Canonicalise the complete
    -- bounded chain (C1 -> C2 -> C3), retain every earlier assignment as audit
    -- history, and fail closed on cycles or branches.
    with recursive direct_edges as (
      select role.member_id::uuid member_timesheet_id,role.correction_kind,
        unit->>'reviewed_existing_correction_id' superseded_correction_id,
        unit->>'correction_id' canonical_correction_id,
        array_agg(distinct (unit->>'operation_id')::uuid order by (unit->>'operation_id')::uuid) operation_ids
      from jsonb_array_elements(v_operation_evidence) unit
      cross join lateral (values
        ('CHANGED_HOURS_REVERSAL'::text,unit->>'reversal_timesheet_id'),
        ('CHANGED_HOURS_REPLACEMENT'::text,unit->>'replacement_timesheet_id')
      ) role(correction_kind,member_id)
      where unit->>'route'='AMEND_EXISTING_REPLACEMENT'
        and unit->>'repair_identity_mode'='FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED'
        and nullif(unit->>'reviewed_existing_correction_id','') is not null
        and unit->>'reviewed_existing_correction_id'<>unit->>'correction_id'
        and role.member_id~*v_uuid_re
        and coalesce(unit->'reviewed_existing_member_ids','[]'::jsonb) @> jsonb_build_array(role.member_id)
        and not exists(
          select 1
          from public.invoice_lines historical_line
          join public.invoices historical_invoice on historical_invoice.id=historical_line.invoice_id
          where (historical_line.timesheet_id=role.member_id::uuid
              or historical_line.meta_json->>'timesheet_id'=role.member_id)
            and historical_line.created_at<=coalesce((unit->>'evidence_at')::timestamptz,'infinity'::timestamptz)
            and upper(coalesce(historical_line.meta_json->>'line_type',''))
              !~ '^(EXPENSE(_.*)?|MILEAGE|TRAVEL|ACCOMMODATION|REIMBURSEMENT|ADDITION)$'
            and (
              historical_invoice.status::text='DRAFT'
              or historical_invoice.issued_at_utc is null
              or historical_invoice.active_document_operation_id is not null
              or historical_invoice.active_issue_operation_id is not null
              or upper(coalesce(historical_invoice.issue_state,'')) not in ('','IDLE','COMPLETE','COMPLETED','ISSUED')
              or (historical_invoice.status::text in ('ISSUED','PAID','ON_HOLD')
                and historical_invoice.issued_at_utc is not null)
            ))
      group by role.member_id,role.correction_kind,
        unit->>'reviewed_existing_correction_id',unit->>'correction_id'
    ), walk as (
      select edge.member_timesheet_id,edge.correction_kind,
        edge.superseded_correction_id root_correction_id,
        edge.canonical_correction_id reached_correction_id,
        array[edge.superseded_correction_id,edge.canonical_correction_id]::text[] path,
        edge.operation_ids,1 depth,
        edge.canonical_correction_id=edge.superseded_correction_id cycle
      from direct_edges edge
      union all
      select walk.member_timesheet_id,walk.correction_kind,walk.root_correction_id,
        edge.canonical_correction_id,walk.path||edge.canonical_correction_id,
        walk.operation_ids||edge.operation_ids,walk.depth+1,
        edge.canonical_correction_id=any(walk.path)
      from walk
      join direct_edges edge
        on edge.member_timesheet_id=walk.member_timesheet_id
       and edge.correction_kind=walk.correction_kind
       and edge.superseded_correction_id=walk.reached_correction_id
      where not walk.cycle and walk.depth<p_max_operations_per_source
    ), closure as (
      select distinct on (member_timesheet_id,correction_kind,root_correction_id,reached_correction_id)
        member_timesheet_id,correction_kind,root_correction_id,reached_correction_id,
        operation_ids,depth,path
      from walk
      where not cycle and root_correction_id<>reached_correction_id
      order by member_timesheet_id,correction_kind,root_correction_id,reached_correction_id,depth desc,operation_ids::text
    ), conflicts as (
      select exists(
        select 1 from direct_edges edge
        group by edge.member_timesheet_id,edge.correction_kind,edge.superseded_correction_id
        having count(distinct edge.canonical_correction_id)<>1
      )
      or exists(select 1 from walk where cycle)
      or exists(
        select 1 from walk
        where depth=p_max_operations_per_source
          and exists(select 1 from direct_edges edge
            where edge.member_timesheet_id=walk.member_timesheet_id
              and edge.correction_kind=walk.correction_kind
              and edge.superseded_correction_id=walk.reached_correction_id)
      )
      or exists(
        select 1
        from walk terminal
        where not terminal.cycle and not exists(select 1 from direct_edges edge
          where edge.member_timesheet_id=terminal.member_timesheet_id
            and edge.correction_kind=terminal.correction_kind
            and edge.superseded_correction_id=terminal.reached_correction_id)
        group by terminal.member_timesheet_id,terminal.correction_kind,terminal.root_correction_id
        having count(distinct terminal.reached_correction_id)<>1
      ) as has_conflict
    )
    select coalesce((select jsonb_agg(jsonb_build_object(
        'operation_id',entry.operation_ids[cardinality(entry.operation_ids)],
        'operation_ids',to_jsonb(entry.operation_ids),
        'member_timesheet_id',entry.member_timesheet_id,
        'correction_kind',entry.correction_kind,
        'superseded_correction_id',entry.root_correction_id,
        'canonical_correction_id',entry.reached_correction_id,
        'supersession_depth',entry.depth,
        'supersession_path',to_jsonb(entry.path)
      ) order by entry.member_timesheet_id,entry.correction_kind,
        entry.root_correction_id,entry.depth,entry.reached_correction_id)
      from closure entry),'[]'::jsonb),
      coalesce((select has_conflict from conflicts),false)
    into v_member_supersession_map,v_member_supersession_conflict;
    v_operation_evidence_conflict:=v_operation_evidence_conflict or v_member_supersession_conflict;

    select coalesce(array_agg(distinct member_id order by member_id),array[]::uuid[])
    into v_operation_member_ids
    from (
      select (unit->>'reversal_timesheet_id')::uuid member_id
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select (unit->>'replacement_timesheet_id')::uuid
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select member.value::uuid
      from jsonb_array_elements(v_operation_evidence) unit
      cross join lateral jsonb_array_elements_text(unit->'applied_member_ids') member(value)
    ) ids;

    select coalesce(array_agg(distinct timesheet_id order by timesheet_id),array[]::uuid[])
    into v_hist_ids
    from (
      select v_source_timesheet_id timesheet_id
      union all select s.timesheet_id from public.nhsp_shifts s where s.id=v_source_shift_id
      union all select unnest(v_audit_ids)
      union all select unnest(v_operation_member_ids)
      union all
      select t.timesheet_id
      from public.timesheets t
      join public.timesheets_financials tf_scope on tf_scope.timesheet_id=t.timesheet_id
      where tf_scope.candidate_id=v_candidate_id and t.contract_id=v_contract_id and t.week_ending_date=v_week_ending_date
        and (
          (jsonb_typeof(t.actual_schedule_json)='array' and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key)))
          or t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_shift_id}'=v_source_shift_id::text
          or t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_row_key}'=v_external_row_key
        )
    ) ids where timesheet_id is not null;
    select coalesce(array_agg(t.timesheet_id order by t.timesheet_id),array[]::uuid[])
    into v_archived_ids from public.timesheets t where t.timesheet_id=any(v_hist_ids) and t.archived_at_utc is not null;
    select coalesce(array_agg(t.timesheet_id order by t.timesheet_id),array[]::uuid[])
    into v_active_ids from public.timesheets t where t.timesheet_id=any(v_hist_ids) and t.is_current and t.archived_at_utc is null;
    select coalesce(array_agg(x order by x),array[]::uuid[]) into v_missing_ids
    from (select distinct unnest(v_audit_ids||v_operation_member_ids) x) missing
    where not exists(select 1 from public.timesheets t where t.timesheet_id=missing.x);

    -- Canonical source/role ownership is established once and then reused by
    -- both invoice balance and generation classification.  Higher-authority
    -- evidence may fill a missing identity but contradictory identities fail
    -- closed instead of being resolved by arbitrary precedence.
    with evidence as (
      select (unit->>'reversal_timesheet_id')::uuid timesheet_id,unit->>'correction_id' correction_id,
        'CHANGED_HOURS_REVERSAL'::text correction_kind,'COMPLETED_OPERATION'::text evidence_source,
        1 priority,true operation_proven
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select (unit->>'replacement_timesheet_id')::uuid,unit->>'correction_id',
        'CHANGED_HOURS_REPLACEMENT','COMPLETED_OPERATION',1,true
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select raw.member_id,ae.after_json->>'correction_id','CHANGED_HOURS_REVERSAL','CORRECTION_AUDIT',2,false
      from public.audit_events ae
      cross join lateral (select case when ae.after_json->>'reversal_timesheet_id'~*v_uuid_re
        then (ae.after_json->>'reversal_timesheet_id')::uuid end member_id) raw
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text or ae.after_json->>'external_row_key'=v_external_row_key)
        and nullif(ae.after_json->>'correction_id','') is not null and raw.member_id is not null
      union all
      select raw.member_id,ae.after_json->>'correction_id','CHANGED_HOURS_REPLACEMENT','CORRECTION_AUDIT',2,false
      from public.audit_events ae
      cross join lateral (select case when ae.after_json->>'replacement_timesheet_id'~*v_uuid_re
        then (ae.after_json->>'replacement_timesheet_id')::uuid end member_id) raw
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text or ae.after_json->>'external_row_key'=v_external_row_key)
        and nullif(ae.after_json->>'correction_id','') is not null and raw.member_id is not null
      union all
      select raw.member_id,ae.after_json->>'correction_id',ae.after_json->>'correction_kind','CORRECTION_AUDIT',2,false
      from public.audit_events ae
      cross join lateral (select case when ae.after_json->>'timesheet_id'~*v_uuid_re
        then (ae.after_json->>'timesheet_id')::uuid end member_id) raw
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text or ae.after_json->>'external_row_key'=v_external_row_key)
        and ae.after_json->>'correction_kind' in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
        and nullif(ae.after_json->>'correction_id','') is not null and raw.member_id is not null
      union all
      select t.timesheet_id,t.correction_id,t.correction_kind::text,'LIVE_ROW',4,false
      from public.timesheets t
      where t.timesheet_id=any(v_hist_ids) and nullif(t.correction_id,'') is not null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
    ), conflicts as (
      select distinct left_evidence.timesheet_id
      from evidence left_evidence
      join evidence right_evidence on right_evidence.timesheet_id=left_evidence.timesheet_id
        and (right_evidence.correction_id,right_evidence.correction_kind)
          is distinct from (left_evidence.correction_id,left_evidence.correction_kind)
      where left_evidence.correction_kind<>right_evidence.correction_kind
        or (left_evidence.correction_id<>right_evidence.correction_id
          and not exists(
            select 1 from jsonb_array_elements(v_member_supersession_map) edge
            where edge->>'member_timesheet_id'=left_evidence.timesheet_id::text
              and edge->>'correction_kind'=left_evidence.correction_kind
              and ((edge->>'superseded_correction_id'=left_evidence.correction_id
                    and edge->>'canonical_correction_id'=right_evidence.correction_id)
                or (edge->>'superseded_correction_id'=right_evidence.correction_id
                    and edge->>'canonical_correction_id'=left_evidence.correction_id))))
    ), canonical as (
      select distinct on (e.timesheet_id) e.*
      from evidence e
      where not exists(select 1 from conflicts c where c.timesheet_id=e.timesheet_id)
        and not exists(
          select 1 from jsonb_array_elements(v_member_supersession_map) edge
          where edge->>'member_timesheet_id'=e.timesheet_id::text
            and edge->>'correction_kind'=e.correction_kind
            and edge->>'superseded_correction_id'=e.correction_id
            and edge->>'canonical_correction_id'<>e.correction_id)
      order by e.timesheet_id,e.priority,e.correction_id,e.correction_kind
    )
    select
      coalesce((select jsonb_agg(jsonb_build_object(
        'timesheet_id',c.timesheet_id,'correction_id',c.correction_id,'correction_kind',c.correction_kind,
        'evidence_source',c.evidence_source,'operation_proven',c.operation_proven,
        'source_system',v_source_system,'source_identity',v_source_identity,'source_shift_id',v_source_shift_id
      ) order by c.timesheet_id) from canonical c),'[]'::jsonb),
      exists(select 1 from conflicts)
    into v_member_role_map,v_member_role_conflict;

    v_effective_invoice_ids:=array[]::uuid[];
    v_effective_line_ids:=array[]::uuid[];
    v_issued_invoice_ids:=array[]::uuid[];
    v_issued_line_ids:=array[]::uuid[];
    v_pending_invoice_ids:=array[]::uuid[];
    v_pending_line_ids:=array[]::uuid[];
    v_credit_line_ids:=array[]::uuid[];
    v_effective_component_count:=0;
    v_issued_component_count:=0;
    v_pending_component_count:=0;
    v_b_day:=0; v_b_night:=0; v_b_sat:=0; v_b_sun:=0; v_b_bh:=0;
    v_b_pay:=0; v_b_charge:=0; v_b_margin:=0;
    v_issued_day:=0; v_issued_night:=0; v_issued_sat:=0; v_issued_sun:=0; v_issued_bh:=0;
    v_issued_pay:=0; v_issued_charge:=0; v_issued_margin:=0;
    v_pending_day:=0; v_pending_night:=0; v_pending_sat:=0; v_pending_sun:=0; v_pending_bh:=0;
    v_pending_pay:=0; v_pending_charge:=0; v_pending_margin:=0;
    v_b_schedule:='[]'::jsonb; v_candidate_schedule:='[]'::jsonb; v_candidate_hours:='{}'::jsonb;
    v_schedule_candidates:='[]'::jsonb; v_candidate_policy_fingerprint:=null; v_b_policy_fingerprint:=null;
    v_terminal_generation_id:=null; v_terminal_positive_timesheet_id:=null; v_terminal_positive_member_count:=0;
    v_terminal_frozen_candidate:=null; v_terminal_frozen_candidate_count:=0;
    v_terminal_frozen_schedule_variant_count:=0; v_terminal_frozen_policy_variant_count:=0;
    v_terminal_operation:=null; v_terminal_operation_schedule:='[]'::jsonb; v_terminal_operation_hours:='{}'::jsonb;
    v_terminal_operation_policy_fingerprint:=null; v_terminal_operation_id:=null;
    v_terminal_policy_snapshot:='{}'::jsonb; v_terminal_policy_snapshot_fingerprint:=null;
    v_terminal_derived_day:=0; v_terminal_derived_night:=0; v_terminal_derived_sat:=0;
    v_terminal_derived_sun:=0; v_terminal_derived_bh:=0; v_terminal_derived_total:=0;
    v_terminal_frozen_matches_b:=false; v_terminal_operation_matches_b:=false;
    v_terminal_schedule_authority_conflict:=false; v_terminal_policy_authority_conflict:=false;
    v_b_standard_schedule_authority:='NONE'; v_b_standard_schedule_authority_timesheet_id:=null;
    v_b_standard_schedule_authority_correction_id:=null; v_b_standard_schedule_authority_operation_id:=null;
    v_b_standard_schedule_authority_policy_fingerprint:=null; v_b_standard_schedule_authority_fingerprint:=null;
    v_b_standard_schedule_authority_diagnostic:=null;
    v_line_evidence:='[]'::jsonb;
    v_ignored_nonhours_line_ids:=array[]::uuid[];
    v_generation_role_evidence:='[]'::jsonb;
    v_fully_invoiced_generation_ids:=array[]::text[];
    v_partial_generation_ids:=array[]::text[];
    v_mutable_generation_ids:=array[]::text[];
    v_archived_history_roles:='[]'::jsonb;
    v_role_evidence_conflicts:='[]'::jsonb;
    v_repair_identity_mode:=null;
    v_reversal_repair_required:=false;
    v_replacement_repair_required:=false;
    v_role_partial_invoice_state:=false;
    v_role_active_invoice_activity:=false;
    v_role_scope_unprovable:=false;
    v_scope_unprovable:=false; v_credit_ambiguous:=false; v_stream_conflict:=false;
    v_archived_invoice_conflict:=false; v_active_invoice_activity:=false;
    v_financial_position_requires_amendment:=false;

    with directly_scoped as (
      select il.id
      from public.invoice_lines il
      where il.timesheet_id=any(v_hist_ids)
        or case when coalesce(il.meta_json->>'timesheet_id','')~*v_uuid_re
          then (il.meta_json->>'timesheet_id')::uuid=any(v_hist_ids) else false end
    ), scoped as (
      select il.id
      from public.invoice_lines il where il.id in(select id from directly_scoped)
      union
      select credit.id
      from public.invoice_lines credit
      where coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id','')~*v_uuid_re
        and coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id')::uuid in(select id from directly_scoped)
    )
    select count(*)::integer into v_line_count from scoped;
    if v_line_count>p_max_invoice_lines_per_source then
      raise exception 'IMPORT_REVIEW_INVOICE_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    for v_line in
      with directly_scoped as (
        select il.id
        from public.invoice_lines il
        where il.timesheet_id=any(v_hist_ids)
          or case when coalesce(il.meta_json->>'timesheet_id','')~*v_uuid_re
            then (il.meta_json->>'timesheet_id')::uuid=any(v_hist_ids) else false end
      ), scoped as (
        select il.id from public.invoice_lines il where il.id in(select id from directly_scoped)
        union
        select credit.id from public.invoice_lines credit
        where coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id','')~*v_uuid_re
          and coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id')::uuid in(select id from directly_scoped)
      )
      select il.*,i.type::text invoice_type,i.status::text invoice_status,i.issued_at_utc,
        i.client_id invoice_client_id,i.original_invoice_id,
        i.active_document_operation_id,i.active_issue_operation_id,i.issue_state
      from scoped s join public.invoice_lines il on il.id=s.id join public.invoices i on i.id=il.invoice_id
      order by i.issued_at_utc nulls last,il.id
    loop
      -- Archived rows are audit-only.  They cannot contribute to the current
      -- source balance or make an otherwise repairable generation block.
      if v_line.timesheet_id=any(v_archived_ids)
         or (coalesce(v_line.meta_json->>'timesheet_id','')~*v_uuid_re
           and (v_line.meta_json->>'timesheet_id')::uuid=any(v_archived_ids)) then
        continue;
      end if;

      v_tf:=null; v_seg:=null; v_seg_count:=0; v_matching_seg_count:=0;
      if coalesce(v_line.meta_json->>'tsfin_id','')~*v_uuid_re then
        select tf.* into v_tf from public.timesheets_financials tf where tf.id=(v_line.meta_json->>'tsfin_id')::uuid;
      elsif v_line.timesheet_id is not null then
        select tf.* into v_tf from public.timesheets_financials tf
        where tf.timesheet_id=v_line.timesheet_id
        order by case when tf.is_current then 0 else 1 end,tf.computed_at_utc desc nulls last,tf.id desc limit 1;
      end if;
      if v_tf.id is not null then
        select count(*)::integer,
          count(*) filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text or seg->>'external_row_key'=v_external_row_key)::integer,
          (array_agg(seg order by case when seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text then 0 else 1 end)
            filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text or seg->>'external_row_key'=v_external_row_key))[1]
        into v_seg_count,v_matching_seg_count,v_seg
        from jsonb_array_elements(case when jsonb_typeof(v_tf.invoice_breakdown_json->'segments')='array' then v_tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg;
      end if;

      v_original_line:=null;
      v_original_invoice:=null;
      v_original_tf:=null;
      v_original_seg:=null;
      v_original_seg_count:=0;
      v_original_matching_seg_count:=0;
      v_original_line_id:=null;
      if v_line.invoice_type='CREDIT_NOTE' then
        if coalesce(v_line.meta_json->>'original_invoice_line_id',v_line.meta_json->>'credit_of_line_id','')~*v_uuid_re then
          v_original_line_id:=coalesce(v_line.meta_json->>'original_invoice_line_id',v_line.meta_json->>'credit_of_line_id')::uuid;
          select original.* into v_original_line from public.invoice_lines original where original.id=v_original_line_id;
        end if;
        if v_original_line.id is null then
          v_scope_unprovable:=true;
          continue;
        end if;
        select original_invoice.* into v_original_invoice
        from public.invoices original_invoice
        where original_invoice.id=v_original_line.invoice_id;
        -- A credit is admissible only when the header, original physical line,
        -- client and exact source identity all prove one lineage.  The line's
        -- signed values are never trusted in isolation.
        if v_original_invoice.id is null
          or v_line.original_invoice_id is distinct from v_original_line.invoice_id
          or v_line.meta_json->>'original_invoice_line_id' is distinct from v_original_line.id::text
          or v_line.invoice_client_id is distinct from v_original_invoice.client_id
          or not (
            coalesce(v_line.timesheet_id::text,
              case when coalesce(v_line.meta_json->>'timesheet_id','')~*v_uuid_re
                then v_line.meta_json->>'timesheet_id' end)
              is not distinct from
            coalesce(v_original_line.timesheet_id::text,
              case when coalesce(v_original_line.meta_json->>'timesheet_id','')~*v_uuid_re
                then v_original_line.meta_json->>'timesheet_id' end)
            or (
              exists(select 1 from jsonb_array_elements(v_member_role_map) credit_member
                where credit_member->>'timesheet_id'=coalesce(v_line.timesheet_id::text,v_line.meta_json->>'timesheet_id')
                  and credit_member->>'source_identity'=v_source_identity)
              and exists(select 1 from jsonb_array_elements(v_member_role_map) original_member
                where original_member->>'timesheet_id'=coalesce(v_original_line.timesheet_id::text,v_original_line.meta_json->>'timesheet_id')
                  and original_member->>'source_identity'=v_source_identity)
            )
          ) then
          v_scope_unprovable:=true;
          continue;
        end if;
        if coalesce(v_original_line.meta_json->>'tsfin_id','')~*v_uuid_re then
          select tf.* into v_original_tf from public.timesheets_financials tf where tf.id=(v_original_line.meta_json->>'tsfin_id')::uuid;
        elsif v_original_line.timesheet_id is not null then
          select tf.* into v_original_tf from public.timesheets_financials tf
          where tf.timesheet_id=v_original_line.timesheet_id
          order by case when tf.is_current then 0 else 1 end,tf.computed_at_utc desc nulls last,tf.id desc limit 1;
        end if;
        if v_original_tf.id is not null then
          select count(*)::integer,
            count(*) filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text
              or seg->>'shift_id'=v_source_shift_id::text
              or seg->>'external_row_key'=v_external_row_key)::integer,
            (array_agg(seg order by seg::text) filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text
              or seg->>'shift_id'=v_source_shift_id::text
              or seg->>'external_row_key'=v_external_row_key))[1]
          into v_original_seg_count,v_original_matching_seg_count,v_original_seg
          from jsonb_array_elements(case when jsonb_typeof(v_original_tf.invoice_breakdown_json->'segments')='array'
            then v_original_tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg;
        end if;
        -- The credit writer preserves the original hour buckets and writes one
        -- exact signed monetary mirror.  Reject partial or contradictory credit
        -- shapes before allocating any source component.
        if coalesce(v_line.hours_day,0)<>coalesce(v_original_line.hours_day,0)
          or coalesce(v_line.hours_night,0)<>coalesce(v_original_line.hours_night,0)
          or coalesce(v_line.hours_sat,0)<>coalesce(v_original_line.hours_sat,0)
          or coalesce(v_line.hours_sun,0)<>coalesce(v_original_line.hours_sun,0)
          or coalesce(v_line.hours_bh,0)<>coalesce(v_original_line.hours_bh,0)
          or round(coalesce(v_line.total_pay_ex_vat,0),2)<>-round(coalesce(v_original_line.total_pay_ex_vat,0),2)
          or round(coalesce(v_line.total_charge_ex_vat,0),2)<>-round(coalesce(v_original_line.total_charge_ex_vat,0),2)
          or round(coalesce(v_line.margin_ex_vat,v_line.total_charge_ex_vat-v_line.total_pay_ex_vat,0),2)
            <>-round(coalesce(v_original_line.margin_ex_vat,v_original_line.total_charge_ex_vat-v_original_line.total_pay_ex_vat,0),2)
          or (v_original_seg_count>0 and v_original_matching_seg_count<>1)
          or (v_original_seg_count>1 and (v_original_seg is null
            or nullif(v_original_seg->>'pay_amount','') is null
            or nullif(v_original_seg->>'charge_amount','') is null)) then
          v_scope_unprovable:=true;
          continue;
        end if;
      end if;

      v_line_type:=upper(nullif(btrim(coalesce(v_line.meta_json->>'line_type','')),''));
      v_original_line_type:=upper(nullif(btrim(coalesce(v_original_line.meta_json->>'line_type','')),''));
      v_is_separable_nonhours:=coalesce(case when v_line.invoice_type='CREDIT_NOTE' then v_original_line_type else v_line_type end,'')
        ~ '^(EXPENSE(_.*)?|MILEAGE|TRAVEL|ACCOMMODATION|REIMBURSEMENT|ADDITION)$';
      if v_is_separable_nonhours then
        v_ignored_nonhours_line_ids:=array_append(v_ignored_nonhours_line_ids,v_line.id);
        continue;
      end if;
      v_is_weekly_hours:=coalesce(case when v_line.invoice_type='CREDIT_NOTE' then v_original_line_type else v_line_type end,'')='HOURS_WEEKLY';
      if not v_is_weekly_hours then
        -- Legacy lines are acceptable only when a single frozen source segment
        -- proves the exact Weekly component for this shift.
        v_is_weekly_hours:=case when v_line.invoice_type='CREDIT_NOTE'
          then v_original_seg is not null
          else v_matching_seg_count=1 end;
      end if;
      if not v_is_weekly_hours then
        v_scope_unprovable:=true;
        continue;
      end if;

      v_component_timesheet_id:=case
        when v_line.invoice_type='CREDIT_NOTE' then coalesce(
          v_original_line.timesheet_id,
          case when coalesce(v_original_line.meta_json->>'timesheet_id','')~*v_uuid_re
            then (v_original_line.meta_json->>'timesheet_id')::uuid end)
        else coalesce(v_line.timesheet_id,
          case when coalesce(v_line.meta_json->>'timesheet_id','')~*v_uuid_re
            then (v_line.meta_json->>'timesheet_id')::uuid end)
        end;
      v_component_correction_id:=null;
      v_component_correction_kind:=null;
      v_operation_member_scope_proven:=false;
      select member->>'correction_id',member->>'correction_kind',
        coalesce((member->>'operation_proven')::boolean,false)
      into v_component_correction_id,v_component_correction_kind,v_operation_member_scope_proven
      from jsonb_array_elements(v_member_role_map) member
      where member->>'timesheet_id'=v_component_timesheet_id::text
      limit 1;

      if v_line.active_document_operation_id is not null or v_line.active_issue_operation_id is not null
         or upper(coalesce(v_line.issue_state,'')) not in ('','IDLE','NOT_STARTED','COMPLETE','COMPLETED','ISSUED') then
        v_active_invoice_activity:=true;
      end if;
      if v_line.invoice_type='CREDIT_NOTE'
         and v_line.invoice_status in ('ISSUED','PAID','ON_HOLD') and v_line.issued_at_utc is not null and (
        select count(*) from public.invoice_lines other_credit
        join public.invoices other_credit_invoice on other_credit_invoice.id=other_credit.invoice_id
        where coalesce(other_credit.meta_json->>'original_invoice_line_id',other_credit.meta_json->>'credit_of_line_id','')
          =coalesce(v_line.meta_json->>'original_invoice_line_id',v_line.meta_json->>'credit_of_line_id','')
          and other_credit_invoice.type='CREDIT_NOTE' and other_credit_invoice.status in ('ISSUED','PAID','ON_HOLD')
          and other_credit_invoice.issued_at_utc is not null
      )>1 then
        v_credit_ambiguous:=true;
      end if;
      v_single_source:=v_matching_seg_count=1 and v_seg_count=1;
      if not v_single_source and v_line.timesheet_id is not null then
        select jsonb_typeof(t.actual_schedule_json)='array' and jsonb_array_length(t.actual_schedule_json)=1
          and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key))
        into v_single_source from public.timesheets t where t.timesheet_id=v_line.timesheet_id;
        v_single_source:=coalesce(v_single_source,false);
      end if;
      v_line_scope_proven:=case when v_line.invoice_type='CREDIT_NOTE'
        then v_original_seg is not null
          or coalesce(v_original_line.timesheet_id=any(v_hist_ids),false)
          or (v_operation_member_scope_proven and v_original_line_type='HOURS_WEEKLY')
        else v_single_source or v_matching_seg_count=1
          or (v_operation_member_scope_proven and v_line_type='HOURS_WEEKLY') end;
      if not v_line_scope_proven or (v_line.invoice_type='CREDIT_NOTE' and v_original_seg is null
          and not coalesce(v_original_line.timesheet_id=any(v_hist_ids),false)
          and not (v_operation_member_scope_proven and v_original_line_type='HOURS_WEEKLY')) then
        v_scope_unprovable:=true;
        continue;
      end if;
      if (v_line.invoice_type<>'CREDIT_NOTE' and v_tf.id is not null
            and (case when upper(coalesce(v_tf.basis::text,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end)<>v_invoice_stream)
         or (v_line.invoice_type='CREDIT_NOTE' and v_original_tf.id is not null
            and (case when upper(coalesce(v_original_tf.basis::text,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end)<>v_invoice_stream) then
        v_stream_conflict:=true;
        continue;
      end if;

      v_component_economic_state:=case
        when v_line.invoice_status='DRAFT' or v_line.issued_at_utc is null then 'PENDING'
        else 'EFFECTIVE' end;

      if v_line.invoice_type='CREDIT_NOTE' then
        -- Hours are the negative of the exact original frozen component.  A
        -- multi-source credit receives the matching source segment's money,
        -- never the whole aggregate line's money.
        if v_original_seg is not null then
          v_component_day:=-coalesce((v_original_seg->>'hours_day')::numeric,0);
          v_component_night:=-coalesce((v_original_seg->>'hours_night')::numeric,0);
          v_component_sat:=-coalesce((v_original_seg->>'hours_sat')::numeric,0);
          v_component_sun:=-coalesce((v_original_seg->>'hours_sun')::numeric,0);
          v_component_bh:=-coalesce((v_original_seg->>'hours_bh')::numeric,0);
        else
          v_component_day:=-coalesce(v_original_line.hours_day,0);
          v_component_night:=-coalesce(v_original_line.hours_night,0);
          v_component_sat:=-coalesce(v_original_line.hours_sat,0);
          v_component_sun:=-coalesce(v_original_line.hours_sun,0);
          v_component_bh:=-coalesce(v_original_line.hours_bh,0);
        end if;
        if v_original_seg_count>1 then
          v_component_pay:=-coalesce((v_original_seg->>'pay_amount')::numeric,0);
          v_component_charge:=-coalesce((v_original_seg->>'charge_amount')::numeric,0);
          v_component_margin:=v_component_charge-v_component_pay;
        else
          v_component_pay:=coalesce(v_line.total_pay_ex_vat,0);
          v_component_charge:=coalesce(v_line.total_charge_ex_vat,0);
          v_component_margin:=coalesce(v_line.margin_ex_vat,v_component_charge-v_component_pay);
        end if;
      elsif v_single_source or v_operation_member_scope_proven then
        v_component_day:=coalesce(v_line.hours_day,0); v_component_night:=coalesce(v_line.hours_night,0);
        v_component_sat:=coalesce(v_line.hours_sat,0); v_component_sun:=coalesce(v_line.hours_sun,0); v_component_bh:=coalesce(v_line.hours_bh,0);
        v_component_pay:=coalesce(v_line.total_pay_ex_vat,0); v_component_charge:=coalesce(v_line.total_charge_ex_vat,0); v_component_margin:=coalesce(v_line.margin_ex_vat,v_component_charge-v_component_pay);
      else
        v_component_day:=coalesce((v_seg->>'hours_day')::numeric,0); v_component_night:=coalesce((v_seg->>'hours_night')::numeric,0);
        v_component_sat:=coalesce((v_seg->>'hours_sat')::numeric,0); v_component_sun:=coalesce((v_seg->>'hours_sun')::numeric,0); v_component_bh:=coalesce((v_seg->>'hours_bh')::numeric,0);
        v_component_pay:=coalesce((v_seg->>'pay_amount')::numeric,0); v_component_charge:=coalesce((v_seg->>'charge_amount')::numeric,0); v_component_margin:=v_component_charge-v_component_pay;
      end if;
      v_b_day:=v_b_day+v_component_day; v_b_night:=v_b_night+v_component_night; v_b_sat:=v_b_sat+v_component_sat; v_b_sun:=v_b_sun+v_component_sun; v_b_bh:=v_b_bh+v_component_bh;
      v_b_pay:=v_b_pay+v_component_pay; v_b_charge:=v_b_charge+v_component_charge; v_b_margin:=v_b_margin+v_component_margin;
      v_effective_component_count:=v_effective_component_count+1;
      v_effective_invoice_ids:=array_append(v_effective_invoice_ids,v_line.invoice_id);
      v_effective_line_ids:=array_append(v_effective_line_ids,v_line.id);
      if v_component_economic_state='PENDING' then
        v_pending_day:=v_pending_day+v_component_day; v_pending_night:=v_pending_night+v_component_night;
        v_pending_sat:=v_pending_sat+v_component_sat; v_pending_sun:=v_pending_sun+v_component_sun; v_pending_bh:=v_pending_bh+v_component_bh;
        v_pending_pay:=v_pending_pay+v_component_pay; v_pending_charge:=v_pending_charge+v_component_charge; v_pending_margin:=v_pending_margin+v_component_margin;
        v_pending_component_count:=v_pending_component_count+1;
        v_pending_invoice_ids:=array_append(v_pending_invoice_ids,v_line.invoice_id);
        v_pending_line_ids:=array_append(v_pending_line_ids,v_line.id);
      else
        v_issued_day:=v_issued_day+v_component_day; v_issued_night:=v_issued_night+v_component_night;
        v_issued_sat:=v_issued_sat+v_component_sat; v_issued_sun:=v_issued_sun+v_component_sun; v_issued_bh:=v_issued_bh+v_component_bh;
        v_issued_pay:=v_issued_pay+v_component_pay; v_issued_charge:=v_issued_charge+v_component_charge; v_issued_margin:=v_issued_margin+v_component_margin;
        v_issued_component_count:=v_issued_component_count+1;
        v_issued_invoice_ids:=array_append(v_issued_invoice_ids,v_line.invoice_id);
        v_issued_line_ids:=array_append(v_issued_line_ids,v_line.id);
      end if;
      if v_line.invoice_type='CREDIT_NOTE' then v_credit_line_ids:=array_append(v_credit_line_ids,v_line.id); end if;
      v_line_evidence:=v_line_evidence||jsonb_build_array(jsonb_build_object(
        'invoice_id',v_line.invoice_id,'invoice_line_id',v_line.id,'invoice_type',v_line.invoice_type,
        'economic_state',v_component_economic_state,'timesheet_id',v_component_timesheet_id,
        'correction_id',v_component_correction_id,'correction_kind',v_component_correction_kind,
        'hours',jsonb_build_object('hours_day',v_component_day,'hours_night',v_component_night,'hours_sat',v_component_sat,'hours_sun',v_component_sun,'hours_bh',v_component_bh),
        'pay_ex_vat',v_component_pay,'charge_ex_vat',v_component_charge,'margin_ex_vat',v_component_margin));
      if v_seg is not null and (v_component_day+v_component_night+v_component_sat+v_component_sun+v_component_bh)>0 then
        v_candidate_schedule:=jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
          'date',coalesce(v_seg->>'date',(v_a_schedule->0)->>'date'),
          'start_utc',v_seg->>'start_utc','end_utc',v_seg->>'end_utc',
          'break_mins',coalesce((v_seg->>'break_mins')::integer,0),
          'shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key,
          'import_id',coalesce(v_seg->>'import_id',v_authoritative_import_id::text),
          'ref_num',coalesce(v_seg->>'ref_num',v_seg->>'reference_number',(v_a_schedule->0)->>'ref_num')
        )));
        v_candidate_hours:=jsonb_build_object('hours_day',v_component_day,'hours_night',v_component_night,'hours_sat',v_component_sat,'hours_sun',v_component_sun,'hours_bh',v_component_bh,'total_hours',v_component_day+v_component_night+v_component_sat+v_component_sun+v_component_bh);
        v_candidate_policy_fingerprint:=case
          when v_component_correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT') then
            case when jsonb_typeof(v_tf.policy_snapshot_json->'correction_financials_policy_envelope')='object'
                and nullif(coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                  v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'),'') is not null
                and v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'
                  =coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                    v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
                and encode(digest(convert_to(((v_tf.policy_snapshot_json->'correction_financials_policy_envelope')
                    -'envelope_fingerprint'::text)::text,'UTF8'),'sha256'),'hex')
                  =coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                    v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
              then coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}') end
          else coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
            v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
            encode(digest(convert_to(coalesce(v_tf.policy_snapshot_json,'{}'::jsonb)::text,'UTF8'),'sha256'),'hex')) end;
        v_schedule_candidates:=v_schedule_candidates||jsonb_build_array(jsonb_build_object(
          'timesheet_id',v_component_timesheet_id,'correction_id',v_component_correction_id,
          'correction_kind',v_component_correction_kind,'invoice_line_id',v_line.id,
          'schedule_json',v_candidate_schedule,'hours',v_candidate_hours,
          'policy_fingerprint',v_candidate_policy_fingerprint));
      end if;
    end loop;

    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_effective_invoice_ids from unnest(v_effective_invoice_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_effective_line_ids from unnest(v_effective_line_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_issued_invoice_ids from unnest(v_issued_invoice_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_issued_line_ids from unnest(v_issued_line_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_pending_invoice_ids from unnest(v_pending_invoice_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_pending_line_ids from unnest(v_pending_line_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_credit_line_ids from unnest(v_credit_line_ids) x;
    v_effective_fingerprint:=encode(digest(convert_to(concat_ws('|','effective-invoice-v1',v_source_identity,v_line_evidence::text,v_b_day,v_b_night,v_b_sat,v_b_sun,v_b_bh,v_b_pay,v_b_charge,v_b_margin),'UTF8'),'sha256'),'hex');

    -- Classify each generation from the same admitted, signed Weekly-hours
    -- component ledger used for B.  Raw invoice-line existence is never a
    -- second financial authority, and archived identities are audit-only.
    with correction_seed as (
      select correction_id,max(evidence_at) evidence_at
      from (
        select member->>'correction_id' correction_id,
          coalesce((select max(coalesce(t.updated_at,t.created_at)) from public.timesheets t
            where t.timesheet_id=(member->>'timesheet_id')::uuid),'-infinity'::timestamptz) evidence_at
        from jsonb_array_elements(v_member_role_map) member
        union all
        select unit->>'correction_id',coalesce((unit->>'evidence_at')::timestamptz,'-infinity'::timestamptz)
        from jsonb_array_elements(v_operation_evidence) unit
      ) seeded where nullif(correction_id,'') is not null group by correction_id
    ), roles as (
      select seed.correction_id,seed.evidence_at,role
      from correction_seed seed
      cross join lateral unnest(array['CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT']) role
    ), role_state as (
      select r.correction_id,r.evidence_at,r.role,
        coalesce((select array_agg(distinct (member->>'timesheet_id')::uuid order by (member->>'timesheet_id')::uuid)
          from jsonb_array_elements(v_member_role_map) member
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role),array[]::uuid[]) member_ids,
        coalesce((select array_agg(distinct t.timesheet_id order by t.timesheet_id)
          from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.is_current and t.archived_at_utc is null),array[]::uuid[]) active_ids,
        coalesce((select array_agg(distinct t.timesheet_id order by t.timesheet_id)
          from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.archived_at_utc is not null),array[]::uuid[]) archived_ids,
        coalesce((select array_agg(distinct (member->>'timesheet_id')::uuid order by (member->>'timesheet_id')::uuid)
          from jsonb_array_elements(v_member_role_map) member
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and coalesce((member->>'operation_proven')::boolean,false)
            and not exists(select 1 from public.timesheets t where t.timesheet_id=(member->>'timesheet_id')::uuid)),array[]::uuid[]) operation_missing_ids,
        exists(select 1 from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING')) has_effective_history,
        exists(select 1 from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='PENDING')
          or exists(select 1
            from jsonb_array_elements(v_member_role_map) member
            join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
            left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
            left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
            where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
              and t.is_current and t.archived_at_utc is null
              and (tf.locked_by_invoice_id is not null or upper(coalesce(cw.status::text,''))='INVOICED'
                or exists(select 1 from jsonb_array_elements(case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
                  then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg
                  where nullif(seg->>'invoice_locked_invoice_id','') is not null))) pending_invoice,
        exists(select 1
          from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.is_current and t.archived_at_utc is null and tf.paid_at_utc is not null) paid,
        (select count(*) from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.is_current and t.archived_at_utc is null)>1 active_duplicate,
        (select count(distinct component->>'timesheet_id') from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING'))>1 economic_member_duplicate,
        coalesce((select sum(coalesce((component#>>'{hours,hours_day}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING')),0) net_day,
        coalesce((select sum(coalesce((component#>>'{hours,hours_night}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING')),0) net_night,
        coalesce((select sum(coalesce((component#>>'{hours,hours_sat}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING')),0) net_sat,
        coalesce((select sum(coalesce((component#>>'{hours,hours_sun}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING')),0) net_sun,
        coalesce((select sum(coalesce((component#>>'{hours,hours_bh}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING')),0) net_bh
      from roles r
    ), generation_state as (
      select correction_id,max(evidence_at) evidence_at,
        count(*) filter(where cardinality(member_ids)>0) proven_roles,
        count(*) filter(where has_effective_history) effective_roles,
        count(*) filter(where pending_invoice) pending_roles,
        count(*) filter(where cardinality(active_ids)>0) active_role_count,
        count(*) filter(where cardinality(operation_missing_ids)>0) missing_operation_role_count,
        count(*) filter(where cardinality(active_ids)=0 and cardinality(archived_ids)>0
          and cardinality(operation_missing_ids)=0) archived_only_role_count,
        bool_or(paid) paid,bool_or(active_duplicate) active_duplicate,
        bool_or(economic_member_duplicate) economic_member_duplicate,
        jsonb_agg(jsonb_build_object(
          'role',role,'member_ids',to_jsonb(member_ids),'active_member_ids',to_jsonb(active_ids),
          'archived_member_ids',to_jsonb(archived_ids),'operation_proven_missing_member_ids',to_jsonb(operation_missing_ids),
          'has_effective_history',has_effective_history,'effective_state',case
            when has_effective_history and net_day+net_night+net_sat+net_sun+net_bh=0 then 'SETTLED_ZERO_HISTORY'
            when has_effective_history then 'EFFECTIVE_HISTORY'
            when pending_invoice then 'PENDING_INVOICE'
            when cardinality(active_ids)>0 then 'ACTIVE_MUTABLE'
            when cardinality(operation_missing_ids)>0 then 'PHYSICALLY_MISSING_MUTABLE'
            when cardinality(archived_ids)>0 then 'ARCHIVED_AUDIT_ONLY'
            else 'UNPROVABLE' end,
          'signed_net_hours',jsonb_build_object('hours_day',net_day,'hours_night',net_night,
            'hours_sat',net_sat,'hours_sun',net_sun,'hours_bh',net_bh,
            'total_hours',net_day+net_night+net_sat+net_sun+net_bh),
          'pending_invoice',pending_invoice,'paid',paid,'economic_member_duplicate',economic_member_duplicate
        ) order by role) role_evidence
      from role_state group by correction_id
    )
    select
      coalesce((select jsonb_agg(jsonb_build_object('correction_id',g.correction_id,'state',case
          when g.active_duplicate or g.economic_member_duplicate then 'UNPROVABLE'
          when g.effective_roles=2 then 'FULLY_INVOICED'
          when g.effective_roles=1 and g.proven_roles=2 then 'PARTIALLY_INVOICED'
          when g.effective_roles=0 and g.pending_roles=0 and g.proven_roles=2 and not g.paid
            and (g.active_role_count>0 or g.missing_operation_role_count=2) then 'MUTABLE'
          when g.effective_roles=0 and g.pending_roles=0 and g.active_role_count=0
            and g.archived_only_role_count=2 then 'ARCHIVED_AUDIT_ONLY'
          else 'UNPROVABLE' end,
          'roles',g.role_evidence) order by g.evidence_at,g.correction_id) from generation_state g),'[]'::jsonb),
      coalesce((select array_agg(g.correction_id order by g.evidence_at,g.correction_id) from generation_state g
        where g.effective_roles=2 and not g.active_duplicate and not g.economic_member_duplicate),array[]::text[]),
      coalesce((select array_agg(g.correction_id order by g.evidence_at,g.correction_id) from generation_state g
        where g.effective_roles=1 and g.proven_roles=2 and not g.active_duplicate and not g.economic_member_duplicate),array[]::text[]),
      coalesce((select array_agg(g.correction_id order by g.evidence_at,g.correction_id) from generation_state g
        where g.effective_roles=0 and g.pending_roles=0 and g.proven_roles=2 and not g.paid
          and not g.active_duplicate and not g.economic_member_duplicate
          and (g.active_role_count>0 or g.missing_operation_role_count=2)),array[]::text[]),
      (select g.correction_id from generation_state g
        where g.effective_roles=0 and g.pending_roles=0 and g.proven_roles=2 and not g.paid
          and not g.active_duplicate and not g.economic_member_duplicate
          and (g.active_role_count>0 or g.missing_operation_role_count=2)
        order by g.evidence_at desc,g.correction_id desc limit 1),
      coalesce((select jsonb_agg(jsonb_build_object('correction_id',r.correction_id,'role',r.role,
        'timesheet_ids',to_jsonb(r.archived_ids)) order by r.correction_id,r.role)
        from role_state r where cardinality(r.archived_ids)>0),'[]'::jsonb),
      coalesce((select jsonb_agg(jsonb_build_object('correction_id',g.correction_id,'reason',case
          when g.active_duplicate then 'ACTIVE_ROLE_DUPLICATE'
          when g.economic_member_duplicate then 'DUPLICATE_EFFECTIVE_ROLE_WITHOUT_REPAIR_LINEAGE'
          else 'ROLE_IDENTITY_UNPROVABLE' end) order by g.evidence_at,g.correction_id)
        from generation_state g where g.active_duplicate or g.economic_member_duplicate
          or (g.proven_roles<2 and (g.effective_roles>0 or g.pending_roles>0))),'[]'::jsonb),
      exists(select 1 from generation_state g where g.effective_roles=1 and g.proven_roles=2
        and not g.active_duplicate and not g.economic_member_duplicate),
      -- A fully issued generation legitimately retains TSFIN/invoice locks.
      -- Those locks are immutable history, not active invoice activity.  Keep
      -- blocking only while at least one expected role is not yet effective.
      exists(select 1 from generation_state g where g.pending_roles>0 and g.effective_roles<2),
      exists(select 1 from generation_state g where g.active_duplicate or g.economic_member_duplicate
        or (g.proven_roles<2 and (g.effective_roles>0 or g.pending_roles>0)))
    into v_generation_role_evidence,v_fully_invoiced_generation_ids,v_partial_generation_ids,v_mutable_generation_ids,
      v_mutable_correction_id,v_archived_history_roles,v_role_evidence_conflicts,v_role_partial_invoice_state,
      v_role_active_invoice_activity,v_role_scope_unprovable;

    v_partial_invoice_state:=v_role_partial_invoice_state;
    -- Stable DRAFT lines are planned economics, not active invoice work.  Only
    -- a real document/issue operation or an import operation still in progress
    -- blocks review.  One-sided placement is reported separately below.
    v_active_invoice_activity:=v_active_invoice_activity or v_operation_in_progress;
    v_scope_unprovable:=v_scope_unprovable or v_role_scope_unprovable
      or v_operation_evidence_conflict or v_member_role_conflict;

    if v_mutable_correction_id is not null then
      v_repair_identity_mode:=case when exists(select 1 from public.timesheets archived
        where archived.correction_id=v_mutable_correction_id and archived.archived_at_utc is not null
          and archived.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'))
        then 'FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED' else 'RETAIN_EXISTING_CORRECTION_ID' end;
    end if;
    v_mutable_member_ids:=array[]::uuid[]; v_mutable_missing_roles:=array[]::text[];
    v_mutable_parent_id:=null; v_m_day:=0; v_m_night:=0; v_m_sat:=0; v_m_sun:=0; v_m_bh:=0;
    v_m_pay:=0; v_m_charge:=0; v_m_margin:=0; v_m_financials_complete:=true; v_paid_mutable_state:=false;
    if v_mutable_correction_id is not null then
      select coalesce(array_agg(t.timesheet_id order by t.correction_kind,t.timesheet_id),array[]::uuid[]),
        (array_agg(t.parent_timesheet_id order by t.created_at,t.timesheet_id))[1],
        coalesce(sum(tf.hours_day),0),coalesce(sum(tf.hours_night),0),coalesce(sum(tf.hours_sat),0),coalesce(sum(tf.hours_sun),0),coalesce(sum(tf.hours_bh),0),
        coalesce(sum(tf.total_pay_ex_vat),0),coalesce(sum(tf.total_charge_ex_vat),0),coalesce(sum(tf.margin_ex_vat),0),
        count(*)=count(tf.id) and bool_and(not coalesce(tf.is_stale,true) and not coalesce(tf.has_rate_issue,false) and not coalesce(tf.has_pay_channel_issue,false)),
        bool_or(tf.paid_at_utc is not null)
      into v_mutable_member_ids,v_mutable_parent_id,v_m_day,v_m_night,v_m_sat,v_m_sun,v_m_bh,v_m_pay,v_m_charge,v_m_margin,v_m_financials_complete,v_paid_mutable_state
      from public.timesheets t left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT');
      if not exists(select 1 from public.timesheets t where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REVERSAL') then
        v_mutable_missing_roles:=array_append(v_mutable_missing_roles,'CHANGED_HOURS_REVERSAL');
      end if;
      if not exists(select 1 from public.timesheets t where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REPLACEMENT') then
        v_mutable_missing_roles:=array_append(v_mutable_missing_roles,'CHANGED_HOURS_REPLACEMENT');
      end if;
    end if;
    v_mutable_fingerprint:=encode(digest(convert_to(concat_ws('|','mutable-v1',v_mutable_correction_id,v_mutable_member_ids::text,v_mutable_missing_roles::text,v_m_day,v_m_night,v_m_sat,v_m_sun,v_m_bh,v_m_pay,v_m_charge,v_m_margin),'UTF8'),'sha256'),'hex');

    -- A schedule is valid only when it belongs to the exact terminal positive
    -- member.  An older positive may have the same buckets while representing
    -- different work times, so mere candidate presence or bucket equality is
    -- never provenance authority.
    if cardinality(v_fully_invoiced_generation_ids)>0 then
      v_terminal_generation_id:=v_fully_invoiced_generation_ids[cardinality(v_fully_invoiced_generation_ids)];
      select count(distinct (member->>'timesheet_id')::uuid),
        (array_agg(distinct (member->>'timesheet_id')::uuid order by (member->>'timesheet_id')::uuid))[1]
      into v_terminal_positive_member_count,v_terminal_positive_timesheet_id
      from jsonb_array_elements(v_member_role_map) member
      where member->>'correction_id'=v_terminal_generation_id
        and member->>'correction_kind'='CHANGED_HOURS_REPLACEMENT';
      if v_terminal_positive_member_count<>1 then
        v_scope_unprovable:=true;
        v_b_standard_schedule_authority_diagnostic:='TERMINAL_POSITIVE_MEMBER_UNPROVABLE';
      end if;
    elsif (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0 then
      v_terminal_positive_timesheet_id:=v_source_timesheet_id;
      v_terminal_positive_member_count:=case when v_source_timesheet_id is null then 0 else 1 end;
    end if;

    if v_terminal_positive_timesheet_id is not null then
      select count(*)::integer,
        count(distinct candidate->'schedule_json'::text)::integer,
        count(distinct coalesce(candidate->>'policy_fingerprint','<NULL>'))::integer
      into v_terminal_frozen_candidate_count,v_terminal_frozen_schedule_variant_count,
        v_terminal_frozen_policy_variant_count
      from jsonb_array_elements(v_schedule_candidates) candidate
      where candidate->>'timesheet_id'=v_terminal_positive_timesheet_id::text
        and (v_terminal_generation_id is null or (
          candidate->>'correction_id'=v_terminal_generation_id
          and candidate->>'correction_kind'='CHANGED_HOURS_REPLACEMENT'));

      select candidate
      into v_terminal_frozen_candidate
      from jsonb_array_elements(v_schedule_candidates) candidate
      where candidate->>'timesheet_id'=v_terminal_positive_timesheet_id::text
        and (v_terminal_generation_id is null or (
          candidate->>'correction_id'=v_terminal_generation_id
          and candidate->>'correction_kind'='CHANGED_HOURS_REPLACEMENT'))
      order by candidate->>'invoice_line_id'
      limit 1;
    end if;

    if v_terminal_generation_id is not null and v_terminal_positive_timesheet_id is not null then
      select unit
      into v_terminal_operation
      from jsonb_array_elements(v_operation_evidence) unit
      where unit->>'correction_id'=v_terminal_generation_id
        and unit->>'replacement_timesheet_id'=v_terminal_positive_timesheet_id::text
      order by (unit->>'evidence_at')::timestamptz desc,unit->>'operation_id' desc
      limit 1;
    end if;

    v_candidate_schedule:=coalesce(v_terminal_frozen_candidate->'schedule_json','[]'::jsonb);
    v_candidate_hours:=coalesce(v_terminal_frozen_candidate->'hours','{}'::jsonb);
    v_candidate_policy_fingerprint:=nullif(v_terminal_frozen_candidate->>'policy_fingerprint','');
    v_terminal_operation_schedule:=coalesce(v_terminal_operation->'A_schedule_json','[]'::jsonb);
    v_terminal_operation_hours:=coalesce(v_terminal_operation->'A_hours','{}'::jsonb);
    v_terminal_operation_policy_fingerprint:=nullif(v_terminal_operation->>'policy_envelope_fingerprint','');
    v_terminal_operation_id:=case when coalesce(v_terminal_operation->>'operation_id','')~*v_uuid_re
      then (v_terminal_operation->>'operation_id')::uuid end;

    -- Historical request envelopes created before bucketed A-hours were
    -- populated can legitimately contain total_hours with zeroed buckets.
    -- Recover the buckets only from the exact terminal replacement member:
    -- its validated completed-operation schedule plus its matching frozen
    -- correction policy.  Never borrow a surviving older positive schedule.
    if v_terminal_positive_timesheet_id is not null and v_terminal_operation is not null then
      select tf.policy_snapshot_json,
        coalesce(tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
          tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
      into v_terminal_policy_snapshot,v_terminal_policy_snapshot_fingerprint
      from public.timesheets_financials tf
      where tf.timesheet_id=v_terminal_positive_timesheet_id
        and tf.is_current
        and jsonb_typeof(tf.policy_snapshot_json->'correction_financials_policy_envelope')='object'
        and coalesce(tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
          tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
          =v_terminal_operation_policy_fingerprint
        and tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'
          =v_terminal_operation_policy_fingerprint
        -- jsonb considers numerically equivalent values (for example 1 and
        -- 1.0) equal even though their text encodings hash differently.  The
        -- completed operation envelope was already independently re-attested;
        -- require the terminal TSFIN copy to be semantically identical and to
        -- carry that exact validated fingerprint.
        and tf.policy_snapshot_json->'correction_financials_policy_envelope'
          =v_terminal_operation->'policy_envelope'
      order by tf.computed_at_utc desc nulls last,tf.id desc
      limit 1;

      if v_terminal_policy_snapshot_fingerprint=v_terminal_operation_policy_fingerprint
        and jsonb_typeof(v_terminal_operation_schedule)='array'
        and jsonb_array_length(v_terminal_operation_schedule)=1
        and nullif(coalesce((v_terminal_operation_schedule->0)->>'start_utc',
          (v_terminal_operation_schedule->0)->>'start'),'') is not null
        and nullif(coalesce((v_terminal_operation_schedule->0)->>'end_utc',
          (v_terminal_operation_schedule->0)->>'end'),'') is not null then
        begin
          select bucket.hours_day,bucket.hours_night,bucket.hours_sat,bucket.hours_sun,
            bucket.hours_bh,bucket.total_hours
          into v_terminal_derived_day,v_terminal_derived_night,v_terminal_derived_sat,
            v_terminal_derived_sun,v_terminal_derived_bh,v_terminal_derived_total
          from public._wkimp_bucket_hours_from_policy(
            v_terminal_policy_snapshot,
            coalesce((v_terminal_operation_schedule->0)->>'start_utc',
              (v_terminal_operation_schedule->0)->>'start')::timestamptz,
            coalesce((v_terminal_operation_schedule->0)->>'end_utc',
              (v_terminal_operation_schedule->0)->>'end')::timestamptz,
            coalesce(nullif(coalesce((v_terminal_operation_schedule->0)->>'break_mins',
              (v_terminal_operation_schedule->0)->>'break_minutes'),'')::integer,0)
          ) bucket;
          if v_terminal_derived_total=
            coalesce((v_terminal_operation_hours->>'total_hours')::numeric,v_terminal_derived_total) then
            v_terminal_operation_hours:=jsonb_build_object(
              'hours_day',v_terminal_derived_day,'hours_night',v_terminal_derived_night,
              'hours_sat',v_terminal_derived_sat,'hours_sun',v_terminal_derived_sun,
              'hours_bh',v_terminal_derived_bh,'total_hours',v_terminal_derived_total);
          end if;
        exception when others then
          v_b_standard_schedule_authority_diagnostic:='TERMINAL_OPERATION_SCHEDULE_BUCKET_DERIVATION_FAILED';
        end;
      end if;
    end if;

    v_terminal_frozen_matches_b:=v_terminal_frozen_candidate_count>0
      and jsonb_array_length(v_candidate_schedule)=1
      and coalesce((v_candidate_hours->>'hours_day')::numeric,0)=v_b_day
      and coalesce((v_candidate_hours->>'hours_night')::numeric,0)=v_b_night
      and coalesce((v_candidate_hours->>'hours_sat')::numeric,0)=v_b_sat
      and coalesce((v_candidate_hours->>'hours_sun')::numeric,0)=v_b_sun
      and coalesce((v_candidate_hours->>'hours_bh')::numeric,0)=v_b_bh;
    v_terminal_operation_matches_b:=v_terminal_operation is not null
      and jsonb_typeof(v_terminal_operation_schedule)='array'
      and jsonb_array_length(v_terminal_operation_schedule)=1
      and coalesce((v_terminal_operation_hours->>'hours_day')::numeric,0)=v_b_day
      and coalesce((v_terminal_operation_hours->>'hours_night')::numeric,0)=v_b_night
      and coalesce((v_terminal_operation_hours->>'hours_sat')::numeric,0)=v_b_sat
      and coalesce((v_terminal_operation_hours->>'hours_sun')::numeric,0)=v_b_sun
      and coalesce((v_terminal_operation_hours->>'hours_bh')::numeric,0)=v_b_bh;

    v_terminal_schedule_authority_conflict:=v_terminal_frozen_schedule_variant_count>1
      or (v_terminal_frozen_candidate_count>0 and v_terminal_operation is not null and (
        not v_terminal_frozen_matches_b or not v_terminal_operation_matches_b
        or jsonb_strip_nulls(jsonb_build_object(
          'date',coalesce((v_candidate_schedule->0)->>'date',(v_candidate_schedule->0)->>'work_date'),
          'start_utc',coalesce((v_candidate_schedule->0)->>'start_utc',(v_candidate_schedule->0)->>'start'),
          'end_utc',coalesce((v_candidate_schedule->0)->>'end_utc',(v_candidate_schedule->0)->>'end'),
          'break_mins',coalesce((v_candidate_schedule->0)->>'break_mins',(v_candidate_schedule->0)->>'break_minutes','0'),
          'shift_id',(v_candidate_schedule->0)->>'shift_id',
          'external_row_key',(v_candidate_schedule->0)->>'external_row_key'))
          is distinct from
        jsonb_strip_nulls(jsonb_build_object(
          'date',coalesce((v_terminal_operation_schedule->0)->>'date',(v_terminal_operation_schedule->0)->>'work_date'),
          'start_utc',coalesce((v_terminal_operation_schedule->0)->>'start_utc',(v_terminal_operation_schedule->0)->>'start'),
          'end_utc',coalesce((v_terminal_operation_schedule->0)->>'end_utc',(v_terminal_operation_schedule->0)->>'end'),
          'break_mins',coalesce((v_terminal_operation_schedule->0)->>'break_mins',(v_terminal_operation_schedule->0)->>'break_minutes','0'),
          'shift_id',(v_terminal_operation_schedule->0)->>'shift_id',
          'external_row_key',(v_terminal_operation_schedule->0)->>'external_row_key'))));
    v_terminal_policy_authority_conflict:=v_terminal_frozen_policy_variant_count>1
      or (v_terminal_frozen_candidate_count>0 and v_terminal_operation is not null
        and v_candidate_policy_fingerprint is distinct from v_terminal_operation_policy_fingerprint);

    if v_terminal_schedule_authority_conflict then
      v_scope_unprovable:=true;
      v_b_standard_schedule_authority_diagnostic:='TERMINAL_SCHEDULE_AUTHORITY_CONFLICT';
    elsif v_terminal_policy_authority_conflict then
      v_scope_unprovable:=true;
      v_b_standard_schedule_authority_diagnostic:='TERMINAL_POLICY_AUTHORITY_CONFLICT';
    elsif v_terminal_frozen_matches_b then
      if v_candidate_policy_fingerprint is null then
        v_scope_unprovable:=true;
        v_b_standard_schedule_authority_diagnostic:='TERMINAL_POLICY_AUTHORITY_MISSING';
      else
        v_b_schedule:=v_candidate_schedule;
        v_b_policy_fingerprint:=v_candidate_policy_fingerprint;
        v_b_standard_schedule_authority:=case when v_terminal_generation_id is null
          then 'ORIGINAL_SOURCE_FROZEN_SEGMENT' else 'TERMINAL_REPLACEMENT_FROZEN_SEGMENT' end;
        v_b_standard_schedule_authority_timesheet_id:=v_terminal_positive_timesheet_id;
        v_b_standard_schedule_authority_correction_id:=v_terminal_generation_id;
        v_b_standard_schedule_authority_operation_id:=v_terminal_operation_id;
        v_b_standard_schedule_authority_policy_fingerprint:=v_candidate_policy_fingerprint;
      end if;
    elsif v_terminal_operation_matches_b then
      if v_terminal_operation_policy_fingerprint is null then
        v_scope_unprovable:=true;
        v_b_standard_schedule_authority_diagnostic:='TERMINAL_POLICY_AUTHORITY_MISSING';
      else
        v_b_schedule:=v_terminal_operation_schedule;
        v_b_policy_fingerprint:=v_terminal_operation_policy_fingerprint;
        v_b_standard_schedule_authority:='TERMINAL_COMPLETED_OPERATION_A_SCHEDULE';
        v_b_standard_schedule_authority_timesheet_id:=v_terminal_positive_timesheet_id;
        v_b_standard_schedule_authority_correction_id:=v_terminal_generation_id;
        v_b_standard_schedule_authority_operation_id:=v_terminal_operation_id;
        v_b_standard_schedule_authority_policy_fingerprint:=v_terminal_operation_policy_fingerprint;
      end if;
    end if;

    v_b_hours_zero:=v_b_day=0 and v_b_night=0 and v_b_sat=0 and v_b_sun=0 and v_b_bh=0;
    v_b_money_zero:=round(v_b_pay,2)=0 and round(v_b_charge,2)=0 and round(v_b_margin,2)=0;
    if v_b_hours_zero and v_b_money_zero then
      v_b_schedule:='[]'::jsonb;
      v_b_policy_fingerprint:=null;
      v_b_standard_schedule_authority:='NONE';
      v_b_standard_schedule_authority_timesheet_id:=null;
      v_b_standard_schedule_authority_correction_id:=null;
      v_b_standard_schedule_authority_operation_id:=null;
      v_b_standard_schedule_authority_policy_fingerprint:=null;
    end if;
    v_b_standard_representable:=(v_b_hours_zero and v_b_money_zero)
      or (v_b_day>=0 and v_b_night>=0 and v_b_sat>=0 and v_b_sun>=0 and v_b_bh>=0
        and v_b_standard_schedule_authority<>'NONE' and jsonb_array_length(v_b_schedule)=1);
    v_b_standard_schedule_authority_fingerprint:=encode(digest(convert_to(jsonb_build_object(
      'contract','B-standard-schedule-authority-v1','source_scope_fingerprint',v_scope_fingerprint,
      'authority',v_b_standard_schedule_authority,'timesheet_id',v_b_standard_schedule_authority_timesheet_id,
      'correction_id',v_b_standard_schedule_authority_correction_id,
      'operation_id',v_b_standard_schedule_authority_operation_id,
      'schedule_json',v_b_schedule,
      'B_hours',jsonb_build_object('hours_day',v_b_day,'hours_night',v_b_night,'hours_sat',v_b_sat,
        'hours_sun',v_b_sun,'hours_bh',v_b_bh,'total_hours',v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh),
      'policy_fingerprint',v_b_standard_schedule_authority_policy_fingerprint)::text,'UTF8'),'sha256'),'hex');

    v_role_evidence_fingerprint:=encode(digest(convert_to(concat_ws('|','role-evidence-v4',
      v_operation_evidence::text,v_member_supersession_map::text,v_member_role_map::text,v_generation_role_evidence::text,
      v_archived_history_roles::text,v_role_evidence_conflicts::text,v_b_standard_schedule_authority_fingerprint),'UTF8'),'sha256'),'hex');
    v_effective_fingerprint:=encode(digest(convert_to(concat_ws('|','effective-invoice-v4',v_source_identity,
      v_line_evidence::text,v_ignored_nonhours_line_ids::text,v_role_evidence_fingerprint,
      v_b_day,v_b_night,v_b_sat,v_b_sun,v_b_bh,v_b_pay,v_b_charge,v_b_margin,
      v_issued_line_ids::text,v_pending_line_ids::text,
      v_issued_day,v_issued_night,v_issued_sat,v_issued_sun,v_issued_bh,v_issued_pay,v_issued_charge,v_issued_margin,
      v_pending_day,v_pending_night,v_pending_sat,v_pending_sun,v_pending_bh,v_pending_pay,v_pending_charge,v_pending_margin,
      v_b_standard_schedule_authority_fingerprint),'UTF8'),'sha256'),'hex');

    if v_mutable_correction_id is not null then
      v_reversal_repair_required:=not exists(select 1 from public.timesheets t
        where t.correction_id=v_mutable_correction_id and t.correction_kind='CHANGED_HOURS_REVERSAL'
          and t.is_current and t.archived_at_utc is null
          and t.actual_schedule_json is not distinct from v_b_schedule);
      v_replacement_repair_required:=not exists(select 1 from public.timesheets t
        where t.correction_id=v_mutable_correction_id and t.correction_kind='CHANGED_HOURS_REPLACEMENT'
          and t.is_current and t.archived_at_utc is null
          and t.actual_schedule_json is not distinct from v_a_schedule);
    end if;

    v_effective_zero:=v_b_hours_zero and v_b_money_zero;
    v_source_protection:=public._import_review_timesheet_protection_core_v1(v_source_timesheet_id);
    select count(*)::integer
    into v_current_source_count
    from public.timesheets t
    where t.timesheet_id=v_source_timesheet_id and t.is_current and t.archived_at_utc is null
      and coalesce(t.correction_kind::text,'') not in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      and t.contract_id=v_contract_id and t.week_ending_date=v_week_ending_date
      and jsonb_typeof(t.actual_schedule_json)='array'
      and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
        'shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key));
    v_current_source_invoice_lined:=exists(select 1 from public.invoice_lines il
      where il.timesheet_id=v_source_timesheet_id
        or il.meta_json->>'timesheet_id'=v_source_timesheet_id::text);
    select coalesce(bool_or(tf.paid_at_utc is not null),false),
      coalesce(bool_and(tf.locked_by_invoice_id is null),false),
      count(*)=1 and coalesce(bool_and(not coalesce(tf.is_stale,true)
        and not coalesce(tf.has_rate_issue,false) and not coalesce(tf.has_pay_channel_issue,false)),false),
      count(*)=1 and coalesce(bool_and(not exists(
        select 1 from jsonb_array_elements(case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
          then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg
        where nullif(seg->>'invoice_locked_invoice_id','') is not null)),false)
    into v_current_source_paid,v_current_source_unlocked,v_current_source_fresh,v_current_source_segment_unlocked
    from public.timesheets_financials tf
    where tf.timesheet_id=v_source_timesheet_id and tf.is_current
      and tf.candidate_id=v_candidate_id;
    select count(*)=1 and bool_and(upper(coalesce(cw.status::text,'')) not in ('INVOICED','CANCELLED'))
    into v_current_source_contract_week_safe
    from public.contract_weeks cw
    where cw.timesheet_id=v_source_timesheet_id and cw.contract_id=v_contract_id
      and cw.week_ending_date=v_week_ending_date;
    v_current_source_contract_week_safe:=coalesce(v_current_source_contract_week_safe,false);
    v_current_source_invoice_operation_clear:=not exists(
      select 1
      from public.invoice_lines il
      join public.invoices i on i.id=il.invoice_id
      where (il.timesheet_id=v_source_timesheet_id or il.meta_json->>'timesheet_id'=v_source_timesheet_id::text)
        and (i.active_document_operation_id is not null or i.active_issue_operation_id is not null
          or upper(coalesce(i.issue_state,'')) not in ('','IDLE','COMPLETE','COMPLETED','ISSUED'))
    );
    v_current_source_safe:=v_current_source_count=1
      and not v_current_source_invoice_lined
      and not v_current_source_paid
      and v_current_source_unlocked and v_current_source_fresh and v_current_source_segment_unlocked
      and v_current_source_contract_week_safe and v_current_source_invoice_operation_clear
      and not coalesce((v_source_protection->>'paid')::boolean,false)
      and not coalesce((v_source_protection->>'invoice_locked')::boolean,false)
      and not coalesce((v_source_protection->>'active_pay_draft')::boolean,false);
    v_current_source_safety_reason:=case
      when v_current_source_safe then 'SAFE_CURRENT_ORDINARY_SOURCE'
      when v_current_source_paid and v_current_source_invoice_lined then 'CURRENT_SOURCE_PAID_AND_INVOICE_LINED'
      when v_current_source_invoice_lined then 'CURRENT_SOURCE_INVOICE_LINED_AFTER_EFFECTIVE_ZERO'
      when v_current_source_paid then 'CURRENT_SOURCE_PAID_AFTER_EFFECTIVE_ZERO'
      when v_current_source_count<>1 then 'NO_EXACT_CURRENT_ORDINARY_SOURCE'
      when not v_current_source_unlocked then 'CURRENT_SOURCE_INVOICE_LOCKED'
      when not v_current_source_fresh then 'CURRENT_SOURCE_TSFIN_NOT_FRESH'
      when not v_current_source_segment_unlocked then 'CURRENT_SOURCE_SEGMENT_LOCKED'
      when not v_current_source_invoice_operation_clear then 'CURRENT_SOURCE_INVOICE_OPERATION_ACTIVE'
      when not v_current_source_contract_week_safe then 'CURRENT_SOURCE_CONTRACT_WEEK_UNSAFE'
      when coalesce((v_source_protection->>'active_pay_draft')::boolean,false) then 'CURRENT_SOURCE_ACTIVE_PAY_DRAFT'
      else 'CURRENT_SOURCE_LIFECYCLE_UNSAFE' end;

    -- The authoritative decision is economic whenever an invoiced position or
    -- a mutable correction generation exists.  The live operational source row
    -- is not the financial authority in that case.  Compare every rate bucket,
    -- using B + M for an active mutable generation and B otherwise.
    v_financial_position_requires_amendment:=case
      when v_mutable_correction_id is not null then
        v_b_day+v_m_day is distinct from coalesce((v_a_hours->>'hours_day')::numeric,0)
        or v_b_night+v_m_night is distinct from coalesce((v_a_hours->>'hours_night')::numeric,0)
        or v_b_sat+v_m_sat is distinct from coalesce((v_a_hours->>'hours_sat')::numeric,0)
        or v_b_sun+v_m_sun is distinct from coalesce((v_a_hours->>'hours_sun')::numeric,0)
        or v_b_bh+v_m_bh is distinct from coalesce((v_a_hours->>'hours_bh')::numeric,0)
      when v_effective_component_count>0 and not v_effective_zero then
        v_b_day is distinct from coalesce((v_a_hours->>'hours_day')::numeric,0)
        or v_b_night is distinct from coalesce((v_a_hours->>'hours_night')::numeric,0)
        or v_b_sat is distinct from coalesce((v_a_hours->>'hours_sat')::numeric,0)
        or v_b_sun is distinct from coalesce((v_a_hours->>'hours_sun')::numeric,0)
        or v_b_bh is distinct from coalesce((v_a_hours->>'hours_bh')::numeric,0)
      else false
    end;

    v_blocking_code:=case
      when v_partial_invoice_state then 'IMPORT_REVIEW_CORRECTION_PAIR_PLACEMENT_INCOMPLETE'
      when v_active_invoice_activity then 'IMPORT_REVIEW_INVOICE_ACTIVITY_IN_PROGRESS'
      when v_credit_ambiguous then 'IMPORT_REVIEW_EFFECTIVE_CREDIT_AMBIGUOUS'
      when v_scope_unprovable or v_stream_conflict then 'IMPORT_REVIEW_INVOICE_COMPONENT_SCOPE_UNPROVABLE'
      when v_paid_mutable_state then 'IMPORT_REVIEW_PAID_MUTABLE_GENERATION_ROLLOVER_UNAVAILABLE'
      when v_b_hours_zero and not v_b_money_zero then 'IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE'
      when v_effective_zero and v_effective_component_count>0 and v_mutable_correction_id is null
        and coalesce((v_a_hours->>'total_hours')::numeric,0)>0 and not v_current_source_safe
        then 'IMPORT_REVIEW_EFFECTIVE_ZERO_NO_ACTIVE_SOURCE'
      when (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)<0 then 'IMPORT_REVIEW_INVOICE_STATE_UNSUPPORTED'
      when (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0 and not v_b_standard_representable then 'IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE'
      else null end;
    v_reconciliation_fingerprint:=encode(digest(convert_to(concat_ws('|','reconciliation-v4',v_scope_fingerprint,v_operation_ids::text,v_member_supersession_map::text,
      v_effective_fingerprint,v_mutable_fingerprint,v_a_fingerprint,v_blocking_code,v_b_policy_fingerprint,
      v_b_standard_schedule_authority,v_b_standard_schedule_authority_timesheet_id,
      v_b_standard_schedule_authority_correction_id,v_b_standard_schedule_authority_operation_id,
      v_b_standard_schedule_authority_policy_fingerprint,v_b_standard_schedule_authority_fingerprint,
      v_current_source_safe,v_current_source_safety_reason,v_current_source_invoice_lined,v_current_source_paid,
      v_current_source_unlocked,v_current_source_fresh,v_current_source_segment_unlocked,
      v_current_source_contract_week_safe,v_current_source_invoice_operation_clear,v_b_hours_zero,v_b_money_zero,
      v_financial_position_requires_amendment),'UTF8'),'sha256'),'hex');

    source_identity:=v_source_identity;
    balance_json:=jsonb_build_object(
      'schema_version','IMPORT_AUTHORITATIVE_RECONCILIATION_BALANCE_V1',
      'source_identity',v_source_identity,'source_system',v_source_system,'source_shift_id',v_source_shift_id,
      'external_row_key',v_external_row_key,'source_timesheet_id',v_source_timesheet_id,
      'candidate_id',v_candidate_id,'client_id',v_client_id,'contract_id',v_contract_id,
      'week_ending_date',v_week_ending_date,'invoice_stream',v_invoice_stream,
      'source_scope_fingerprint',v_scope_fingerprint,
      'archived_timesheet_ids',to_jsonb(v_archived_ids),'archived_history_timesheet_ids',to_jsonb(v_archived_ids),
      'archived_history_roles',v_archived_history_roles,'active_timesheet_ids',to_jsonb(v_active_ids),
      'historical_missing_timesheet_ids',to_jsonb(v_missing_ids),
      'effective_invoice_ids',to_jsonb(v_effective_invoice_ids),'effective_invoice_line_ids',to_jsonb(v_effective_line_ids),
      'effective_credit_line_ids',to_jsonb(v_credit_line_ids),'effective_invoice_component_count',v_effective_component_count,
      'effective_hours_component_count',v_effective_component_count,
      'issued_invoice_ids',to_jsonb(v_issued_invoice_ids),'issued_invoice_line_ids',to_jsonb(v_issued_line_ids),
      'issued_invoice_component_count',v_issued_component_count,
      'pending_invoice_ids',to_jsonb(v_pending_invoice_ids),'pending_invoice_line_ids',to_jsonb(v_pending_line_ids),
      'pending_invoice_component_count',v_pending_component_count,
      'ignored_nonhours_invoice_line_ids',to_jsonb(v_ignored_nonhours_line_ids)
    ) || jsonb_build_object(
      'generation_role_evidence',v_generation_role_evidence,
      'validated_completed_operation_evidence_count',jsonb_array_length(v_operation_evidence),
      'validated_completed_operation_evidence_fingerprint',encode(digest(convert_to(v_operation_evidence::text,'UTF8'),'sha256'),'hex'),
      'correction_member_supersession_lineage',v_member_supersession_map,
      'fully_invoiced_generation_ids',to_jsonb(v_fully_invoiced_generation_ids),
      'partial_generation_ids',to_jsonb(v_partial_generation_ids),
      'mutable_generation_ids',to_jsonb(v_mutable_generation_ids),
      'role_evidence_conflicts',v_role_evidence_conflicts,
      'role_evidence_fingerprint',v_role_evidence_fingerprint,
      'effective_invoice_fingerprint',v_effective_fingerprint,
      'B_hours',jsonb_build_object('hours_day',v_b_day,'hours_night',v_b_night,'hours_sat',v_b_sat,'hours_sun',v_b_sun,'hours_bh',v_b_bh,'total_hours',v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh),
      'B_issued_hours',jsonb_build_object('hours_day',v_issued_day,'hours_night',v_issued_night,'hours_sat',v_issued_sat,'hours_sun',v_issued_sun,'hours_bh',v_issued_bh,'total_hours',v_issued_day+v_issued_night+v_issued_sat+v_issued_sun+v_issued_bh),
      'P_pending_hours',jsonb_build_object('hours_day',v_pending_day,'hours_night',v_pending_night,'hours_sat',v_pending_sat,'hours_sun',v_pending_sun,'hours_bh',v_pending_bh,'total_hours',v_pending_day+v_pending_night+v_pending_sat+v_pending_sun+v_pending_bh),
      'planned_invoice_hours',jsonb_build_object('hours_day',v_b_day,'hours_night',v_b_night,'hours_sat',v_b_sat,'hours_sun',v_b_sun,'hours_bh',v_b_bh,'total_hours',v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh),
      'effective_hours_net_is_zero',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)=0,
      'effective_money_net_is_zero',v_b_money_zero,
      'effective_position_net_is_zero',v_effective_zero,
      'effective_hours_net_is_positive',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0,
      'effective_hours_net_is_negative',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)<0,
      'B_financials',jsonb_build_object('pay_ex_vat',v_b_pay,'charge_ex_vat',v_b_charge,'margin_ex_vat',v_b_margin),
      'B_issued_financials',jsonb_build_object('pay_ex_vat',v_issued_pay,'charge_ex_vat',v_issued_charge,'margin_ex_vat',v_issued_margin),
      'P_pending_financials',jsonb_build_object('pay_ex_vat',v_pending_pay,'charge_ex_vat',v_pending_charge,'margin_ex_vat',v_pending_margin),
      'planned_invoice_financials',jsonb_build_object('pay_ex_vat',v_b_pay,'charge_ex_vat',v_b_charge,'margin_ex_vat',v_b_margin),
      'B_standard_schedule_json',v_b_schedule,'B_policy_fingerprint',v_b_policy_fingerprint,'B_standard_representable',v_b_standard_representable,
      'B_standard_schedule_authority',v_b_standard_schedule_authority,
      'B_standard_schedule_authority_timesheet_id',v_b_standard_schedule_authority_timesheet_id,
      'B_standard_schedule_authority_correction_id',v_b_standard_schedule_authority_correction_id,
      'B_standard_schedule_authority_operation_id',v_b_standard_schedule_authority_operation_id,
      'B_standard_schedule_authority_policy_fingerprint',v_b_standard_schedule_authority_policy_fingerprint,
      'B_standard_schedule_authority_fingerprint',v_b_standard_schedule_authority_fingerprint,
      'B_standard_schedule_authority_diagnostic',v_b_standard_schedule_authority_diagnostic,
      'active_mutable_generation',v_mutable_correction_id is not null,'active_mutable_member_ids',to_jsonb(v_mutable_member_ids),
      'active_mutable_missing_roles',to_jsonb(v_mutable_missing_roles),'active_mutable_correction_id',v_mutable_correction_id,
      'physically_missing_mutable_roles',to_jsonb(v_mutable_missing_roles),
      'reviewed_existing_correction_id',v_mutable_correction_id,'repair_identity_mode',v_repair_identity_mode,
      'reversal_repair_required',v_reversal_repair_required,'replacement_repair_required',v_replacement_repair_required
    ) || jsonb_build_object(
      'active_mutable_parent_timesheet_id',v_mutable_parent_id,'active_mutable_fingerprint',v_mutable_fingerprint,
      'M_hours',jsonb_build_object('hours_day',v_m_day,'hours_night',v_m_night,'hours_sat',v_m_sat,'hours_sun',v_m_sun,'hours_bh',v_m_bh,'total_hours',v_m_day+v_m_night+v_m_sat+v_m_sun+v_m_bh),
      'M_existing_financials',jsonb_build_object('pay_ex_vat',v_m_pay,'charge_ex_vat',v_m_charge,'margin_ex_vat',v_m_margin),'M_financials_complete',v_m_financials_complete,
      'A_schedule_json',v_a_schedule,'A_hours',v_a_hours,'A_evidence_fingerprint',v_a_fingerprint,
      'partial_invoice_state',v_partial_invoice_state,'active_invoice_activity',v_active_invoice_activity,
      'archived_active_conflict',false,'archived_invoice_conflict',false,
      'paid_mutable_state',v_paid_mutable_state,
      'current_source_safe_for_effective_zero_amendment',v_current_source_safe,
      'effective_zero_source_safety_reason',v_current_source_safety_reason,
      'current_source_invoice_lined',v_current_source_invoice_lined,'current_source_paid',v_current_source_paid,
      'financial_position_requires_amendment',v_financial_position_requires_amendment,
      'recommended_route_inputs',jsonb_build_object('B_positive',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0,
        'has_mutable_generation',v_mutable_correction_id is not null,'source_timesheet_active',v_source_timesheet_id=any(v_active_ids),
        'current_source_safe_for_effective_zero_amendment',v_current_source_safe),
      'blocking_code',v_blocking_code,'reconciliation_fingerprint',v_reconciliation_fingerprint
    );
    return next;
  end loop;
end
$function$;

alter function public._import_review_effective_invoice_balance_core_v1(uuid,jsonb,integer,integer,integer,integer) owner to postgres;
revoke all on function public._import_review_effective_invoice_balance_core_v1(uuid,jsonb,integer,integer,integer,integer) from public,anon,authenticated,service_role;

create or replace function public._import_review_action_catalog_core_v1(
  p_import_id uuid,
  p_preview_generation integer,
  p_max_actions integer default 5000
)
returns table (
  action_id text,
  action_kind text,
  action_category text,
  target_key text,
  source_identity text,
  hr_row_id uuid,
  timesheet_id uuid,
  shift_id uuid,
  client_id uuid,
  candidate_id uuid,
  contract_id uuid,
  issue_id uuid,
  evidence_fingerprint text,
  selectable boolean,
  default_selected boolean,
  blocking boolean,
  summary_json jsonb
)
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare v_count integer; v_weekly_preview jsonb;
begin
  if p_import_id is null or p_preview_generation<1 or p_max_actions<1 or p_max_actions>5000 then
    raise exception 'IMPORT_REVIEW_ACTION_CATALOG_INPUT_INVALID' using errcode='22023';
  end if;

  create temporary table if not exists pg_temp.import_review_catalog_v1 (
    action_id text, action_kind text, action_category text, target_key text, source_identity text,
    hr_row_id uuid, timesheet_id uuid, shift_id uuid, client_id uuid, candidate_id uuid,
    contract_id uuid, issue_id uuid, evidence_fingerprint text, selectable boolean,
    default_selected boolean, blocking boolean, summary_json jsonb
  ) on commit drop;
  truncate pg_temp.import_review_catalog_v1;

  insert into pg_temp.import_review_catalog_v1
  with import_row as (
    select hi.* from public.hr_imports hi where hi.id=p_import_id
  ), raw as (
    select r.*, i.source_system::text as source_system, upper(coalesce(i.import_scope,'')) as import_scope,
      i.client_id as import_client_id,
      coalesce(nullif(r.staff_raw,''),nullif(r.payload_json->>'staff_name',''),nullif(r.staff_norm,'')) as staff_label,
      nullif(regexp_replace(lower(coalesce(nullif(r.staff_raw,''),r.payload_json->>'staff_name',r.staff_norm,'')),'[^a-z0-9]+','','g'),'') as staff_key,
      coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),nullif(r.unit_raw,''),nullif(r.unit_hint,'')) as client_label,
      nullif(regexp_replace(lower(coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),r.unit_raw,r.unit_hint,'')),'[^a-z0-9]+','','g'),'') as client_key,
      lower(btrim(coalesce(nullif(r.assignment_grade_norm,''),r.payload_json->>'grade_raw',r.payload_json->>'Request_Grade',''))) as grade_key,
      coalesce(nullif(r.external_row_key,''),'hr-row:'||r.id::text) as source_row_key
    from public.hr_rows r join import_row i on true where r.import_id=p_import_id
    order by r.id limit 501
  ), mapped as (
    select raw.*,
      coalesce(c_alias.id,c_map.candidate_id,c_exact.candidate_id) as resolved_candidate_id,
      coalesce(raw.import_client_id,ch.client_id,c_client.client_id) as resolved_client_id
    from raw
    left join lateral (
      select c.id from public.candidates c
      where c.nhsp_hr_name_aliases is not null and raw.staff_key is not null
        and c.nhsp_hr_name_aliases @> to_jsonb(array[raw.staff_key]::text[])
      order by c.id limit 1
    ) c_alias on true
    left join lateral (
      select hm.candidate_id from public.hr_name_mappings hm
      where hm.active and hm.hr_name_norm in (lower(btrim(coalesce(raw.staff_label,''))),raw.staff_key)
      order by hm.created_at desc,hm.id limit 1
    ) c_map on c_alias.id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end as candidate_id
      from public.candidates c where c.active and raw.staff_key is not null
        and (regexp_replace(lower(coalesce(c.first_name,'')||coalesce(c.last_name,'')),'[^a-z0-9]+','','g')=raw.staff_key
          or regexp_replace(lower(coalesce(c.last_name,'')||coalesce(c.first_name,'')),'[^a-z0-9]+','','g')=raw.staff_key)
    ) c_exact on c_alias.id is null and c_map.candidate_id is null
    left join lateral (
      select ch.client_id from public.client_hospitals ch
      where raw.client_key is not null and ch.hospital_name_norm @> to_jsonb(array[raw.client_key]::text[])
      order by ch.id limit 1
    ) ch on raw.import_client_id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end as client_id
      from public.clients c where raw.client_key is not null
        and regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+','','g')=raw.client_key
    ) c_client on raw.import_client_id is null and ch.client_id is null
  ), weekly_phase as materialized (
    -- weekly_import_phase2 remains the single authority for assignment-code
    -- mapping precedence and contract choice.  The review catalogue consumes
    -- its answer rather than maintaining a second resolver.
    select w.*
    from import_row i
    cross join lateral public.weekly_import_phase2(
      p_import_id,
      case when i.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end
    ) w
    where not (upper(i.source_system::text)='HEALTHROSTER_DAILY'
      or upper(coalesce(i.import_scope,'')) like '%DAILY%')
  ), classified as (
    select m.*,
      case when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
        then case when rtsx.contract_id is not null then 1 else 0 end
        else con.contract_count end as contract_count,
      case when wp.hr_row_id is not null then wp.contract_id
        when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
          then rtsx.contract_id
        else con.contract_id end as resolved_contract_id,
      wp.action as weekly_resolution_action,wp.reason as weekly_resolution_reason,
      wp.incoming_code as weekly_incoming_code,
      wp.week_ending_date as resolved_week_ending_date,
      wm.has_weekly_mapping,wm.mapping_evidence as weekly_mapping_evidence,
      dgm.mapping_count as daily_mapping_count,dgm.mapping_id as daily_mapping_id,
      dgm.role_code as daily_mapped_role,dgm.band_norm as daily_mapped_band,
      dgm.updated_at as daily_mapping_updated_at,(coalesce(dgm.mapping_count,0)=1) as has_grade_mapping,
      tsx.timesheet_count,tsx.timesheet_ids,tsx.auto_timesheet_id,tsx.timesheet_evidence_hash,
      dtsx.submitted_timesheet_count as daily_submitted_timesheet_count,
      dtsx.submitted_timesheet_evidence_hash as daily_submitted_timesheet_evidence_hash,
      tsx.timesheet_contract_ids,dcon.contract_ids as eligible_contract_ids,dcon.contract_evidence_hash,
      cr.route_eligible as contract_route_eligible,cr.rate_complete as contract_rate_complete,
      cr.import_authoritative,cr.authority_mode,cr.authority_fingerprint,
      cr.rate_evidence as contract_rate_evidence,
      wopts.options as weekly_contract_options,dopts.options as daily_role_options,
      res.resolved_timesheet_id as stored_timesheet_id,res.status as resolution_status,
      coalesce(case when res.resolved_timesheet_id=any(coalesce(tsx.timesheet_ids,array[]::uuid[])) then res.resolved_timesheet_id end,
        tsx.auto_timesheet_id) as resolved_timesheet_id,
      nss.id as existing_shift_id,nss.timesheet_id as existing_shift_timesheet_id,
      nss.start_utc as existing_shift_start_utc,nss.end_utc as existing_shift_end_utc,
      nss.break_mins as existing_shift_break_minutes,nss.pay_minutes as existing_shift_paid_minutes,
      nss.assignment_code as existing_shift_role,
      case when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%' then true else false end as is_daily
    from mapped m
    left join weekly_phase wp on wp.hr_row_id=m.id
    left join lateral (
      select count(*)::integer contract_count,
             case when count(*)=1 then (array_agg(c.id order by c.id))[1] end contract_id
      from public.contracts c
      where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
        and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
    ) con on true
    left join lateral (
      select count(*)::integer mapping_count,
        (array_agg(gm.id order by gm.updated_at desc,gm.id))[1] mapping_id,
        (array_agg(gm.role_code order by gm.updated_at desc,gm.id))[1] role_code,
        (array_agg(gm.band_norm order by gm.updated_at desc,gm.id))[1] band_norm,
        (array_agg(gm.updated_at order by gm.updated_at desc,gm.id))[1] updated_at
      from public.hr_daily_grade_role_mappings gm
      where gm.client_id=m.resolved_client_id and gm.incoming_grade_norm=m.grade_key and gm.active
    ) dgm on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select count(*)::integer contract_count,
        case when count(*)=1 then (array_agg(c.id order by c.id))[1] end contract_id,
        array_agg(c.id order by c.id) contract_ids,
        public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',c.id,c.updated_at,c.role,c.band,a.authority_fingerprint),',' order by c.id),'')) contract_evidence_hash
      from public.contracts c
      cross join lateral public._import_review_effective_authority_core_v1('HR_DAILY',c.id,c.client_id,m.date_local) a
      where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
        and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
        and coalesce(dgm.mapping_count,0)=1 and a.route_eligible
        and lower(btrim(coalesce(c.role,'')))=lower(btrim(coalesce(dgm.role_code,'')))
        and (nullif(btrim(coalesce(dgm.band_norm,'')),'') is null
          or lower(btrim(coalesce(c.band,'')))=lower(btrim(dgm.band_norm)))
    ) dcon on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select count(*)::integer submitted_timesheet_count,
        public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',t.timesheet_id,t.worked_start_iso,
          t.worked_end_iso,t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status,
          t.tsfin_role,t.tsfin_band,ts.contract_id,ts.updated_at),',' order by t.timesheet_id),''))
          submitted_timesheet_evidence_hash
      from public.v_timesheets_daily_match t
      join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
      where t.candidate_id=m.resolved_candidate_id and t.client_id=m.resolved_client_id
        and t.sheet_scope::text='DAILY'
        and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
    ) dtsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      with candidates as (
        select abm.*,
          case when abm.candidate_id=m.resolved_candidate_id and abm.client_id=m.resolved_client_id then 3
            when abm.candidate_id=m.resolved_candidate_id and abm.client_id is null then 2
            when abm.candidate_id is null and abm.client_id=m.resolved_client_id then 1 else 0 end specificity
        from public.assignment_band_mappings abm
        where abm.active and upper(btrim(abm.system_type))=
          case when upper(m.source_system)='NHSP' then 'NHSP' else 'HR_WEEKLY' end
          and lower(btrim(abm.incoming_code))=m.grade_key
          and ((abm.candidate_id=m.resolved_candidate_id and abm.client_id=m.resolved_client_id)
            or (abm.candidate_id=m.resolved_candidate_id and abm.client_id is null)
            or (abm.candidate_id is null and abm.client_id=m.resolved_client_id)
            or (abm.candidate_id is null and abm.client_id is null))
      ), chosen as (select * from candidates where specificity=(select max(specificity) from candidates))
      select exists(select 1 from chosen) has_weekly_mapping,
        public._import_review_hash_v1(coalesce((select string_agg(concat_ws('|',id,updated_at,target_contract_id,band_match_pattern),',' order by id)
          from chosen),'')) mapping_evidence
    ) wm on not (upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%')
    left join lateral (
      select count(*)::integer timesheet_count,
             array_agg(t.timesheet_id order by t.worked_start_iso,t.timesheet_id) timesheet_ids,
             array_agg(ts.contract_id order by t.worked_start_iso,t.timesheet_id) timesheet_contract_ids,
             case when count(*)=1 then (array_agg(t.timesheet_id order by t.timesheet_id))[1] end auto_timesheet_id,
             public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',t.timesheet_id,t.worked_start_iso,t.worked_end_iso,
               t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status,t.tsfin_role,t.tsfin_band,
               ts.contract_id,ts.updated_at),',' order by t.timesheet_id),'')) timesheet_evidence_hash
      from public.v_timesheets_daily_match t
      join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
      where t.candidate_id=m.resolved_candidate_id and t.client_id=m.resolved_client_id
        and t.sheet_scope::text='DAILY'
        and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
        and coalesce(dgm.mapping_count,0)=1
        and lower(btrim(coalesce(t.tsfin_role,'')))=lower(btrim(coalesce(dgm.role_code,'')))
        and (nullif(btrim(coalesce(dgm.band_norm,'')),'') is null
          or lower(btrim(coalesce(t.tsfin_band,'')))=lower(btrim(dgm.band_norm)))
    ) tsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.import_review_daily_timesheet_resolutions res
      on res.import_id=p_import_id and res.hr_row_id=m.id and res.status in ('CURRENT','APPLIED')
    left join lateral (
      select ts.contract_id
      from public.timesheets ts
      where ts.timesheet_id=coalesce(
        case when res.resolved_timesheet_id=any(coalesce(tsx.timesheet_ids,array[]::uuid[])) then res.resolved_timesheet_id end,
        tsx.auto_timesheet_id)
        and ts.is_current and ts.revoked_at is null
      order by ts.updated_at desc limit 1
    ) rtsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select a.route_eligible,a.import_authoritative,a.authority_mode,a.authority_fingerprint,
        (jsonb_typeof(c.rates_json)='object'
          and upper(coalesce(c.pay_method_snapshot,'')) in ('PAYE','UMBRELLA')
          and case when upper(c.pay_method_snapshot)='PAYE' then
            (c.rates_json->>'paye_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_sun')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_bh')~'^-?[0-9]+([.][0-9]+)?$'
          else
            (c.rates_json->>'umb_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_sun')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_bh')~'^-?[0-9]+([.][0-9]+)?$' end
          and (c.rates_json->>'charge_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_night')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_sun')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_bh')~'^-?[0-9]+([.][0-9]+)?$') rate_complete,
        public._import_review_hash_v1(concat_ws('|',c.id,c.updated_at,c.start_date,c.end_date,c.role,c.band,
          c.pay_method_snapshot,c.rates_json,c.overrideclientsettings,c.is_nhsp,c.autoprocess_hr,c.requires_hr,
          c.no_timesheet_required,a.client_settings_id,a.client_settings_updated_at,
          a.effective_is_nhsp,a.effective_autoprocess_hr,a.effective_requires_hr,
          a.effective_no_timesheet_required,a.authority_fingerprint)) rate_evidence
      from public.contracts c
      cross join lateral public._import_review_effective_authority_core_v1(
        case when upper(m.source_system)='NHSP' then 'NHSP'
          when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%' then 'HR_DAILY'
          else 'HR_WEEKLY' end,c.id,c.client_id,coalesce(wp.week_ending_date,m.date_local)) a
      where c.id=case when wp.hr_row_id is not null then wp.contract_id
        when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
          then rtsx.contract_id
        else con.contract_id end
    ) cr on true
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'option_id','contract:'||o.id::text,'contract_id',o.id,'candidate_id',o.candidate_id,'client_id',o.client_id,
        'role',o.role,'band',o.band,'site',o.display_site,'start_date',o.start_date,'end_date',o.end_date,
        'source_route_eligible',coalesce(o.route_eligible,false),'rate_complete',coalesce(o.rate_complete,false),
        'authority_mode',o.authority_mode,
        -- Choosing a contract records the server-approved assignment mapping;
        -- it does not apply the import or grant financial authority.  An
        -- authoritative contract with incomplete rates must therefore remain
        -- selectable here and will still be blocked by the refreshed action
        -- catalogue before final application.
        'selectable',coalesce(o.route_eligible,false),
        'disabled_reason_code',case when not coalesce(o.route_eligible,false) then 'CONTRACT_NOT_ELIGIBLE' end,
        'display_label',concat_ws(' · ',nullif(o.role,''),nullif(o.band,''),nullif(o.display_site,''),
          to_char(o.start_date,'DD Mon YYYY')||' to '||coalesce(to_char(o.end_date,'DD Mon YYYY'),'open ended'))
      ) order by lower(coalesce(o.role,'')),lower(coalesce(o.band,'')),o.start_date desc,o.id),'[]'::jsonb) options
      from (
        select c.*,a.route_eligible,a.import_authoritative,a.authority_mode,
          (jsonb_typeof(c.rates_json)='object'
          and upper(coalesce(c.pay_method_snapshot,'')) in ('PAYE','UMBRELLA')
          and case when upper(c.pay_method_snapshot)='PAYE' then
            (c.rates_json->>'paye_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_bh')~'^-?[0-9]+([.][0-9]+)?$'
          else (c.rates_json->>'umb_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_bh')~'^-?[0-9]+([.][0-9]+)?$' end
          and (c.rates_json->>'charge_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_night')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_bh')~'^-?[0-9]+([.][0-9]+)?$') rate_complete
        from public.contracts c
        cross join lateral public._import_review_effective_authority_core_v1(
          case when upper(m.source_system)='NHSP' then 'NHSP' else 'HR_WEEKLY' end,
          c.id,c.client_id,coalesce(wp.week_ending_date,m.date_local)) a
        where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
          and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
        order by c.start_date desc,c.id limit 25
      ) o
    ) wopts on not (upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%')
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'option_id','daily-role:'||public._import_review_hash_v1(lower(concat_ws('|',o.role,o.band))),
        'role_code',o.role,'band_norm',o.band,'selectable',true,
        'display_label',concat_ws(' · ',nullif(o.role,''),coalesce(nullif(o.band,''),'No band'))
      ) order by lower(o.role),lower(coalesce(o.band,''))),'[]'::jsonb) options
      from (
        select distinct t.tsfin_role role,t.tsfin_band band
        from public.v_timesheets_daily_match t
        where t.candidate_id=m.resolved_candidate_id
          and t.client_id=m.resolved_client_id
          and t.sheet_scope::text='DAILY'
          and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
          and nullif(btrim(t.tsfin_role),'') is not null
        order by t.tsfin_role,t.tsfin_band
        limit 25
      ) o
    ) dopts on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.nhsp_shifts nss
      on nss.external_row_key=m.source_row_key and nss.source_system::text=m.source_system
      and nss.cancelled_at_utc is null
  ), facts as (
    select c.*,
      ts.worked_start_iso,ts.worked_end_iso,ts.break_minutes as ts_break_minutes,ts.worked_minutes,
      ts.reference_number,ts.processing_status::text,ts.tsfin_role,ts.tsfin_band,
      coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id) as authoritative_target_timesheet_id,
      public._import_review_timesheet_has_calculated_expenses_core_v1(
        coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id)
      ) as authoritative_timesheet_has_calculated_expenses,
      mutable_replacement.timesheet_id as mutable_replacement_timesheet_id,
      mutable_replacement.protection as mutable_replacement_protection,
      source_timesheet.authorised_at_server as source_authorised_at_server,
      source_tf.authorised_at_utc as source_tsfin_authorised_at_utc,
      source_tf.policy_snapshot_json as source_policy_snapshot_json,
      source_tf.basis::text as source_tsfin_basis,
      authoritative_hours.hours_day as authoritative_hours_day,
      authoritative_hours.hours_night as authoritative_hours_night,
      authoritative_hours.hours_sat as authoritative_hours_sat,
      authoritative_hours.hours_sun as authoritative_hours_sun,
      authoritative_hours.hours_bh as authoritative_hours_bh,
      authoritative_hours.total_hours as authoritative_total_hours,
      coalesce((auto_authorise.value->>'effective_value')::boolean,false) as effective_auto_authorise,
      public._import_review_timesheet_protection_core_v1(coalesce(
        c.resolved_timesheet_id,c.existing_shift_timesheet_id,base_week.timesheet_id
      )) as protection
    from classified c
    left join public.v_timesheets_daily_match ts on ts.timesheet_id=c.resolved_timesheet_id
    left join lateral (
      select cw.timesheet_id
      from public.contract_weeks cw
      where not c.is_daily
        and coalesce(c.import_authoritative,false)
        and cw.contract_id=c.resolved_contract_id
        and cw.week_ending_date=coalesce(
          c.resolved_week_ending_date,
          c.date_local + ((7-extract(dow from c.date_local)::integer)%7)
        )
        and cw.is_adjustment=false
        and coalesce(cw.additional_seq,0)=0
      order by cw.id
      limit 1
    ) base_week on true
    left join public.timesheets source_timesheet
      on source_timesheet.timesheet_id=coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id)
    left join public.timesheets_financials source_tf
      on source_tf.timesheet_id=source_timesheet.timesheet_id and source_tf.is_current=true
    left join lateral public._wkimp_bucket_hours_from_policy(
      coalesce(source_tf.policy_snapshot_json,'{}'::jsonb),
      (c.payload_json->>'start_utc')::timestamptz,
      (c.payload_json->>'end_utc')::timestamptz,
      coalesce((c.payload_json->>'actual_break_mins')::integer,
        (c.payload_json->>'actual_break_minutes')::integer,
        (c.payload_json->>'break_mins')::integer,
        (c.payload_json->>'break_minutes')::integer,0)
    ) authoritative_hours on not c.is_daily and coalesce(c.import_authoritative,false)
      and c.existing_shift_id is not null
    left join lateral (
      select case
        when not c.is_daily
          and coalesce(c.import_authoritative,false)
          and c.resolved_client_id is not null
          and c.resolved_contract_id is not null
        then public.import_auto_authorise_policy_resolve_v1(
          case when upper(c.source_system)='NHSP' then 'NHSP'::public.hr_source_enum else 'HEALTHROSTER'::public.hr_source_enum end,
          c.resolved_client_id,c.resolved_contract_id,false
        )
        else null::jsonb
      end as value
    ) auto_authorise on true
    left join lateral (
      select replacement_candidate.timesheet_id,replacement_candidate.protection
      from (
        select
          replacement_timesheet.timesheet_id,
          replacement_timesheet.updated_at,
          replacement_timesheet.created_at,
          public._import_review_timesheet_protection_core_v1(
            replacement_timesheet.timesheet_id
          ) as protection
        from public.timesheets replacement_timesheet
        where not c.is_daily
          and c.existing_shift_id is not null
          and replacement_timesheet.is_adjustment is true
          and replacement_timesheet.is_current is true
          and replacement_timesheet.correction_kind='CHANGED_HOURS_REPLACEMENT'
          and jsonb_typeof(replacement_timesheet.actual_schedule_json)='array'
          and replacement_timesheet.actual_schedule_json @> jsonb_build_array(
            jsonb_build_object(
              'shift_id',c.existing_shift_id::text,
              'external_row_key',c.source_row_key
            )
          )
      ) replacement_candidate
      where coalesce(
          (replacement_candidate.protection->>'paid')::boolean,
          false
        ) is false
        and coalesce(
          (replacement_candidate.protection->>'invoice_locked')::boolean,
          false
        ) is false
      order by
        replacement_candidate.updated_at desc nulls last,
        replacement_candidate.created_at desc nulls last
      limit 1
    ) mutable_replacement on true
  ), reconciliation_source_rows as (
    select
      f.*,
      ((row_number() over (order by f.source_row_key) - 1) / 100)::integer as reconciliation_batch
    from facts f
    where not f.is_daily and coalesce(f.import_authoritative,false) and f.existing_shift_id is not null
  ), reconciliation_inputs as (
    select coalesce(jsonb_agg(jsonb_build_object(
      'source_identity',f.source_row_key,
      'source_system',case when upper(f.source_system)='NHSP' then 'NHSP' else 'HEALTHROSTER' end,
      'source_shift_id',f.existing_shift_id,
      'external_row_key',f.source_row_key,
      'hr_row_id',f.id,
      'source_timesheet_id',coalesce(f.existing_shift_timesheet_id,f.authoritative_target_timesheet_id),
      'candidate_id',f.resolved_candidate_id,'client_id',f.resolved_client_id,'contract_id',f.resolved_contract_id,
      'week_ending_date',coalesce(f.resolved_week_ending_date,f.date_local+((7-extract(dow from f.date_local)::integer)%7)),
      'invoice_stream',case when upper(coalesce(f.source_tsfin_basis,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end,
      'authoritative_import_id',p_import_id,
      'authoritative_schedule_json',jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
        'date',f.date_local,'start_utc',f.payload_json->>'start_utc','end_utc',f.payload_json->>'end_utc',
        'break_mins',coalesce((f.payload_json->>'actual_break_mins')::integer,(f.payload_json->>'actual_break_minutes')::integer,
          (f.payload_json->>'break_mins')::integer,(f.payload_json->>'break_minutes')::integer,0),
        'shift_id',f.existing_shift_id,'external_row_key',f.source_row_key,'import_id',p_import_id,
        'ref_num',coalesce(f.hr_request_id,f.payload_json->>'ref_num',f.payload_json->>'reference_number')
      ))),
      'authoritative_hours',jsonb_build_object(
        'hours_day',coalesce(f.authoritative_hours_day,0),'hours_night',coalesce(f.authoritative_hours_night,0),
        'hours_sat',coalesce(f.authoritative_hours_sat,0),'hours_sun',coalesce(f.authoritative_hours_sun,0),
        'hours_bh',coalesce(f.authoritative_hours_bh,0),'total_hours',coalesce(f.authoritative_total_hours,f.hours_worked,0)
      )
    ) order by f.source_row_key),'[]'::jsonb) items
    from reconciliation_source_rows f
    group by f.reconciliation_batch
  ), reconciliation_balances as materialized (
    select b.source_identity,b.balance_json
    from reconciliation_inputs i
    cross join lateral public._import_review_effective_invoice_balance_core_v1(
      p_import_id,i.items,100,512,256,128
    ) b
  ), evidenced as (
    select c.*,
      rb.balance_json as reconciliation_balance,
      public._import_review_hash_v1(concat_ws('|','row-evidence-v1',c.source_row_key,c.staff_key,c.client_key,c.date_local,
        c.start_time_local,c.end_time_local,c.hours_worked,c.hr_request_id,c.resolved_candidate_id,c.resolved_client_id,
        c.resolved_contract_id,c.weekly_resolution_action,c.weekly_incoming_code,c.weekly_mapping_evidence,c.contract_rate_evidence,
        c.daily_mapping_id,c.daily_mapping_updated_at,c.daily_mapped_role,c.daily_mapped_band,
        c.timesheet_evidence_hash,c.daily_submitted_timesheet_evidence_hash,c.contract_evidence_hash,c.authority_fingerprint,
        c.authoritative_target_timesheet_id,c.authoritative_timesheet_has_calculated_expenses,
        c.mutable_replacement_timesheet_id,coalesce(c.mutable_replacement_protection::text,''),
        coalesce(c.eligible_contract_ids::text,''),coalesce(c.timesheet_ids::text,''),
        coalesce(c.timesheet_contract_ids::text,''),c.protection::text,coalesce(rb.balance_json::text,''),
        coalesce(c.payload_json::text,''))) as evidence_hash
    from facts c
    left join reconciliation_balances rb on rb.source_identity=c.source_row_key
  ), main_actions as (
    select
      case
        when f.resolved_candidate_id is null then 'ADVISORY'
        when f.resolved_client_id is null then 'ADVISORY'
        when f.is_daily and not coalesce(f.has_grade_mapping,false) then 'ADVISORY'
        when not f.is_daily and coalesce(f.weekly_resolution_action,'')<>'OK' then 'ADVISORY'
        when not f.is_daily and coalesce(f.contract_count,0)=0 then 'ADVISORY'
        when not f.is_daily and not coalesce(f.contract_route_eligible,false) then 'ADVISORY'
        when f.is_daily and coalesce(f.timesheet_count,0)=0 then 'ADVISORY'
        when f.is_daily and f.resolved_timesheet_id is null then 'DAILY_TIMESHEET_RESOLUTION'
        when f.is_daily then 'NO_ACTION'
        when not coalesce(f.import_authoritative,false) then 'NO_ACTION'
        when not coalesce(f.contract_rate_complete,false) then 'ADVISORY'
        when coalesce(f.authoritative_timesheet_has_calculated_expenses,false) then 'ADVISORY'
        when f.existing_shift_id is null then 'INCLUDE_SHIFT'
        when coalesce((f.reconciliation_balance->>'financial_position_requires_amendment')::boolean,false)
          then 'APPLY_AMENDMENT'
        when (f.payload_json->>'start_utc')::timestamptz is distinct from (select n.start_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or (f.payload_json->>'end_utc')::timestamptz is distinct from (select n.end_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or ((f.payload_json->>'break_mins') is not null
            and (f.payload_json->>'break_mins')::integer is distinct from
              coalesce((select n.break_mins from public.nhsp_shifts n where n.id=f.existing_shift_id),0))
          then 'APPLY_AMENDMENT'
        else 'NO_ACTION'
      end action_kind,
      f.*
    from evidenced f
  ), rendered as (
    select
      public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,m.action_kind,m.source_row_key)) action_id,
      m.action_kind,
      case when m.action_kind='ADVISORY'
             or nullif(m.reconciliation_balance->>'blocking_code','') is not null
             or coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED'
           when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'PENDING'
           when m.action_kind='NO_ACTION' then 'NO_ACTION' else 'READY' end action_category,
      'hr-row:'||m.id::text target_key,m.source_row_key source_identity,m.id hr_row_id,
      coalesce(m.resolved_timesheet_id,m.existing_shift_timesheet_id) timesheet_id,m.existing_shift_id shift_id,
      m.resolved_client_id client_id,m.resolved_candidate_id candidate_id,m.resolved_contract_id contract_id,
      null::uuid issue_id,m.evidence_hash evidence_fingerprint,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and nullif(m.reconciliation_balance->>'blocking_code','') is null
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) selectable,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and nullif(m.reconciliation_balance->>'blocking_code','') is null
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) default_selected,
      (m.action_kind in ('ADVISORY','DAILY_TIMESHEET_RESOLUTION')
        or nullif(m.reconciliation_balance->>'blocking_code','') is not null
        or coalesce((m.protection->>'active_pay_draft')::boolean,false)) blocking,
      jsonb_strip_nulls(jsonb_build_object(
        'reason_code',case
          when m.resolved_candidate_id is null then 'CANDIDATE_UNRESOLVED'
          when m.resolved_client_id is null then 'CLIENT_UNRESOLVED'
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then 'GRADE_MAPPING_REQUIRED'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT' then 'CONTRACT_MISSING'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH'
            and not coalesce(m.has_weekly_mapping,false) then 'GRADE_MAPPING_REQUIRED'
          when not m.is_daily and coalesce(m.weekly_resolution_action,'')<>'OK' then 'CONTRACT_OUT_OF_SCOPE'
          when not m.is_daily and coalesce(m.contract_count,0)=0 then 'CONTRACT_MISSING'
          when not m.is_daily and not coalesce(m.contract_route_eligible,false) then 'CONTRACT_OUT_OF_SCOPE'
          when not m.is_daily and coalesce(m.import_authoritative,false)
            and not coalesce(m.contract_rate_complete,false) then 'CONTRACT_RATES_INCOMPLETE'
          when not m.is_daily and coalesce(m.import_authoritative,false)
            and coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
            then 'TIMESHEET_OCCUPIED_BY_EXPENSES'
          when m.is_daily and coalesce(m.timesheet_count,0)=0
            and coalesce(m.daily_submitted_timesheet_count,0)=0 then 'DAILY_TIMESHEET_NOT_SUBMITTED'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'DAILY_SHIFT_ABSENT_FROM_TIMESHEET'
          when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_AMBIGUOUS'
          when nullif(m.reconciliation_balance->>'blocking_code','') is not null
            then m.reconciliation_balance->>'blocking_code'
          when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT'
          else null end,
        'source_system',m.source_system,'source_route',m.import_scope,'is_daily',m.is_daily,
        'existing_shift_id',m.existing_shift_id,
        'invoice_stream',m.reconciliation_balance->>'invoice_stream',
        'authority_mode',coalesce(m.authority_mode,case when m.is_daily or not coalesce(m.import_authoritative,false)
          then 'VALIDATION_ONLY' else 'AUTHORITATIVE' end),
        'authority_fingerprint',m.authority_fingerprint,
        'amendment_route',case
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false)
            then 'AMEND_EXISTING_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'effective_hours_net_is_positive')::boolean,false)
            and coalesce((m.reconciliation_balance->>'B_standard_representable')::boolean,false)
            then 'CREATE_REVERSAL_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.protection->>'paid')::boolean,false)
            then 'AMEND_PAID_UNINVOICED_SOURCE'
          when m.action_kind='APPLY_AMENDMENT' then 'AMEND_SOURCE'
          else null
        end,
        'reconciliation_mode',case
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'FROZEN_INVOICE_BALANCE'
          when m.action_kind='APPLY_AMENDMENT' then 'ORDINARY_SOURCE'
          else null end,
        'mutable_replacement_timesheet_id',coalesce(
          (select x.value::uuid from jsonb_array_elements_text(coalesce(m.reconciliation_balance->'active_mutable_member_ids','[]'::jsonb)) x(value)
            join public.timesheets mutable_ts on mutable_ts.timesheet_id=x.value::uuid and mutable_ts.correction_kind='CHANGED_HOURS_REPLACEMENT' limit 1),
          m.mutable_replacement_timesheet_id),
        'correction_id',m.reconciliation_balance->>'active_mutable_correction_id',
        'reviewed_existing_correction_id',m.reconciliation_balance->>'reviewed_existing_correction_id',
        'repair_identity_mode',m.reconciliation_balance->>'repair_identity_mode',
        'physically_missing_mutable_roles',coalesce(m.reconciliation_balance->'physically_missing_mutable_roles','[]'::jsonb),
        'archived_ignored_roles',coalesce(m.reconciliation_balance->'archived_history_roles','[]'::jsonb),
        'reversal_repair_required',coalesce((m.reconciliation_balance->>'reversal_repair_required')::boolean,false),
        'replacement_repair_required',coalesce((m.reconciliation_balance->>'replacement_repair_required')::boolean,false),
        'correction_generation_required',coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
          and not coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false),
        'standard_representable',coalesce((m.reconciliation_balance->>'B_standard_representable')::boolean,true),
        'B_hours',m.reconciliation_balance->'B_hours','B_financials',m.reconciliation_balance->'B_financials',
        'B_standard_schedule_json',m.reconciliation_balance->'B_standard_schedule_json',
        'B_policy_fingerprint',m.reconciliation_balance->>'B_policy_fingerprint',
        'review_policy_basis_kind','IMPORT_AUTHORITATIVE_WEEKLY_V1',
        'review_policy_basis_fingerprint',public._import_review_hash_v1(concat_ws('|','review-policy-basis-v1',
          m.reconciliation_balance->>'source_scope_fingerprint',m.reconciliation_balance->>'effective_invoice_fingerprint',
          m.reconciliation_balance->>'role_evidence_fingerprint',m.authority_fingerprint,
          m.reconciliation_balance->>'B_policy_fingerprint',m.reconciliation_balance->>'invoice_stream')),
        'effective_invoice_ids',m.reconciliation_balance->'effective_invoice_ids',
        'effective_invoice_line_ids',m.reconciliation_balance->'effective_invoice_line_ids',
        'M_hours',m.reconciliation_balance->'M_hours','M_existing_financials',m.reconciliation_balance->'M_existing_financials',
        'A_hours',m.reconciliation_balance->'A_hours','A_schedule_json',m.reconciliation_balance->'A_schedule_json',
        'effective_invoice_fingerprint',m.reconciliation_balance->>'effective_invoice_fingerprint',
        'mutable_generation_fingerprint',m.reconciliation_balance->>'active_mutable_fingerprint',
        'authoritative_evidence_fingerprint',m.reconciliation_balance->>'A_evidence_fingerprint',
        'reconciliation_fingerprint',m.reconciliation_balance->>'reconciliation_fingerprint',
        'source_scope_fingerprint',m.reconciliation_balance->>'source_scope_fingerprint'
      ) || jsonb_build_object(
        'archived_timesheet_ids',m.reconciliation_balance->'archived_timesheet_ids',
        'archived_history_timesheet_ids',m.reconciliation_balance->'archived_history_timesheet_ids',
        'archived_history_roles',m.reconciliation_balance->'archived_history_roles',
        'historical_missing_timesheet_ids',m.reconciliation_balance->'historical_missing_timesheet_ids',
        'active_mutable_member_ids',m.reconciliation_balance->'active_mutable_member_ids',
        'missing_mutable_roles',m.reconciliation_balance->'active_mutable_missing_roles',
        'active_mutable_parent_timesheet_id',m.reconciliation_balance->>'active_mutable_parent_timesheet_id',
        'pre_apply_authorised',m.source_authorised_at_server is not null or m.source_tsfin_authorised_at_utc is not null,
        'effective_auto_authorise',m.effective_auto_authorise,
        'intended_authorisation_action',case
          when m.source_authorised_at_server is not null or m.source_tsfin_authorised_at_utc is not null then 'REAUTHORISE'
          when m.effective_auto_authorise then 'AUTHORISE' else 'LEAVE_UNAUTHORISED' end,
        'financial_validation_mode',case
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'CORRECTION_NEGATIVE_MUST_REVERSE_FROZEN_B_AND_POSITIVE_TSFIN_DEFINES_A'
          when m.action_kind='APPLY_AMENDMENT' then 'ORDINARY_TSFIN_DEFINES_A' end,
        'candidate_name',m.staff_label,'client_name',m.client_label,'work_date',m.date_local,
        'week_ending_date',m.date_local + ((7-extract(dow from m.date_local)::integer)%7),
        'start_time',m.start_time_local,'end_time',m.end_time_local,
        'break_minutes',coalesce((m.payload_json->>'actual_break_mins')::integer,(m.payload_json->>'actual_break_minutes')::integer,
          (m.payload_json->>'break_mins')::integer,(m.payload_json->>'break_minutes')::integer),
        'hours_worked',m.hours_worked,'role',coalesce(m.weekly_incoming_code,m.assignment_grade_norm),
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',m.date_local,'start',m.start_time_local,'end',m.end_time_local,
          'break_minutes',coalesce((m.payload_json->>'actual_break_mins')::integer,(m.payload_json->>'actual_break_minutes')::integer,
            (m.payload_json->>'break_mins')::integer,(m.payload_json->>'break_minutes')::integer),
          'worked_hours',m.hours_worked,'worked_minutes',case when m.hours_worked is null then null else round(m.hours_worked*60) end,
          'reference',m.hr_request_id,'role',coalesce(m.weekly_incoming_code,m.assignment_grade_norm),'grade',m.grade_key)),
        'current_evidence',case when m.is_daily and m.resolved_timesheet_id is not null then jsonb_strip_nulls(jsonb_build_object(
          'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'start',m.worked_start_iso,'end',m.worked_end_iso,
          'break_minutes',m.ts_break_minutes,'elapsed_minutes',m.worked_minutes,
          'worked_minutes',greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0),
          'worked_hours',round(greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0)/60.0,2),
          'reference',m.reference_number,'role',m.tsfin_role,'band',m.tsfin_band,'timesheet_id',m.resolved_timesheet_id))
          when not m.is_daily and m.existing_shift_id is not null then jsonb_strip_nulls(jsonb_build_object(
          'work_date',m.date_local,'start',m.existing_shift_start_utc,'end',m.existing_shift_end_utc,
          'break_minutes',m.existing_shift_break_minutes,'worked_minutes',m.existing_shift_paid_minutes,
          'role',m.existing_shift_role,'timesheet_id',m.existing_shift_timesheet_id,'shift_id',m.existing_shift_id)) end,
        'difference_codes',to_jsonb(array_remove(array[
          case when m.existing_shift_id is null and not m.is_daily then 'NEW_SHIFT'::text end,
          case when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_SELECTION_REQUIRED'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.start_time_local is distinct from
            (m.worked_start_iso at time zone 'Europe/London')::time then 'START_TIME'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.end_time_local is distinct from
            (m.worked_end_iso at time zone 'Europe/London')::time then 'END_TIME'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null
            and coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            and (m.payload_json->>'break_mins')::integer is distinct from coalesce(m.ts_break_minutes,0)
            then 'BREAK_MINUTES'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.hours_worked is not null
            and abs((m.hours_worked*60)-greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0))>1
            then 'WORKED_HOURS'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.start_time_local is distinct from
            (m.existing_shift_start_utc at time zone 'Europe/London')::time then 'START_TIME'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.end_time_local is distinct from
            (m.existing_shift_end_utc at time zone 'Europe/London')::time then 'END_TIME'::text end,
          case when not m.is_daily and m.existing_shift_id is not null
            and coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            and (m.payload_json->>'break_mins')::integer is distinct from coalesce(m.existing_shift_break_minutes,0)
            then 'BREAK_MINUTES'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.hours_worked is not null
            and abs((m.hours_worked*60)-m.existing_shift_paid_minutes)>1 then 'WORKED_HOURS'::text end,
          case when not m.is_daily and coalesce((m.reconciliation_balance->>'financial_position_requires_amendment')::boolean,false)
            then 'FINANCIAL_POSITION'::text end
        ],null)),
        'outcome_label',case
          when not m.is_daily and not coalesce(m.import_authoritative,false) then 'Validate candidate timesheet'
          when m.is_daily and coalesce(m.timesheet_count,0)=0
            and coalesce(m.daily_submitted_timesheet_count,0)=0 then 'Request timesheet from candidate'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'Candidate timesheet states they did not work this shift'
          when not m.is_daily and coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
            then 'Timesheet occupied by expenses'
          when m.action_kind='INCLUDE_SHIFT' then 'TMS will add shift'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false)
            then 'TMS will repair current correction generation'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'TMS will create correction generation'
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.protection->>'paid')::boolean,false)
            then 'TMS will amend paid uninvoiced shift'
          when m.action_kind='APPLY_AMENDMENT' then 'TMS will amend shift'
          when m.action_kind='APPLY_CANCELLATION' then case when coalesce((m.protection->>'paid')::boolean,false)
            or coalesce((m.protection->>'invoice_locked')::boolean,false)
            then 'TMS will reverse shift' else 'TMS will cancel shift' end
          when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'Choose existing timesheet' when m.action_kind='NO_ACTION' then 'No action required'
          else 'Resolve before continuing' end,
        'resolution_kind',case
          when m.resolved_candidate_id is null then 'CANDIDATE_LINK'
          when m.resolved_client_id is null then 'CLIENT_LINK'
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then 'DAILY_GRADE_ROLE'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH'
            and not coalesce(m.has_weekly_mapping,false) then 'WEEKLY_ASSIGNMENT_CONTRACT'
          when m.is_daily and m.resolved_timesheet_id is null and coalesce(m.timesheet_count,0)>0 then 'DAILY_EXISTING_TIMESHEET' end,
        'resolution_options',case
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then m.daily_role_options
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH' then m.weekly_contract_options
          else '[]'::jsonb end,
        'mapping_evidence',case when m.is_daily then jsonb_strip_nulls(jsonb_build_object(
          'mapping_id',m.daily_mapping_id,'updated_at',m.daily_mapping_updated_at,'role',m.daily_mapped_role,'band',m.daily_mapped_band))
          else jsonb_strip_nulls(jsonb_build_object('mapping_fingerprint',m.weekly_mapping_evidence,
            'resolution_action',m.weekly_resolution_action,'resolution_reason',m.weekly_resolution_reason)) end,
        'timesheet_options',case when m.is_daily then to_jsonb(coalesce(m.timesheet_ids,array[]::uuid[])) else null end,
        'occupied_timesheet_id',case when coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
          then m.authoritative_target_timesheet_id end,
        'protection',m.protection
      )) summary_json
    from main_actions m
  )
  select * from rendered;

  -- Daily mismatch/query actions are independent of the evidence association.
  insert into pg_temp.import_review_catalog_v1
  with r as (
    select h.*,d.resolved_timesheet_id as timesheet_id,t.candidate_id,t.client_id,t.worked_start_iso,t.worked_end_iso,
      t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status::text,
      c.id contract_id,public._import_review_timesheet_protection_core_v1(d.resolved_timesheet_id) protection
    from public.hr_rows h
    join public.hr_imports i on i.id=h.import_id
    join public.import_review_daily_timesheet_resolutions d on d.import_id=h.import_id and d.hr_row_id=h.id and d.status in ('CURRENT','APPLIED')
    join public.v_timesheets_daily_match t on t.timesheet_id=d.resolved_timesheet_id
    left join public.contracts c on c.id=(select ts.contract_id from public.timesheets ts where ts.timesheet_id=t.timesheet_id)
    where h.import_id=p_import_id and (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
    order by h.id limit 501
  ), mismatch as (
    select r.*,
      case
        when r.hours_worked is not null and r.worked_minutes is not null
          and abs((r.hours_worked*60)-greatest(r.worked_minutes-coalesce(r.break_minutes,0),0))>1
          then 'ACTUAL_HOURS_MISMATCH'
        when r.start_time_local is distinct from (r.worked_start_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when r.end_time_local is distinct from (r.worked_end_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when coalesce((r.payload_json->>'break_evidence_supplied')::boolean,false)
          and (r.payload_json->>'break_mins')::integer is distinct from coalesce(r.break_minutes,0)
          then 'BREAK_MINUTES_MISMATCH'
      end reason_code
    from r
  ), issues as (
    select m.*,public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_DAILY',m.reason_code,m.timesheet_id,m.hr_request_id,
      lower(coalesce(m.staff_norm,'')),m.date_local,m.start_time_local,m.end_time_local,m.hours_worked,m.worked_minutes)) issue_fingerprint,
      jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
        'comparison_key','hr-row:'||m.id::text,
        'work_date',m.date_local,
        'match_status',m.reason_code,
        'timesheet_start',to_char(m.worked_start_iso at time zone 'Europe/London','HH24:MI'),
        'timesheet_end',to_char(m.worked_end_iso at time zone 'Europe/London','HH24:MI'),
        'timesheet_break_mins',m.break_minutes,
        'healthroster_start',to_char(m.start_time_local,'HH24:MI'),
        'healthroster_end',to_char(m.end_time_local,'HH24:MI'),
        'healthroster_break_mins',case
          when coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            then nullif(m.payload_json->>'break_mins','')::integer end,
        'healthroster_unit',coalesce(nullif(m.payload_json->>'Unit',''),nullif(m.payload_json->>'unit',''),
          nullif(m.unit_raw,''),nullif(m.unit_hint,'')),
        'healthroster_hospital',coalesce(nullif(m.payload_json->>'hospital_or_trust',''),
          nullif(m.payload_json->>'trust','')),
        'healthroster_request_grade',coalesce(nullif(m.payload_json->>'Request Grade',''),
          nullif(m.payload_json->>'Request_Grade',''),nullif(m.payload_json->>'grade_raw',''),
          nullif(m.assignment_grade_norm,'')),
        'ref_before',m.reference_number,
        'ref_after',m.hr_request_id
      ))) email_comparisons,
      lower(btrim(case when coalesce(m.contract_id is not null and
        (select c.send_ts_queries_to_different_email from public.contracts c where c.id=m.contract_id),false)
        then (select c.ts_queries_alt_email_address from public.contracts c where c.id=m.contract_id)
        else (select c.ts_queries_email from public.clients c where c.id=m.client_id) end)) route_email
    from mismatch m where m.reason_code is not null
  )
  select
    public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
      case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,i.issue_fingerprint)),
    case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,'EMAIL',
    'issue:'||i.issue_fingerprint,i.issue_fingerprint,i.id,i.timesheet_id,null::uuid,i.client_id,i.candidate_id,i.contract_id,e.id,
    public._import_review_hash_v1(concat_ws('|','issue-evidence-v2',i.issue_fingerprint,i.email_comparisons::text,i.protection::text,
      coalesce(e.delivery_history_status,'NEW'),coalesce(e.sent_count,0),
      case when coalesce(i.contract_id is not null and (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then (select concat_ws('|',c.updated_at,c.ts_queries_alt_email_address) from public.contracts c where c.id=i.contract_id)
        else (select concat_ws('|',c.rev,c.updated_at,c.ts_queries_email) from public.clients c where c.id=i.client_id) end)),
    not coalesce((i.protection->>'active_pay_draft')::boolean,false) and length(coalesce(i.route_email,'')) between 3 and 320 and position('@' in i.route_email)>1,
    e.id is null and not coalesce((i.protection->>'active_pay_draft')::boolean,false) and length(coalesce(i.route_email,'')) between 3 and 320 and position('@' in i.route_email)>1,
    false,
    jsonb_build_object('reason_code',i.reason_code,'issue_fingerprint',i.issue_fingerprint,'work_date',i.date_local,
      'candidate_name',i.staff_raw,'timesheet_id',i.timesheet_id,'recipient_scope_key',
      case when coalesce(i.contract_id is not null and (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then 'CONTRACT_OVERRIDE:'||i.contract_id::text else 'CLIENT_DEFAULT:'||i.client_id::text end,
      'recipient_route_fingerprint',case when coalesce(i.contract_id is not null and
        (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CONTRACT_OVERRIDE:'||c.id::text,
          lower(btrim(coalesce(c.ts_queries_alt_email_address,''))),c.updated_at)) from public.contracts c where c.id=i.contract_id)
        else (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CLIENT_DEFAULT:'||c.id::text,
          lower(btrim(coalesce(c.ts_queries_email,''))),c.rev,c.updated_at)) from public.clients c where c.id=i.client_id) end,
      'delivery_history_status',coalesce(e.delivery_history_status,'NEW'),'sent_count',coalesce(e.sent_count,0),
      'comparisons',i.email_comparisons,
      'default_excluded_reason',case when e.id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
        when length(coalesce(i.route_email,'')) not between 3 and 320 or position('@' in coalesce(i.route_email,''))<=1 then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
        when coalesce((i.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
      'protection',i.protection)
  from issues i left join public.hr_issue_emails e on e.issue_fingerprint=i.issue_fingerprint;

  -- Weekly validation-only issues use the installed comparison engine, but
  -- normalise every user choice into the same server-owned decision catalogue.
  if exists(select 1 from public.hr_imports i where i.id=p_import_id
      and i.source_system='HEALTHROSTER'::public.hr_source_enum
      and upper(coalesce(i.import_scope,'HR_WEEKLY')) not like '%DAILY%') then
    v_weekly_preview:=public.hr_weekly_validation_preview(p_import_id);

    -- Validation-only Weekly evidence has two distinct, server-proven states.
    -- Neither state is an instruction to mutate CloudTMS financial records.
    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
    ), eligible_validation_groups as (
      select d.candidate_id,d.summary_json->>'week_ending_date' week_ending_date
      from pg_temp.import_review_catalog_v1 d
      where d.candidate_id is not null
        and d.summary_json->>'source_route' not like '%DAILY%'
        and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
      group by d.candidate_id,d.summary_json->>'week_ending_date'
      having bool_and(d.action_kind='NO_ACTION' and not d.blocking)
    ), missing_timesheets as (
      select p.row_json,p.timesheet_id,p.candidate_id,
        d.hr_row_id shift_hr_row_id,d.client_id shift_client_id,
        d.contract_id shift_contract_id,d.source_identity shift_source_identity,
        d.evidence_fingerprint shift_evidence_fingerprint,d.summary_json shift_summary_json
      from preview_rows p
      join eligible_validation_groups g on g.candidate_id=p.candidate_id
        and g.week_ending_date=p.row_json->>'week_ending_date'
      join pg_temp.import_review_catalog_v1 d on d.candidate_id=p.candidate_id
        and d.summary_json->>'week_ending_date'=p.row_json->>'week_ending_date'
        and d.summary_json->>'source_route' not like '%DAILY%'
        and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
        and d.action_kind='NO_ACTION' and not d.blocking
      where p.row_json->>'overall_status'='MISSING_TIMESHEET'
    ), omitted_shifts as (
      select p.*,cx.value comparison_json
      from preview_rows p
      join eligible_validation_groups g on g.candidate_id=p.candidate_id
        and g.week_ending_date=p.row_json->>'week_ending_date'
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
      where p.timesheet_id is not null and cx.value->>'match_status'='HR_ONLY'
    ), confirmed_exceptions as (
      select p.*,cx.value exception_json
      from preview_rows p
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'confirmed_exceptions','[]'::jsonb)) cx(value)
      where p.timesheet_id is not null
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_TIMESHEET_NOT_SUBMITTED',m.shift_hr_row_id)),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-timesheet-not-submitted',m.shift_hr_row_id),
      m.shift_source_identity,
      m.shift_hr_row_id,null::uuid,null::uuid,m.shift_client_id,m.candidate_id,m.shift_contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-timesheet-not-submitted-v2',
        m.shift_evidence_fingerprint,m.row_json::text)),
      false,false,true,
      jsonb_strip_nulls(m.shift_summary_json||jsonb_build_object(
        'reason_code','WEEKLY_TIMESHEET_NOT_SUBMITTED','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'candidate_name',m.row_json->>'candidate_name','week_ending_date',m.row_json->>'week_ending_date',
        'difference_codes',jsonb_build_array('TIMESHEET_NOT_SUBMITTED'),
        'outcome_label','Request timesheet from candidate'))
    from missing_timesheets m
    union all
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_CANDIDATE_DID_NOT_WORK',o.comparison_json->>'hr_row_id')),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-candidate-did-not-work',o.comparison_json->>'hr_row_id'),
      concat_ws('|',o.timesheet_id,o.comparison_json->>'work_date',
        o.comparison_json->>'healthroster_start',o.comparison_json->>'healthroster_end'),
      nullif(o.comparison_json->>'hr_row_id','')::uuid,o.timesheet_id,null::uuid,o.client_id,o.candidate_id,o.contract_id,null::uuid,
      o.comparison_json->>'exception_evidence_fingerprint',
      false,false,true,
      jsonb_build_object(
        'reason_code','WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'resolution_kind','WEEKLY_CANDIDATE_DID_NOT_WORK',
        'candidate_name',o.row_json->>'candidate_name','week_ending_date',o.row_json->>'week_ending_date',
        'work_date',o.comparison_json->>'work_date',
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',o.comparison_json->>'work_date','start',o.comparison_json->>'healthroster_start',
          'end',o.comparison_json->>'healthroster_end',
          'break_minutes',nullif(o.comparison_json->>'healthroster_break_mins','')::integer,
          'reference',o.comparison_json->>'ref_after')),
        'current_evidence',jsonb_build_object('timesheet_id',o.timesheet_id),
        'difference_codes',jsonb_build_array('HR_ONLY'),
        'outcome_label','Confirm candidate did not work this shift')
    from omitted_shifts o
    union all
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_CANDIDATE_DID_NOT_WORK',c.exception_json->>'hr_row_id')),
      'NO_ACTION','NO_ACTION',
      concat_ws(':','weekly-candidate-did-not-work',c.exception_json->>'hr_row_id'),
      concat_ws('|',c.timesheet_id,c.exception_json->>'work_date',
        c.exception_json->>'healthroster_start',c.exception_json->>'healthroster_end'),
      nullif(c.exception_json->>'hr_row_id','')::uuid,c.timesheet_id,null::uuid,c.client_id,c.candidate_id,c.contract_id,null::uuid,
      c.exception_json->>'evidence_fingerprint',
      false,false,false,
      jsonb_build_object(
        'reason_code','CANDIDATE_DID_NOT_WORK_CONFIRMED','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'resolution_kind','WEEKLY_CANDIDATE_DID_NOT_WORK',
        'candidate_name',c.row_json->>'candidate_name','week_ending_date',c.row_json->>'week_ending_date',
        'validation_total_shift_count',jsonb_array_length(coalesce(c.row_json->'comparisons','[]'::jsonb))
          + coalesce(nullif(c.row_json->>'confirmed_exception_count','')::integer,0),
        'confirmed_exception_count',coalesce(nullif(c.row_json->>'confirmed_exception_count','')::integer,0),
        'work_date',c.exception_json->>'work_date',
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',c.exception_json->>'work_date','start',c.exception_json->>'healthroster_start',
          'end',c.exception_json->>'healthroster_end',
          'break_minutes',nullif(c.exception_json->>'healthroster_break_mins','')::integer,
          'reference',c.exception_json->>'reference')),
        'current_evidence',jsonb_build_object('timesheet_id',c.timesheet_id),
        'difference_codes',jsonb_build_array('CONFIRMED_EXCEPTION'),
        'outcome_label','Passed with confirmed exception')
    from confirmed_exceptions c;

    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id,
        nullif(r.value->>'issue_fingerprint','') issue_fingerprint
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
    ), email_filtered as (
      select p.*,
        coalesce((select jsonb_agg(cx.value||jsonb_strip_nulls(jsonb_build_object(
            'healthroster_unit',coalesce(nullif(hr.payload_json->>'Unit',''),nullif(hr.payload_json->>'unit',''),
              nullif(hr.unit_raw,''),nullif(hr.unit_hint,''),nullif(cx.value->>'location_after','')),
            'healthroster_hospital',coalesce(nullif(hr.payload_json->>'hospital_or_trust',''),
              nullif(hr.payload_json->>'trust','')),
            'healthroster_request_grade',coalesce(nullif(hr.payload_json->>'Request Grade',''),
              nullif(hr.payload_json->>'Request_Grade',''),nullif(hr.payload_json->>'grade_raw',''),
              nullif(hr.assignment_grade_norm,''))
          )) order by cx.value->>'work_date',cx.value->>'comparison_key')
          from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
          left join public.hr_rows hr on hr.id=nullif(cx.value->>'hr_row_id','')::uuid
          where (
            coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
            or coalesce((cx.value->>'ref_changed')::boolean,false)
          )),'[]'::jsonb) email_comparisons,
        coalesce((select jsonb_agg(day_json.value order by day_json.value->>'date')
          from jsonb_array_elements(coalesce(p.row_json->'days','[]'::jsonb)) day_json(value)
          where exists (
            select 1
            from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
            where cx.value->>'work_date'=day_json.value->>'date'
              and (
                coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
                or coalesce((cx.value->>'ref_changed')::boolean,false)
              )
          )),'[]'::jsonb) email_days,
        coalesce((select jsonb_agg(to_jsonb(fr.value))
          from jsonb_array_elements_text(coalesce(p.row_json->'failure_reasons','[]'::jsonb)) fr(value)
          where fr.value<>'HealthRoster has a shift not present on the timesheet.'),'[]'::jsonb) email_failure_reasons
      from preview_rows p
    ), routed as (
      select p.*,public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_WEEKLY','validation-email-v2',
          p.timesheet_id,p.row_json->>'week_ending_date',p.email_comparisons::text)) email_issue_fingerprint,
        c.rev client_rev,c.updated_at client_updated_at,c.ts_queries_email,
        ct.send_ts_queries_to_different_email,ct.ts_queries_alt_email_address,ct.updated_at contract_updated_at,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE:'||ct.id::text else 'CLIENT_DEFAULT:'||c.id::text end recipient_scope_key,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE' else 'CLIENT_DEFAULT' end recipient_scope,
        lower(btrim(case when coalesce(ct.send_ts_queries_to_different_email,false)
          then ct.ts_queries_alt_email_address else c.ts_queries_email end)) recipient_email,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection,
        e.id issue_id,e.delivery_history_status,e.sent_count
      from email_filtered p
      join public.clients c on c.id=p.client_id
      left join public.contracts ct on ct.id=p.contract_id and ct.client_id=p.client_id
      left join public.hr_issue_emails e on e.issue_fingerprint=public._import_review_hash_v1(concat_ws('|',
        'HEALTHROSTER_WEEKLY','validation-email-v2',p.timesheet_id,p.row_json->>'week_ending_date',p.email_comparisons::text))
      where p.timesheet_id is not null and p.issue_fingerprint is not null
        and coalesce((p.row_json->>'has_mismatch')::boolean,false)
        and exists (
          select 1 from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
          where coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
            or coalesce((cx.value->>'ref_changed')::boolean,false)
        )
    ), email_actions as (
      select r.*,
        public._import_review_hash_v1(concat_ws('|','query-route-v1',r.recipient_scope_key,r.recipient_email,
          case when r.recipient_scope='CONTRACT_OVERRIDE' then r.contract_updated_at::text
            else concat_ws('|',r.client_rev,r.client_updated_at) end)) route_fingerprint,
        length(coalesce(r.recipient_email,'')) between 3 and 320
          and r.recipient_email~* '^[A-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+$' valid_email
      from routed r
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,a.email_issue_fingerprint)),
      case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,
      case when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED' else 'EMAIL' end,
      'issue:'||a.email_issue_fingerprint,a.email_issue_fingerprint,null::uuid,a.timesheet_id,null::uuid,
      a.client_id,a.candidate_id,a.contract_id,a.issue_id,
      public._import_review_hash_v1(concat_ws('|','weekly-query-evidence-v2',a.timesheet_id,
        a.row_json->>'candidate_name',a.row_json->>'week_ending_date',
        a.email_comparisons::text,a.email_days::text,a.email_failure_reasons::text,a.protection::text,
        a.route_fingerprint,coalesce(a.delivery_history_status,'NEW'),coalesce(a.sent_count,0))),
      a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      a.issue_id is null and a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      coalesce((a.protection->>'active_pay_draft')::boolean,false),
      jsonb_build_object('reason_code','HEALTHROSTER_WEEKLY','issue_fingerprint',a.email_issue_fingerprint,
        'candidate_name',a.row_json->>'candidate_name','week_ending_date',a.row_json->>'week_ending_date',
        'validation_total_shift_count',jsonb_array_length(coalesce(a.row_json->'comparisons','[]'::jsonb))
          + coalesce(nullif(a.row_json->>'confirmed_exception_count','')::integer,0),
        'validation_difference_count',jsonb_array_length(a.email_comparisons),
      'failure_reasons',a.email_failure_reasons,
        'days',a.email_days,'comparisons',a.email_comparisons,
        'evidence_rows',coalesce((
          select jsonb_agg(jsonb_build_object(
            'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
              'work_date',cx.value->>'work_date','start',cx.value->>'healthroster_start',
              'end',cx.value->>'healthroster_end','break_minutes',nullif(cx.value->>'healthroster_break_mins','')::integer,
              'worked_minutes',nullif(day_json.value->>'hr_minutes','')::integer,'reference',cx.value->>'ref_after')),
            'current_evidence',jsonb_strip_nulls(jsonb_build_object(
              'work_date',cx.value->>'work_date','start',cx.value->>'timesheet_start',
              'end',cx.value->>'timesheet_end','break_minutes',nullif(cx.value->>'timesheet_break_mins','')::integer,
              'worked_minutes',nullif(day_json.value->>'ts_minutes','')::integer,'reference',cx.value->>'ref_before')),
            'difference_codes',to_jsonb(array_remove(array[
              case when coalesce(cx.value->>'match_status','MATCH')<>'MATCH' then cx.value->>'match_status' end,
              case when coalesce((cx.value->>'ref_changed')::boolean,false) then 'REFERENCE' end,
              case when coalesce(day_json.value->>'day_status','OK')<>'OK' then 'WORKED_HOURS' end
            ],null))
          ) order by cx.value->>'work_date',cx.value->>'comparison_key')
          from jsonb_array_elements(a.email_comparisons) cx(value)
          left join lateral (select d.value from jsonb_array_elements(coalesce(a.row_json->'days','[]'::jsonb)) d(value)
            where d.value->>'date'=cx.value->>'work_date' limit 1) day_json on true
        ),'[]'::jsonb),
        'outcome_label',case when a.issue_id is null then 'Request amend shift' else 'Request amend shift reminder' end,
        'recipient_scope_key',a.recipient_scope_key,'recipient_route_fingerprint',a.route_fingerprint,
        'delivery_history_status',coalesce(a.delivery_history_status,'NEW'),'sent_count',coalesce(a.sent_count,0),
        'default_excluded_reason',case when a.issue_id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
          when not a.valid_email then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
          when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
        'protection',a.protection)
    from email_actions a;

    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
      where nullif(r.value->>'timesheet_id','') is not null
    ), invalidations as (
      select p.*,cx.value comparison_json,nullif(btrim(cx.value->>'comparison_key'),'') comparison_key,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection
      from preview_rows p
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
      where coalesce((cx.value->>'is_destructive_invalidation')::boolean,false)
        and exists(select 1 from public.hr_imports hi where hi.id=p_import_id
          and hi.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES'))
        and nullif(btrim(cx.value->>'comparison_key'),'') is not null
        and nullif(btrim(cx.value->>'ref_before'),'') is not null
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'INVALIDATE_REFERENCE',i.timesheet_id,i.comparison_key)),
      'INVALIDATE_REFERENCE','PENDING','timesheet:'||i.timesheet_id::text||':'||i.comparison_key,
      i.comparison_key,null::uuid,i.timesheet_id,null::uuid,i.client_id,i.candidate_id,i.contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-reference-invalidation-v1',i.timesheet_id,i.comparison_json::text,i.protection::text)),
      not coalesce((i.protection->>'protected')::boolean,false),false,false,
      jsonb_build_object('reason_code','REFERENCE_ON_SHIFT_MISSING_OR_MISMATCHED_IN_COMPLETE_IMPORT',
        'candidate_name',i.row_json->>'candidate_name','week_ending_date',i.row_json->>'week_ending_date',
        'timesheet_id',i.timesheet_id,'comparison_key',i.comparison_key,'comparison',i.comparison_json,
        'protection',i.protection,'default_excluded_reason','REFERENCE_INVALIDATION_REQUIRES_EXPLICIT_SELECTION')
    from invalidations i;
  end if;

  -- Complete Daily coverage also exposes existing timesheets that are absent
  -- from the file.  Missing rows are query-email candidates; reference
  -- invalidation is a separate, explicit, default-off decision.
  insert into pg_temp.import_review_catalog_v1
  with i as (
    select * from public.hr_imports where id=p_import_id
  ), missing as (
    select t.*,ts.contract_id,c.first_name,c.last_name,cl.name as client_name,
      public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection,
      lower(btrim(case when coalesce(ts.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=ts.contract_id),false)
        then (select ct.ts_queries_alt_email_address from public.contracts ct where ct.id=ts.contract_id)
        else cl.ts_queries_email end)) route_email,
      public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_DAILY','MISSING_FROM_IMPORT',
        t.timesheet_id,t.candidate_id,t.client_id,(t.worked_start_iso at time zone 'Europe/London')::date,
        coalesce(t.reference_number,''))) issue_fingerprint
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join public.candidates c on c.id=t.candidate_id
    join public.clients cl on cl.id=t.client_id
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists (
        select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED')
      )
    order by t.timesheet_id limit 501
  )
  select
    public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
      case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,m.issue_fingerprint)),
    case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,'EMAIL',
    'issue:'||m.issue_fingerprint,m.issue_fingerprint,null::uuid,m.timesheet_id,null::uuid,
    m.client_id,m.candidate_id,m.contract_id,e.id,
    public._import_review_hash_v1(concat_ws('|','missing-daily-email-v1',m.issue_fingerprint,m.protection::text,
      coalesce(e.delivery_history_status,'NEW'),coalesce(e.sent_count,0),
      case when coalesce(m.contract_id is not null and (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then (select concat_ws('|',ct.updated_at,ct.ts_queries_alt_email_address) from public.contracts ct where ct.id=m.contract_id)
        else (select concat_ws('|',cl.rev,cl.updated_at,cl.ts_queries_email) from public.clients cl where cl.id=m.client_id) end)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false) and length(coalesce(m.route_email,'')) between 3 and 320 and position('@' in m.route_email)>1,
    e.id is null and not coalesce((m.protection->>'active_pay_draft')::boolean,false) and length(coalesce(m.route_email,'')) between 3 and 320 and position('@' in m.route_email)>1,false,
    jsonb_build_object('reason_code','MISSING_FROM_IMPORT','issue_fingerprint',m.issue_fingerprint,
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,
      'week_ending_date',(m.worked_start_iso at time zone 'Europe/London')::date
        + ((7-extract(dow from (m.worked_start_iso at time zone 'Europe/London')::date)::integer)%7),
      'candidate_name',btrim(concat_ws(' ',m.first_name,m.last_name)),'client_name',m.client_name,
      'timesheet_id',m.timesheet_id,'reference_number',m.reference_number,
      'start_time',(m.worked_start_iso at time zone 'Europe/London')::time,
      'end_time',(m.worked_end_iso at time zone 'Europe/London')::time,
      'break_minutes',m.break_minutes,'role',m.tsfin_role,
      'recipient_scope_key',case when coalesce(m.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then 'CONTRACT_OVERRIDE:'||m.contract_id::text else 'CLIENT_DEFAULT:'||m.client_id::text end,
      'recipient_route_fingerprint',case when coalesce(m.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CONTRACT_OVERRIDE:'||ct.id::text,
          lower(btrim(coalesce(ct.ts_queries_alt_email_address,''))),ct.updated_at)) from public.contracts ct where ct.id=m.contract_id)
        else (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CLIENT_DEFAULT:'||cl.id::text,
          lower(btrim(coalesce(cl.ts_queries_email,''))),cl.rev,cl.updated_at)) from public.clients cl where cl.id=m.client_id) end,
      'delivery_history_status',coalesce(e.delivery_history_status,'NEW'),'sent_count',coalesce(e.sent_count,0),
      'default_excluded_reason',case when e.id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
        when length(coalesce(m.route_email,'')) not between 3 and 320 or position('@' in coalesce(m.route_email,''))<=1 then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
        when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
      'protection',m.protection)
  from missing m left join public.hr_issue_emails e on e.issue_fingerprint=m.issue_fingerprint;

  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select t.*,ts.contract_id,public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists(select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED'))
    order by t.timesheet_id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'MARK_VALIDATION_ERROR',m.timesheet_id)),
    'MARK_VALIDATION_ERROR','READY','timesheet:'||m.timesheet_id::text,'missing-daily:'||m.timesheet_id::text,
    null::uuid,m.timesheet_id,null::uuid,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-daily-validation-v1',m.timesheet_id,m.worked_start_iso,
      m.worked_end_iso,m.break_minutes,m.worked_minutes,m.reference_number,m.protection::text)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    coalesce((m.protection->>'active_pay_draft')::boolean,false),
    jsonb_build_object('reason_code',case when coalesce((m.protection->>'active_pay_draft')::boolean,false)
      then 'BLOCKED_ACTIVE_PAY_DRAFT' else 'MISSING_FROM_IMPORT' end,
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'timesheet_id',m.timesheet_id,
      'reference_number',m.reference_number,'start_time',(m.worked_start_iso at time zone 'Europe/London')::time,
      'end_time',(m.worked_end_iso at time zone 'Europe/London')::time,'break_minutes',m.break_minutes,
      'hours_worked',m.worked_minutes/60.0,'role',m.tsfin_role,'protection',m.protection)
  from missing m;

  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select t.*,ts.contract_id,public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and nullif(btrim(t.reference_number),'') is not null
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists (select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED'))
    order by t.timesheet_id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'INVALIDATE_REFERENCE',m.timesheet_id)),
    'INVALIDATE_REFERENCE','PENDING','timesheet:'||m.timesheet_id::text,'missing-daily:'||m.timesheet_id::text,
    null::uuid,m.timesheet_id,null::uuid,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-daily-reference-v1',m.timesheet_id,m.reference_number,m.protection::text)),
    not coalesce((m.protection->>'protected')::boolean,false),false,false,
    jsonb_build_object('reason_code','REFERENCE_ON_SHIFT_MISSING_FROM_COMPLETE_IMPORT',
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'timesheet_id',m.timesheet_id,
      'reference_number',m.reference_number,'protection',m.protection,
      'default_excluded_reason','REFERENCE_INVALIDATION_REQUIRES_EXPLICIT_SELECTION')
  from missing m;

  -- Omitted existing shifts are proposed only inside immutable complete coverage.
  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select s.*,public._import_review_timesheet_protection_core_v1(s.timesheet_id) protection
    from public.nhsp_shifts s
    join i on true
    cross join lateral public._import_review_effective_authority_core_v1(
      case when i.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end,
      s.contract_id,s.client_id,coalesce(s.week_ending_date,s.work_date)) authority
    where i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and s.source_system=i.source_system
      and authority.import_authoritative
      and s.cancelled_at_utc is null
      and s.work_date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or s.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=s.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists (
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=s.candidate_id))
      and not exists (select 1 from public.hr_rows h where h.import_id=i.id and h.external_row_key=s.external_row_key)
    order by s.id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'APPLY_CANCELLATION',m.id)),
    'APPLY_CANCELLATION','READY','shift:'||m.id::text,m.external_row_key,null::uuid,m.timesheet_id,m.id,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-shift-v1',m.id,m.updated_at,m.timesheet_id,m.protection::text)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    coalesce((m.protection->>'active_pay_draft')::boolean,false),
    jsonb_build_object('reason_code',case when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' else 'MISSING_FROM_COMPLETE_IMPORT' end,
      'work_date',m.work_date,'week_ending_date',m.week_ending_date,'candidate_id',m.candidate_id,'client_id',m.client_id,
      'start_time',m.start_utc,'end_time',m.end_utc,'break_minutes',m.break_mins,'role',m.assignment_code,'protection',m.protection)
  from missing m;

  -- Query emails can be committed only with one exact, complete, current
  -- timesheet PDF.  Invoice-linked validation records are never eligible for
  -- validation.  The same evidence fingerprint is frozen into the decision so
  -- document or invoice lifecycle movement makes a reviewed action stale.
  with evidence as materialized (
    select c.action_id,public._import_review_query_evidence_core_v1(c.timesheet_id) evidence_json
    from pg_temp.import_review_catalog_v1 c
    where c.timesheet_id is not null
      and (
        c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
        or c.summary_json->>'authority_mode'='VALIDATION_ONLY'
        or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
      )
  )
  update pg_temp.import_review_catalog_v1 c
  set evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','query-evidence-decision-v1',
        c.evidence_fingerprint,e.evidence_json->>'evidence_fingerprint')),
      action_category=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then 'PENDING'
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then 'PENDING'
        else c.action_category end,
      selectable=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then false
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then false
        else c.selectable end,
      default_selected=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then false
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then false
        else c.default_selected end,
      blocking=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then true
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then true
        else c.blocking end,
      summary_json=c.summary_json||jsonb_build_object(
        'attachment_evidence',e.evidence_json,
        'attachment_fingerprint',e.evidence_json->>'evidence_fingerprint'
      )||case
        when (
          c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null
        ) or (
          (
            c.summary_json->>'authority_mode'='VALIDATION_ONLY'
            or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
          ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED'
        ) then jsonb_build_object(
          'reason_code',e.evidence_json->>'reason_code',
          'default_excluded_reason',e.evidence_json->>'reason_code',
          'outcome_label',case e.evidence_json->>'reason_code'
            when 'TIMESHEET_PRESENT_BUT_INVOICED' then 'Timesheet present but invoiced'
            when 'TIMESHEET_EVIDENCE_PREPARING' then 'Preparing timesheet evidence'
            else 'Timesheet evidence incomplete' end
        ) else '{}'::jsonb end
  from evidence e
  where e.action_id=c.action_id;

  -- An invoice-linked validation shift owns this blocker.  Do not also expose
  -- the aggregate query-email action as a second visual copy of the same
  -- shift.  Preserve any delivery history on the owning shift before removing
  -- only that redundant email row; genuine query emails remain untouched.
  update pg_temp.import_review_catalog_v1 owner_row
  set summary_json=owner_row.summary_json||jsonb_strip_nulls(jsonb_build_object(
        'delivery_history_status',email_row.summary_json->>'delivery_history_status',
        'sent_count',nullif(email_row.summary_json->>'sent_count','')::integer,
        'previously_queried',coalesce(nullif(email_row.summary_json->>'sent_count','')::integer,0)>0
      ))
  from pg_temp.import_review_catalog_v1 email_row
  where owner_row.timesheet_id=email_row.timesheet_id
    and owner_row.action_id<>email_row.action_id
    and owner_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and owner_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED'
    and email_row.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
    and email_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED';

  delete from pg_temp.import_review_catalog_v1 email_row
  using pg_temp.import_review_catalog_v1 owner_row
  where email_row.timesheet_id=owner_row.timesheet_id
    and email_row.action_id<>owner_row.action_id
    and email_row.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
    and email_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED'
    and owner_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and owner_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED';

  -- Daily validation is atomic per Daily timesheet.  An email, document hold
  -- or invoice blocker for that record prevents only that record from entering
  -- validation and TSFIN work.
  update pg_temp.import_review_catalog_v1 current_row
  set selectable=false,
      default_selected=false,
      evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','daily-validation-held-v1',
        current_row.evidence_fingerprint,current_row.timesheet_id)),
      summary_json=current_row.summary_json||jsonb_build_object(
        'daily_validation_held',true,
        'validation_hold_label','Validation held: resolve this Daily timesheet first'
      )
  where current_row.action_kind='NO_ACTION'
    and coalesce(nullif(current_row.summary_json->>'is_daily','')::boolean,false)
    and current_row.timesheet_id is not null
    and exists (
      select 1 from pg_temp.import_review_catalog_v1 hold
      where hold.timesheet_id=current_row.timesheet_id
        and hold.action_id<>current_row.action_id
        and (hold.blocking or hold.action_category in ('EMAIL','PENDING','BLOCKED'))
    );

  -- Weekly validation is all-or-nothing per candidate/client/contract/week.
  -- One mismatch, unresolved exception, missing attachment or invoice blocker
  -- holds the whole Weekly timesheet while leaving the actual issue visible.
  update pg_temp.import_review_catalog_v1 current_row
  set selectable=false,
      default_selected=false,
      evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','weekly-validation-held-v2',
        current_row.evidence_fingerprint,current_row.candidate_id,current_row.client_id,
        current_row.contract_id,current_row.summary_json->>'week_ending_date')),
      summary_json=current_row.summary_json||jsonb_build_object(
        'weekly_validation_held',true,
        'weekly_validation_badge_code','WEEKLY_VALIDATION_HELD',
        'weekly_validation_badge_label','Validation held: resolve outstanding shift',
        'validation_hold_label','Validation held: one or more shifts still require action'
      )
  where current_row.action_kind='NO_ACTION'
    and current_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and current_row.summary_json->>'source_route' not like '%DAILY%'
    and exists (
      select 1 from pg_temp.import_review_catalog_v1 hold
      where hold.candidate_id=current_row.candidate_id
        and hold.client_id=current_row.client_id
        and hold.contract_id is not distinct from current_row.contract_id
        and hold.summary_json->>'week_ending_date'=current_row.summary_json->>'week_ending_date'
        and hold.action_id<>current_row.action_id
        and (hold.blocking or hold.action_category in ('EMAIL','PENDING','BLOCKED'))
    );

  select count(*) into v_count from pg_temp.import_review_catalog_v1;
  if v_count>p_max_actions then
    raise exception 'IMPORT_REVIEW_ACTION_LIMIT_EXCEEDED' using errcode='54000',
      detail=jsonb_build_object('count',v_count,'max',p_max_actions)::text;
  end if;

  return query select c.action_id,c.action_kind,c.action_category,c.target_key,c.source_identity,
    c.hr_row_id,c.timesheet_id,c.shift_id,c.client_id,c.candidate_id,c.contract_id,c.issue_id,
    c.evidence_fingerprint,c.selectable,c.default_selected,c.blocking,c.summary_json
  from pg_temp.import_review_catalog_v1 c order by c.action_id;
end
$function$;

create or replace function public._import_review_refresh_core_v1(
  p_import_id uuid,
  p_expected_state_version bigint,
  p_actor_user_id uuid,
  p_max_actions integer default 5000
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_state public.import_review_states%rowtype;
  v_generation integer;
  v_fingerprint text;
  v_changed integer:=0; v_retired integer:=0; v_inserted integer:=0;
  v_blockers integer; v_selected integer; v_status text; v_auto integer:=0;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  select * into v_state from public.import_review_states where import_id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v_state.status not in ('STAGED','IN_REVIEW','BLOCKED','READY') then
    raise exception 'IMPORT_REVIEW_REFRESH_NOT_ALLOWED' using errcode='55000',detail=v_state.status;
  end if;
  if p_expected_state_version is not null and v_state.state_version<>p_expected_state_version then
    raise exception 'IMPORT_REVIEW_VERSION_CONFLICT' using errcode='40001',detail=v_state.state_version::text;
  end if;
  v_generation:=v_state.preview_generation+1;

  create temporary table if not exists pg_temp.review_next_actions on commit drop as
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_generation,p_max_actions) with no data;
  truncate pg_temp.review_next_actions;
  insert into pg_temp.review_next_actions
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_generation,p_max_actions);

  delete from pg_temp.review_next_actions n
  using public.import_review_action_outcomes o
  where o.import_id=p_import_id and o.action_id=n.action_id;

  -- Persist database-unambiguous Daily associations in the same normalised table.
  -- This is evidence linkage only and never edits the selected timesheet.
  insert into public.import_review_daily_timesheet_resolutions(
    import_id,hr_row_id,resolved_timesheet_id,resolution_method,status,evidence_fingerprint,
    preview_generation,state_version,selected_by_user_id
  )
  select p_import_id,n.hr_row_id,n.timesheet_id,'AUTO_MATCHED','CURRENT',
    public._import_review_hash_v1(concat_ws('|','daily-resolution-evidence-v1',
      n.source_identity,n.hr_row_id,n.timesheet_id,n.candidate_id,n.client_id,n.contract_id,
      coalesce((n.summary_json->'imported_evidence')::text,''),
      coalesce((n.summary_json->'current_evidence')::text,''),
      coalesce((n.summary_json->'mapping_evidence')::text,''),
      coalesce(n.summary_json->>'authority_fingerprint',''))),
    v_generation,v_state.state_version,p_actor_user_id
  from pg_temp.review_next_actions n
  where n.action_kind='NO_ACTION' and n.hr_row_id is not null and n.timesheet_id is not null
    and coalesce((n.summary_json->>'is_daily')::boolean,false)
  on conflict(import_id,hr_row_id) do update set
    resolved_timesheet_id=excluded.resolved_timesheet_id,resolution_method='AUTO_MATCHED',status='CURRENT',
    evidence_fingerprint=excluded.evidence_fingerprint,preview_generation=excluded.preview_generation,
    state_version=excluded.state_version,selected_at_utc=now(),selected_by_user_id=excluded.selected_by_user_id,
    stale_at_utc=null,stale_reason_code=null,updated_at_utc=now()
  where public.import_review_daily_timesheet_resolutions.status<>'APPLIED'
    and (public.import_review_daily_timesheet_resolutions.resolved_timesheet_id is distinct from excluded.resolved_timesheet_id
      or public.import_review_daily_timesheet_resolutions.status<>'CURRENT'
      or public.import_review_daily_timesheet_resolutions.evidence_fingerprint is distinct from excluded.evidence_fingerprint);
  get diagnostics v_auto=row_count;
  if v_auto>0 then
    truncate pg_temp.review_next_actions;
    insert into pg_temp.review_next_actions
      select * from public._import_review_action_catalog_core_v1(p_import_id,v_generation,p_max_actions);
    delete from pg_temp.review_next_actions n
    using public.import_review_action_outcomes o
    where o.import_id=p_import_id and o.action_id=n.action_id;
  end if;
  select public._import_review_hash_v1(coalesce(string_agg(action_id||':'||evidence_fingerprint,'|' order by action_id),''))
  into v_fingerprint from pg_temp.review_next_actions;

  update public.import_review_decisions d set is_current=false,refreshed_at_utc=now()
  where d.import_id=p_import_id and d.is_current
    and not exists(select 1 from pg_temp.review_next_actions n where n.action_id=d.action_id);
  get diagnostics v_retired=row_count;

  update public.import_review_decisions d set
    action_kind=n.action_kind,action_category=n.action_category,target_key=n.target_key,source_identity=n.source_identity,
    hr_row_id=n.hr_row_id,timesheet_id=n.timesheet_id,shift_id=n.shift_id,client_id=n.client_id,
    candidate_id=n.candidate_id,contract_id=n.contract_id,issue_id=n.issue_id,
    preview_generation=v_generation,evidence_fingerprint=n.evidence_fingerprint,selectable=n.selectable,
    default_selected=n.default_selected,
    selected=case when d.evidence_fingerprint=n.evidence_fingerprint and n.selectable then d.selected else false end,
    blocking=n.blocking,requires_reconfirmation=(d.evidence_fingerprint<>n.evidence_fingerprint and d.selected),
    is_current=true,summary_json=n.summary_json,refreshed_at_utc=now(),
    selected_at_utc=case when d.evidence_fingerprint=n.evidence_fingerprint then d.selected_at_utc else null end,
    selected_by_user_id=case when d.evidence_fingerprint=n.evidence_fingerprint then d.selected_by_user_id else null end
  from pg_temp.review_next_actions n
  where d.action_id=n.action_id;
  get diagnostics v_changed=row_count;

  insert into public.import_review_decisions(action_id,import_id,action_kind,action_category,target_key,source_identity,
    hr_row_id,timesheet_id,shift_id,client_id,candidate_id,contract_id,issue_id,preview_generation,evidence_fingerprint,
    selectable,default_selected,selected,blocking,summary_json)
  select n.action_id,p_import_id,n.action_kind,n.action_category,n.target_key,n.source_identity,n.hr_row_id,n.timesheet_id,
    n.shift_id,n.client_id,n.candidate_id,n.contract_id,n.issue_id,v_generation,n.evidence_fingerprint,n.selectable,
    n.default_selected,n.default_selected,n.blocking,n.summary_json
  from pg_temp.review_next_actions n
  where not exists(select 1 from public.import_review_decisions d where d.action_id=n.action_id);
  get diagnostics v_inserted=row_count;

  -- A changed daily source/target evidence association is stale unless already applied.
  update public.import_review_daily_timesheet_resolutions r set status='STALE',stale_at_utc=now(),
    stale_reason_code='EVIDENCE_CHANGED',updated_at_utc=now()
  where r.import_id=p_import_id and r.status='CURRENT' and not exists (
    select 1 from pg_temp.review_next_actions n
    where n.hr_row_id=r.hr_row_id
      and public._import_review_hash_v1(concat_ws('|','daily-resolution-evidence-v1',
        n.source_identity,n.hr_row_id,n.timesheet_id,n.candidate_id,n.client_id,n.contract_id,
        coalesce((n.summary_json->'imported_evidence')::text,''),
        coalesce((n.summary_json->'current_evidence')::text,''),
        coalesce((n.summary_json->'mapping_evidence')::text,''),
        coalesce(n.summary_json->>'authority_fingerprint','')))=r.evidence_fingerprint);

  -- A Weekly candidate-did-not-work exception remains current only while the
  -- same server-proved HR row/timesheet evidence is still represented by the
  -- current catalogue.  The confirmed display action deliberately retains the
  -- same identity after the blocking advisory has been resolved.
  update public.import_review_weekly_validation_resolutions r
  set status='STALE',stale_at_utc=now(),stale_reason_code='EVIDENCE_CHANGED',updated_at_utc=now()
  where r.import_id=p_import_id and r.status='CURRENT' and not exists (
    select 1 from pg_temp.review_next_actions n
    where n.hr_row_id=r.hr_row_id
      and n.timesheet_id=r.timesheet_id
      and n.evidence_fingerprint=r.evidence_fingerprint
      and n.summary_json->>'resolution_kind'='WEEKLY_CANDIDATE_DID_NOT_WORK'
  );

  select count(*) filter(where blocking),count(*) filter(where selected)
  into v_blockers,v_selected from public.import_review_decisions
  where import_id=p_import_id and is_current;
  v_status:=case when v_blockers>0 then 'BLOCKED' else 'READY' end;
  update public.import_review_states set status=v_status,state_version=state_version+1,
    preview_generation=v_generation,preview_fingerprint=v_fingerprint,updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v_state;
  insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json)
  values(p_import_id,v_state.state_version,'PREVIEW_REFRESHED',p_actor_user_id,jsonb_build_object(
    'preview_generation',v_generation,'preview_fingerprint',v_fingerprint,'inserted',v_inserted,
    'reconciled',v_changed,'retired',v_retired,'blockers',v_blockers));
  return jsonb_build_object('ok',true,'schema_contract_version',v_state.schema_contract_version,
    'import_id',p_import_id,'status',v_state.status,
    'state_version',v_state.state_version,'preview_generation',v_generation,'preview_fingerprint',v_fingerprint,
    'action_count',(select count(*) from pg_temp.review_next_actions),'blocker_count',v_blockers,'selected_count',v_selected,
    'inserted_count',v_inserted,'reconciled_count',v_changed,'retired_count',v_retired);
end
$function$;

revoke all on function public._import_review_hash_v1(text) from public,anon,authenticated,service_role;
revoke all on function public._import_review_assert_actor_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function public._import_review_ready_action_ids_core_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function public._import_review_validate_ui_state_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function public._import_review_timesheet_protection_core_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function public._import_review_query_evidence_core_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function public._import_review_auto_authorise_targets_core_v1(uuid[],public.hr_source_enum,boolean) from public,anon,authenticated,service_role;
revoke all on function public._import_review_effective_invoice_balance_core_v1(uuid,jsonb,integer,integer,integer,integer) from public,anon,authenticated,service_role;
revoke all on function public._import_review_action_catalog_core_v1(uuid,integer,integer) from public,anon,authenticated,service_role;
revoke all on function public._import_review_refresh_core_v1(uuid,bigint,uuid,integer) from public,anon,authenticated,service_role;
revoke all on function public._import_review_apply_envelope_core_v1(uuid) from public,anon,authenticated,service_role;
