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
  ), matched as (
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
    order by active.import_id,
      case when current.client_id is not null and current.client_id=other_client.client_id then 0 else 1 end
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
begin
  select * into v_state from public.import_review_states where import_id=p_import_id;
  select * into v_import from public.hr_imports where id=p_import_id;
  if v_state.import_id is null or v_import.id is null then
    raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002';
  end if;
  select coalesce(array_agg(d.action_id order by d.action_id),array[]::text[])
  into v_selected_ids from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selectable and d.selected;
  select coalesce(array_agg(d.action_id order by d.action_id),array[]::text[])
  into v_invalidation_ids from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selectable and d.selected
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
  where d.import_id=p_import_id and d.is_current and d.selectable and d.selected
    and d.action_kind in ('APPLY_AMENDMENT','APPLY_CANCELLATION') and d.timesheet_id is not null
    and (coalesce((pr.protection->>'paid')::boolean,false)
      or coalesce((pr.protection->>'invoice_locked')::boolean,false))
    and coalesce((select a.import_authoritative
      from public._import_review_effective_authority_core_v1(
        case when v_import.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end,
        d.contract_id,d.client_id,coalesce(d.summary_json->>'work_date',d.summary_json->>'week_ending_date')::date) a),false);
  return jsonb_build_object(
    'schema_version','IMPORT_REVIEW_APPLY_V1','import_id',p_import_id,
    'selected_action_ids',to_jsonb(v_selected_ids),'coverage_fingerprint',v_import.coverage_fingerprint,
    'preview_fingerprint',v_state.preview_fingerprint,
    'reference_invalidation_action_ids',to_jsonb(v_invalidation_ids),
    'correction_units',v_correction_units);
end
$function$;

create or replace function public._import_review_validate_ui_state_v1(p_ui_state jsonb)
returns jsonb
language plpgsql
immutable
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare v jsonb:=coalesce(p_ui_state,'{}'::jsonb); v_key text;
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
  if v::text ~* '(recipient|email|amount|rate|timesheet_id|financial|action_id|selected)' then
    raise exception 'IMPORT_REVIEW_UI_STATE_CONTAINS_AUTHORITY' using errcode='22023';
  end if;
  if exists (
    select 1 from jsonb_each(v) e
    where jsonb_array_length(case when jsonb_typeof(e.value)='array' then e.value else '[]'::jsonb end)>500
  ) then raise exception 'IMPORT_REVIEW_UI_STATE_ARRAY_LIMIT_EXCEEDED' using errcode='22023'; end if;
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
begin
  if p_timesheet_id is null then
    return jsonb_build_object('active_pay_draft',false,'paid',false,'invoice_locked',false,'protected',false);
  end if;

  select coalesce(tf.paid_at_utc is not null,false),
         coalesce(tf.locked_by_invoice_id is not null,false),
         tf.processing_status::text
  into v_paid,v_invoice_locked,v_processing_status
  from public.timesheets_financials tf
  where tf.timesheet_id=p_timesheet_id and tf.is_current=true
  order by tf.updated_at desc nulls last
  limit 1;

  v_invoice_locked := coalesce(v_invoice_locked,false) or exists (
    select 1 from public.invoice_lines il
    join public.invoices i on i.id=il.invoice_id
    where il.timesheet_id=p_timesheet_id
      and (i.issued_at_utc is not null or upper(coalesce(i.status::text,''))<>'DRAFT')
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

create or replace function public._import_review_action_catalog_core_v1(
  p_import_id uuid,
  p_preview_generation integer,
  p_max_actions integer default 500
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
  if p_import_id is null or p_preview_generation<1 or p_max_actions<1 or p_max_actions>500 then
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
        then case when rtsx.contract_id is not null then 1 else dcon.contract_count end
        else con.contract_count end as contract_count,
      case when wp.hr_row_id is not null then wp.contract_id
        when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
          then coalesce(rtsx.contract_id,dcon.contract_id)
        else con.contract_id end as resolved_contract_id,
      wp.action as weekly_resolution_action,wp.reason as weekly_resolution_reason,
      wp.incoming_code as weekly_incoming_code,
      wm.has_weekly_mapping,wm.mapping_evidence as weekly_mapping_evidence,
      dgm.mapping_count as daily_mapping_count,dgm.mapping_id as daily_mapping_id,
      dgm.role_code as daily_mapped_role,dgm.band_norm as daily_mapped_band,
      dgm.updated_at as daily_mapping_updated_at,(coalesce(dgm.mapping_count,0)=1) as has_grade_mapping,
      tsx.timesheet_count,tsx.timesheet_ids,tsx.auto_timesheet_id,tsx.timesheet_evidence_hash,
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
        and ts.contract_id=any(coalesce(dcon.contract_ids,array[]::uuid[]))
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
          then coalesce(rtsx.contract_id,dcon.contract_id)
        else con.contract_id end
    ) cr on true
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'option_id','contract:'||o.id::text,'contract_id',o.id,'candidate_id',o.candidate_id,'client_id',o.client_id,
        'role',o.role,'band',o.band,'site',o.display_site,'start_date',o.start_date,'end_date',o.end_date,
        'source_route_eligible',coalesce(o.route_eligible,false),'rate_complete',coalesce(o.rate_complete,false),
        'authority_mode',o.authority_mode,
        'selectable',coalesce(o.route_eligible,false)
          and (not coalesce(o.import_authoritative,false) or coalesce(o.rate_complete,false)),
        'disabled_reason_code',case when not coalesce(o.route_eligible,false) then 'CONTRACT_NOT_ELIGIBLE'
          when coalesce(o.import_authoritative,false) and not coalesce(o.rate_complete,false)
            then 'CONTRACT_RATES_INCOMPLETE' end,
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
      from (select distinct c.role,c.band from public.contracts c
        cross join lateral public._import_review_effective_authority_core_v1('HR_DAILY',c.id,c.client_id,m.date_local) a
        where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
          and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
          and a.route_eligible
          and nullif(btrim(c.role),'') is not null order by c.role,c.band limit 25) o
    ) dopts on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.nhsp_shifts nss
      on nss.external_row_key=m.source_row_key and nss.source_system::text=m.source_system
      and nss.cancelled_at_utc is null
  ), facts as (
    select c.*,
      ts.worked_start_iso,ts.worked_end_iso,ts.break_minutes as ts_break_minutes,ts.worked_minutes,
      ts.reference_number,ts.processing_status::text,ts.tsfin_role,ts.tsfin_band,
      public._import_review_timesheet_protection_core_v1(coalesce(c.resolved_timesheet_id,c.existing_shift_timesheet_id)) as protection
    from classified c
    left join public.v_timesheets_daily_match ts on ts.timesheet_id=c.resolved_timesheet_id
  ), evidenced as (
    select c.*,
      public._import_review_hash_v1(concat_ws('|','row-evidence-v1',c.source_row_key,c.staff_key,c.client_key,c.date_local,
        c.start_time_local,c.end_time_local,c.hours_worked,c.hr_request_id,c.resolved_candidate_id,c.resolved_client_id,
        c.resolved_contract_id,c.weekly_resolution_action,c.weekly_incoming_code,c.weekly_mapping_evidence,c.contract_rate_evidence,
        c.daily_mapping_id,c.daily_mapping_updated_at,c.daily_mapped_role,c.daily_mapped_band,
        c.timesheet_evidence_hash,c.contract_evidence_hash,c.authority_fingerprint,
        coalesce(c.eligible_contract_ids::text,''),coalesce(c.timesheet_ids::text,''),
        coalesce(c.timesheet_contract_ids::text,''),c.protection::text,
        coalesce(c.payload_json::text,''))) as evidence_hash
    from facts c
  ), main_actions as (
    select
      case
        when f.resolved_candidate_id is null then 'ADVISORY'
        when f.resolved_client_id is null then 'ADVISORY'
        when f.is_daily and not coalesce(f.has_grade_mapping,false) then 'ADVISORY'
        when not f.is_daily and coalesce(f.weekly_resolution_action,'')<>'OK' then 'ADVISORY'
        when coalesce(f.contract_count,0)=0 then 'ADVISORY'
        when f.is_daily and f.contract_count>1 then 'ADVISORY'
        when not f.is_daily and not coalesce(f.contract_route_eligible,false) then 'ADVISORY'
        when f.is_daily and coalesce(f.timesheet_count,0)=0 then 'ADVISORY'
        when f.is_daily and f.resolved_timesheet_id is null then 'DAILY_TIMESHEET_RESOLUTION'
        when f.is_daily then 'NO_ACTION'
        when not coalesce(f.import_authoritative,false) and f.existing_shift_id is null then 'ADVISORY'
        when not coalesce(f.import_authoritative,false) then 'NO_ACTION'
        when not coalesce(f.contract_rate_complete,false) then 'ADVISORY'
        when f.existing_shift_id is null then 'INCLUDE_SHIFT'
        when (f.payload_json->>'start_utc')::timestamptz is distinct from (select n.start_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or (f.payload_json->>'end_utc')::timestamptz is distinct from (select n.end_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or coalesce((f.payload_json->>'break_mins')::integer,0) is distinct from (select n.break_mins from public.nhsp_shifts n where n.id=f.existing_shift_id)
          then 'APPLY_AMENDMENT'
        else 'NO_ACTION'
      end action_kind,
      f.*
    from evidenced f
  ), rendered as (
    select
      public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,m.action_kind,m.source_row_key)) action_id,
      m.action_kind,
      case when m.action_kind='ADVISORY' or coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED'
           when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'PENDING'
           when m.action_kind='NO_ACTION' then 'NO_ACTION' else 'READY' end action_category,
      'hr-row:'||m.id::text target_key,m.source_row_key source_identity,m.id hr_row_id,
      coalesce(m.resolved_timesheet_id,m.existing_shift_timesheet_id) timesheet_id,m.existing_shift_id shift_id,
      m.resolved_client_id client_id,m.resolved_candidate_id candidate_id,m.resolved_contract_id contract_id,
      null::uuid issue_id,m.evidence_hash evidence_fingerprint,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) selectable,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) default_selected,
      (m.action_kind in ('ADVISORY','DAILY_TIMESHEET_RESOLUTION')
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
          when coalesce(m.contract_count,0)=0 then 'CONTRACT_MISSING'
          when m.is_daily and m.contract_count>1 then 'CONTRACT_AMBIGUOUS'
          when not m.is_daily and not coalesce(m.contract_route_eligible,false) then 'CONTRACT_OUT_OF_SCOPE'
          when not m.is_daily and not coalesce(m.import_authoritative,false) and m.existing_shift_id is null
            then 'TIMESHEET_NOT_FOUND'
          when not m.is_daily and coalesce(m.import_authoritative,false)
            and not coalesce(m.contract_rate_complete,false) then 'CONTRACT_RATES_INCOMPLETE'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'TIMESHEET_NOT_FOUND'
          when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_AMBIGUOUS'
          when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT'
          else null end,
        'source_system',m.source_system,'source_route',m.import_scope,'is_daily',m.is_daily,
        'authority_mode',coalesce(m.authority_mode,case when m.is_daily or not coalesce(m.import_authoritative,false)
          then 'VALIDATION_ONLY' else 'AUTHORITATIVE' end),
        'authority_fingerprint',m.authority_fingerprint,
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
          'break_minutes',m.ts_break_minutes,'worked_minutes',m.worked_minutes,'worked_hours',round(m.worked_minutes/60.0,2),
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
          case when m.is_daily and m.resolved_timesheet_id is not null and coalesce((m.payload_json->>'actual_break_mins')::integer,
            (m.payload_json->>'actual_break_minutes')::integer,(m.payload_json->>'break_mins')::integer,
            (m.payload_json->>'break_minutes')::integer,0) is distinct from coalesce(m.ts_break_minutes,0) then 'BREAK_MINUTES'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.hours_worked is not null
            and abs((m.hours_worked*60)-m.worked_minutes)>1 then 'WORKED_HOURS'::text end
        ],null)),
        'outcome_label',case
          when not m.is_daily and not coalesce(m.import_authoritative,false) and m.existing_shift_id is null then 'Request new shift'
          when not m.is_daily and not coalesce(m.import_authoritative,false) then 'Validate existing timesheet only'
          when m.action_kind='INCLUDE_SHIFT' then 'TMS will add shift'
          when m.action_kind='APPLY_AMENDMENT' then case when coalesce((m.protection->>'paid')::boolean,false)
            or coalesce((m.protection->>'invoice_locked')::boolean,false)
            then 'TMS will reverse shift and create replacement shift' else 'TMS will amend shift' end
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
        when r.hours_worked is not null and r.worked_minutes is not null and abs((r.hours_worked*60)-r.worked_minutes)>1 then 'ACTUAL_HOURS_MISMATCH'
        when r.start_time_local is distinct from (r.worked_start_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when r.end_time_local is distinct from (r.worked_end_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when coalesce((r.payload_json->>'break_mins')::integer,(r.payload_json->>'break_minutes')::integer,0) is distinct from coalesce(r.break_minutes,0) then 'BREAK_MINUTES_MISMATCH'
      end reason_code
    from r
  ), issues as (
    select m.*,public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_DAILY',m.reason_code,m.timesheet_id,m.hr_request_id,
      lower(coalesce(m.staff_norm,'')),m.date_local,m.start_time_local,m.end_time_local,m.hours_worked,m.worked_minutes)) issue_fingerprint,
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
    public._import_review_hash_v1(concat_ws('|','issue-evidence-v1',i.issue_fingerprint,i.protection::text,
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
    ), routed as (
      select p.*,c.rev client_rev,c.updated_at client_updated_at,c.ts_queries_email,
        ct.send_ts_queries_to_different_email,ct.ts_queries_alt_email_address,ct.updated_at contract_updated_at,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE:'||ct.id::text else 'CLIENT_DEFAULT:'||c.id::text end recipient_scope_key,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE' else 'CLIENT_DEFAULT' end recipient_scope,
        lower(btrim(case when coalesce(ct.send_ts_queries_to_different_email,false)
          then ct.ts_queries_alt_email_address else c.ts_queries_email end)) recipient_email,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection,
        e.id issue_id,e.delivery_history_status,e.sent_count
      from preview_rows p
      join public.clients c on c.id=p.client_id
      left join public.contracts ct on ct.id=p.contract_id and ct.client_id=p.client_id
      left join public.hr_issue_emails e on e.issue_fingerprint=p.issue_fingerprint
      where p.timesheet_id is not null and p.issue_fingerprint is not null
        and coalesce((p.row_json->>'has_mismatch')::boolean,false)
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
        case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,a.issue_fingerprint)),
      case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,
      case when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED' else 'EMAIL' end,
      'issue:'||a.issue_fingerprint,a.issue_fingerprint,null::uuid,a.timesheet_id,null::uuid,
      a.client_id,a.candidate_id,a.contract_id,a.issue_id,
      public._import_review_hash_v1(concat_ws('|','weekly-query-evidence-v1',a.row_json::text,a.protection::text,
        a.route_fingerprint,coalesce(a.delivery_history_status,'NEW'),coalesce(a.sent_count,0))),
      a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      a.issue_id is null and a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      coalesce((a.protection->>'active_pay_draft')::boolean,false),
      jsonb_build_object('reason_code','HEALTHROSTER_WEEKLY','issue_fingerprint',a.issue_fingerprint,
        'candidate_name',a.row_json->>'candidate_name','week_ending_date',a.row_json->>'week_ending_date',
      'failure_reasons',coalesce(a.row_json->'failure_reasons','[]'::jsonb),
        'days',coalesce(a.row_json->'days','[]'::jsonb),'comparisons',coalesce(a.row_json->'comparisons','[]'::jsonb),
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
          from jsonb_array_elements(coalesce(a.row_json->'comparisons','[]'::jsonb)) cx(value)
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
  p_max_actions integer default 500
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

  -- Persist database-unambiguous Daily associations in the same normalised table.
  -- This is evidence linkage only and never edits the selected timesheet.
  insert into public.import_review_daily_timesheet_resolutions(
    import_id,hr_row_id,resolved_timesheet_id,resolution_method,status,evidence_fingerprint,
    preview_generation,state_version,selected_by_user_id
  )
  select p_import_id,n.hr_row_id,n.timesheet_id,'AUTO_MATCHED','CURRENT',n.evidence_fingerprint,
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
      or public.import_review_daily_timesheet_resolutions.status<>'CURRENT');
  get diagnostics v_auto=row_count;
  if v_auto>0 then
    truncate pg_temp.review_next_actions;
    insert into pg_temp.review_next_actions
      select * from public._import_review_action_catalog_core_v1(p_import_id,v_generation,p_max_actions);
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
    where n.hr_row_id=r.hr_row_id and n.evidence_fingerprint=r.evidence_fingerprint);

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
revoke all on function public._import_review_validate_ui_state_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function public._import_review_timesheet_protection_core_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function public._import_review_action_catalog_core_v1(uuid,integer,integer) from public,anon,authenticated,service_role;
revoke all on function public._import_review_refresh_core_v1(uuid,bigint,uuid,integer) from public,anon,authenticated,service_role;
revoke all on function public._import_review_apply_envelope_core_v1(uuid) from public,anon,authenticated,service_role;
