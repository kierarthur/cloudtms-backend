-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: e60feb166e3e.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.hr_daily_apply_transactional(p_import_id uuid, p_payload jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_src public.hr_source_enum;
  v_import_client_id uuid;
  v_validation_policy jsonb := '{}'::jsonb;
  v_auto_authorise_timesheet_ids uuid[] := array[]::uuid[];

  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_validation_rows_json jsonb := coalesce(v_payload->'validation_rows', '[]'::jsonb);
  v_email_actions_json   jsonb := coalesce(v_payload->'email_actions',   '[]'::jsonb);

  -- ✅ NEW: destructive invalidation selections (MODE_A parity)
  v_invalidation_actions_json jsonb := coalesce(v_payload->'invalidation_actions', '[]'::jsonb);
  v_invalidation_actions_count int := 0;

  v_validations_upserted int := 0;
  v_timesheets_ref_updated int := 0;
  v_timesheets_ref_cleared int := 0;

  v_email_logs_upserted int := 0;
  v_email_jobs jsonb := '[]'::jsonb;

  v_email_selected_count int := 0;

  -- ✅ TSFIN recompute support (validation changes + reference updates)
  v_daily_validation_changed_timesheet_ids uuid[] := array[]::uuid[];
  v_ref_updated_timesheet_ids uuid[] := array[]::uuid[];
  v_ref_cleared_timesheet_ids uuid[] := array[]::uuid[];
  v_affected_timesheet_ids uuid[] := array[]::uuid[];

begin
  -- 0) Validate import exists + is HEALTHROSTER_DAILY
  select hi.source_system, hi.client_id
    into v_src, v_import_client_id
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_src is null then
    raise exception 'hr_daily_apply_transactional: import % not found in hr_imports.', p_import_id;
  end if;

  if v_src <> 'HEALTHROSTER_DAILY'::public.hr_source_enum then
    raise exception 'hr_daily_apply_transactional: import % source_system=%; expected HEALTHROSTER_DAILY.', p_import_id, v_src::text;
  end if;

  -- 1) Validate payload shapes. Invalidation must be explicit so omission can
  -- never be interpreted as an instruction to clear imported reference truth.
  if not (v_payload ? 'invalidation_actions') then
    raise exception using message='INVALIDATION_ACTIONS_REQUIRED', errcode='22023',
      detail=jsonb_build_object('code','INVALIDATION_ACTIONS_REQUIRED','expected','JSON_ARRAY_INCLUDING_EMPTY')::text;
  end if;

  if jsonb_typeof(v_validation_rows_json) <> 'array' then
    raise exception 'hr_daily_apply_transactional: validation_rows must be a JSON array.';
  end if;

  if jsonb_typeof(v_email_actions_json) <> 'array' then
    raise exception 'hr_daily_apply_transactional: email_actions must be a JSON array.';
  end if;

  if jsonb_typeof(v_invalidation_actions_json) <> 'array' then
    raise exception 'hr_daily_apply_transactional: invalidation_actions must be a JSON array.';
  end if;

  v_invalidation_actions_count := jsonb_array_length(v_invalidation_actions_json);

  -- 2) Normalise validation rows (dedupe per timesheet_id and keep worst-case)
  create temporary table tmp_val_raw(
    timesheet_id uuid not null,
    status_text text null,
    reason_code text null,
    hr_request_id text null,
    -- ✅ NEW: allow apply payload to carry the matched hr_rows.id for stable evidence linkage
    hr_row_id uuid null
  ) on commit drop;

  insert into tmp_val_raw(timesheet_id, status_text, reason_code, hr_request_id, hr_row_id)
  select
    nullif(btrim(j.value->>'timesheet_id'), '')::uuid as timesheet_id,
    nullif(btrim(j.value->>'status'), '') as status_text,
    nullif(btrim(j.value->>'reason_code'), '') as reason_code,
    nullif(btrim(j.value->>'hr_request_id'), '') as hr_request_id,
    nullif(btrim(j.value->>'hr_row_id'), '')::uuid as hr_row_id
  from jsonb_array_elements(v_validation_rows_json) as j(value)
  where nullif(btrim(j.value->>'timesheet_id'), '') is not null;

  -- ✅ NEW: stamp matched REAL hr_rows rows with resolved_timesheet_id (evidence linkage)
  -- Only rows that carry hr_row_id are stamped; synthetic "missing_from_import" rows have no hr_row_id.
  create temporary table tmp_hr_row_links(
    hr_row_id uuid primary key,
    timesheet_id uuid not null
  ) on commit drop;

  insert into tmp_hr_row_links(hr_row_id, timesheet_id)
  select distinct
    vr.hr_row_id,
    vr.timesheet_id
  from tmp_val_raw vr
  where vr.hr_row_id is not null
    and vr.timesheet_id is not null
  on conflict (hr_row_id) do update
    set timesheet_id = excluded.timesheet_id;

  update public.hr_rows hrl
     set payload_json = jsonb_set(
       coalesce(hrl.payload_json, '{}'::jsonb),
       '{resolved_timesheet_id}',
       to_jsonb(lk.timesheet_id::text),
       true
     )
    from tmp_hr_row_links lk
   where hrl.id = lk.hr_row_id
     and hrl.import_id = p_import_id
     and (hrl.payload_json->>'resolved_timesheet_id') is distinct from lk.timesheet_id::text;

  create temporary table tmp_val_by_ts(
    timesheet_id uuid primary key,
    has_error boolean not null,
    chosen_reason_code text null,
    chosen_hr_request_id text null
  ) on commit drop;

  insert into tmp_val_by_ts(timesheet_id, has_error, chosen_reason_code, chosen_hr_request_id)
  select
    vr.timesheet_id,
    bool_or(
      not (upper(coalesce(vr.status_text, '')) in ('VALIDATION_OK','OK','PASS','VALID'))
    ) as has_error,
    case
      when bool_or(
        not (upper(coalesce(vr.status_text, '')) in ('VALIDATION_OK','OK','PASS','VALID'))
      )
      then
        min(vr.reason_code) filter (
          where not (upper(coalesce(vr.status_text, '')) in ('VALIDATION_OK','OK','PASS','VALID'))
        )
      else
        'HEALTHROSTER_DAILY'
    end as chosen_reason_code,
    max(vr.hr_request_id) as chosen_hr_request_id
  from tmp_val_raw vr
  group by vr.timesheet_id;

  -- ✅ compute which timesheets will have a meaningful validation row change (before upsert)
  -- ✅ UPDATED: compute new_pre_validated (true only when OK and timesheet not authorised yet)
  create temporary table tmp_val_upsert on commit drop as
  select
    vt.timesheet_id,
    case
      when vt.has_error
        then 'VALIDATION_ERROR'::public.validation_status_enum
      else 'VALIDATION_OK'::public.validation_status_enum
    end as new_status,
    vt.chosen_reason_code as new_reason_code,
    case when vt.has_error then null else v_now end as new_validated_at_utc,
    p_import_id as new_last_source,
    vt.chosen_hr_request_id as new_hr_request_id,
    case
      when vt.has_error is false
       and tsu.timesheet_id is not null
       and tsu.authorised_at_server is null
      then true
      else false
    end as new_pre_validated
  from tmp_val_by_ts vt
  left join public.timesheets tsu
    on tsu.timesheet_id = vt.timesheet_id
   and tsu.is_current = true;

  select coalesce(array_agg(distinct x.timesheet_id order by x.timesheet_id), array[]::uuid[])
  into v_daily_validation_changed_timesheet_ids
  from (
    select u.timesheet_id
    from tmp_val_upsert u
    left join public.timesheet_validations tv
      on tv.timesheet_id = u.timesheet_id
    where tv.timesheet_id is null
       or tv.status is distinct from u.new_status
       or tv.validated_at_utc is distinct from u.new_validated_at_utc
       or tv.last_source is distinct from u.new_last_source
       or tv.reason_code is distinct from u.new_reason_code
       or tv.hr_request_id is distinct from u.new_hr_request_id
       or tv.pre_validated is distinct from u.new_pre_validated
  ) as x;

  -- 3) Upsert timesheet_validations (required + transactional)
  -- ✅ UPDATED: include pre_validated
  insert into public.timesheet_validations(
    timesheet_id,
    status,
    reason_code,
    validated_at_utc,
    last_source,
    pre_validated,
    updated_at,
    hr_request_id,
    hr_request_source,
    hr_request_set_by,
    hr_request_set_at_utc
  )
  select
    u.timesheet_id,
    u.new_status,
    u.new_reason_code,
    u.new_validated_at_utc,
    u.new_last_source,
    u.new_pre_validated,
    v_now,
    u.new_hr_request_id,
    case when u.new_hr_request_id is null then null else 'IMPORTED'::public.reference_source_enum end,
    case when u.new_hr_request_id is null then null else p_actor_user_id end,
    case when u.new_hr_request_id is null then null else v_now end
  from tmp_val_upsert u
  on conflict (timesheet_id) do update
    set status               = excluded.status,
        reason_code           = excluded.reason_code,
        validated_at_utc      = excluded.validated_at_utc,
        last_source           = excluded.last_source,
        pre_validated         = excluded.pre_validated,
        updated_at            = excluded.updated_at,
        hr_request_id         = excluded.hr_request_id,
        hr_request_source     = excluded.hr_request_source,
        hr_request_set_by     = excluded.hr_request_set_by,
        hr_request_set_at_utc = excluded.hr_request_set_at_utc;

  get diagnostics v_validations_upserted = row_count;

  select public.import_auto_authorise_policy_resolve_v1(
    p_source_system := 'HEALTHROSTER_DAILY'::public.hr_source_enum,
    p_client_id := v_import_client_id,
    p_contract_id := null,
    p_validation_context := true
  ) into v_validation_policy;

  if coalesce((v_validation_policy->>'effective_value')::boolean,false) then
    select coalesce(array_agg(distinct u.timesheet_id order by u.timesheet_id), array[]::uuid[])
      into v_auto_authorise_timesheet_ids
    from tmp_val_upsert u
    join public.timesheets t on t.timesheet_id=u.timesheet_id and t.is_current=true
    where u.new_status='VALIDATION_OK'::public.validation_status_enum
      and u.new_pre_validated=true
      and t.authorised_at_server is null;
  end if;

  -- 4) DAILY reference truth:
  -- When VALIDATION_OK and chosen_hr_request_id present, set timesheets.reference_number
  -- ✅ Must NOT update invoiced/paid/locked timesheets
  -- ✅ Must only touch DAILY timesheets
  create temporary table tmp_ref_updated_ids(
    timesheet_id uuid primary key
  ) on commit drop;

  insert into tmp_ref_updated_ids(timesheet_id)
  select distinct ts2.timesheet_id
  from public.timesheets ts2
  join tmp_val_by_ts vt2
    on vt2.timesheet_id = ts2.timesheet_id
  left join public.timesheets_financials tf2
    on tf2.timesheet_id = ts2.timesheet_id
   and tf2.is_current = true
  where ts2.is_current = true
    and upper(coalesce(ts2.sheet_scope::text, '')) = 'DAILY'
    and vt2.has_error is false
    and vt2.chosen_hr_request_id is not null
    and (ts2.reference_number is distinct from vt2.chosen_hr_request_id)
    and (tf2.timesheet_id is null or (tf2.locked_by_invoice_id is null and tf2.paid_at_utc is null));

  update public.timesheets tsu
     set reference_number = vt3.chosen_hr_request_id,
         updated_at = v_now
    from tmp_val_by_ts vt3
    join tmp_ref_updated_ids tri
      on tri.timesheet_id = vt3.timesheet_id
   where tsu.timesheet_id = vt3.timesheet_id
     and tsu.is_current = true
     and upper(coalesce(tsu.sheet_scope::text, '')) = 'DAILY';

  get diagnostics v_timesheets_ref_updated = row_count;

  select coalesce(array_agg(tri.timesheet_id order by tri.timesheet_id), array[]::uuid[])
  into v_ref_updated_timesheet_ids
  from tmp_ref_updated_ids tri;

  -- 4.1) ✅ MODE_A parity: destructive invalidation actions (clear refs)
  -- Schema aligns to weekly: {timesheet_id, comparison_key, invalidate:true|false}
  create temporary table tmp_invalidation_actions(
    timesheet_id uuid not null,
    comparison_key text not null,
    invalidate boolean not null,
    primary key (timesheet_id, comparison_key)
  ) on commit drop;

  if v_invalidation_actions_count > 0 then
    insert into tmp_invalidation_actions(timesheet_id, comparison_key, invalidate)
    select
      nullif(btrim(a.value->>'timesheet_id'), '')::uuid as timesheet_id,
      nullif(btrim(a.value->>'comparison_key'), '') as comparison_key,
      (lower(coalesce(a.value->>'invalidate','true')) in ('true','1')) as invalidate
    from jsonb_array_elements(v_invalidation_actions_json) as a(value)
    where nullif(btrim(a.value->>'timesheet_id'), '') is not null
      and nullif(btrim(a.value->>'comparison_key'), '') is not null
    on conflict (timesheet_id, comparison_key) do update
      set invalidate = excluded.invalidate;
  end if;

  create temporary table tmp_ref_cleared_ids(
    timesheet_id uuid primary key
  ) on commit drop;

  insert into tmp_ref_cleared_ids(timesheet_id)
  select distinct ia.timesheet_id
  from tmp_invalidation_actions ia
  where ia.invalidate is true
    and ia.timesheet_id is not null
  on conflict do nothing;

  -- Clear timesheets.reference_number (DAILY only; never when locked/paid)
  with upd as (
    update public.timesheets tsclr
       set reference_number = null,
           updated_at = v_now
      from tmp_ref_cleared_ids rc
      left join public.timesheets_financials tfc
        on tfc.timesheet_id = rc.timesheet_id
       and tfc.is_current = true
     where tsclr.is_current = true
       and upper(coalesce(tsclr.sheet_scope::text, '')) = 'DAILY'
       and tsclr.timesheet_id = rc.timesheet_id
       and tsclr.reference_number is not null
       and (tfc.timesheet_id is null or (tfc.locked_by_invoice_id is null and tfc.paid_at_utc is null))
    returning tsclr.timesheet_id
  )
  select coalesce(array_agg(upd.timesheet_id order by upd.timesheet_id), array[]::uuid[])
  into v_ref_cleared_timesheet_ids
  from upd;

  v_timesheets_ref_cleared := coalesce(array_length(v_ref_cleared_timesheet_ids, 1), 0);

  -- For parity, also clear validation HR-request fields and force re-validation (PENDING)
  -- (This ensures the final outcome is not "validated OK" after a destructive clear.)
  create temporary table tmp_val_invalidation_upd(
    timesheet_id uuid primary key
  ) on commit drop;

  with upd_tv as (
    update public.timesheet_validations tvc
       set status = 'PENDING'::public.validation_status_enum,
           pre_validated = false,
           validated_at_utc = null,
           reason_code = 'TIMESHEET_CHANGED',
           last_source = p_import_id,
           updated_at = v_now,
           hr_request_id = null,
           hr_request_source = null,
           hr_request_set_by = null,
           hr_request_set_at_utc = null
      from tmp_ref_cleared_ids rc2
     where tvc.timesheet_id = rc2.timesheet_id
    returning tvc.timesheet_id
  )
  insert into tmp_val_invalidation_upd(timesheet_id)
  select upd_tv.timesheet_id
  from upd_tv
  where upd_tv.timesheet_id is not null
  on conflict do nothing;

  -- Ensure validation_affected includes invalidation-updated rows
  select coalesce(array_agg(distinct x.tsid order by x.tsid), array[]::uuid[])
  into v_daily_validation_changed_timesheet_ids
  from (
    select unnest(coalesce(v_daily_validation_changed_timesheet_ids, array[]::uuid[])) as tsid
    union
    select u.timesheet_id as tsid
    from tmp_val_invalidation_upd u
  ) as x
  where x.tsid is not null;

  -- Ensure ref_updated_timesheet_ids includes both ref-set and ref-cleared
  select coalesce(array_agg(distinct x.tsid order by x.tsid), array[]::uuid[])
  into v_ref_updated_timesheet_ids
  from (
    select unnest(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])) as tsid
    union
    select unnest(coalesce(v_ref_cleared_timesheet_ids, array[]::uuid[])) as tsid
  ) as x
  where x.tsid is not null;

  -- 5) Email actions: upsert hr_issue_emails (transactional) + return email_jobs
  -- ✅ UPDATED: allow Alternative Email override (MODE_A parity)
  create temporary table tmp_email_actions(
    timesheet_id uuid not null,
    issue_fingerprint text not null,
    reason_code text null,
    hr_row_id uuid null,
    staff_norm text null,
    hospital_norm text null,
    work_date date null,
    alternative_email text null
  ) on commit drop;

  insert into tmp_email_actions(timesheet_id, issue_fingerprint, reason_code, hr_row_id, staff_norm, hospital_norm, work_date, alternative_email)
  select
    nullif(btrim(e.value->>'timesheet_id'), '')::uuid as timesheet_id,
    nullif(btrim(e.value->>'issue_fingerprint'), '') as issue_fingerprint,
    nullif(btrim(e.value->>'reason_code'), '') as reason_code,
    nullif(btrim(e.value->>'hr_row_id'), '')::uuid as hr_row_id,
    nullif(btrim(e.value->>'staff_norm'), '') as staff_norm,
    nullif(btrim(e.value->>'hospital_norm'), '') as hospital_norm,
    nullif(btrim(e.value->>'work_date'), '')::date as work_date,
    nullif(
      btrim(
        coalesce(
          e.value->>'alternative_email',
          e.value->>'alt_email',
          e.value->>'alt_recipient_email'
        )
      ),
      ''
    ) as alternative_email
  from jsonb_array_elements(v_email_actions_json) as e(value)
  where nullif(btrim(e.value->>'timesheet_id'), '') is not null
    and nullif(btrim(e.value->>'issue_fingerprint'), '') is not null;

  select count(*)::int
  into v_email_selected_count
  from tmp_email_actions;

  create temporary table tmp_email_enriched on commit drop as
  select
    ea.timesheet_id,
    ea.issue_fingerprint,
    coalesce(nullif(ea.reason_code,''), 'actual_hours_mismatch') as reason_code,
    ea.hr_row_id,
    ea.staff_norm,
    ea.hospital_norm,
    ea.work_date,
    ea.alternative_email,
    coalesce(tf.client_id, ct.client_id) as client_id,
    cli.ts_queries_email as default_recipient_email,
    coalesce(ea.alternative_email, cli.ts_queries_email) as effective_recipient_email,
    exists(
      select 1
      from public.hr_issue_emails hie
      where hie.issue_fingerprint = ea.issue_fingerprint
      limit 1
    ) as emailed_already
  from tmp_email_actions ea
  left join public.timesheets_financials tf
    on tf.timesheet_id = ea.timesheet_id
   and tf.is_current = true
  left join public.timesheets ts
    on ts.timesheet_id = ea.timesheet_id
   and ts.is_current = true
  left join public.contracts ct
    on ct.id = ts.contract_id
  left join public.clients cli
    on cli.id = coalesce(tf.client_id, ct.client_id);

  insert into public.hr_issue_emails(
    source_system,
    import_id,
    client_id,
    timesheet_id,
    hr_row_id,
    staff_norm,
    hospital_norm,
    work_date,
    reason_code,
    issue_fingerprint,
    last_sent_at,
    created_at,
    updated_at
  )
  select
    'HEALTHROSTER_DAILY',
    p_import_id,
    te.client_id,
    te.timesheet_id,
    te.hr_row_id,
    te.staff_norm,
    te.hospital_norm,
    te.work_date,
    te.reason_code,
    te.issue_fingerprint,
    v_now,
    v_now,
    v_now
  from tmp_email_enriched te
  on conflict (issue_fingerprint) do update
    set last_sent_at  = excluded.last_sent_at,
        updated_at    = excluded.updated_at,
        import_id     = excluded.import_id,
        client_id     = excluded.client_id,
        timesheet_id  = excluded.timesheet_id,
        hr_row_id     = excluded.hr_row_id,
        staff_norm    = excluded.staff_norm,
        hospital_norm = excluded.hospital_norm,
        work_date     = excluded.work_date,
        reason_code   = excluded.reason_code;

  get diagnostics v_email_logs_upserted = row_count;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'timesheet_id', te.timesheet_id::text,
        'issue_fingerprint', te.issue_fingerprint,
        'client_id', case when te.client_id is null then null else te.client_id::text end,

        -- ✅ effective recipient (alt overrides default)
        'recipient_email', te.effective_recipient_email,
        'recipient_missing', (te.effective_recipient_email is null or length(btrim(te.effective_recipient_email)) = 0),

        'email_kind', case when te.emailed_already then 'REEMAIL' else 'EMAIL' end,
        'reason_code', te.reason_code,

        -- Optional diagnostics (safe for UI/logs)
        'default_recipient_email', te.default_recipient_email,
        'alternative_email', te.alternative_email,

        'hr_row_id', case when te.hr_row_id is null then null else te.hr_row_id::text end,
        'work_date', case when te.work_date is null then null else te.work_date::text end
      )
      order by te.timesheet_id::text
    ),
    '[]'::jsonb
  )
  into v_email_jobs
  from tmp_email_enriched te;

  -- ✅ build affected_timesheet_ids = (validation changed) ∪ (reference updated/cleared)
  select coalesce(array_agg(distinct a.tsid order by a.tsid), array[]::uuid[])
  into v_affected_timesheet_ids
  from (
    select unnest(coalesce(v_daily_validation_changed_timesheet_ids, array[]::uuid[])) as tsid
    union
    select unnest(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])) as tsid
  ) as a
  where a.tsid is not null;

  -- ✅ enqueue TSFIN priority for affected timesheets (if any)
  if array_length(v_affected_timesheet_ids, 1) is not null then
    perform public.enqueue_ts_financials_priority(
      v_affected_timesheet_ids,
      'CONTEXT_CHANGED'::public.ts_fin_reason_enum
    );
  end if;

  -- 6) Mark import applied (inside transaction)
  update public.hr_imports hi2
     set import_scope = 'HR_DAILY',
         applied_at = v_now
   where hi2.id = p_import_id;

  return jsonb_build_object(
    'import_id', p_import_id,
    'source_system', v_src::text,
    'validations_upserted', v_validations_upserted,

    -- Existing key kept; includes "set" updates only (as before)
    'timesheets_reference_updated', v_timesheets_ref_updated,

    -- ✅ NEW: explicit cleared count (MODE_A parity)
    'timesheets_reference_cleared', v_timesheets_ref_cleared,

    -- Existing key kept; now includes both set + cleared ids
    'ref_updated_timesheet_ids', to_jsonb(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])),
    'ref_cleared_timesheet_ids', to_jsonb(coalesce(v_ref_cleared_timesheet_ids, array[]::uuid[])),

    'email_actions_received', v_email_selected_count,
    'email_logs_upserted', v_email_logs_upserted,
    'email_jobs', v_email_jobs,
    'affected_timesheet_ids', to_jsonb(coalesce(v_affected_timesheet_ids, array[]::uuid[])),
    'auto_authorise_policy', v_validation_policy,
    'auto_authorise_timesheet_ids', to_jsonb(coalesce(v_auto_authorise_timesheet_ids, array[]::uuid[])),
    'validation_affected_timesheet_ids', to_jsonb(coalesce(v_daily_validation_changed_timesheet_ids, array[]::uuid[]))
  );
end;
$function$;
-- CloudTMS deployment metadata preserved from the installed TEST definition.
ALTER FUNCTION public.hr_daily_apply_transactional(uuid, jsonb, uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.hr_daily_apply_transactional(uuid, jsonb, uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.hr_daily_apply_transactional(uuid, jsonb, uuid) TO postgres, authenticated, service_role;
