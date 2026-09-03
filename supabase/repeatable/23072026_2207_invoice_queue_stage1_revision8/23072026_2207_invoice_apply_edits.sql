CREATE OR REPLACE FUNCTION public.invoice_apply_edits(p_invoice_id uuid, p_payload jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;


-- =====================================================
-- DEBUG (invoice_debug): single audit row per RPC call
-- =====================================================
v_invoice_debug boolean := false;
v_dbg_started_at timestamptz := now();
v_dbg_steps jsonb := '[]'::jsonb;
v_dbg_sqlstate text := null;
v_dbg_error text := null;
v_dbg_stats jsonb := '{}'::jsonb;


v_dbg_lines_deleted int := 0;
v_dbg_timesheets_unlocked int := 0;
v_dbg_seg_add_refs int := 0;
v_dbg_seg_remove_refs int := 0;
v_dbg_seg_tsfins int := 0;
v_dbg_seg_timesheets_rebuilt int := 0;
v_dbg_seg_timesheets_removed int := 0;
v_dbg_add_timesheets_found int := 0;
v_dbg_add_timesheets_skipped int := 0;

v_rc int := 0;

  v_inv record;
  v_week_start date;
  v_week_end date;

  v_remove_ids uuid[];
  v_add_ts_ids uuid[];

-- segment move payload (explicit segment add/remove)
v_remove_seg_refs jsonb;
v_add_seg_refs jsonb;
v_has_seg_ops boolean := false;
v_refresh_hr_cache boolean := false;
v_seg_tsfin_ids uuid[] := array[]::uuid[];
v_seg_ts_ids uuid[] := array[]::uuid[];
v_seg_refs_to_lock jsonb := '[]'::jsonb;
v_ref jsonb;
v_tsfin_id uuid;
v_seg_id text;
v_has_additional boolean;
v_has_expense_or_mileage boolean;

  -- reference updates (refs-to-issue)
  v_reference_updates jsonb;
  v_refupd jsonb;
  v_refupd_ts_id uuid;
  v_refupd_count int := 0;
  v_refupd_applied int := 0;
  v_refupd_set_refnum boolean;
  v_refupd_set_dayrefs boolean;
  v_refupd_set_sched boolean;
  v_refupd_dayrefs jsonb;
  v_refupd_sched jsonb;
  v_refupd_refnum text;

  -- timesheet hospital / ward source edits
  v_location_updates jsonb;
  v_location_update jsonb;
  v_location_ts_id uuid;
  v_location_count int := 0;
  v_location_applied int := 0;
  v_location_set_hospital boolean;
  v_location_set_ward boolean;
  v_location_hospital_norm text;
  v_location_ward_norm text;
  v_location_ts_ids uuid[] := array[]::uuid[];
  -- Source-edit contract state. Reference and location inputs are merged into
  -- one desired row per timesheet, then written by one statement so the
  -- statement-level invalidation trigger advances each source exactly once.
  v_source_updates_map jsonb := '{}'::jsonb;
  v_source_update jsonb;
  v_source_edit_key text;
  v_source_expected_revision bigint;
  v_source_changed_ts_ids uuid[] := array[]::uuid[];
  v_source_changed_revisions jsonb := '[]'::jsonb;
  v_source_edit_requested boolean := false;
  v_allowed_reference_fields text[]:=array[
    'timesheet_id','expected_document_revision','reference_number',
    'day_references_json','actual_schedule_json'];
  v_allowed_location_fields text[]:=array[
    'timesheet_id','expected_document_revision','hospital_norm','ward_norm'];
  v_allowed_reference_segment_fields text[]:=array[
    'segment_id','date','start_utc','end_utc','start','end','ref_num',
    'source_system'];
  v_source_unsupported_field text;
  v_source_financial_id uuid;
  v_source_mode text;
  v_source_ib jsonb;
  v_source_payload_schedule jsonb;
  v_source_canonical_schedule jsonb;
  v_source_new_segments jsonb;
  v_source_segment_updates jsonb;
  v_source_segment_reference_changed boolean;
  v_source_timesheet_row_changed boolean;
  v_source_match_count integer;
  v_source_match_segment jsonb;
  v_source_payload_segment jsonb;
  v_source_current_segment jsonb;
  v_source_payload_ord bigint;
  v_source_start timestamptz;
  v_source_end timestamptz;
  v_source_marker_rows jsonb:='{}'::jsonb;
  v_source_marker jsonb:='{}'::jsonb;
  v_source_edit_preexisting_preview boolean := false;
  v_source_edit_invoice_replacement_required boolean := false;
  v_pre_edit_invoice_revision bigint;
  v_pre_edit_preview_document_version_id uuid;
  v_pre_edit_active_document_operation_id uuid;
  v_expected_command_count int := 0;
  v_operation_result jsonb;
  v_operation_id uuid;
  v_document_version_id uuid;
  v_command_no int;
  v_command_type text;
  v_operation_status text;
  v_operation_input jsonb;
  v_validated_operations jsonb := '[]'::jsonb;
  v_document_commands jsonb := '[]'::jsonb;
  v_started_operations jsonb := '[]'::jsonb;

  -- reference update side-effects (meta refresh / segment ref sync)
  v_refupd_ts_ids uuid[] := array[]::uuid[];
  v_refupd_ts_ids_distinct uuid[] := array[]::uuid[];
  v_refupd_meta_rows_updated int := 0;
  v_refupd_tsfin_rows_updated int := 0;
  v_refupd_tsfin_segments_updated int := 0;
  v_refupd_invoice_fallback_invalidated boolean := false;

  -- temp vars for reference-derived meta and optional tsfin segment ref sync
  v_ref_ts_id uuid;
  v_ref_ts record;
  v_ref_schedule_refs jsonb := '[]'::jsonb;
  v_ref_schedule_refs_distinct jsonb := '[]'::jsonb;
  v_ref_sched_map jsonb := '{}'::jsonb;
  v_ref_seen_keys jsonb := '{}'::jsonb;
  v_ref_sched_key text;
  v_ref_sched_ref text;
  v_ref_tsfin_id uuid;
  v_ref_ib jsonb;
  v_ref_new_segments jsonb := '[]'::jsonb;
  v_ref_seg_obj jsonb;
  v_ref_seg_start text;
  v_ref_seg_end text;
    v_ref_seg_id text;
v_ref_seg_cur_ref text;
  v_ref_seg_new_ref text;
  v_ref_seg_has_update boolean := false;
  v_ref_seg_updates_this_ts int := 0;
  v_ref_seg_matches_this_ts int := 0;

  -- audit (history) accumulators (NOT debug-only)
  v_hist_adj jsonb := '[]'::jsonb;
  v_hist_seg_add jsonb := '[]'::jsonb;
  v_hist_seg_remove jsonb := '[]'::jsonb;
  v_hist_lines_removed jsonb := '[]'::jsonb;
  v_hist_add_ts jsonb := '[]'::jsonb;

  -- contract week status touch set
  v_cw_ts_ids uuid[] := array[]::uuid[];

  v_ts_ids_touched uuid[] := array[]::uuid[];
  -- FIX: timesheets fully removed from this invoice (no remaining invoice_lines) should be unlocked
  v_ts_ids_fully_removed uuid[] := array[]::uuid[];

  v_vat_chargeable boolean := true;
  v_vat_rate numeric := 0;

  -- adjustments
  adj jsonb;
  v_adj_token text;
  v_adj_desc text;
  v_adj_ex numeric;
  v_adj_vat numeric;
  v_adj_inc numeric;
  v_adj_source_key text;
  v_meta jsonb;

  -- timesheet loop
  tsid uuid;
  snap record;
  ts record;
  pc record;
  contract_id uuid;
  v_settings_authority jsonb;
  c_daily_calc boolean := false;
  c_bucket_labels jsonb := null;
  c_role text := null;
  c_display_site text := null;
  c_ward_hint text := null;

  -- segment filtering
  v_seg jsonb;
  segments jsonb := '[]'::jsonb;
  seg_target date;
  seg_date text;
  natural_start date;
  seg_locked text;
  seg_ref text;

  -- aggregation for weekly line
  h_day numeric; h_night numeric; h_sat numeric; h_sun numeric; h_bh numeric;
  pay_ex numeric; chg_ex numeric; margin_ex numeric;
  vat_amt numeric; inc_amt numeric;
  line_desc text;
  v_source_key text;
  v_exact_timesheet_document_r2_key text;

  -- daily aggregation record
  r_day record;

  -- segment refs for locking
  seg_refs jsonb := '[]'::jsonb;

  -- additional units
  kv record;
  ex jsonb;
  code text;
  unit_count numeric;
  bucket_name text;
  unit_name text;

  -- expenses notes
  v_note_travel text;
  v_note_accom text;
  v_note_other text;

  -- recompute totals
  v_new_ex numeric := 0;
  v_new_vat numeric := 0;
  v_new_inc numeric := 0;

  -- header meta counters (keep header_snapshot_json.meta in sync with current invoice state)
  v_hdr_ts_count_lines int := 0;
  v_hdr_ts_count_seglocks int := 0;
  v_hdr_seg_locked_count int := 0;
  v_hdr_meta_timesheet_count int := 0;
  v_hdr_meta_segment_count int := 0;

  v_manifest jsonb;
  v_correction_placement_only boolean:=false;
  v_correction_placement_ts_id uuid:=null;
  v_correction_placement_scope jsonb:='{}'::jsonb;
  v_correction_placement_states jsonb:='[]'::jsonb;
  v_service boolean:=coalesce(auth.role(),'')='service_role';
  v_actor_role text;
begin

  if not v_service
     and(auth.uid() is null or auth.uid() is distinct from p_actor_user_id) then
    raise exception using errcode='42501',
      message='AUTHENTICATED_ACTOR_MISMATCH';
  end if;
  select lower(btrim(coalesce(u.role,''))) into v_actor_role
  from public.tms_users u
  where u.id=p_actor_user_id and u.is_active;
  if(not found or v_actor_role<>'admin') and not v_service then
    raise exception using errcode='42501',
      message='INVOICE_ADMINISTRATOR_PERMISSION_REQUIRED';
  end if;

  perform public._ctms_assert_invoice_mutable_draft_v1(
    p_invoice_id,'INVOICE_APPLY_EDITS',true
  );
  v_correction_placement_only:=
    jsonb_typeof(coalesce(p_payload,'{}'::jsonb))='object'
    and not exists(select 1 from jsonb_object_keys(coalesce(p_payload,'{}'::jsonb)) payload_key
      where payload_key not in ('remove_invoice_line_ids','add_timesheet_ids'))
    and coalesce(jsonb_array_length(case when jsonb_typeof(p_payload->'remove_invoice_line_ids')='array'
      then p_payload->'remove_invoice_line_ids' else '[]'::jsonb end),0)
      +coalesce(jsonb_array_length(case when jsonb_typeof(p_payload->'add_timesheet_ids')='array'
      then p_payload->'add_timesheet_ids' else '[]'::jsonb end),0)=1
    and (
      (case when coalesce(p_payload#>>'{remove_invoice_line_ids,0}','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then exists(
          select 1 from public.invoice_lines placement_line
          where placement_line.id=(p_payload#>>'{remove_invoice_line_ids,0}')::uuid
            and placement_line.invoice_id=p_invoice_id
            and coalesce((public._ctms_import_correction_classify_v1(placement_line.timesheet_id)
              ->>'is_import_authoritative_correction')::boolean,false)) else false end)
      or
      (case when coalesce(p_payload#>>'{add_timesheet_ids,0}','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
        coalesce((public._ctms_import_correction_classify_v1(
          (p_payload#>>'{add_timesheet_ids,0}')::uuid)->>'is_import_authoritative_correction')::boolean,false)
        else false end)
    );
  if public._ctms_invoice_payload_has_financial_edit_v1(p_payload)
     and not v_correction_placement_only
     and exists (
       select 1 from public.invoice_lines il
       where il.invoice_id=p_invoice_id and il.timesheet_id is not null
         and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
           ->>'is_import_authoritative_correction')::boolean,false)
     ) then
    raise exception 'IMPORT_CORRECTION_INVOICE_FINANCIAL_EDIT_FORBIDDEN'
      using errcode='P0001',detail=jsonb_build_object('invoice_id',p_invoice_id)::text;
  end if;
-- Load invoice_debug flag (safe even if column not yet present)
begin
  select coalesce(sd.invoice_debug, false)
  into v_invoice_debug
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;
exception when undefined_column then
  v_invoice_debug := false;
end;

if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','start',
      'at_utc', public._inv_iso_utc(v_dbg_started_at),
      'invoice_id', p_invoice_id::text
    )
  );
end if;

  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  select *
  into v_inv
  from public.invoices i
  where i.id = p_invoice_id
  for update
  limit 1;

  if not found then
    raise exception 'Invoice not found';
  end if;

  -- Editable gate: DRAFT/ON_HOLD and unpaid
  if v_inv.status::text not in ('DRAFT','ON_HOLD') then
    raise exception 'Invoice is not editable (status=%)', v_inv.status::text;
  end if;

  if v_inv.paid_at_utc is not null then
    raise exception 'Invoice is not editable (already paid)';
  end if;



if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','invoice_loaded',
      'status', coalesce(v_inv.status::text,''),
      'paid_at_utc', case when v_inv.paid_at_utc is null then null else public._inv_iso_utc(v_inv.paid_at_utc) end
    )
  );
end if;

  -- Require invoice_week_start in header_snapshot_json.meta
  if v_inv.header_snapshot_json is null
     or btrim(coalesce(v_inv.header_snapshot_json #>> '{meta,invoice_week_start}','')) = ''
     or (v_inv.header_snapshot_json #>> '{meta,invoice_week_start}') !~ '^\d{4}-\d{2}-\d{2}$'
  then
    raise exception 'Invoice header_snapshot_json.meta.invoice_week_start is required for edits';
  end if;

  v_week_start := (v_inv.header_snapshot_json #>> '{meta,invoice_week_start}')::date;
  v_week_end := (v_week_start + interval '6 days')::date;


if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','invoice_week_loaded',
      'invoice_week_start', v_week_start::text,
      'invoice_week_end', v_week_end::text
    )
  );
end if;

  -- VAT settings from invoice snapshot
  if jsonb_typeof(v_inv.header_snapshot_json->'vat_chargeable') = 'boolean' then
    v_vat_chargeable := (v_inv.header_snapshot_json->>'vat_chargeable')::boolean;
  else
    v_vat_chargeable := true;
  end if;

  if (v_inv.header_snapshot_json ? 'applied_vat_rate_pct') then
    begin
      v_vat_rate := (v_inv.header_snapshot_json->>'applied_vat_rate_pct')::numeric;
    exception when others then
      v_vat_rate := 0;
    end;
  else
    v_vat_rate := 0;
  end if;

  if v_vat_chargeable = false then
    v_vat_rate := 0;
  end if;

  -- Parse payload arrays
  v_remove_ids := null;
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'remove_invoice_line_ids') then
    select array_agg((x)::uuid)
    into v_remove_ids
    from jsonb_array_elements_text(coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb)) x
    where nullif(btrim(coalesce(x,'')),'') is not null;
  end if;

  v_add_ts_ids := null;
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_timesheet_ids') then
    select array_agg((x)::uuid)
    into v_add_ts_ids
    from jsonb_array_elements_text(coalesce(p_payload->'add_timesheet_ids','[]'::jsonb)) x
    where nullif(btrim(coalesce(x,'')),'') is not null;
  end if;

  IF v_correction_placement_only THEN
    IF COALESCE(cardinality(v_remove_ids),0)=1 THEN
      SELECT il.timesheet_id INTO v_correction_placement_ts_id
      FROM public.invoice_lines il
      WHERE il.id=v_remove_ids[1] AND il.invoice_id=p_invoice_id
      FOR UPDATE;
      IF v_correction_placement_ts_id IS NULL
         OR COALESCE((public._ctms_import_correction_classify_v1(v_correction_placement_ts_id)
             ->>'is_import_authoritative_correction')::boolean,false) IS NOT TRUE THEN
        RAISE EXCEPTION 'IMPORT_CORRECTION_PLACEMENT_ONLY_TARGET_INVALID' USING ERRCODE='P0001';
      END IF;
      v_correction_placement_scope:=public.invoice_correction_pair_scope_v1(
        v_correction_placement_ts_id,null::uuid,p_actor_user_id,true,100);
      IF COALESCE((v_correction_placement_scope->>'valid')::boolean,false) IS NOT TRUE
         OR v_correction_placement_scope->>'placement_state' NOT IN (
           'COMPLETE_SAME_INVOICE','COMPLETE_SPLIT_INVOICES') THEN
        RAISE EXCEPTION 'INVOICE_CORRECTION_PAIR_PLACEMENT_MOVE_NOT_STARTABLE'
          USING ERRCODE='P0001',DETAIL=v_correction_placement_scope::text;
      END IF;
    ELSIF COALESCE(cardinality(v_add_ts_ids),0)=1 THEN
      v_correction_placement_ts_id:=v_add_ts_ids[1];
      IF COALESCE((public._ctms_import_correction_classify_v1(v_correction_placement_ts_id)
             ->>'is_import_authoritative_correction')::boolean,false) IS NOT TRUE THEN
        RAISE EXCEPTION 'IMPORT_CORRECTION_PLACEMENT_ONLY_TARGET_INVALID' USING ERRCODE='P0001';
      END IF;
      v_correction_placement_scope:=public.invoice_correction_pair_scope_v1(
        v_correction_placement_ts_id,p_invoice_id,p_actor_user_id,true,100);
      IF COALESCE((v_correction_placement_scope->>'valid')::boolean,false) IS NOT TRUE
         OR v_correction_placement_scope->>'placement_state'<>'INCOMPLETE_MOVE'
         OR v_correction_placement_scope->>'missing_member_kind' IS DISTINCT FROM
              (SELECT t.correction_kind FROM public.timesheets t
               WHERE t.timesheet_id=v_correction_placement_ts_id)
         OR COALESCE((v_correction_placement_scope->>'target_appendable')::boolean,false) IS NOT TRUE THEN
        RAISE EXCEPTION 'INVOICE_CORRECTION_PAIR_PLACEMENT_TARGET_INVALID'
          USING ERRCODE='P0001',DETAIL=v_correction_placement_scope::text;
      END IF;
    ELSE
      RAISE EXCEPTION 'IMPORT_CORRECTION_PLACEMENT_ONLY_PAYLOAD_INVALID' USING ERRCODE='22023';
    END IF;
  END IF;


-- Parse segment move payloads (tsfin_id + segment_id)
v_remove_seg_refs := null;
if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'remove_segment_refs') then
  v_remove_seg_refs := coalesce(p_payload->'remove_segment_refs','[]'::jsonb);
  if jsonb_typeof(v_remove_seg_refs) <> 'array' then
    v_remove_seg_refs := '[]'::jsonb;
  end if;
end if;

v_add_seg_refs := null;
if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_segment_refs') then
  v_add_seg_refs := coalesce(p_payload->'add_segment_refs','[]'::jsonb);
  if jsonb_typeof(v_add_seg_refs) <> 'array' then
    v_add_seg_refs := '[]'::jsonb;
  end if;
end if;



-- Parse and strictly validate source edits. The expected source revision is
-- mandatory because values displayed by an older modal must never overwrite a
-- newer timesheet revision.
v_reference_updates := null;
if p_payload is not null and jsonb_typeof(p_payload)='object'
   and p_payload ? 'reference_updates' then
  v_reference_updates:=p_payload->'reference_updates';
  if jsonb_typeof(v_reference_updates)<>'array' then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
      detail=jsonb_build_object('field','reference_updates','reason','ARRAY_REQUIRED')::text;
  end if;
  v_refupd_count:=jsonb_array_length(v_reference_updates);
  for v_refupd in select value from jsonb_array_elements(v_reference_updates) value loop
    if v_refupd is not null and jsonb_typeof(v_refupd)='object' then
      select min(field_name) into v_source_unsupported_field
      from jsonb_object_keys(v_refupd) field_name
      where not (field_name=any(v_allowed_reference_fields));
      if v_source_unsupported_field is not null then
        raise exception using errcode='22023',
          message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
          detail=jsonb_build_object(
            'field','reference_updates','reason','UNSUPPORTED_FIELD',
            'unsupported_field',v_source_unsupported_field)::text;
      end if;
    end if;
    if v_refupd is null or jsonb_typeof(v_refupd)<>'object'
       or nullif(btrim(coalesce(v_refupd->>'timesheet_id','')),'') is null
       or coalesce(v_refupd->>'timesheet_id','') !~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
       or coalesce(v_refupd->>'expected_document_revision','') !~ '^[1-9][0-9]*$'
       or not (
         v_refupd ? 'reference_number'
         or v_refupd ? 'day_references_json'
         or v_refupd ? 'actual_schedule_json'
       )
       or (
         v_refupd ? 'reference_number'
         and jsonb_typeof(v_refupd->'reference_number') not in('string','null')
       )
       or (
         v_refupd ? 'day_references_json'
         and jsonb_typeof(v_refupd->'day_references_json') not in('object','null')
       )
       or (
         v_refupd ? 'actual_schedule_json'
         and jsonb_typeof(v_refupd->'actual_schedule_json') not in('array','null')
       )
    then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
        detail=jsonb_build_object('field','reference_updates','reason','INVALID_ITEM')::text;
    end if;
    v_source_edit_key:=lower(v_refupd->>'timesheet_id');
    if v_source_updates_map ? v_source_edit_key then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_DUPLICATE_TIMESHEET',
        detail=jsonb_build_object('timesheet_id',v_source_edit_key,'field','reference_updates')::text;
    end if;
    v_source_updates_map:=jsonb_set(
      v_source_updates_map,array[v_source_edit_key],
      jsonb_build_object(
        'timesheet_id',v_source_edit_key,
        'expected_document_revision',(v_refupd->>'expected_document_revision')::bigint,
        'has_reference_number',v_refupd ? 'reference_number',
        'reference_number',case when v_refupd ? 'reference_number'
          then to_jsonb(nullif(btrim(coalesce(v_refupd->>'reference_number','')),''))
          else 'null'::jsonb end,
        'has_day_references_json',v_refupd ? 'day_references_json',
        'day_references_json',case
          when not (v_refupd ? 'day_references_json') then 'null'::jsonb
          when jsonb_typeof(v_refupd->'day_references_json')='null' then 'null'::jsonb
          else v_refupd->'day_references_json' end,
        'has_actual_schedule_json',v_refupd ? 'actual_schedule_json',
        'actual_schedule_json',case
          when not (v_refupd ? 'actual_schedule_json') then 'null'::jsonb
          when jsonb_typeof(v_refupd->'actual_schedule_json')='null' then 'null'::jsonb
          else v_refupd->'actual_schedule_json' end,
        'has_hospital_norm',false,
        'has_ward_norm',false),
      true);
  end loop;
end if;

-- Hospital/ward edits merge into the same desired source row. A source may
-- appear once in each array, but both items must carry the same revision.
v_location_updates:=null;
if p_payload is not null and jsonb_typeof(p_payload)='object'
   and p_payload ? 'timesheet_location_updates' then
  v_location_updates:=p_payload->'timesheet_location_updates';
  if jsonb_typeof(v_location_updates)<>'array' then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
      detail=jsonb_build_object('field','timesheet_location_updates','reason','ARRAY_REQUIRED')::text;
  end if;
  v_location_count:=jsonb_array_length(v_location_updates);
  for v_location_update in select value from jsonb_array_elements(v_location_updates) value loop
    if v_location_update is not null and jsonb_typeof(v_location_update)='object' then
      select min(field_name) into v_source_unsupported_field
      from jsonb_object_keys(v_location_update) field_name
      where not (field_name=any(v_allowed_location_fields));
      if v_source_unsupported_field is not null then
        raise exception using errcode='22023',
          message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
          detail=jsonb_build_object(
            'field','timesheet_location_updates','reason','UNSUPPORTED_FIELD',
            'unsupported_field',v_source_unsupported_field)::text;
      end if;
    end if;
    if v_location_update is null or jsonb_typeof(v_location_update)<>'object'
       or nullif(btrim(coalesce(v_location_update->>'timesheet_id','')),'') is null
       or coalesce(v_location_update->>'timesheet_id','') !~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
       or coalesce(v_location_update->>'expected_document_revision','') !~ '^[1-9][0-9]*$'
       or not (v_location_update ? 'hospital_norm' or v_location_update ? 'ward_norm')
       or (
         v_location_update ? 'hospital_norm'
         and jsonb_typeof(v_location_update->'hospital_norm') not in('string','null')
       )
       or (
         v_location_update ? 'ward_norm'
         and jsonb_typeof(v_location_update->'ward_norm') not in('string','null')
       )
    then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
        detail=jsonb_build_object('field','timesheet_location_updates','reason','INVALID_ITEM')::text;
    end if;
    v_source_edit_key:=lower(v_location_update->>'timesheet_id');
    v_source_expected_revision:=(v_location_update->>'expected_document_revision')::bigint;
    if v_source_updates_map ? v_source_edit_key
       and (
         coalesce((v_source_updates_map#>>
           array[v_source_edit_key,'has_hospital_norm'])::boolean,false)
         or coalesce((v_source_updates_map#>>
           array[v_source_edit_key,'has_ward_norm'])::boolean,false)
       ) then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_DUPLICATE_TIMESHEET',
        detail=jsonb_build_object('timesheet_id',v_source_edit_key,'field','timesheet_location_updates')::text;
    end if;
    if v_source_updates_map ? v_source_edit_key
       and (v_source_updates_map#>>array[v_source_edit_key,'expected_document_revision'])::bigint
           is distinct from v_source_expected_revision then
      raise exception using errcode='40001',
        message='INVOICE_SOURCE_EDIT_STALE_REVISION',
        detail=jsonb_build_object('timesheet_id',v_source_edit_key,'reason','EXPECTED_REVISION_CONFLICT')::text;
    end if;
    v_source_update:=coalesce(v_source_updates_map->v_source_edit_key,
      jsonb_build_object(
        'timesheet_id',v_source_edit_key,
        'expected_document_revision',v_source_expected_revision,
        'has_reference_number',false,
        'has_day_references_json',false,
        'has_actual_schedule_json',false));
    v_source_update:=v_source_update||jsonb_build_object(
      'has_hospital_norm',v_location_update ? 'hospital_norm',
      'hospital_norm',case when v_location_update ? 'hospital_norm'
        then to_jsonb(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'hospital_norm','')),
          '[[:space:]]+',' ','g')))
        else 'null'::jsonb end,
      'has_ward_norm',v_location_update ? 'ward_norm',
      'ward_norm',case when v_location_update ? 'ward_norm'
        then to_jsonb(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'ward_norm','')),
          '[[:space:]]+',' ','g')))
        else 'null'::jsonb end);
    v_source_updates_map:=jsonb_set(v_source_updates_map,array[v_source_edit_key],v_source_update,true);
  end loop;
end if;
v_source_edit_requested:=v_source_updates_map<>'{}'::jsonb;
v_has_seg_ops :=
  (v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0)
  or (v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0);

  v_refresh_hr_cache := (coalesce(array_length(v_add_ts_ids,1),0) > 0) or coalesce(v_has_seg_ops,false);

  if v_invoice_debug then
    v_dbg_stats := v_dbg_stats || jsonb_build_object(
      'remove_invoice_line_ids_count', coalesce(array_length(v_remove_ids,1),0),
      'add_timesheet_ids_count', coalesce(array_length(v_add_ts_ids,1),0),
      'add_adjustments_count', case when p_payload is not null and jsonb_typeof(p_payload)='object' and (p_payload ? 'add_adjustments') and jsonb_typeof(p_payload->'add_adjustments')='array' then jsonb_array_length(p_payload->'add_adjustments') else 0 end,
      'remove_segment_refs_count', case when v_remove_seg_refs is null then 0 else jsonb_array_length(coalesce(v_remove_seg_refs,'[]'::jsonb)) end,
      'add_segment_refs_count', case when v_add_seg_refs is null then 0 else jsonb_array_length(coalesce(v_add_seg_refs,'[]'::jsonb)) end,
      'timesheet_location_updates_count',v_location_count,
      'has_segment_ops', v_has_seg_ops
    );
    v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','payload_parsed','stats',v_dbg_stats));
  end if;

  if v_source_edit_requested then
    -- Source edits are a narrower authority than ordinary draft edits.
    if v_inv.issued_at_utc is not null then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_ISSUED';
    end if;
    if v_inv.paid_at_utc is not null then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_PAID';
    end if;
    if v_inv.active_issue_operation_id is not null
       or upper(coalesce(v_inv.issue_state,'')) in
          ('VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
       or exists(
         select 1
         from public.invoice_operation_chunks c
         where c.chunk_type='ISSUE_INVOICE'
           and c.entity_type='INVOICE'
           and c.entity_id=p_invoice_id
           and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
       )
    then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_ISSUE_IN_PROGRESS';
    end if;

    v_pre_edit_invoice_revision:=v_inv.document_revision;
    v_pre_edit_preview_document_version_id:=v_inv.preview_document_version_id;
    v_pre_edit_active_document_operation_id:=v_inv.active_document_operation_id;

    -- Only an exact current-revision V8 preview is authoritative. Legacy PDF
    -- summary fields do not independently force or suppress replacement.
    select exists(
      select 1
      from public.invoice_document_versions dv
      left join public.invoice_operations op on op.id=dv.operation_id
      where dv.entity_type='INVOICE'
        and dv.entity_id=p_invoice_id
        and dv.purpose='DRAFT_PREVIEW'
        and dv.source_revision=v_pre_edit_invoice_revision::text
        and (
          (dv.status='READY'
            and nullif(btrim(coalesce(dv.r2_key,'')),'') is not null
            and nullif(btrim(coalesce(dv.sha256,'')),'') is not null
            and coalesce(dv.size_bytes,0)>0
            and coalesce(dv.page_count,0)>0)
          or (
            dv.id=v_pre_edit_preview_document_version_id
            and dv.status in(
              'PLANNING','WAITING_FOR_INPUTS','RENDERING',
              'ASSEMBLING','VERIFYING','READY'))
          or (
            dv.operation_id=v_pre_edit_active_document_operation_id
            and op.operation_type='BUILD_DOCUMENT'
            and op.entity_type='INVOICE'
            and op.entity_id=p_invoice_id
            and op.source_revision=v_pre_edit_invoice_revision::text
            and coalesce(op.input_json->>'purpose','')='DRAFT_PREVIEW'
            and op.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
        )
    ) into v_source_edit_preexisting_preview;

    -- Clear any caller-controlled or earlier transaction-local value before
    -- establishing this function's exact-once invalidation marker.
    perform set_config('cloudtms.invoice_source_edit_marker','{}',true);

    -- Lock every target and its current financial snapshot in deterministic
    -- order before validating ownership, revisions or segment identities.
    for v_ref_ts in
      select t.*
      from public.timesheets t
      join jsonb_each(v_source_updates_map) desired on true
      where t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      order by t.timesheet_id
      for update of t
    loop
      null;
    end loop;
    for v_ref_ts in
      select tf.id
      from public.timesheets_financials tf
      join jsonb_each(v_source_updates_map) desired
        on tf.timesheet_id=(desired.value->>'timesheet_id')::uuid
      where tf.is_current
      order by tf.timesheet_id,tf.id
      for update of tf
    loop
      null;
    end loop;

    if (
      select count(*)
      from public.timesheets t
      join jsonb_each(v_source_updates_map) desired
        on t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      where t.is_current
    )<>(select count(*) from jsonb_each(v_source_updates_map)) then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_SOURCE_NOT_CURRENT';
    end if;

    if exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      join public.timesheets t
        on t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      where t.document_revision is distinct from
        (desired.value->>'expected_document_revision')::bigint
    ) then
      raise exception using errcode='40001',
        message='INVOICE_SOURCE_EDIT_STALE_REVISION';
    end if;

    if exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      where not exists(
        select 1
        from public.invoice_lines owned_line
        where owned_line.invoice_id=p_invoice_id
          and (
            owned_line.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              owned_line.timesheet_id is null
              and coalesce(owned_line.meta_json->>'timesheet_id','') ~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (owned_line.meta_json->>'timesheet_id')::uuid=
                  (desired.value->>'timesheet_id')::uuid
            )
          )
      )
    ) then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_SOURCE_NOT_OWNED';
    end if;

    -- Preserve the established import-authoritative correction lock across
    -- both supported source carriers.
    if exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      where coalesce((
        public._ctms_import_correction_classify_v1(
          (desired.value->>'timesheet_id')::uuid)
          ->>'is_import_authoritative_correction')::boolean,false)
    ) then
      raise exception using errcode='55000',
        message='IMPORT_AUTHORITATIVE_CORRECTION_SOURCE_EDIT_FORBIDDEN';
    end if;

    -- Reject a mixed command that edits a source while removing every carrier
    -- for that source in the same Save.
    if coalesce(array_length(v_remove_ids,1),0)>0 and exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      where exists(
        select 1
        from public.invoice_lines carrier
        where carrier.invoice_id=p_invoice_id
          and (
            carrier.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              carrier.timesheet_id is null
              and coalesce(carrier.meta_json->>'timesheet_id','') ~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (carrier.meta_json->>'timesheet_id')::uuid=
                  (desired.value->>'timesheet_id')::uuid
            )
          )
      )
      and not exists(
        select 1
        from public.invoice_lines retained
        where retained.invoice_id=p_invoice_id
          and not (retained.id=any(v_remove_ids))
          and (
            retained.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              retained.timesheet_id is null
              and coalesce(retained.meta_json->>'timesheet_id','') ~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (retained.meta_json->>'timesheet_id')::uuid=
                  (desired.value->>'timesheet_id')::uuid
            )
          )
      )
    ) then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_CONFLICTING_COMMAND';
    end if;

    -- Classify each source from the locked current financial snapshot. A
    -- SEGMENTS payload is an editable reference projection only: it can never
    -- replace timesheets.actual_schedule_json or any financial segment object.
    for v_ref_ts in
      select t.*,desired.value desired_value,
        tf.id financial_id,tf.invoice_breakdown_json financial_breakdown
      from public.timesheets t
      join jsonb_each(v_source_updates_map) desired
        on t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      left join lateral(
        select f.id,f.invoice_breakdown_json
        from public.timesheets_financials f
        where f.timesheet_id=t.timesheet_id and f.is_current
        order by f.updated_at desc nulls last,f.created_at desc nulls last,f.id desc
        limit 1
      ) tf on true
      where t.is_current
      order by t.timesheet_id
    loop
      v_source_update:=v_ref_ts.desired_value;
      v_source_financial_id:=v_ref_ts.financial_id;
      v_source_ib:=v_ref_ts.financial_breakdown;
      v_source_mode:=upper(coalesce(v_source_ib->>'mode',''));
      v_source_payload_schedule:=v_source_update->'actual_schedule_json';
      v_source_canonical_schedule:=v_ref_ts.actual_schedule_json;
      v_source_new_segments:=null;
      v_source_segment_updates:='[]'::jsonb;
      v_source_segment_reference_changed:=false;

      if (v_source_update->>'has_actual_schedule_json')::boolean then
        if jsonb_typeof(v_source_payload_schedule)<>'array' then
          raise exception using errcode='22023',
            message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
            detail=jsonb_build_object(
              'field','actual_schedule_json','reason','ARRAY_REQUIRED',
              'timesheet_id',v_ref_ts.timesheet_id)::text;
        end if;

        if v_source_mode='SEGMENTS' then
          if v_source_financial_id is null
             or jsonb_typeof(v_source_ib->'segments')<>'array' then
            raise exception using errcode='55000',
              message='INVOICE_SOURCE_EDIT_SOURCE_NOT_CURRENT';
          end if;

          v_ref_seen_keys:='{}'::jsonb;
          for v_source_payload_segment,v_source_payload_ord in
            select value,ord
            from jsonb_array_elements(v_source_payload_schedule)
              with ordinality payload(value,ord)
          loop
            if jsonb_typeof(v_source_payload_segment)<>'object' then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','INVALID_SEGMENT',
                  'ordinal',v_source_payload_ord)::text;
            end if;
            select min(field_name) into v_source_unsupported_field
            from jsonb_object_keys(v_source_payload_segment) field_name
            where not (field_name=any(v_allowed_reference_segment_fields));
            if v_source_unsupported_field is not null then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','UNSUPPORTED_FIELD',
                  'unsupported_field',v_source_unsupported_field,
                  'ordinal',v_source_payload_ord)::text;
            end if;
            if not (v_source_payload_segment ? 'ref_num') then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','REFERENCE_REQUIRED',
                  'ordinal',v_source_payload_ord)::text;
            end if;

            v_ref_seg_id:=nullif(btrim(coalesce(
              v_source_payload_segment->>'segment_id','')),'');
            v_source_match_count:=0;
            v_source_match_segment:=null;
            if v_ref_seg_id is not null then
              select count(*),min(seg.value::text)::jsonb
              into v_source_match_count,v_source_match_segment
              from jsonb_array_elements(v_source_ib->'segments') seg(value)
              where nullif(btrim(coalesce(seg.value->>'segment_id','')),'')=v_ref_seg_id
                and nullif(btrim(coalesce(
                  seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text;
            end if;

            if v_source_match_count=0 then
              begin
                v_source_start:=nullif(btrim(coalesce(
                  v_source_payload_segment->>'start_utc',
                  v_source_payload_segment->>'start','')),'')::timestamptz;
                v_source_end:=nullif(btrim(coalesce(
                  v_source_payload_segment->>'end_utc',
                  v_source_payload_segment->>'end','')),'')::timestamptz;
              exception when others then
                raise exception using errcode='22023',
                  message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                  detail=jsonb_build_object(
                    'field','actual_schedule_json','reason','INVALID_SEGMENT_TIME',
                    'ordinal',v_source_payload_ord)::text;
              end;
              if v_source_start is null or v_source_end is null then
                raise exception using errcode='22023',
                  message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                  detail=jsonb_build_object(
                    'field','actual_schedule_json','reason','SEGMENT_IDENTITY_REQUIRED',
                    'ordinal',v_source_payload_ord)::text;
              end if;
              select count(*),min(seg.value::text)::jsonb
              into v_source_match_count,v_source_match_segment
              from jsonb_array_elements(v_source_ib->'segments') seg(value)
              where nullif(btrim(coalesce(
                    seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text
                and nullif(btrim(coalesce(
                    seg.value->>'start_utc',seg.value->>'start','')),'')::timestamptz
                    =v_source_start
                and nullif(btrim(coalesce(
                    seg.value->>'end_utc',seg.value->>'end','')),'')::timestamptz
                    =v_source_end;
            end if;

            if v_source_match_count<>1 or v_source_match_segment is null then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts.timesheet_id,
                  'ordinal',v_source_payload_ord,
                  'match_count',v_source_match_count)::text;
            end if;

            v_ref_sched_key:=coalesce(
              'SID:'||nullif(btrim(coalesce(
                v_source_match_segment->>'segment_id','')),''),
              'SE:'||coalesce(v_source_match_segment->>'start_utc',
                v_source_match_segment->>'start','')||'|'||
                coalesce(v_source_match_segment->>'end_utc',
                  v_source_match_segment->>'end',''));
            if v_ref_seen_keys ? v_ref_sched_key then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts.timesheet_id,
                  'reason','DUPLICATE_IDENTITY')::text;
            end if;
            v_ref_seen_keys:=jsonb_set(
              v_ref_seen_keys,array[v_ref_sched_key],'true'::jsonb,true);
            v_ref_seg_new_ref:=nullif(btrim(coalesce(
              v_source_payload_segment->>'ref_num','')),'');
            v_source_segment_reference_changed:=
              v_source_segment_reference_changed or (
                nullif(btrim(coalesce(
                  v_source_match_segment->>'ref_num','')),'')
                is distinct from v_ref_seg_new_ref);
            v_source_segment_updates:=v_source_segment_updates||
              jsonb_build_array(jsonb_build_object(
                'segment_id',nullif(btrim(coalesce(
                  v_source_match_segment->>'segment_id','')),''),
                'start_identity',coalesce(v_source_match_segment->>'start_utc',
                  v_source_match_segment->>'start'),
                'end_identity',coalesce(v_source_match_segment->>'end_utc',
                  v_source_match_segment->>'end'),
                'ref_num',v_ref_seg_new_ref));
          end loop;

          v_source_new_segments:='[]'::jsonb;
          for v_source_current_segment in
            select value
            from jsonb_array_elements(v_source_ib->'segments')
              with ordinality current_segment(value,ord)
            order by ord
          loop
            v_source_payload_segment:=null;
            select update_item.value into v_source_payload_segment
            from jsonb_array_elements(v_source_segment_updates) update_item(value)
            where (
              nullif(update_item.value->>'segment_id','') is not null
              and nullif(btrim(coalesce(
                v_source_current_segment->>'segment_id','')),'')
                  =nullif(update_item.value->>'segment_id','')
            ) or (
              nullif(update_item.value->>'segment_id','') is null
              and coalesce(v_source_current_segment->>'start_utc',
                    v_source_current_segment->>'start')
                  is not distinct from update_item.value->>'start_identity'
              and coalesce(v_source_current_segment->>'end_utc',
                    v_source_current_segment->>'end')
                  is not distinct from update_item.value->>'end_identity'
            )
            limit 1;
            if v_source_payload_segment is not null then
              v_source_current_segment:=jsonb_set(
                v_source_current_segment,'{ref_num}',
                coalesce(v_source_payload_segment->'ref_num','null'::jsonb),true);
            end if;
            v_source_new_segments:=v_source_new_segments||
              jsonb_build_array(v_source_current_segment);
          end loop;
        else
          v_source_canonical_schedule:=coalesce(
            v_ref_ts.actual_schedule_json,'[]'::jsonb);
          if jsonb_typeof(v_source_canonical_schedule)<>'array'
             or jsonb_array_length(v_source_payload_schedule)
                <>jsonb_array_length(v_source_canonical_schedule) then
            raise exception using errcode='22023',
              message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
              detail=jsonb_build_object(
                'field','actual_schedule_json','reason','STRUCTURE_MISMATCH',
                'timesheet_id',v_ref_ts.timesheet_id)::text;
          end if;
          v_ref_new_segments:='[]'::jsonb;
          for v_source_payload_segment,v_source_current_segment,v_source_payload_ord in
            select p.value,c.value,p.ord
            from jsonb_array_elements(v_source_payload_schedule)
              with ordinality p(value,ord)
            join jsonb_array_elements(v_source_canonical_schedule)
              with ordinality c(value,ord) using(ord)
            order by p.ord
          loop
            if jsonb_typeof(v_source_payload_segment)<>'object'
               or jsonb_typeof(v_source_current_segment)<>'object'
               or not (v_source_payload_segment ? 'ref_num')
               or (v_source_payload_segment-'ref_num') is distinct from
                  (v_source_current_segment-'ref_num') then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','STRUCTURE_MISMATCH',
                  'ordinal',v_source_payload_ord)::text;
            end if;
            v_ref_new_segments:=v_ref_new_segments||jsonb_build_array(
              jsonb_set(v_source_current_segment,'{ref_num}',
                coalesce(v_source_payload_segment->'ref_num','null'::jsonb),true));
          end loop;
          v_source_canonical_schedule:=v_ref_new_segments;
        end if;
      end if;

      v_source_timesheet_row_changed:=(
        ((v_source_update->>'has_reference_number')::boolean
          and v_ref_ts.reference_number is distinct from
            nullif(btrim(coalesce(v_source_update->>'reference_number','')),''))
        or ((v_source_update->>'has_day_references_json')::boolean
          and v_ref_ts.day_references_json is distinct from
            case when jsonb_typeof(v_source_update->'day_references_json')='null'
              then null else v_source_update->'day_references_json' end)
        or ((v_source_update->>'has_actual_schedule_json')::boolean
          and v_source_mode<>'SEGMENTS'
          and v_ref_ts.actual_schedule_json is distinct from
            v_source_canonical_schedule)
        or ((v_source_update->>'has_hospital_norm')::boolean
          and lower(regexp_replace(
                btrim(coalesce(v_ref_ts.hospital_norm,'')),
                '[[:space:]]+',' ','g')) is distinct from
            coalesce(v_source_update->>'hospital_norm',''))
        or ((v_source_update->>'has_ward_norm')::boolean
          and lower(regexp_replace(
                btrim(coalesce(v_ref_ts.ward_norm,'')),
                '[[:space:]]+',' ','g')) is distinct from
            coalesce(v_source_update->>'ward_norm',''))
      );
      v_source_update:=v_source_update||jsonb_build_object(
        'source_mode',v_source_mode,
        'financial_id',v_source_financial_id,
        'canonical_actual_schedule_json',v_source_canonical_schedule,
        'new_financial_segments',v_source_new_segments,
        'segment_updates',v_source_segment_updates,
        'segment_reference_changed',v_source_segment_reference_changed,
        'timesheet_row_changed',v_source_timesheet_row_changed);
      v_source_updates_map:=jsonb_set(
        v_source_updates_map,array[v_ref_ts.timesheet_id::text],
        v_source_update,true);
    end loop;

    select coalesce(array_agg(
      (desired.value->>'timesheet_id')::uuid order by desired.key),
      array[]::uuid[])
    into v_source_changed_ts_ids
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_row_changed')::boolean
       or (desired.value->>'segment_reference_changed')::boolean;

    select coalesce(array_agg(
      (desired.value->>'timesheet_id')::uuid order by desired.key),
      array[]::uuid[])
    into v_refupd_ts_ids
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_id')::uuid=any(v_source_changed_ts_ids)
      and (
        (desired.value->>'has_reference_number')::boolean
        or (desired.value->>'has_day_references_json')::boolean
        or (
          (desired.value->>'has_actual_schedule_json')::boolean
          and (
            (desired.value->>'timesheet_row_changed')::boolean
            or (desired.value->>'segment_reference_changed')::boolean
          )
        )
      );

    select coalesce(array_agg(
      (desired.value->>'timesheet_id')::uuid order by desired.key),
      array[]::uuid[])
    into v_location_ts_ids
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_row_changed')::boolean
      and (
        (desired.value->>'has_hospital_norm')::boolean
        or (desired.value->>'has_ward_norm')::boolean
      );

    if exists(
      select 1 from jsonb_each(v_source_updates_map) desired
      where (desired.value->>'timesheet_row_changed')::boolean
    ) then
      with desired as materialized(
        select (value->>'timesheet_id')::uuid as timesheet_id,value
        from jsonb_each(v_source_updates_map)
      )
      update public.timesheets t
      set updated_at=v_now,
          reference_number=case when (d.value->>'has_reference_number')::boolean
            then nullif(btrim(coalesce(d.value->>'reference_number','')),'')
            else t.reference_number end,
          day_references_json=case when (d.value->>'has_day_references_json')::boolean
            then case when jsonb_typeof(d.value->'day_references_json')='null'
              then null else d.value->'day_references_json' end
            else t.day_references_json end,
          actual_schedule_json=case
            when (d.value->>'has_actual_schedule_json')::boolean
              and d.value->>'source_mode'<>'SEGMENTS'
            then d.value->'canonical_actual_schedule_json'
            else t.actual_schedule_json end,
          hospital_norm=case when (d.value->>'has_hospital_norm')::boolean
            then coalesce(d.value->>'hospital_norm','') else t.hospital_norm end,
          ward_norm=case when (d.value->>'has_ward_norm')::boolean
            then coalesce(d.value->>'ward_norm','') else t.ward_norm end
      from desired d
      where t.timesheet_id=d.timesheet_id
        and (d.value->>'timesheet_row_changed')::boolean
        and t.is_current;
    end if;

    v_refupd_applied:=cardinality(v_refupd_ts_ids);
    v_location_applied:=cardinality(v_location_ts_ids);

    -- Marker rows exist only where the timesheet statement has already advanced
    -- the source revision and the following SEGMENTS ref update would otherwise
    -- invalidate the same source a second time.
    select coalesce(jsonb_object_agg(
      t.timesheet_id::text,
      jsonb_build_object('expected_revision',t.document_revision)),
      '{}'::jsonb)
    into v_source_marker_rows
    from jsonb_each(v_source_updates_map) desired
    join public.timesheets t
      on t.timesheet_id=(desired.value->>'timesheet_id')::uuid and t.is_current
    where (desired.value->>'timesheet_row_changed')::boolean
      and (desired.value->>'segment_reference_changed')::boolean
      and t.document_revision=
        (desired.value->>'expected_document_revision')::bigint+1;
    if (select count(*) from jsonb_each(v_source_marker_rows))<>(
      select count(*) from jsonb_each(v_source_updates_map) desired
      where (desired.value->>'timesheet_row_changed')::boolean
        and (desired.value->>'segment_reference_changed')::boolean
    ) then
      raise exception using errcode='55000',
        message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
        detail=jsonb_build_object(
          'reason','SOURCE_REVISION_ADVANCE_NOT_EXACT')::text;
    end if;
    v_source_marker:=jsonb_build_object(
      'txid',txid_current()::text,'rows',v_source_marker_rows);
    perform set_config(
      'cloudtms.invoice_source_edit_marker',v_source_marker::text,true);

    -- The legacy per-array loops below remain structurally compatible for old
    -- payload code, but the strict merged statement above is now the sole
    -- execution path for accepted source edits.
    v_reference_updates:=null;
    v_location_updates:=null;
  end if;





  -- 0) Apply reference updates to timesheets (does NOT recompute TSFIN; it updates the source timesheet refs)
  if v_reference_updates is not null and jsonb_typeof(v_reference_updates)='array' and jsonb_array_length(v_reference_updates) > 0 then
    for v_refupd in
      select value from jsonb_array_elements(v_reference_updates) value
    loop
      if v_refupd is null or jsonb_typeof(v_refupd) <> 'object' then
        continue;
      end if;

      if nullif(btrim(coalesce(v_refupd->>'timesheet_id','')), '') is null then
        continue;
      end if;

      v_refupd_ts_id := (v_refupd->>'timesheet_id')::uuid;

      v_refupd_set_refnum := (v_refupd ? 'reference_number');
      v_refupd_set_dayrefs := (v_refupd ? 'day_references_json');
      v_refupd_set_sched := (v_refupd ? 'actual_schedule_json');

      v_refupd_refnum := null;
      if v_refupd_set_refnum then
        v_refupd_refnum := nullif(btrim(coalesce(v_refupd->>'reference_number','')), '');
      end if;

      v_refupd_dayrefs := null;
      if v_refupd_set_dayrefs then
        v_refupd_dayrefs := v_refupd->'day_references_json';
        if v_refupd_dayrefs is not null and jsonb_typeof(v_refupd_dayrefs) = 'null' then
          v_refupd_dayrefs := null;
        end if;
      end if;

      v_refupd_sched := null;
      if v_refupd_set_sched then
        v_refupd_sched := v_refupd->'actual_schedule_json';
        if v_refupd_sched is not null and jsonb_typeof(v_refupd_sched) = 'null' then
          v_refupd_sched := null;
        end if;
      end if;

      update public.timesheets tsu
      set
        updated_at = v_now,
        reference_number = case when v_refupd_set_refnum then v_refupd_refnum else tsu.reference_number end,
        day_references_json = case when v_refupd_set_dayrefs then v_refupd_dayrefs else tsu.day_references_json end,
        actual_schedule_json = case when v_refupd_set_sched then v_refupd_sched else tsu.actual_schedule_json end
      where tsu.timesheet_id = v_refupd_ts_id
        and tsu.is_current = true
        and exists(
          select 1
          from public.invoice_lines owned_line
          where owned_line.invoice_id=p_invoice_id
            and (
              owned_line.timesheet_id=tsu.timesheet_id
              or (
                owned_line.timesheet_id is null
                and coalesce(owned_line.meta_json->>'timesheet_id','')
                  ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                and (owned_line.meta_json->>'timesheet_id')::uuid=tsu.timesheet_id
              )
            )
        )
        and (
          (v_refupd_set_refnum and tsu.reference_number is distinct from v_refupd_refnum)
          or (v_refupd_set_dayrefs and tsu.day_references_json is distinct from v_refupd_dayrefs)
          or (v_refupd_set_sched and tsu.actual_schedule_json is distinct from v_refupd_sched)
        );

      get diagnostics v_rc = row_count;
      if coalesce(v_rc,0) > 0 then
        v_refupd_applied := v_refupd_applied + 1;
        v_refupd_ts_ids := v_refupd_ts_ids || v_refupd_ts_id;
      end if;

    end loop;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object('step','reference_updates_applied','count_requested',v_refupd_count,'count_applied',v_refupd_applied)
      );
    end if;
  end if;

  -- 0a) Apply only material hospital/ward changes to current timesheets that
  -- are owned by this invoice. Identical canonical values perform no UPDATE,
  -- so they cannot dirty documents or queue replacement work.
  if v_location_updates is not null
     and jsonb_typeof(v_location_updates)='array'
     and jsonb_array_length(v_location_updates)>0 then
    for v_location_update in
      select value from jsonb_array_elements(v_location_updates) value
    loop
      if v_location_update is null
         or jsonb_typeof(v_location_update)<>'object'
         or nullif(btrim(coalesce(v_location_update->>'timesheet_id','')),'') is null then
        continue;
      end if;

      v_location_ts_id := (v_location_update->>'timesheet_id')::uuid;
      v_location_set_hospital := v_location_update ? 'hospital_norm';
      v_location_set_ward := v_location_update ? 'ward_norm';
      if not v_location_set_hospital and not v_location_set_ward then
        continue;
      end if;

      v_location_hospital_norm := null;
      if v_location_set_hospital then
        v_location_hospital_norm := nullif(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'hospital_norm','')),
          '[[:space:]]+',' ','g')), '');
      end if;
      v_location_ward_norm := null;
      if v_location_set_ward then
        v_location_ward_norm := nullif(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'ward_norm','')),
          '[[:space:]]+',' ','g')), '');
      end if;

      update public.timesheets tsu
      set updated_at=v_now,
          hospital_norm=case when v_location_set_hospital
            then v_location_hospital_norm else tsu.hospital_norm end,
          ward_norm=case when v_location_set_ward
            then v_location_ward_norm else tsu.ward_norm end
      where tsu.timesheet_id=v_location_ts_id
        and tsu.is_current=true
        and exists(
          select 1
          from public.invoice_lines owned_line
          where owned_line.invoice_id=p_invoice_id
            and (
              owned_line.timesheet_id=tsu.timesheet_id
              or (
                owned_line.timesheet_id is null
                and coalesce(owned_line.meta_json->>'timesheet_id','')
                  ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                and (owned_line.meta_json->>'timesheet_id')::uuid=tsu.timesheet_id
              )
            )
        )
        and (
          (v_location_set_hospital
            and tsu.hospital_norm is distinct from v_location_hospital_norm)
          or (v_location_set_ward
            and tsu.ward_norm is distinct from v_location_ward_norm)
        );

      get diagnostics v_rc=row_count;
      if coalesce(v_rc,0)>0 then
        v_location_applied:=v_location_applied+1;
        v_location_ts_ids:=v_location_ts_ids||v_location_ts_id;
      end if;
    end loop;

    if v_invoice_debug then
      v_dbg_steps:=v_dbg_steps||jsonb_build_array(jsonb_build_object(
        'step','timesheet_location_updates_applied',
        'count_requested',v_location_count,
        'count_applied',v_location_applied));
    end if;
  end if;
  -- 0b) After reference updates: refresh invoice_lines meta (ts_reference_number / schedule_ref_nums) and
  -- synchronise only the approved ref_num fields into the locked SEGMENTS
  -- financial projection. The full financial objects and their order are
  -- preserved from the locked current snapshot.
  if v_refupd_ts_ids is not null and coalesce(array_length(v_refupd_ts_ids,1),0) > 0 then

    with desired as materialized(
      select value
      from jsonb_each(v_source_updates_map)
      where (value->>'segment_reference_changed')::boolean
    )
    update public.timesheets_financials tf
    set invoice_breakdown_json=jsonb_set(
      coalesce(tf.invoice_breakdown_json,'{}'::jsonb),
      '{segments}',d.value->'new_financial_segments',true)
    from desired d
    where tf.id=(d.value->>'financial_id')::uuid
      and tf.is_current
      and tf.timesheet_id=(d.value->>'timesheet_id')::uuid
      and upper(coalesce(tf.invoice_breakdown_json->>'mode',''))='SEGMENTS'
      and tf.invoice_breakdown_json->'segments'
        is distinct from d.value->'new_financial_segments';
    get diagnostics v_refupd_tsfin_rows_updated=row_count;
    select coalesce(sum(jsonb_array_length(
      coalesce(value->'segment_updates','[]'::jsonb))),0)::integer
    into v_refupd_tsfin_segments_updated
    from jsonb_each(v_source_updates_map)
    where (value->>'segment_reference_changed')::boolean;

    -- The marker has served its single synchronisation statement. Clear it so
    -- no later statement in this transaction can inherit suppression authority.
    perform set_config('cloudtms.invoice_source_edit_marker','{}',true);

    select array_agg(distinct x)
    into v_refupd_ts_ids_distinct
    from unnest(v_refupd_ts_ids) x
    where x is not null;

    v_refupd_ts_ids_distinct := coalesce(v_refupd_ts_ids_distinct, array[]::uuid[]);

    foreach v_ref_ts_id in array v_refupd_ts_ids_distinct loop
      if v_ref_ts_id is null then
        continue;
      end if;

      select tsu.*
      into v_ref_ts
      from public.timesheets tsu
      where tsu.timesheet_id = v_ref_ts_id
        and tsu.is_current = true
      limit 1;

      if not found then
        continue;
      end if;
      v_source_update:=v_source_updates_map->v_ref_ts_id::text;
      v_source_mode:=upper(coalesce(v_source_update->>'source_mode',''));
      v_source_canonical_schedule:=case when v_source_mode='SEGMENTS'
        then coalesce(v_source_update->'new_financial_segments','[]'::jsonb)
        else coalesce(v_ref_ts.actual_schedule_json,'[]'::jsonb) end;

      -- Build the cache from the exact authority used by this invoice. SEGMENTS
      -- refs come from invoice-owned financial segments; other modes use the
      -- structurally preserved timesheet schedule.
      v_ref_schedule_refs := '[]'::jsonb;

      if nullif(btrim(coalesce(v_ref_ts.reference_number,'')), '') is not null then
        v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(nullif(btrim(v_ref_ts.reference_number),'')));
      end if;

      if v_ref_ts.day_references_json is not null and jsonb_typeof(v_ref_ts.day_references_json) = 'object' then
        for kv in
          select key as k, value as v
          from jsonb_each_text(v_ref_ts.day_references_json)
        loop
          if nullif(btrim(coalesce(kv.v,'')), '') is not null then
            v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(nullif(btrim(kv.v),'')));
          end if;
        end loop;
      end if;

      if jsonb_typeof(v_source_canonical_schedule)='array' then
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_source_canonical_schedule) value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            continue;
          end if;
          v_ref_sched_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
          if v_ref_sched_ref is not null then
            v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(v_ref_sched_ref));
          end if;
        end loop;
      end if;

      select coalesce(jsonb_agg(to_jsonb(x) order by x), '[]'::jsonb)
      into v_ref_schedule_refs_distinct
      from (
        select distinct btrim(t.x) as x
        from jsonb_array_elements_text(coalesce(v_ref_schedule_refs,'[]'::jsonb)) as t(x)
        where nullif(btrim(coalesce(t.x,'')), '') is not null
      ) q;

      -- Refresh meta_json on all invoice lines for this timesheet
      update public.invoice_lines ilu
      set meta_json = coalesce(ilu.meta_json, '{}'::jsonb) || jsonb_build_object(
        'ts_reference_number', nullif(btrim(coalesce(v_ref_ts.reference_number,'')), ''),
        'schedule_ref_nums', coalesce(v_ref_schedule_refs_distinct, '[]'::jsonb),
        'schedule_ref_count', jsonb_array_length(coalesce(v_ref_schedule_refs_distinct, '[]'::jsonb))
      )
      where ilu.invoice_id = p_invoice_id
        and (
          ilu.timesheet_id=v_ref_ts_id
          or (
            ilu.timesheet_id is null
            and coalesce(ilu.meta_json->>'timesheet_id','') ~
              '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
            and (ilu.meta_json->>'timesheet_id')::uuid=v_ref_ts_id
          )
        )
        and (
          ilu.meta_json->'ts_reference_number' is distinct from
            coalesce(to_jsonb(nullif(btrim(coalesce(v_ref_ts.reference_number,'')),'')),'null'::jsonb)
          or ilu.meta_json->'schedule_ref_nums' is distinct from
            coalesce(v_ref_schedule_refs_distinct,'[]'::jsonb)
          or ilu.meta_json->'schedule_ref_count' is distinct from
            to_jsonb(jsonb_array_length(coalesce(v_ref_schedule_refs_distinct,'[]'::jsonb)))
        );

      get diagnostics v_rc = row_count;
      v_refupd_meta_rows_updated := v_refupd_meta_rows_updated + coalesce(v_rc,0);

      -- Best-effort sync: update tsfin.invoice_breakdown_json segments[].ref_num by matching start/end fields
      v_ref_seg_updates_this_ts := 0;
      v_ref_seg_matches_this_ts := 0;
      v_ref_tsfin_id := null;
      v_ref_ib := null;

        select tfu.id, tfu.invoice_breakdown_json
      into v_ref_tsfin_id, v_ref_ib
      from public.timesheets_financials tfu
      where tfu.timesheet_id = v_ref_ts_id
        and tfu.is_current = true
      limit 1
      for update;


      if v_ref_tsfin_id is not null
         and v_source_mode<>'SEGMENTS'
         and v_ref_ib is not null
         and jsonb_typeof(v_ref_ib) = 'object'
         and upper(coalesce(v_ref_ib->>'mode','')) = 'SEGMENTS'
         and jsonb_typeof(v_ref_ib->'segments') = 'array'
         and v_ref_ts.actual_schedule_json is not null
         and jsonb_typeof(v_ref_ts.actual_schedule_json) = 'array'
       then
         -- Build a map of segment_id (preferred) or start|end -> ref_num from actual_schedule_json
        v_ref_sched_map := '{}'::jsonb;
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_ref_ts.actual_schedule_json) value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            continue;
          end if;

          v_ref_sched_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
          v_ref_seg_id := nullif(btrim(coalesce(v_ref_seg_obj->>'segment_id','')), '');
          if v_ref_seg_id is not null then
            v_ref_sched_key := 'SID:' || v_ref_seg_id;
            if v_ref_sched_map ? v_ref_sched_key then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                  'side','TIMESHEET_SCHEDULE')::text;
            end if;
            v_ref_sched_map := jsonb_set(v_ref_sched_map, array[v_ref_sched_key], case when v_ref_sched_ref is null then 'null'::jsonb else to_jsonb(v_ref_sched_ref) end, true);
            continue;
          end if;

          v_ref_seg_start := nullif(btrim(coalesce(v_ref_seg_obj->>'start_utc', v_ref_seg_obj->>'start', '')), '');
          v_ref_seg_end := nullif(btrim(coalesce(v_ref_seg_obj->>'end_utc', v_ref_seg_obj->>'end', '')), '');
          if v_ref_seg_start is null or v_ref_seg_end is null then
            continue;
          end if;

          v_ref_sched_key := 'SE:' || v_ref_seg_start || '|' || v_ref_seg_end;
          if v_ref_sched_map ? v_ref_sched_key then
            raise exception using errcode='22023',
              message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
              detail=jsonb_build_object(
                'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                'side','TIMESHEET_SCHEDULE')::text;
          end if;
          v_ref_sched_map := jsonb_set(v_ref_sched_map, array[v_ref_sched_key], case when v_ref_sched_ref is null then 'null'::jsonb else to_jsonb(v_ref_sched_ref) end, true);
        end loop;

        v_ref_new_segments := '[]'::jsonb;
        v_ref_seen_keys := '{}'::jsonb;
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_ref_ib->'segments') value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            v_ref_new_segments := v_ref_new_segments || jsonb_build_array(v_ref_seg_obj);
            continue;
          end if;

          v_ref_seg_new_ref := null;
          v_ref_seg_has_update := false;

          v_ref_seg_id := nullif(btrim(coalesce(v_ref_seg_obj->>'segment_id','')), '');
          if v_ref_seg_id is not null then
            v_ref_sched_key := 'SID:' || v_ref_seg_id;
            if v_ref_seen_keys ? v_ref_sched_key then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                  'side','FINANCIAL_SEGMENTS')::text;
            end if;
            v_ref_seen_keys:=jsonb_set(v_ref_seen_keys,array[v_ref_sched_key],'true'::jsonb,true);
            if v_ref_sched_map ? v_ref_sched_key then
              v_ref_seg_new_ref := nullif(btrim(coalesce(v_ref_sched_map->>v_ref_sched_key,'')), '');
              v_ref_seg_has_update := true;
              v_ref_seg_matches_this_ts := v_ref_seg_matches_this_ts + 1;
            end if;
          end if;

          if not v_ref_seg_has_update then
            v_ref_seg_start := nullif(btrim(coalesce(v_ref_seg_obj->>'start_utc', v_ref_seg_obj->>'start', '')), '');
            v_ref_seg_end := nullif(btrim(coalesce(v_ref_seg_obj->>'end_utc', v_ref_seg_obj->>'end', '')), '');

            if v_ref_seg_start is not null and v_ref_seg_end is not null then
              v_ref_sched_key := 'SE:' || v_ref_seg_start || '|' || v_ref_seg_end;
              if v_ref_seen_keys ? v_ref_sched_key then
                raise exception using errcode='22023',
                  message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                  detail=jsonb_build_object(
                    'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                    'side','FINANCIAL_SEGMENTS')::text;
              end if;
              v_ref_seen_keys:=jsonb_set(v_ref_seen_keys,array[v_ref_sched_key],'true'::jsonb,true);
              if v_ref_sched_map ? v_ref_sched_key then
                v_ref_seg_new_ref := nullif(btrim(coalesce(v_ref_sched_map->>v_ref_sched_key,'')), '');
                v_ref_seg_has_update := true;
                v_ref_seg_matches_this_ts := v_ref_seg_matches_this_ts + 1;
              end if;
            end if;
          end if;

          if v_ref_seg_has_update then
            v_ref_seg_cur_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
            if v_ref_seg_cur_ref is distinct from v_ref_seg_new_ref then
              v_ref_seg_obj := jsonb_set(
                v_ref_seg_obj,
                '{ref_num}',
                case when v_ref_seg_new_ref is null then 'null'::jsonb else to_jsonb(v_ref_seg_new_ref) end,
                true
              );
              v_ref_seg_updates_this_ts := v_ref_seg_updates_this_ts + 1;
            end if;
          end if;

          v_ref_new_segments := v_ref_new_segments || jsonb_build_array(v_ref_seg_obj);
        end loop;



        if v_ref_sched_map is not null
           and jsonb_typeof(v_ref_sched_map) = 'object'
           and v_ref_sched_map <> '{}'::jsonb
           and coalesce(jsonb_array_length(coalesce(v_ref_ib->'segments','[]'::jsonb)),0) > 0
           and coalesce(v_ref_seg_matches_this_ts,0) = 0
        then
          raise exception 'SEGMENTS reference sync failed: no segments matched schedule keys (timesheet_id=% tsfin_id=%)', v_ref_ts_id, v_ref_tsfin_id;
        end if;

        if v_ref_seg_updates_this_ts > 0 then
              update public.timesheets_financials tfu2
          set invoice_breakdown_json = jsonb_set(coalesce(tfu2.invoice_breakdown_json, '{}'::jsonb), '{segments}', v_ref_new_segments, true)
          where tfu2.id = v_ref_tsfin_id
            and tfu2.is_current = true;


          get diagnostics v_rc = row_count;
          if coalesce(v_rc,0) > 0 then
            v_refupd_tsfin_rows_updated := v_refupd_tsfin_rows_updated + 1;
            v_refupd_tsfin_segments_updated := v_refupd_tsfin_segments_updated + v_ref_seg_updates_this_ts;
          end if;
        end if;

      end if;

    end loop;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','reference_updates_meta_refreshed',
          'timesheets_count', coalesce(array_length(v_refupd_ts_ids_distinct,1),0),
          'invoice_line_rows_updated', v_refupd_meta_rows_updated,
          'tsfin_rows_updated', v_refupd_tsfin_rows_updated,
          'tsfin_segments_refnum_updated', v_refupd_tsfin_segments_updated
        )
      );
    end if;
  end if;

  -- 1) Removals (by invoice_line_id)
  if v_remove_ids is not null and coalesce(array_length(v_remove_ids,1),0) > 0 then
    -- collect timesheet_ids touched (any)
    select array_agg(distinct l.timesheet_id) filter (where l.timesheet_id is not null)
    into v_ts_ids_touched
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and l.id = any(v_remove_ids);

    -- record removed lines for history
    v_hist_lines_removed := coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb);

    -- Only unlock TSFIN when HOURS lines were removed (prevents accidental unlock when deleting only expenses/other lines)
    -- IMPORTANT: compute BEFORE deletion because we match on the removed invoice_line ids.
    select array_agg(distinct l.timesheet_id) filter (where l.timesheet_id is not null)
    into v_cw_ts_ids
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and l.id = any(v_remove_ids)
      and upper(coalesce(l.meta_json->>'line_type','')) in ('HOURS_WEEKLY','HOURS_DAILY');

    v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);



    if coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
      v_refresh_hr_cache := true;
    end if;
delete from public.invoice_lines
    where invoice_id = p_invoice_id
      and id = any(v_remove_ids);
    get diagnostics v_rc = row_count;
    v_dbg_lines_deleted := v_dbg_lines_deleted + coalesce(v_rc,0);

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','lines_removed',
          'rows_deleted', coalesce(v_rc,0),
          'timesheets_touched', coalesce(array_length(v_ts_ids_touched,1),0),
          'timesheets_to_unlock_count', coalesce(array_length(v_cw_ts_ids,1),0)
        )
      );
    end if;

    -- FIX: if a touched timesheet now has NO remaining invoice_lines on this invoice,
    -- unlock it even if the removed lines were expenses/mileage/additional (expense-only/SEGMENTS-empty case).
    v_ts_ids_fully_removed := array[]::uuid[];
    select array_agg(distinct x)
    into v_ts_ids_fully_removed
    from unnest(coalesce(v_ts_ids_touched, array[]::uuid[])) x
    where x is not null
      and not exists (
        select 1
        from public.invoice_lines l2
        where l2.invoice_id = p_invoice_id
          and l2.timesheet_id = x
      );

    v_ts_ids_fully_removed := coalesce(v_ts_ids_fully_removed, array[]::uuid[]);

    if coalesce(array_length(v_ts_ids_fully_removed,1),0) > 0 then
      -- unlock any segments locked to this invoice (safe no-op for SEGMENTS-empty)
      perform public._inv_unlock_segments_for_invoice(p_invoice_id, v_ts_ids_fully_removed);

      -- clear whole-timesheet lock if it was set for this invoice (SEGMENTS-empty / non-segments / pseudo segment_id null locks)
      update public.timesheets_financials tfu_lock
      set
        locked_by_invoice_id = null,
        locked_at_utc = null,
        updated_at = v_now
      where tfu_lock.is_current = true
        and tfu_lock.timesheet_id = any(v_ts_ids_fully_removed)
        and tfu_lock.locked_by_invoice_id = p_invoice_id;

      get diagnostics v_rc = row_count;
      v_dbg_timesheets_unlocked := v_dbg_timesheets_unlocked + coalesce(v_rc,0);

      v_refresh_hr_cache := true;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','timesheets_unlocked_after_full_removal',
            'timesheets_fully_removed_count', coalesce(array_length(v_ts_ids_fully_removed,1),0),
            'tsfin_rows_unlocked_count', coalesce(v_rc,0)
          )
        );
      end if;
    end if;

    v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);

    if v_cw_ts_ids is not null and coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
      perform public._inv_unlock_segments_for_invoice(p_invoice_id, v_cw_ts_ids);
      v_dbg_timesheets_unlocked := v_dbg_timesheets_unlocked + coalesce(array_length(v_cw_ts_ids,1),0);

      -- Cleanup: if no segments remain on THIS invoice for a touched timesheet, remove ALL remaining invoice lines for that timesheet
      foreach tsid in array v_cw_ts_ids loop
        if tsid is null then continue; end if;

        -- detect if any segments are still locked to THIS invoice
        select tf.*
        into snap
        from public.timesheets_financials tf
        where tf.is_current = true
          and tf.timesheet_id = tsid
        limit 1;

        if not found then
          continue;
        end if;

        segments := '[]'::jsonb;
        if snap.invoice_breakdown_json is not null
           and jsonb_typeof(snap.invoice_breakdown_json)='object'
           and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
           and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
        then
          for v_seg in
            select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
          loop
            if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
              continue;
            end if;
            seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
            if seg_locked = p_invoice_id::text then
              segments := segments || jsonb_build_array(v_seg);
            end if;
          end loop;

          if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
            delete from public.invoice_lines
            where invoice_id = p_invoice_id
              and timesheet_id = tsid;
          end if;
        else
          -- Non-segments: if snapshot is no longer locked to this invoice, remove all remaining invoice lines for this timesheet
          if snap.locked_by_invoice_id is null then
            delete from public.invoice_lines
            where invoice_id = p_invoice_id
              and timesheet_id = tsid;
          end if;
        end if;
      end loop;
    end if;

    -- History event (always)
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_LINES_REMOVED',
      null,
      jsonb_build_object('remove_invoice_line_ids', v_hist_lines_removed, 'timesheet_ids_touched', coalesce(to_jsonb(v_ts_ids_touched), '[]'::jsonb)),
      null,
      p_actor_user_id
    );

  end if;


  -- 1b) Segment edits (SEGMENTS mode only)
-- NOTE: Segment moves are NOT allowed when additional rates OR expenses/mileage exist on the TSFIN snapshot.
-- Payload contract uses tsfin_id + segment_id.
if v_has_seg_ops then

  v_dbg_seg_add_refs := case when v_add_seg_refs is null then 0 else jsonb_array_length(coalesce(v_add_seg_refs,'[]'::jsonb)) end;
  v_dbg_seg_remove_refs := case when v_remove_seg_refs is null then 0 else jsonb_array_length(coalesce(v_remove_seg_refs,'[]'::jsonb)) end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','segment_ops_start',
        'add_segment_refs_count', v_dbg_seg_add_refs,
        'remove_segment_refs_count', v_dbg_seg_remove_refs
      )
    );
  end if;

  -- Collect distinct tsfin_ids involved in segment ops
  select array_agg(distinct (x->>'tsfin_id')::uuid)
  into v_seg_tsfin_ids
  from (
    select value as x
    from jsonb_array_elements(coalesce(v_remove_seg_refs,'[]'::jsonb))
    union all
    select value as x
    from jsonb_array_elements(coalesce(v_add_seg_refs,'[]'::jsonb))
  ) u
  where jsonb_typeof(x) = 'object'
    and nullif(btrim(coalesce(x->>'tsfin_id','')), '') is not null;

  v_seg_tsfin_ids := coalesce(v_seg_tsfin_ids, array[]::uuid[]);

  v_dbg_seg_tsfins := coalesce(array_length(v_seg_tsfin_ids,1),0);
  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','segment_ops_targets','tsfin_count',v_dbg_seg_tsfins));
  end if;

  if coalesce(array_length(v_seg_tsfin_ids,1),0) > 0 then

    -- Validate that each snapshot is current SEGMENTS mode and has NO additional/expenses/mileage
    foreach v_tsfin_id in array v_seg_tsfin_ids loop
      select *
      into snap
      from public.timesheets_financials tf
      where tf.id = v_tsfin_id
      limit 1;

      if not found then
        raise exception 'Segment edit refers to unknown tsfin_id %', v_tsfin_id;
      end if;

      if snap.is_current is not true then
        raise exception 'Segment edit requires current tsfin snapshot (tsfin_id=%)', v_tsfin_id;
      end if;

      if snap.client_id is distinct from v_inv.client_id then
        raise exception 'Segment edit timesheet client mismatch (tsfin_id=%)', v_tsfin_id;
      end if;

      if snap.invoice_breakdown_json is null
         or jsonb_typeof(snap.invoice_breakdown_json) <> 'object'
         or upper(coalesce(snap.invoice_breakdown_json->>'mode','')) <> 'SEGMENTS'
         or jsonb_typeof(snap.invoice_breakdown_json->'segments') <> 'array'
      then
        raise exception 'Segment edit is only supported for SEGMENTS timesheets (tsfin_id=%)', v_tsfin_id;
      end if;

      v_has_additional :=
        public._inv_round2(coalesce(snap.additional_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.additional_charge_ex_vat,0)) <> 0
        or (snap.additional_units_json is not null and jsonb_typeof(snap.additional_units_json)='object' and snap.additional_units_json <> '{}'::jsonb);

      v_has_expense_or_mileage :=
        public._inv_round2(coalesce(snap.expenses_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.expenses_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_units,0)) <> 0
        or public._inv_round2(coalesce(snap.travel_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.travel_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.accommodation_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.accommodation_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.other_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.other_charge_ex_vat,0)) <> 0;

      if v_has_additional then
        raise exception 'Segments cannot be moved when additional rates exist (timesheet_id=% tsfin_id=%)', snap.timesheet_id, v_tsfin_id;
      end if;

      if v_has_expense_or_mileage then
        raise exception 'Segments cannot be moved when expenses or mileage exist (timesheet_id=% tsfin_id=%)', snap.timesheet_id, v_tsfin_id;
      end if;
    end loop;

    -- Validate and apply add_segment_refs (lock selected segments)
    v_seg_refs_to_lock := '[]'::jsonb;
    if v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0 then
      for v_ref in
        select value from jsonb_array_elements(v_add_seg_refs) value
      loop
        if v_ref is null or jsonb_typeof(v_ref) <> 'object' then
          continue;
        end if;

        if nullif(btrim(coalesce(v_ref->>'tsfin_id','')), '') is null then
          continue;
        end if;

        v_tsfin_id := (v_ref->>'tsfin_id')::uuid;
        v_seg_id := nullif(btrim(coalesce(v_ref->>'segment_id','')), '');

        if v_seg_id is null then
          raise exception 'add_segment_refs requires segment_id (tsfin_id=%)', v_tsfin_id;
        end if;

          -- Load snapshot + timesheet + precheck (must be OK)
        select
          tf.*,
          tsr.sheet_scope::text as sheet_scope,
          coalesce(tsr.submission_mode::text,'') as submission_mode,
          tsr.day_references_json,
          tsr.actual_schedule_json,
          tsr.week_ending_date,
          cw.contract_id
        into snap
        from public.timesheets_financials tf
        join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
        left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
        join public.v_ts_invoice_precheck pcv on pcv.timesheet_id = tf.timesheet_id
        where tf.id = v_tsfin_id
          and tf.is_current = true
          and tf.client_id = v_inv.client_id
          and upper(coalesce(pcv.precheck_status,''))
            in ('OK','BLOCK_NO_PDF')
        limit 1;

        if not found then
          raise exception 'Segment add failed eligibility (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        natural_start := (snap.week_ending_date::date - 6);

        -- Locate the segment object
        v_seg := null;
        for v_seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;
          if nullif(btrim(coalesce(v_seg->>'segment_id','')), '') = v_seg_id then
            exit;
          end if;
          v_seg := null;
        end loop;

        if v_seg is null then
          raise exception 'Segment not found (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
        if seg_locked is not null then
          raise exception 'Segment already invoiced (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        seg_target := nullif(btrim(coalesce(v_seg->>'invoice_target_week_start','')), '')::date;
        seg_ref := btrim(coalesce(v_seg->>'ref_num',''));

        -- segment-level ref gating if required
        select * into pc from public.v_ts_invoice_precheck where timesheet_id = snap.timesheet_id limit 1;
        if pc.require_reference_to_invoice is true and seg_ref = '' then
          raise exception 'Segment missing reference number (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
        end if;

        -- Week eligibility
        if seg_target is null or seg_target = natural_start then
          if v_week_start <> natural_start then
            raise exception 'Segment not eligible for this invoice week (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
        else
          if v_week_start <> seg_target then
            raise exception 'Delayed segment not eligible for this invoice week (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
          if seg_target > v_anchor_ymd then
            raise exception 'Delayed segment cannot be invoiced early (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
        end if;

        v_seg_refs_to_lock := v_seg_refs_to_lock || jsonb_build_array(
          jsonb_build_object(
            'tsfin_id', v_tsfin_id::text,
            'segment_id', v_seg_id
          )
        );
      end loop;

      if jsonb_typeof(v_seg_refs_to_lock) = 'array' and jsonb_array_length(v_seg_refs_to_lock) > 0 then
        perform public._inv_lock_segments_for_invoice(p_invoice_id, v_seg_refs_to_lock);
      end if;
    end if;

      -- Apply remove_segment_refs (unlock selected segments on THIS invoice)
    if v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0 then
      perform public._inv_unlock_segment_refs_for_invoice(p_invoice_id, v_remove_seg_refs::jsonb, p_actor_user_id);
    end if;



    -- History: segment ops (always)
    if v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0 then
      perform public._audit_insert(
        'invoices',
        p_invoice_id::text,
        'INVOICE_SEGMENTS_ADDED',
        null,
        jsonb_build_object('add_segment_refs', v_add_seg_refs),
        null,
        p_actor_user_id
      );
    end if;
    if v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0 then
      perform public._audit_insert(
        'invoices',
        p_invoice_id::text,
        'INVOICE_SEGMENTS_REMOVED',
        null,
        jsonb_build_object('remove_segment_refs', v_remove_seg_refs),
        null,
        p_actor_user_id
      );
    end if;

    -- Rebuild HOURS lines for touched timesheets on this invoice
    select array_agg(distinct tf.timesheet_id)
    into v_seg_ts_ids
    from public.timesheets_financials tf
    where tf.id = any(v_seg_tsfin_ids);

    v_seg_ts_ids := coalesce(v_seg_ts_ids, array[]::uuid[]);

    foreach tsid in array v_seg_ts_ids loop
      if tsid is null then
        continue;
      end if;

      -- Load snapshot + timesheet + contract (no READY_FOR_INVOICE restriction; this is an invoice edit)
      select
        tf.*,
        tsr.booking_id,
        tsr.week_ending_date,
        tsr.reference_number,
        tsr.sheet_scope::text as sheet_scope,
        coalesce(tsr.submission_mode::text,'') as submission_mode,
        tsr.day_references_json,
        tsr.actual_schedule_json,
        cw.contract_id
      into snap
      from public.timesheets_financials tf
      join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
      left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
      where tf.timesheet_id = tsid
        and tf.is_current = true
      limit 1;

      if not found then
        v_dbg_add_timesheets_skipped := v_dbg_add_timesheets_skipped + 1;
        if v_invoice_debug then
          v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_skipped','timesheet_id',tsid::text));
        end if;
        continue;
      end if;

      v_dbg_add_timesheets_found := v_dbg_add_timesheets_found + 1;
      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_loaded','timesheet_id',tsid::text,'tsfin_id',snap.id::text));
      end if;

      select v.r2_key
      into v_exact_timesheet_document_r2_key
      from public.timesheets t
      join public.invoice_document_versions v
        on v.entity_type='TIMESHEET'
       and v.entity_id=t.timesheet_id
       and v.purpose='TIMESHEET'
       and v.source_revision=t.document_revision::text
       and v.status='READY'
       and nullif(v.r2_key,'') is not null
       and nullif(v.sha256,'') is not null
       and coalesce(v.size_bytes,0)>0
       and coalesce(v.page_count,0)>0
      where t.timesheet_id=tsid and t.is_current
      order by v.ready_at_utc desc nulls last,v.id desc
      limit 1;


      contract_id := snap.contract_id;
      c_daily_calc := false;
      c_bucket_labels := null;
      c_display_site := null;

      if contract_id is not null then
        select
          nullif(btrim(coalesce(display_site,'')), '')
        into
          c_display_site
        from public.contracts
        where id = contract_id
        limit 1;
      end if;

      v_settings_authority := private._timesheet_settings_authority_frozen_v1(tsid);
      c_daily_calc := coalesce(
        (v_settings_authority#>>'{values,daily_calc_of_invoices}')::boolean,
        false
      );
      c_bucket_labels := case
        when coalesce((v_settings_authority->>'override_client_settings')::boolean,false)
          then v_settings_authority#>'{values,bucket_labels_json}'
        else null
      end;

      if c_bucket_labels is null then
        c_bucket_labels := jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH');
      end if;

      -- Build segment set locked to THIS invoice
      segments := '[]'::jsonb;
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
         and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
      then
        for v_seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
          if seg_locked = p_invoice_id::text then
            segments := segments || jsonb_build_array(v_seg);
          end if;
        end loop;
      end if;

      if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
        -- If no segments remain on this invoice for this timesheet, remove ALL invoice lines for that timesheet
        delete from public.invoice_lines
        where invoice_id = p_invoice_id
          and timesheet_id = tsid;
        v_dbg_seg_timesheets_removed := v_dbg_seg_timesheets_removed + 1;
        continue;
      end if;

      v_dbg_seg_timesheets_rebuilt := v_dbg_seg_timesheets_rebuilt + 1;

      -- Replace HOURS lines for this timesheet on this invoice
      delete from public.invoice_lines
      where invoice_id = p_invoice_id
        and timesheet_id = tsid
        and upper(coalesce(meta_json->>'line_type','')) in ('HOURS_WEEKLY','HOURS_DAILY');
        -- HOURS lines
        if c_daily_calc then
          -- Daily: group by segment.date
          for r_day in
            with rows as (
              select
                nullif(btrim(coalesce(seg_el->>'date','')), '') as ymd,
                coalesce((seg_el->>'hours_day')::numeric,0)   as h_day,
                coalesce((seg_el->>'hours_night')::numeric,0) as h_night,
                coalesce((seg_el->>'hours_sat')::numeric,0)   as h_sat,
                coalesce((seg_el->>'hours_sun')::numeric,0)   as h_sun,
                coalesce((seg_el->>'hours_bh')::numeric,0)    as h_bh,
                coalesce((seg_el->>'pay_amount')::numeric,0)  as pay_ex,
                coalesce((seg_el->>'charge_amount')::numeric,0) as chg_ex
              from jsonb_array_elements(segments) seg_el
            ),
            agg as (
              select
                ymd,
                sum(rows.h_day)::numeric as hours_day,
                sum(rows.h_night)::numeric as hours_night,
                sum(rows.h_sat)::numeric as hours_sat,
                sum(rows.h_sun)::numeric as hours_sun,
                sum(rows.h_bh)::numeric as hours_bh,
                sum(rows.pay_ex)::numeric as pay_ex,
                sum(rows.chg_ex)::numeric as chg_ex
              from rows
              where ymd is not null and ymd ~ '^\d{4}-\d{2}-\d{2}$'
              group by ymd
            )
            select * from agg order by ymd
          loop
          chg_ex := public._inv_round2(r_day.chg_ex);
if chg_ex = 0 then continue; end if;

if (coalesce(r_day.hours_day,0)+coalesce(r_day.hours_night,0)+coalesce(r_day.hours_sat,0)+coalesce(r_day.hours_sun,0)+coalesce(r_day.hours_bh,0)) = 0 then
  continue;
end if;


            pay_ex := public._inv_round2(r_day.pay_ex);
            margin_ex := public._inv_round2(chg_ex - pay_ex);
            vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
            inc_amt := public._inv_round2(chg_ex + vat_amt);

            line_desc := coalesce(nullif(btrim(coalesce(c_display_site,'')) ,''), ('TS '||tsid::text)) ||
                         ' – '|| r_day.ymd || ' – W/E '|| coalesce(snap.week_ending_date::text,'');

            v_meta := jsonb_build_object(
              'line_type','HOURS_DAILY',
              'timesheet_id', tsid::text,
              'tsfin_id', snap.id::text,
              'candidate_display', coalesce(nullif(btrim(coalesce(c_display_site,'')),''), null),
              'role', c_role,
              'hospital', c_display_site,
              'ward', c_ward_hint,
              'week_ending_date', snap.week_ending_date::text,
              'date', r_day.ymd,
              'bucket_labels', c_bucket_labels
            );

            v_source_key := 'TS:' || tsid::text || ':HOURS:' || r_day.ymd;

            insert into public.invoice_lines(
              invoice_id, timesheet_id, booking_id, description,
              hours_day, hours_night, hours_sat, hours_sun, hours_bh,
              pay_day, pay_night, pay_sat, pay_sun, pay_bh,
              charge_day, charge_night, charge_sat, charge_sun, charge_bh,
              total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
              vat_rate_pct, vat_amount, total_inc_vat,
              paper_ts_r2_key, meta_json, source_key
            )
            values (
              p_invoice_id, tsid, snap.booking_id, line_desc,
              public._inv_round2(r_day.hours_day), public._inv_round2(r_day.hours_night), public._inv_round2(r_day.hours_sat), public._inv_round2(r_day.hours_sun), public._inv_round2(r_day.hours_bh),
              null,null,null,null,null,
              null,null,null,null,null,
              pay_ex, chg_ex, margin_ex,
              v_vat_rate, vat_amt, inc_amt,
              v_exact_timesheet_document_r2_key,
              v_meta,
              v_source_key
            )
            on conflict (invoice_id, source_key) do nothing;
          end loop;
        else
          -- Weekly hours line
          select
            public._inv_round2(coalesce(sum((seg_el->>'hours_day')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_night')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_sat')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_sun')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_bh')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'pay_amount')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'charge_amount')::numeric),0))
          into h_day, h_night, h_sat, h_sun, h_bh, pay_ex, chg_ex
          from jsonb_array_elements(segments) seg_el;

       if chg_ex <> 0 and (coalesce(h_day,0)+coalesce(h_night,0)+coalesce(h_sat,0)+coalesce(h_sun,0)+coalesce(h_bh,0)) <> 0 then
  margin_ex := public._inv_round2(chg_ex - pay_ex);
  vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
  inc_amt := public._inv_round2(chg_ex + vat_amt);

  line_desc := coalesce(nullif(btrim(coalesce(c_display_site,'')) ,''), ('TS '||tsid::text)) ||
               ' – W/E '|| coalesce(snap.week_ending_date::text,'');

  v_meta := jsonb_build_object(
    'line_type','HOURS_WEEKLY',
    'timesheet_id', tsid::text,
    'tsfin_id', snap.id::text,
    'week_ending_date', snap.week_ending_date::text,
    'bucket_labels', c_bucket_labels
  );

  v_source_key := 'TS:' || tsid::text || ':HOURS:WEEK';

  insert into public.invoice_lines(
    invoice_id, timesheet_id, booking_id, description,
    hours_day, hours_night, hours_sat, hours_sun, hours_bh,
    pay_day, pay_night, pay_sat, pay_sun, pay_bh,
    charge_day, charge_night, charge_sat, charge_sun, charge_bh,
    total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
    vat_rate_pct, vat_amount, total_inc_vat,
    paper_ts_r2_key, meta_json, source_key
  )
  values (
    p_invoice_id, tsid, snap.booking_id, line_desc,
    h_day, h_night, h_sat, h_sun, h_bh,
    null,null,null,null,null,
    null,null,null,null,null,
    pay_ex, chg_ex, margin_ex,
    v_vat_rate, vat_amt, inc_amt,
    v_exact_timesheet_document_r2_key,
    v_meta,
    v_source_key
  )
  on conflict (invoice_id, source_key) do nothing;
end if;

        end if;

        -- Additional rates


    end loop;
  end if;
end if;

  -- 2) Add adjustments
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_adjustments') then
    if jsonb_typeof(p_payload->'add_adjustments') = 'array' then
      for adj in
        select value from jsonb_array_elements(p_payload->'add_adjustments') value
      loop
        if adj is null or jsonb_typeof(adj) <> 'object' then
          continue;
        end if;

        v_adj_token := nullif(btrim(coalesce(adj->>'client_token','')), '');
        v_adj_desc  := nullif(btrim(coalesce(adj->>'description','')), '');
        begin
          v_adj_ex := (adj->>'amount_ex_vat')::numeric;
        exception when others then
          v_adj_ex := null;
        end;

        if v_adj_token is null or v_adj_desc is null or v_adj_ex is null then
          continue;
        end if;

        v_adj_vat := public._inv_round2(v_adj_ex * v_vat_rate / 100);
        if v_vat_rate = 0 then v_adj_vat := 0; end if;
        v_adj_inc := public._inv_round2(v_adj_ex + v_adj_vat);

        v_adj_source_key := 'ADJ:' || v_adj_token;

        v_meta := jsonb_build_object(
          'line_type','ADJUSTMENT',
          'client_token', v_adj_token,
          'description', v_adj_desc,
          'amount_ex_vat', public._inv_round2(v_adj_ex),
          'vat_rate_pct', v_vat_rate,
          'vat_chargeable', v_vat_chargeable
        );

        v_hist_adj := v_hist_adj || jsonb_build_array(v_meta);

        insert into public.invoice_lines(
          invoice_id, timesheet_id, booking_id, description,
          hours_day, hours_night, hours_sat, hours_sun, hours_bh,
          pay_day, pay_night, pay_sat, pay_sun, pay_bh,
          charge_day, charge_night, charge_sat, charge_sun, charge_bh,
          total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
          vat_rate_pct, vat_amount, total_inc_vat,
          paper_ts_r2_key, meta_json, source_key
        )
        values (
          p_invoice_id, null, null, v_adj_desc,
          0,0,0,0,0,
          null,null,null,null,null,
          null,null,null,null,null,
          0, public._inv_round2(v_adj_ex), public._inv_round2(v_adj_ex),
          v_vat_rate, v_adj_vat, v_adj_inc,
          null,
          v_meta,
          v_adj_source_key
        )
        on conflict (invoice_id, source_key) do nothing;
      end loop;
    end if;
  end if;

  -- History: adjustments added (always)
  if jsonb_typeof(v_hist_adj)='array' and jsonb_array_length(v_hist_adj) > 0 then
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_ADJUSTMENTS_ADDED',
      null,
      jsonb_build_object('adjustments', v_hist_adj),
      null,
      p_actor_user_id
    );
  end if;


  -- 3) Add timesheets (full parity: hours + additional + expenses + mileage), for THIS invoice week_start
  if v_add_ts_ids is not null and coalesce(array_length(v_add_ts_ids,1),0) > 0 then
    foreach tsid in array v_add_ts_ids loop
      if tsid is null then
        continue;
      end if;

      -- Load snapshot + timesheet + precheck
      select
        tf.*,
        tsr.booking_id,
        tsr.week_ending_date,
        tsr.reference_number,
        tsr.sheet_scope::text as sheet_scope,
        coalesce(tsr.submission_mode::text,'') as submission_mode,
        tsr.day_references_json,
        tsr.actual_schedule_json,
        cw.contract_id
      into snap
      from public.timesheets_financials tf
      join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
      left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
      join public.v_ts_invoice_precheck pcv on pcv.timesheet_id = tf.timesheet_id
      where tf.timesheet_id = tsid
        and tf.is_current = true
        and tf.locked_by_invoice_id is null
        and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        and upper(coalesce(pcv.precheck_status,''))
          in ('OK','BLOCK_NO_PDF')
        and tf.client_id = v_inv.client_id
        and (
          pcv.require_reference_to_invoice is not true
          or public._inv_timesheet_has_invoice_reference(
                tsr.sheet_scope::text,
                coalesce(tsr.submission_mode::text,''),
                tsr.reference_number,
                tsr.day_references_json,
                tsr.actual_schedule_json
             )
        )
      limit 1;

      if not found then
        v_dbg_add_timesheets_skipped := v_dbg_add_timesheets_skipped + 1;
        if v_invoice_debug then
          v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_skipped','timesheet_id',coalesce(tsid::text,'')));
        end if;
        continue;
      end if;

      v_dbg_add_timesheets_found := v_dbg_add_timesheets_found + 1;
      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_loaded','timesheet_id',coalesce(tsid::text,''),'tsfin_id',coalesce(snap.id::text,'')));
      end if;

      select v.r2_key
      into v_exact_timesheet_document_r2_key
      from public.timesheets t
      join public.invoice_document_versions v
        on v.entity_type='TIMESHEET'
       and v.entity_id=t.timesheet_id
       and v.purpose='TIMESHEET'
       and v.source_revision=t.document_revision::text
       and v.status='READY'
       and nullif(v.r2_key,'') is not null
       and nullif(v.sha256,'') is not null
       and coalesce(v.size_bytes,0)>0
       and coalesce(v.page_count,0)>0
      where t.timesheet_id=tsid and t.is_current
      order by v.ready_at_utc desc nulls last,v.id desc
      limit 1;
      contract_id := snap.contract_id;
      c_daily_calc := false;
      c_bucket_labels := null;
      c_role := null;
      c_display_site := null;
      c_ward_hint := null;

      if contract_id is not null then
        select
          nullif(btrim(coalesce(role,'')), ''),
          nullif(btrim(coalesce(display_site,'')), ''),
          nullif(btrim(coalesce(ward_hint,'')), '')
        into
          c_role, c_display_site, c_ward_hint
        from public.contracts
        where id = contract_id
        limit 1;
      end if;

      v_settings_authority := private._timesheet_settings_authority_frozen_v1(tsid);
      c_daily_calc := coalesce(
        (v_settings_authority#>>'{values,daily_calc_of_invoices}')::boolean,
        false
      );
      c_bucket_labels := case
        when coalesce((v_settings_authority->>'override_client_settings')::boolean,false)
          then v_settings_authority#>'{values,bucket_labels_json}'
        else null
      end;

      if c_bucket_labels is null then
        c_bucket_labels := jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH');
      end if;

      natural_start := (snap.week_ending_date::date - 6);

      segments := '[]'::jsonb;

      -- Build invoiceable segment set for THIS invoice week_start
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
         and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
      then
        for v_seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
          if seg_locked is not null then
            continue; -- already invoiced
          end if;

          seg_target := nullif(btrim(coalesce(v_seg->>'invoice_target_week_start','')), '')::date;
          seg_ref := btrim(coalesce(v_seg->>'ref_num',''));

          -- segment-level ref gating if required
          select * into pc from public.v_ts_invoice_precheck where timesheet_id = tsid limit 1;
          if pc.require_reference_to_invoice is true and seg_ref = '' then
            continue;
          end if;

          -- delayed detection
          if seg_target is null or seg_target = natural_start then
            -- not delayed: belongs to natural week only
            if v_week_start <> natural_start then
              continue;
            end if;
            -- allow early is implicit for invoice edits (invoice is already being edited)
            segments := segments || jsonb_build_array(v_seg);
          else
            -- delayed: only if this invoice week matches target AND target has arrived (no early invoicing for delayed)
            if v_week_start = seg_target and seg_target <= v_anchor_ymd then
              segments := segments || jsonb_build_array(v_seg);
            end if;
          end if;
        end loop;
      else
        -- Non-segments: include only when invoice week matches natural week
        if v_week_start = natural_start then
          segments := jsonb_build_array(
            jsonb_build_object(
              'segment_id', null,
              'date', coalesce(snap.week_ending_date::text, v_week_start::text),
              'hours_day', public._inv_round2(coalesce(snap.hours_day,0)),
              'hours_night', public._inv_round2(coalesce(snap.hours_night,0)),
              'hours_sat', public._inv_round2(coalesce(snap.hours_sat,0)),
              'hours_sun', public._inv_round2(coalesce(snap.hours_sun,0)),
              'hours_bh', public._inv_round2(coalesce(snap.hours_bh,0)),
              'pay_amount', public._inv_round2(
                coalesce(snap.total_pay_ex_vat,0)
                - coalesce(snap.additional_pay_ex_vat,0)
                - coalesce(snap.expenses_pay_ex_vat,0)
                - coalesce(snap.mileage_pay_ex_vat,0)
              ),
              'charge_amount', public._inv_round2(
                coalesce(snap.total_charge_ex_vat,0)
                - coalesce(snap.additional_charge_ex_vat,0)
                - coalesce(snap.expenses_charge_ex_vat,0)
                - coalesce(snap.mileage_charge_ex_vat,0)
              ),
              'ref_num', coalesce(snap.reference_number,'')
            )
          );
        end if;
      end if;

      if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
        continue;
      end if;

      -- Build the exact invoice-line economics from the frozen current financial snapshot.
      -- This mirrors Invoice Async V8 generation and never derives economics from mutable live rows.
      select p.vat_rate
      into v_vat_rate
      from private._invoice_generation_vat_policy_batch(jsonb_build_array(jsonb_build_object(
        'source_member_key','invoice-edit:'||tsid::text,
        'source_type','TIMESHEET',
        'source_id',tsid,
        'timesheet_id',tsid,
        'effective_date',snap.week_ending_date
      ))) p
      where p.source_member_key='invoice-edit:'||tsid::text
        and p.valid
        and p.vat_rate is not null
      limit 1;

      if not found then
        raise exception 'INVOICE_EDIT_VAT_POLICY_UNRESOLVED'
          using errcode='P0001',detail=jsonb_build_object(
            'invoice_id',p_invoice_id,'timesheet_id',tsid)::text;
      end if;

      insert into public.invoice_lines(
        invoice_id,timesheet_id,booking_id,description,
        hours_day,hours_night,hours_sat,hours_sun,hours_bh,
        pay_day,pay_night,pay_sat,pay_sun,pay_bh,
        charge_day,charge_night,charge_sat,charge_sun,charge_bh,
        total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,
        vat_rate_pct,vat_amount,total_inc_vat,
        paper_ts_r2_key,meta_json,source_key
      )
      with base as materialized (
        select coalesce(
          (select nullif(btrim(coalesce(cd.display_name,'')),'')
             from public.candidates cd where cd.id=snap.candidate_id limit 1),
          'TS '||tsid::text
        ) candidate_display
      ),
      segment_rows as materialized (
        select
          case when left(coalesce(s.value->>'date',''),10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            then left(s.value->>'date',10) end work_date,
          case when pg_input_is_valid(s.value->>'hours_day','numeric') then (s.value->>'hours_day')::numeric else 0 end h_day,
          case when pg_input_is_valid(s.value->>'hours_night','numeric') then (s.value->>'hours_night')::numeric else 0 end h_night,
          case when pg_input_is_valid(s.value->>'hours_sat','numeric') then (s.value->>'hours_sat')::numeric else 0 end h_sat,
          case when pg_input_is_valid(s.value->>'hours_sun','numeric') then (s.value->>'hours_sun')::numeric else 0 end h_sun,
          case when pg_input_is_valid(s.value->>'hours_bh','numeric') then (s.value->>'hours_bh')::numeric else 0 end h_bh,
          case
            when pg_input_is_valid(s.value->>'pay_amount','numeric') then (s.value->>'pay_amount')::numeric
            when pg_input_is_valid(s.value->>'pay_ex_vat','numeric') then (s.value->>'pay_ex_vat')::numeric
            else 0 end pay_ex,
          case
            when pg_input_is_valid(s.value->>'charge_amount','numeric') then (s.value->>'charge_amount')::numeric
            when pg_input_is_valid(s.value->>'charge_ex_vat','numeric') then (s.value->>'charge_ex_vat')::numeric
            else 0 end charge_ex
        from jsonb_array_elements(segments) s(value)
      ),
      hour_daily as materialized (
        select s.work_date,sum(s.h_day) h_day,sum(s.h_night) h_night,sum(s.h_sat) h_sat,
          sum(s.h_sun) h_sun,sum(s.h_bh) h_bh,sum(s.pay_ex) pay_ex,sum(s.charge_ex) charge_ex
        from segment_rows s
        where c_daily_calc and s.work_date is not null
        group by s.work_date
      ),
      hour_weekly as materialized (
        select sum(s.h_day) h_day,sum(s.h_night) h_night,sum(s.h_sat) h_sat,
          sum(s.h_sun) h_sun,sum(s.h_bh) h_bh,sum(s.pay_ex) pay_ex,sum(s.charge_ex) charge_ex
        from segment_rows s
        having not c_daily_calc or not exists(select 1 from segment_rows d where d.work_date is not null)
      ),
      additional_source as materialized (
        select upper(a.key) code,a.value unit
        from jsonb_each(case when jsonb_typeof(snap.additional_units_json)='object'
          then snap.additional_units_json else '{}'::jsonb end) a
        where jsonb_typeof(a.value)='object' and btrim(a.key)<>''
      ),
      additional_daily as materialized (
        select a.code,left(d.key,10) work_date,
          case when pg_input_is_valid(d.value,'numeric') then d.value::numeric else 0 end units,
          case when pg_input_is_valid(a.unit->>'pay_rate','numeric') then (a.unit->>'pay_rate')::numeric else 0 end pay_rate,
          case when pg_input_is_valid(a.unit->>'charge_rate','numeric') then (a.unit->>'charge_rate')::numeric else 0 end charge_rate,
          coalesce(nullif(a.unit->>'bucket_name',''),a.code) bucket_name,
          coalesce(nullif(a.unit->>'unit_name',''),'units') unit_name,
          a.unit->'frequency' frequency
        from additional_source a
        cross join lateral jsonb_each_text(case when jsonb_typeof(a.unit->'days')='object'
          then a.unit->'days' else '{}'::jsonb end) d
        where c_daily_calc
          and left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
          and pg_input_is_valid(d.value,'numeric')
          and d.value::numeric<>0
          and exists(select 1 from segment_rows where work_date is not null)
      ),
      additional_weekly as materialized (
        select a.code,
          case when pg_input_is_valid(a.unit->>'unit_count','numeric') then (a.unit->>'unit_count')::numeric else 0 end units,
          case when pg_input_is_valid(a.unit->>'pay_rate','numeric') then (a.unit->>'pay_rate')::numeric else 0 end pay_rate,
          case when pg_input_is_valid(a.unit->>'charge_rate','numeric') then (a.unit->>'charge_rate')::numeric else 0 end charge_rate,
          case when pg_input_is_valid(a.unit->>'pay_ex_vat','numeric') then (a.unit->>'pay_ex_vat')::numeric else 0 end pay_ex,
          case when pg_input_is_valid(a.unit->>'charge_ex_vat','numeric') then (a.unit->>'charge_ex_vat')::numeric else 0 end charge_ex,
          coalesce(nullif(a.unit->>'bucket_name',''),a.code) bucket_name,
          coalesce(nullif(a.unit->>'unit_name',''),'units') unit_name,
          a.unit->'frequency' frequency
        from additional_source a
        where not(c_daily_calc
          and exists(select 1 from jsonb_each_text(case when jsonb_typeof(a.unit->'days')='object'
            then a.unit->'days' else '{}'::jsonb end) d
            where left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              and pg_input_is_valid(d.value,'numeric') and d.value::numeric<>0))
          and (
            (case when pg_input_is_valid(a.unit->>'pay_ex_vat','numeric') then (a.unit->>'pay_ex_vat')::numeric else 0 end)<>0
            or (case when pg_input_is_valid(a.unit->>'charge_ex_vat','numeric') then (a.unit->>'charge_ex_vat')::numeric else 0 end)<>0
          )
      ),
      expense_lines as materialized (
        select e.code,e.pay_ex,e.charge_ex
        from (values
          ('TRAVEL',coalesce(snap.travel_pay_ex_vat,0),coalesce(snap.travel_charge_ex_vat,0)),
          ('ACCOMMODATION',coalesce(snap.accommodation_pay_ex_vat,0),coalesce(snap.accommodation_charge_ex_vat,0)),
          ('OTHER',coalesce(snap.other_pay_ex_vat,0),coalesce(snap.other_charge_ex_vat,0)),
          ('EXPENSES_FALLBACK',
            case when coalesce(snap.travel_pay_ex_vat,0)+coalesce(snap.accommodation_pay_ex_vat,0)+coalesce(snap.other_pay_ex_vat,0)=0
              then coalesce(snap.expenses_pay_ex_vat,0) else 0 end,
            case when coalesce(snap.travel_charge_ex_vat,0)+coalesce(snap.accommodation_charge_ex_vat,0)+coalesce(snap.other_charge_ex_vat,0)=0
              then coalesce(snap.expenses_charge_ex_vat,0) else 0 end),
          ('MILEAGE',coalesce(snap.mileage_pay_ex_vat,0),coalesce(snap.mileage_charge_ex_vat,0))
        ) e(code,pay_ex,charge_ex)
        where e.pay_ex<>0 or e.charge_ex<>0
      ),
      line_union as materialized (
        select b.candidate_display||' - '||h.work_date||' - W/E '||coalesce(snap.week_ending_date::text,'' ) description,
          h.h_day,h.h_night,h.h_sat,h.h_sun,h.h_bh,h.pay_ex,h.charge_ex,
          'HOURS_DAILY' line_type,'TS:'||tsid::text||':HOURS:'||h.work_date source_key,
          jsonb_build_object('date',h.work_date) detail
        from hour_daily h cross join base b
        union all
        select b.candidate_display||' - W/E '||coalesce(snap.week_ending_date::text,''),
          h.h_day,h.h_night,h.h_sat,h.h_sun,h.h_bh,h.pay_ex,h.charge_ex,
          'HOURS_WEEKLY','TS:'||tsid::text||':HOURS:WEEK','{}'::jsonb
        from hour_weekly h cross join base b
        union all
        select b.candidate_display||' - '||a.bucket_name||' - '||a.work_date||' - '||a.units||' '||a.unit_name,
          0,0,0,0,0,round(a.units*a.pay_rate,2),round(a.units*a.charge_rate,2),
          'ADDITIONAL_RATE_DAILY','TS:'||tsid::text||':ADD:'||a.code||':'||a.work_date,
          jsonb_build_object('date',a.work_date,'bucket',jsonb_build_object(
            'code',a.code,'bucket_name',a.bucket_name,'unit_name',a.unit_name,'frequency',a.frequency),
            'units',jsonb_build_object('unit_count',a.units,'pay_rate',a.pay_rate,'charge_rate',a.charge_rate))
        from additional_daily a cross join base b
        union all
        select b.candidate_display||' - '||a.bucket_name||' - '||a.units||' '||a.unit_name,
          0,0,0,0,0,a.pay_ex,a.charge_ex,
          'ADDITIONAL_RATE','TS:'||tsid::text||':ADD:'||a.code||':WEEK',
          jsonb_build_object('bucket',jsonb_build_object(
            'code',a.code,'bucket_name',a.bucket_name,'unit_name',a.unit_name,'frequency',a.frequency),
            'units',jsonb_build_object('unit_count',a.units,'pay_rate',a.pay_rate,'charge_rate',a.charge_rate))
        from additional_weekly a cross join base b
        union all
        select case when e.code='MILEAGE' then 'Mileage - '||coalesce(snap.mileage_units,0)||' miles (W/E '||coalesce(snap.week_ending_date::text,'')||')'
          when e.code='EXPENSES_FALLBACK' then 'Expenses (W/E '||coalesce(snap.week_ending_date::text,'')||')'
          else initcap(replace(e.code,'_',' '))||' expenses (W/E '||coalesce(snap.week_ending_date::text,'')||')' end,
          0,0,0,0,0,e.pay_ex,e.charge_ex,
          case when e.code='MILEAGE' then 'MILEAGE'
            when e.code='EXPENSES_FALLBACK' then 'EXPENSES_TOTAL'
            else 'EXPENSE_'||e.code end,
          case when e.code='MILEAGE' then 'TS:'||tsid::text||':MILEAGE'
            when e.code='EXPENSES_FALLBACK' then 'TS:'||tsid::text||':EXP:TOTAL'
            else 'TS:'||tsid::text||':EXP:'||e.code end,
          jsonb_build_object('expense',case when e.code='MILEAGE' then jsonb_build_object(
            'category','MILEAGE','mileage_units',snap.mileage_units,
            'pay_rate',snap.mileage_pay_rate,'charge_rate',snap.mileage_charge_rate,
            'evidence_r2_key',snap.mileage_evidence_r2_key,'evidence_manifest',snap.mileage_evidence_manifest)
            else jsonb_build_object('category',e.code,'note',snap.expenses_description,
              'evidence_r2_key',snap.expenses_evidence_r2_key,'evidence_manifest',snap.expenses_evidence_manifest) end)
        from expense_lines e
      )
      select p_invoice_id,tsid,snap.booking_id,l.description,
        round(l.h_day,2),round(l.h_night,2),round(l.h_sat,2),round(l.h_sun,2),round(l.h_bh,2),
        null,null,null,null,null,
        snap.charge_day,snap.charge_night,snap.charge_sat,snap.charge_sun,snap.charge_bh,
        round(l.pay_ex,2),round(l.charge_ex,2),round(l.charge_ex-l.pay_ex,2),v_vat_rate,
        round(l.charge_ex*v_vat_rate/100,2),round(l.charge_ex+(l.charge_ex*v_vat_rate/100),2),
        v_exact_timesheet_document_r2_key,
        jsonb_build_object('line_type',l.line_type,'timesheet_id',tsid,'tsfin_id',snap.id,
          'candidate_display',l.description,'week_ending_date',snap.week_ending_date,
          'role',c_role,'hospital',c_display_site,'ward',c_ward_hint,'bucket_labels',c_bucket_labels,
          'totals',jsonb_build_object('line_pay_ex_vat',round(l.pay_ex,2),
            'line_charge_ex_vat',round(l.charge_ex,2),'margin_ex_vat',round(l.charge_ex-l.pay_ex,2),
            'vat_rate_pct',v_vat_rate,'vat_amount',round(l.charge_ex*v_vat_rate/100,2),
            'total_inc_vat',round(l.charge_ex+(l.charge_ex*v_vat_rate/100),2)))||l.detail,
        l.source_key
      from line_union l
      on conflict(invoice_id,source_key) do nothing;
      -- Build segment refs to lock
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
      then
        for v_seg in
          select value from jsonb_array_elements(segments) value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          seg_refs := seg_refs || jsonb_build_array(
            jsonb_build_object(
              'tsfin_id', snap.id::text,
              'segment_id', nullif(btrim(coalesce(v_seg->>'segment_id','')), '')
            )
          );
        end loop;
      else
        -- lock whole for non-segments
        seg_refs := seg_refs || jsonb_build_array(
          jsonb_build_object('tsfin_id', snap.id::text, 'segment_id', null)
        );
      end if;
    end loop;

    -- Apply segment locks for the exact selected source rows.
    if jsonb_typeof(seg_refs) = 'array' and jsonb_array_length(seg_refs) > 0 then
      perform public._inv_lock_segments_for_invoice(p_invoice_id, seg_refs);
    end if;

    -- History: timesheets added (always; includes requested ids)
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_TIMESHEETS_ADDED',
      null,
      jsonb_build_object('add_timesheet_ids', coalesce(to_jsonb(v_add_ts_ids), '[]'::jsonb)),
      null,
      p_actor_user_id
    );
  end if;

  -- 3c) Contract week status: set INVOICED only when timesheet is FULLY invoiced (segment-aware), and revert INVOICED -> AUTHORISED if no longer fully invoiced.
  -- Touch set = union of: add_timesheet_ids, line-removal touched (hours), segment-op touched.
  v_cw_ts_ids := array[]::uuid[];
  if v_add_ts_ids is not null and coalesce(array_length(v_add_ts_ids,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_add_ts_ids;
  end if;
  if v_ts_ids_touched is not null and coalesce(array_length(v_ts_ids_touched,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_ts_ids_touched;
  end if;
  if v_seg_ts_ids is not null and coalesce(array_length(v_seg_ts_ids,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_seg_ts_ids;
  end if;

  -- de-dup
  select array_agg(distinct x)
  into v_cw_ts_ids
  from unnest(coalesce(v_cw_ts_ids, array[]::uuid[])) x
  where x is not null;

  v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);

  if coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
    with src as (
      select
        cw.timesheet_id,
        cw.status as cw_status,
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json,
        (
          case
            when tf.invoice_breakdown_json is not null
             and jsonb_typeof(tf.invoice_breakdown_json)='object'
             and coalesce(tf.invoice_breakdown_json->>'mode','')='SEGMENTS'
             and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
             and jsonb_array_length(tf.invoice_breakdown_json->'segments') > 0
            then
              not exists (
                select 1
                from jsonb_array_elements(tf.invoice_breakdown_json->'segments') s(seg)
                where nullif(btrim(coalesce(s.seg->>'invoice_locked_invoice_id','')), '') is null
              )
            else
              (tf.locked_by_invoice_id is not null)
          end
        ) as fully_invoiced
      from public.contract_weeks cw
      join public.timesheets_financials tf
        on tf.is_current = true
       and tf.timesheet_id = cw.timesheet_id
      where cw.timesheet_id = any(v_cw_ts_ids)
    )
    update public.contract_weeks cw
    set status = case
      when src.fully_invoiced then 'INVOICED'::public.contract_week_status_enum
      when cw.status = 'INVOICED'::public.contract_week_status_enum then 'AUTHORISED'::public.contract_week_status_enum
      else cw.status
    end
    from src
    where cw.timesheet_id = src.timesheet_id;
  end if;


  -- 4) Recompute invoice totals from invoice_lines and clear PDF key
  select
    public._inv_round2(coalesce(sum(coalesce(l.total_charge_ex_vat,0)),0)),
    public._inv_round2(coalesce(sum(coalesce(l.vat_amount,0)),0)),
    public._inv_round2(coalesce(sum(coalesce(l.total_inc_vat,0)),0))
  into v_new_ex, v_new_vat, v_new_inc
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id;


  perform public.invoice_recompute_totals(p_invoice_id);

  -- Recompute header_snapshot_json.meta counters to avoid stale values after edits
  select count(distinct l.timesheet_id)
  into v_hdr_ts_count_lines
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null;

  select count(*)
  into v_hdr_seg_locked_count
  from public.timesheets_financials tf
  cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(seg_obj)
  where tf.is_current = true
    and coalesce(seg.seg_obj->>'invoice_locked_invoice_id','') = p_invoice_id::text;

  select count(distinct tf.timesheet_id)
  into v_hdr_ts_count_seglocks
  from public.timesheets_financials tf
  where tf.is_current = true
    and exists (
      select 1
      from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(seg_obj)
      where coalesce(seg.seg_obj->>'invoice_locked_invoice_id','') = p_invoice_id::text
    );

  if coalesce(v_hdr_seg_locked_count,0) > 0 then
    v_hdr_meta_timesheet_count := coalesce(v_hdr_ts_count_seglocks,0);
    v_hdr_meta_segment_count := coalesce(v_hdr_seg_locked_count,0);
  else
    v_hdr_meta_timesheet_count := coalesce(v_hdr_ts_count_lines,0);
    v_hdr_meta_segment_count := coalesce(v_hdr_meta_timesheet_count,0);
  end if;

  -- The statement triggers are the sole document/issue invalidation authority.
  -- This update changes only the authoritative business snapshot counters.
  update public.invoices invu
  set header_snapshot_json=jsonb_set(jsonb_set(coalesce(invu.header_snapshot_json,'{}'::jsonb),
      '{meta,timesheet_count}',to_jsonb(v_hdr_meta_timesheet_count),true),
      '{meta,segment_count}',to_jsonb(v_hdr_meta_segment_count),true),
    updated_at=now()
  where invu.id=p_invoice_id;

-- Refresh invoice-level NHSP/HR cache after timesheet/segment changes
  if coalesce(v_refresh_hr_cache,false) = true then
    perform 1
    from public.invoice_source_rows_collect(p_invoice_id, true) sr
    limit 1;
  end if;

  -- Revalidate ownership after every line and segment move in this Save. A
  -- source edit must not queue work for a source that was detached later in the
  -- same transaction, and every edited SEGMENTS identity must still be locked
  -- to this exact invoice.
  if cardinality(v_source_changed_ts_ids)>0 and exists(
    select 1
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_id')::uuid=any(v_source_changed_ts_ids)
      and not exists(
        select 1 from public.invoice_lines carrier
        where carrier.invoice_id=p_invoice_id
          and (
            carrier.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              carrier.timesheet_id is null
              and coalesce(carrier.meta_json->>'timesheet_id','')~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (carrier.meta_json->>'timesheet_id')::uuid=
                (desired.value->>'timesheet_id')::uuid
            )
          )
      )
      and not exists(
        select 1
        from public.timesheets_financials tf
        cross join lateral jsonb_array_elements(
          coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg(value)
        where tf.timesheet_id=(desired.value->>'timesheet_id')::uuid
          and tf.is_current
          and nullif(btrim(coalesce(
            seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text
      )
  ) then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_CONFLICTING_COMMAND',
      detail=jsonb_build_object('reason','POST_EDIT_SOURCE_DETACHED')::text;
  end if;

  if cardinality(v_source_changed_ts_ids)>0 and exists(
    select 1
    from jsonb_each(v_source_updates_map) desired
    cross join lateral jsonb_array_elements(
      coalesce(desired.value->'segment_updates','[]'::jsonb)) requested(value)
    where desired.value->>'source_mode'='SEGMENTS'
      and not exists(
        select 1
        from public.timesheets_financials tf
        cross join lateral jsonb_array_elements(
          coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg(value)
        where tf.id=(desired.value->>'financial_id')::uuid
          and tf.is_current
          and tf.timesheet_id=(desired.value->>'timesheet_id')::uuid
          and nullif(btrim(coalesce(
            seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text
          and (
            (
              nullif(requested.value->>'segment_id','') is not null
              and nullif(btrim(coalesce(seg.value->>'segment_id','')),'')
                =nullif(requested.value->>'segment_id','')
            ) or (
              nullif(requested.value->>'segment_id','') is null
              and coalesce(seg.value->>'start_utc',seg.value->>'start')
                is not distinct from requested.value->>'start_identity'
              and coalesce(seg.value->>'end_utc',seg.value->>'end')
                is not distinct from requested.value->>'end_identity'
            )
          )
      )
  ) then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_CONFLICTING_COMMAND',
      detail=jsonb_build_object('reason','POST_EDIT_SEGMENT_DETACHED')::text;
  end if;

  -- A material source edit always starts or reuses the exact replacement
  -- timesheet work. Replacement of an existing invoice preview is derived from
  -- the locked pre-edit V8 state; browser flags cannot suppress it.
  if cardinality(v_source_changed_ts_ids)>0 then
    select coalesce(jsonb_agg(jsonb_build_object(
      'command_type','VIEW_TIMESHEET_DOCUMENT',
      'timesheet_id',changed_id,
      'purpose','TIMESHEET',
      'priority_reason','SOURCE_EDIT_REPLACEMENT',
      'template_version','timesheet-professional-v2')
      order by changed_id),'[]'::jsonb)
    into v_document_commands
    from unnest(v_source_changed_ts_ids) changed_id;

    v_source_edit_invoice_replacement_required:=
      v_source_edit_preexisting_preview
      or lower(coalesce(p_payload->>'request_preview','false'))
           in('true','1','yes','on');
    if v_source_edit_invoice_replacement_required then
      v_document_commands:=v_document_commands||jsonb_build_array(
        jsonb_build_object(
          'command_type','VIEW_INVOICE_DOCUMENT',
          'invoice_id',p_invoice_id,
          'purpose','DRAFT_PREVIEW',
          'priority_reason',case when v_source_edit_preexisting_preview
            then 'SOURCE_EDIT_REPLACEMENT' else 'VIEW_NOW' end,
          'template_version','invoice-professional-v2'));
    end if;

    v_expected_command_count:=jsonb_array_length(v_document_commands);
    if v_expected_command_count>1000 then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_TOO_MANY_REPLACEMENTS';
    end if;

    v_started_operations:=public.invoice_operation_start_batch(
      v_document_commands,p_actor_user_id,v_now);

    if jsonb_typeof(v_started_operations)<>'array'
       or jsonb_array_length(v_started_operations)<>v_expected_command_count then
      raise exception using errcode='55000',
        message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
        detail=jsonb_build_object(
          'reason','RESULT_COUNT',
          'expected',v_expected_command_count,
          'actual',case when jsonb_typeof(v_started_operations)='array'
            then jsonb_array_length(v_started_operations) else null end)::text;
    end if;

    for v_command_no in 1..v_expected_command_count loop
      select value into v_operation_result
      from jsonb_array_elements(v_started_operations) value
      where coalesce(value->>'command_no','')~'^[1-9][0-9]*$'
        and (value->>'command_no')::int=v_command_no;

      if not found or (
        select count(*)
        from jsonb_array_elements(v_started_operations) value
        where coalesce(value->>'command_no','')~'^[1-9][0-9]*$'
          and (value->>'command_no')::int=v_command_no
      )<>1 then
        raise exception using errcode='55000',
          message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
          detail=jsonb_build_object('reason','COMMAND_NO','command_no',v_command_no)::text;
      end if;

      v_source_update:=v_document_commands->(v_command_no-1);
      v_command_type:=v_source_update->>'command_type';
      if v_operation_result->>'command_type' is distinct from v_command_type
         or lower(coalesce(v_operation_result->>'accepted','false'))
              not in('true','1','yes','on')
         or lower(coalesce(v_operation_result->>'blocked','false'))
              in('true','1','yes','on') then
        raise exception using errcode='55000',
          message=case
            when lower(coalesce(v_operation_result->>'blocked','false'))
                   in('true','1','yes','on')
              then 'SOURCE_EDIT_REPLACEMENT_BLOCKED'
            else 'SOURCE_EDIT_REPLACEMENT_REJECTED' end,
          detail=jsonb_build_object(
            'command_no',v_command_no,
            'terminal_error',v_operation_result->'terminal_error')::text;
      end if;

      v_operation_status:=upper(coalesce(v_operation_result->>'status',''));
      v_operation_id:=case when coalesce(v_operation_result->>'operation_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (v_operation_result->>'operation_id')::uuid end;
      v_document_version_id:=case when coalesce(v_operation_result->>'document_version_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (v_operation_result->>'document_version_id')::uuid end;

      if v_operation_status='READY' then
        if v_document_version_id is null or v_operation_id is not null
           or not exists(
             select 1
             from public.invoice_document_versions dv
             where dv.id=v_document_version_id
               and dv.entity_type=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then 'TIMESHEET' else 'INVOICE' end
               and dv.entity_id=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (v_source_update->>'timesheet_id')::uuid else p_invoice_id end
               and dv.purpose=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then 'TIMESHEET' else 'DRAFT_PREVIEW' end
               and dv.source_revision=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (
                   select t.document_revision::text from public.timesheets t
                   where t.timesheet_id=(v_source_update->>'timesheet_id')::uuid
                     and t.is_current)
                 else (
                   select i.document_revision::text from public.invoices i
                   where i.id=p_invoice_id)
                 end
               and dv.template_version=v_source_update->>'template_version'
               and dv.status='READY'
               and nullif(btrim(coalesce(dv.r2_key,'')),'') is not null
               and nullif(btrim(coalesce(dv.sha256,'')),'') is not null
               and coalesce(dv.size_bytes,0)>0
               and coalesce(dv.page_count,0)>0
           ) then
          raise exception using errcode='55000',
            message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
            detail=jsonb_build_object(
              'reason','READY_IDENTITY','command_no',v_command_no)::text;
        end if;
      else
        if v_operation_id is null
           or v_operation_status not in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
           or not exists(
             select 1
             from public.invoice_operations op
             where op.id=v_operation_id
               and op.operation_type='BUILD_DOCUMENT'
               and op.entity_type=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then 'TIMESHEET' else 'INVOICE' end
               and op.entity_id=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (v_source_update->>'timesheet_id')::uuid else p_invoice_id end
               and op.source_revision=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (
                   select t.document_revision::text from public.timesheets t
                   where t.timesheet_id=(v_source_update->>'timesheet_id')::uuid
                     and t.is_current)
                 else (
                   select i.document_revision::text from public.invoices i
                   where i.id=p_invoice_id)
                 end
               and op.template_version=v_source_update->>'template_version'
               and coalesce(op.input_json->>'purpose','')=
                 case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                   then 'TIMESHEET' else 'DRAFT_PREVIEW' end
               and op.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
           ) then
          raise exception using errcode='55000',
            message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
            detail=jsonb_build_object(
              'reason','ACTIVE_IDENTITY','command_no',v_command_no)::text;
        end if;
      end if;

      v_validated_operations:=v_validated_operations||jsonb_build_array(v_operation_result);
    end loop;

    select coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',t.timesheet_id,
      'document_revision',t.document_revision)
      order by t.timesheet_id),'[]'::jsonb)
    into v_source_changed_revisions
    from public.timesheets t
    where t.timesheet_id=any(v_source_changed_ts_ids) and t.is_current;
  end if;

-- Return compact state only; never build a manifest or PDF here.
  select jsonb_build_object('invoice_id',i.id,'status',i.status,
    'subtotal_ex_vat',i.subtotal_ex_vat,'vat_amount',i.vat_amount,'total_inc_vat',i.total_inc_vat,
    'document_revision',i.document_revision,'document_state',i.document_state,
    'preview_document_version_id',i.preview_document_version_id,
    'active_document_operation_id',i.active_document_operation_id,
    'issue_state',i.issue_state,'active_issue_operation_id',i.active_issue_operation_id,
    'reference_updates_applied',v_refupd_applied,
    'timesheet_location_updates_applied',v_location_applied,
    'timesheet_source_changed',
      cardinality(v_source_changed_ts_ids)>0,
    'source_edit_queue_contract',case when v_source_edit_requested
      then 'INVOICE_SOURCE_EDIT_QUEUE_V1' else null end,
    'changed_timesheet_ids',coalesce(to_jsonb(v_source_changed_ts_ids),'[]'::jsonb),
    'changed_timesheet_revisions',coalesce(v_source_changed_revisions,'[]'::jsonb),
    'source_edit_preexisting_preview',v_source_edit_preexisting_preview,
    'invoice_replacement_required',v_source_edit_invoice_replacement_required,
    'accepted_operations',coalesce(v_validated_operations,'[]'::jsonb),
    'document_queue_requested',jsonb_array_length(v_document_commands)>0)
  into v_manifest from public.invoices i where i.id=p_invoice_id;

  if v_invoice_debug then
    begin
      -- attach finish marker (avoid extra heavy queries here)
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', public._inv_iso_utc(now()),
          'invoice_total_charge_ex_vat', v_new_ex,
          'invoice_vat_amount', v_new_vat,
          'invoice_total_inc_vat', v_new_inc
        )
      );

      v_dbg_stats := v_dbg_stats || jsonb_build_object(
        'lines_deleted', v_dbg_lines_deleted,
        'timesheets_unlocked_via_line_removal', v_dbg_timesheets_unlocked,
        'segment_add_refs', v_dbg_seg_add_refs,
        'segment_remove_refs', v_dbg_seg_remove_refs,
        'segment_tsfin_count', v_dbg_seg_tsfins,
        'segment_timesheets_rebuilt', v_dbg_seg_timesheets_rebuilt,
        'segment_timesheets_removed', v_dbg_seg_timesheets_removed,
        'add_timesheets_found', v_dbg_add_timesheets_found,
        'add_timesheets_skipped', v_dbg_add_timesheets_skipped
      );

      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_APPLY_EDITS_DEBUG',
        jsonb_build_object(
          'invoice_id', p_invoice_id::text,
          'week_start', v_week_start::text,
          'week_end', v_week_end::text,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'invoices',
        p_invoice_id::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  perform public._ctms_assert_invoice_correction_lines_v1(
    p_invoice_id,p_actor_user_id,false,'INVOICE_APPLY_EDITS_RESULT'
  );

  IF v_correction_placement_ts_id IS NOT NULL THEN
    v_correction_placement_scope:=public.invoice_correction_pair_scope_v1(
      v_correction_placement_ts_id,null::uuid,p_actor_user_id,false,100);
    IF COALESCE((v_correction_placement_scope->>'valid')::boolean,false) IS NOT TRUE
       OR v_correction_placement_scope->>'placement_state' NOT IN (
         'COMPLETE_SAME_INVOICE','COMPLETE_SPLIT_INVOICES','INCOMPLETE_MOVE','UNPLACED') THEN
      RAISE EXCEPTION 'INVOICE_CORRECTION_PAIR_PLACEMENT_RESULT_INVALID'
        USING ERRCODE='P0001',DETAIL=v_correction_placement_scope::text;
    END IF;
    v_correction_placement_states:=jsonb_build_array(jsonb_build_object(
      'correction_id',v_correction_placement_scope->>'correction_id',
      'pair_timesheet_ids',v_correction_placement_scope->'pair_timesheet_ids',
      'placement_state',v_correction_placement_scope->>'placement_state',
      'missing_member_kind',v_correction_placement_scope->>'missing_member_kind',
      'placement_invoices',v_correction_placement_scope->'placement_invoices'));
    v_manifest:=v_manifest||jsonb_build_object(
      'correction_pair_placement_states',v_correction_placement_states);
  END IF;

  return v_manifest;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_APPLY_EDITS_ERROR',
        jsonb_build_object(
          'invoice_id', coalesce(p_invoice_id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'invoices',
        coalesce(p_invoice_id::text,''),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$function$;

revoke all on function public.invoice_apply_edits(uuid,jsonb,uuid)
  from public,anon,authenticated;
grant execute on function public.invoice_apply_edits(uuid,jsonb,uuid) to authenticated,service_role;
