-- Bind only the Source detail actually chosen by the Office decision. This
-- runs before immediate/deferred publication, when the exact segment and
-- immutable provenance IDs are still available. No current-row recovery.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_capture_chosen_source_detail_v1(
  p_head_id uuid,p_bundle_id uuid,p_bundle_revision bigint,
  p_components jsonb,p_segments jsonb,p_copy_head_id uuid default null
) returns integer
language plpgsql security definer
set search_path = pg_catalog, private, public
as $function$
declare
  v_component jsonb;
  v_canonical jsonb;
  v_component_sha bytea;
  v_component_id uuid;
  v_segment jsonb;
  v_match_count integer;
  v_detail jsonb;
  v_detail_sha bytea;
  v_existing record;
  v_count integer:=0;
  v_key text;
  v_start_utc timestamptz;
  v_end_utc timestamptz;
begin
  if p_head_id is null or p_bundle_id is null or p_bundle_revision is null
     or pg_catalog.jsonb_typeof(p_components)<>'array'
     or (p_copy_head_id is null and pg_catalog.jsonb_typeof(p_segments)<>'array')
     or (p_copy_head_id is not null and p_segments is not null) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_SOURCE_DETAIL_INPUT_INVALID';
  end if;
  if not exists (
    select 1 from public.weekly_source_entitlement_decision_bundles b
    where b.decision_bundle_id=p_bundle_id
      and b.bundle_revision=p_bundle_revision
      and p_head_id=any(b.proposed_head_ids)
  ) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SOURCE_DETAIL_DECISION_MISMATCH';
  end if;
  for v_component in select value from pg_catalog.jsonb_array_elements(p_components)
  loop
    if v_component->>'component_kind'<>'WORKED_TIME' then
      continue;
    end if;
    v_canonical:=private.weekly_source_publication_component_canonical_v1(
      v_component,'bpay_next_chosen_detail.component');
    v_component_id:=(v_canonical->>'component_id')::uuid;
    v_component_sha:=private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_component_content_v1(v_canonical));
    if p_copy_head_id is not null then
      select d.detail_json,d.component_sha256
        into v_detail,v_detail_sha
        from private.bpay_next_source_chosen_detail d
        where d.head_id=p_copy_head_id and d.component_id=v_component_id;
      if not found or v_detail_sha<>v_component_sha then
        raise exception using errcode='23514',
          message='BPAY_NEXT_SOURCE_DETAIL_COPY_NOT_EXACT';
      end if;
    else
      select pg_catalog.count(*)::integer,pg_catalog.jsonb_agg(s.value)->0
        into v_match_count,v_segment
        from pg_catalog.jsonb_array_elements(p_segments) s(value)
        where s.value->>'segment_id'=v_canonical->>'segment_id';
      if v_match_count<>1
         or v_segment->>'date' is distinct from v_canonical->>'work_date'
         or (v_segment->>'pay_amount')::numeric
              is distinct from (v_canonical->>'pay_ex_vat')::numeric
         or (v_segment->>'exclude_from_pay')::boolean
              is distinct from (v_canonical->>'exclude_from_pay')::boolean then
        raise exception using errcode='23514',
          message='BPAY_NEXT_SOURCE_DETAIL_SEGMENT_NOT_EXACT';
      end if;
      foreach v_key in array array['hours_day','hours_night','hours_sat','hours_sun','hours_bh']
      loop
        if coalesce((v_segment->>v_key)::numeric,0)
             is distinct from coalesce((v_canonical->>v_key)::numeric,0) then
          raise exception using errcode='23514',
            message='BPAY_NEXT_SOURCE_DETAIL_BUCKET_NOT_EXACT';
        end if;
      end loop;
      if coalesce(v_segment->>'start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
         or coalesce(v_segment->>'end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
         or (v_segment->>'overnight')::boolean is null
         or (v_segment->>'break_mins')::integer is null
         or (v_segment->>'break_mins')::integer<0
         or pg_catalog.jsonb_typeof(v_segment->'weekly_source')<>'object'
         or nullif(v_segment#>>'{weekly_source,work_event_id}','') is null
         or v_segment#>>'{weekly_source,work_event_id}'
              is distinct from v_canonical->>'component_member_identity'
         or v_segment#>>'{weekly_source,calculation_fingerprint}'
              is distinct from v_canonical->>'segment_stable_key'
         or nullif(v_segment#>>'{weekly_source,row_resolution_id}','') is null
         or nullif(v_segment#>>'{weekly_source,economic_snapshot_id}','') is null
         or nullif(v_segment#>>'{weekly_source,movement_id}','') is null
         or nullif(v_segment#>>'{weekly_source,final_revision_id}','') is null
         or pg_catalog.jsonb_typeof(v_segment#>'{weekly_source,pay_vector,rates}')<>'object' then
        raise exception using errcode='23514',
          message='BPAY_NEXT_SOURCE_DETAIL_PROVENANCE_INCOMPLETE';
      end if;
      foreach v_key in array array['day','night','sat','sun','bh']
      loop
        if coalesce((v_canonical->>('hours_'||v_key))::numeric,0)<>0
           and ((v_segment#>>array['weekly_source','pay_vector','rates',v_key])::numeric is null
             or (v_segment#>>array['weekly_source','pay_vector','rates',v_key])::numeric
                 <>pg_catalog.round(
                   (v_segment#>>array['weekly_source','pay_vector','rates',v_key])::numeric,6)) then
          raise exception using errcode='23514',
            message='BPAY_NEXT_SOURCE_DETAIL_USED_RATE_MISSING';
        end if;
      end loop;
      v_detail:=pg_catalog.jsonb_build_object(
        'work_date',v_segment->>'date',
        'start',v_segment->>'start',
        'end',v_segment->>'end',
        'overnight',(v_segment->>'overnight')::boolean,
        'break_minutes',(v_segment->>'break_mins')::integer,
        'work_event_id',(v_segment#>>'{weekly_source,work_event_id}')::uuid,
        'movement_id',(v_segment#>>'{weekly_source,movement_id}')::uuid,
        'final_revision_id',(v_segment#>>'{weekly_source,final_revision_id}')::uuid,
        'row_resolution_id',(v_segment#>>'{weekly_source,row_resolution_id}')::uuid,
        'economic_snapshot_id',(v_segment#>>'{weekly_source,economic_snapshot_id}')::uuid,
        'rates',pg_catalog.jsonb_build_object(
          'day',(v_segment#>>'{weekly_source,pay_vector,rates,day}')::numeric,
          'night',(v_segment#>>'{weekly_source,pay_vector,rates,night}')::numeric,
          'sat',(v_segment#>>'{weekly_source,pay_vector,rates,sat}')::numeric,
          'sun',(v_segment#>>'{weekly_source,pay_vector,rates,sun}')::numeric,
          'bh',(v_segment#>>'{weekly_source,pay_vector,rates,bh}')::numeric));
      -- Retain only already supplied instants from this exact chosen segment.
      -- Missing UTC keeps the old detail shape/hash; no local-clock guess or
      -- current-source lookup enriches a copied or previously captured HEAD.
      if (v_segment->'start_utc' is not null and v_segment->'start_utc'<>'null'::jsonb)
         or (v_segment->'end_utc' is not null and v_segment->'end_utc'<>'null'::jsonb) then
        if pg_catalog.jsonb_typeof(v_segment->'start_utc') is distinct from 'string'
           or pg_catalog.jsonb_typeof(v_segment->'end_utc') is distinct from 'string'
           or coalesce(v_segment->>'start_utc','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$'
           or coalesce(v_segment->>'end_utc','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$' then
          raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_DETAIL_UTC_INVALID';
        end if;
        begin
          v_start_utc:=(v_segment->>'start_utc')::timestamptz;
          v_end_utc:=(v_segment->>'end_utc')::timestamptz;
        exception when invalid_datetime_format or datetime_field_overflow
          or invalid_time_zone_displacement_value then
          raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_DETAIL_UTC_INVALID';
        end;
        if not pg_catalog.isfinite(v_start_utc) or not pg_catalog.isfinite(v_end_utc)
           or v_end_utc<=v_start_utc
           or pg_catalog.to_char(v_start_utc at time zone 'Europe/London','YYYY-MM-DD')<>v_segment->>'date'
           or pg_catalog.to_char(v_start_utc at time zone 'Europe/London','HH24:MI')<>v_segment->>'start'
           or pg_catalog.to_char(v_end_utc at time zone 'Europe/London','HH24:MI')<>v_segment->>'end'
           or (v_end_utc at time zone 'Europe/London')::date<>
             (v_segment->>'date')::date+(case when (v_segment->>'overnight')::boolean then 1 else 0 end) then
          raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_DETAIL_UTC_INVALID';
        end if;
        v_detail:=v_detail||pg_catalog.jsonb_build_object(
          'start_utc',v_segment->'start_utc','end_utc',v_segment->'end_utc');
      end if;
    end if;
    v_detail_sha:=pg_catalog.sha256(pg_catalog.convert_to(v_detail::text,'UTF8'));
    insert into private.bpay_next_source_chosen_detail
      (head_id,component_id,decision_bundle_id,bundle_revision,
       component_sha256,detail_json,detail_sha256)
    values(p_head_id,v_component_id,p_bundle_id,p_bundle_revision,
           v_component_sha,v_detail,v_detail_sha)
    on conflict (head_id,component_id) do nothing;
    select d.decision_bundle_id,d.bundle_revision,d.component_sha256,
           d.detail_json,d.detail_sha256 into strict v_existing
      from private.bpay_next_source_chosen_detail d
      where d.head_id=p_head_id and d.component_id=v_component_id;
    if v_existing.decision_bundle_id<>p_bundle_id
       or v_existing.bundle_revision<>p_bundle_revision
       or v_existing.component_sha256<>v_component_sha
       or v_existing.detail_sha256<>v_detail_sha
       or v_existing.detail_json<>v_detail then
      raise exception using errcode='23514',
        message='BPAY_NEXT_SOURCE_DETAIL_REPLAY_CONFLICT';
    end if;
    v_count:=v_count+1;
  end loop;
  return v_count;
end
$function$;

alter function private.bpay_next_capture_chosen_source_detail_v1(
  uuid,uuid,bigint,jsonb,jsonb,uuid) owner to postgres;
revoke all on function private.bpay_next_capture_chosen_source_detail_v1(
  uuid,uuid,bigint,jsonb,jsonb,uuid)
  from public,anon,authenticated,service_role;
-- The sole two-root publisher calls this after both genuine STAGED inventories
-- exist, but before I6, activation or I1. The arrays retain semantic A/B order;
-- lock ordering must never turn the target into the source. No financial write,
-- intermediate B approval, current-WORK selector or historical repair occurs.
create or replace function private.bpay_next_capture_source_pair_before_detail_v1(
  p_decision_bundle_id uuid,p_bundle_revision bigint,
  p_before_source_event_ids uuid[],p_before_financial_snapshot_ids uuid[]
) returns integer
language plpgsql security definer
set search_path = pg_catalog, private, public
as $function$
declare
  v_bundle public.weekly_source_entitlement_decision_bundles%rowtype;
  v_root public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_auth public.weekly_source_root_authorisations%rowtype;
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_before_head public.weekly_source_entitlement_heads%rowtype;
  v_tf public.timesheets_financials%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_line private.bpay_next_approved_line%rowtype;
  v_shift private.bpay_next_shift_detail%rowtype;
  v_existing private.bpay_next_source_chosen_detail%rowtype;
  v_roots uuid[];
  v_bookings text[];
  v_contracts uuid[];
  v_before_heads uuid[]:=array[null,null]::uuid[];
  v_before_revisions uuid[]:=array[null,null]::uuid[];
  v_tf_components jsonb[]:=array[null,null]::jsonb[];
  v_i integer;
  v_origin integer;
  v_role integer;
  v_count integer:=0;
  v_seen integer;
  v_matches integer;
  v_before_count integer;
  v_expected_rates integer;
  v_actual_rates integer;
  v_hours numeric;
  v_bucket record;
  v_row record;
  v_component public.weekly_source_entitlement_head_components%rowtype;
  v_before_component public.weekly_source_entitlement_head_components%rowtype;
  v_canonical jsonb;
  v_before_canonical jsonb;
  v_component_sha bytea;
  v_composed jsonb;
  v_segment jsonb;
  v_rates jsonb;
  v_rate numeric;
  v_stored_hours numeric;
  v_detail jsonb;
  v_detail_sha bytea;
  v_break jsonb;
  v_breaks jsonb;
  v_break_total integer;
  v_break_no integer;
  v_break_minutes integer;
  v_all_clocked boolean;
  v_start text;
  v_end text;
  v_start_utc timestamptz;
  v_end_utc timestamptz;
  v_blank jsonb;
begin
  if p_decision_bundle_id is null or p_bundle_revision is null or p_bundle_revision<1
     or p_before_source_event_ids is null or p_before_financial_snapshot_ids is null
     or pg_catalog.array_ndims(p_before_source_event_ids) is distinct from 1
     or pg_catalog.array_ndims(p_before_financial_snapshot_ids) is distinct from 1
     or pg_catalog.array_lower(p_before_source_event_ids,1) is distinct from 1
     or pg_catalog.array_lower(p_before_financial_snapshot_ids,1) is distinct from 1
     or pg_catalog.cardinality(p_before_source_event_ids)<>2
     or pg_catalog.cardinality(p_before_financial_snapshot_ids)<>2
     or p_before_source_event_ids[1] is null then
    raise exception using errcode='22023',message='BPAY_NEXT_PAIR_DETAIL_INPUT_INVALID';
  end if;
  perform 1 from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if not found then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into v_bundle from public.weekly_source_entitlement_decision_bundles
    where decision_bundle_id=p_decision_bundle_id and bundle_revision=p_bundle_revision
    for share;
  if not found or v_bundle.bundle_kind is distinct from 'CROSS_CONTRACT_A_B'
     or v_bundle.state not in ('PROPOSED','COMMITTED')
     or v_bundle.source_root_timesheet_id is null or v_bundle.target_root_timesheet_id is null
     or v_bundle.source_root_timesheet_id=v_bundle.target_root_timesheet_id
     or v_bundle.source_contract_id is null or v_bundle.target_contract_id is null
     or v_bundle.source_contract_id=v_bundle.target_contract_id
     or pg_catalog.array_ndims(v_bundle.proposed_head_ids) is distinct from 1
     or pg_catalog.array_lower(v_bundle.proposed_head_ids,1) is distinct from 1
     or pg_catalog.cardinality(v_bundle.proposed_head_ids)<>2
     or v_bundle.proposed_head_ids[1] is null or v_bundle.proposed_head_ids[2] is null
     or v_bundle.proposed_head_ids[1]=v_bundle.proposed_head_ids[2] then
    raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BUNDLE_NOT_EXACT';
  end if;
  v_roots:=array[v_bundle.source_root_timesheet_id,v_bundle.target_root_timesheet_id];
  v_bookings:=array[v_bundle.source_root_family_booking_id,v_bundle.target_root_family_booking_id];
  v_contracts:=array[v_bundle.source_contract_id,v_bundle.target_contract_id];

  -- Source family/root locks already precede this call. Reacquire exact rows
  -- consistently, without resolving old physical versions or choosing history.
  for v_row in select t.timesheet_id from public.timesheets t
    where t.timesheet_id=any(v_roots)
    order by pg_catalog.btrim(t.booking_id),t.timesheet_id for key share
  loop null; end loop;
  for v_row in select c.id from public.contracts c
    where c.id=any(v_contracts) order by c.id for key share
  loop null; end loop;
  for v_role in select i from pg_catalog.generate_series(1,2) r(i)
    order by pg_catalog.btrim(v_bookings[i]),v_roots[i]
  loop
    v_i:=v_role;
    select * into strict v_root from public.timesheets where timesheet_id=v_roots[v_i];
    select * into strict v_contract from public.contracts where id=v_contracts[v_i];
    if v_root.is_current is distinct from true or v_root.archived_at_utc is not null
       or v_root.revoked_at is not null or v_root.booking_id is distinct from v_bookings[v_i]
       or v_root.contract_id is distinct from v_contracts[v_i]
       or v_root.week_ending_date is distinct from v_bundle.week_ending_date
       or v_contract.candidate_id is distinct from v_bundle.candidate_id then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_ROOT_NOT_EXACT';
    end if;
    -- A canonical current collision is a refusal, never an identity remap.
    select pg_catalog.count(*)::integer into v_seen from (
      select t.timesheet_id from public.timesheets t
      where t.is_current=true and t.booking_id is not null and pg_catalog.btrim(t.booking_id)<>''
        and pg_catalog.btrim(t.booking_id)=pg_catalog.btrim(v_bookings[v_i])
      limit 2) actual_current;
    if v_seen<>1 then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_ROOT_NOT_EXACT';
    end if;
    select * into v_head from public.weekly_source_entitlement_heads
      where id=v_bundle.proposed_head_ids[v_i] for share;
    if not found or v_head.state is distinct from 'STAGED'
       or v_head.decision_bundle_id is distinct from p_decision_bundle_id
       or v_head.bundle_revision is distinct from p_bundle_revision
       or v_head.decision_id is distinct from v_bundle.decision_id
       or v_head.decided_by_user_id is distinct from v_bundle.decided_by_user_id
       or v_head.agency_id is distinct from v_bundle.agency_id
       or v_head.root_timesheet_id is distinct from v_roots[v_i]
       or v_head.root_family_booking_id is distinct from v_bookings[v_i]
       or v_head.root_timesheet_version is distinct from v_root.version
       or v_head.candidate_id is distinct from v_bundle.candidate_id
       or v_head.contract_id is distinct from v_contracts[v_i]
       or v_head.week_ending_date is distinct from v_bundle.week_ending_date
       or (select pg_catalog.count(*) from public.weekly_source_entitlement_head_components c
           where c.head_id=v_head.id) is distinct from v_head.component_count::bigint then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_STAGED_NOT_EXACT';
    end if;
    select pg_catalog.count(*)::integer into v_seen from (
      select a.id from public.weekly_source_root_authorisations a
      where a.withdrawn_at_utc is null and (a.root_timesheet_id=v_roots[v_i]
        or pg_catalog.btrim(a.family_booking_id)=pg_catalog.btrim(v_bookings[v_i]))
      order by a.id limit 2 for update) actual_authorisation;
    select * into v_auth from public.weekly_source_root_authorisations a
      where a.root_timesheet_id=v_roots[v_i] and a.withdrawn_at_utc is null;
    select pg_catalog.count(*)::integer into v_before_count from (
      select h.id from public.weekly_source_entitlement_heads h
      where h.state='COMMITTED_CURRENT'
        and (h.root_timesheet_id=v_roots[v_i]
          or pg_catalog.btrim(h.root_family_booking_id)=pg_catalog.btrim(v_bookings[v_i]))
      order by h.id limit 2 for share) actual_before;
    if v_seen>1 or v_before_count>1 then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BEFORE_NOT_EXACT';
    end if;
    if v_before_count=1 then
      select * into strict v_before_head from public.weekly_source_entitlement_heads h
        where h.state='COMMITTED_CURRENT' and (h.root_timesheet_id=v_roots[v_i]
          or pg_catalog.btrim(h.root_family_booking_id)=pg_catalog.btrim(v_bookings[v_i]));
      if v_seen<>1 or v_auth.id is null
         or v_auth.family_booking_id is distinct from v_bookings[v_i]
         or v_auth.timesheet_version is distinct from v_root.version
         or v_auth.current_entitlement_head_id is distinct from v_before_head.id
         or p_before_source_event_ids[v_i] is distinct from v_before_head.id
         or p_before_financial_snapshot_ids[v_i] is not null
         or v_before_head.root_timesheet_id is distinct from v_roots[v_i]
         or v_before_head.root_family_booking_id is distinct from v_bookings[v_i]
         or v_before_head.root_timesheet_version is distinct from v_root.version
         or v_before_head.candidate_id is distinct from v_bundle.candidate_id
         or v_before_head.contract_id is distinct from v_contracts[v_i]
         or v_before_head.week_ending_date is distinct from v_bundle.week_ending_date then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_HEAD_BEFORE_NOT_EXACT';
      end if;
      v_before_heads[v_i]:=v_before_head.id;
    else
      if v_seen=1 then
        if v_auth.id is null or v_auth.id is distinct from p_before_source_event_ids[v_i]
           or v_auth.family_booking_id is distinct from v_bookings[v_i]
           or v_auth.timesheet_version is distinct from v_root.version
           or v_auth.current_entitlement_head_id is not null
           or v_root.authorised_at_server is null then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_INITIAL_BEFORE_NOT_EXACT';
        end if;
      elsif v_i<>2 or p_before_source_event_ids[v_i] is not null then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_NULL_EVENT_NOT_PERMITTED';
      end if;
      if p_before_financial_snapshot_ids[v_i] is null then
        -- Only an actually blank new B has no own before evidence. The caller's
        -- accepted target_auth/signature checks still dominate this helper;
        -- genuine I6 must succeed after capture, before activation.
        v_blank:=private.weekly_source_publication_target_root_blank_v1(v_roots[v_i],v_bookings[v_i]);
        if v_i<>2 or v_seen<>0 or v_blank->'blank' is distinct from 'true'::jsonb then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FINANCIAL_REQUIRED';
        end if;
        v_tf_components[v_i]:='[]'::jsonb;
        continue;
      end if;
      select * into v_tf from public.timesheets_financials
        where id=p_before_financial_snapshot_ids[v_i] for share;
      select pg_catalog.count(*)::integer into v_matches from (
        select f.id from public.timesheets_financials f
        where f.timesheet_id=v_roots[v_i] and f.is_current=true limit 2) actual_financial;
      if v_tf.id is null or v_matches<>1 or v_tf.timesheet_id is distinct from v_roots[v_i]
         or v_tf.timesheet_version is distinct from v_root.version
         or v_tf.is_current is distinct from true or v_tf.is_stale is distinct from false
         or v_tf.candidate_id is distinct from v_bundle.candidate_id
         or v_tf.client_id is distinct from v_contract.client_id
         or pg_catalog.jsonb_typeof(v_tf.invoice_breakdown_json->'segments') is distinct from 'array'
         or (v_seen=1 and v_tf.authorised_at_utc is null) then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FINANCIAL_NOT_EXACT';
      end if;
      if v_seen=0 then
        v_blank:=private.weekly_source_publication_target_root_blank_v1(v_roots[v_i],v_bookings[v_i]);
        if v_blank->'blank' is distinct from 'true'::jsonb and (
          v_bundle.whole_root_review_required is distinct from true
          or v_bundle.whole_root_reviewed_by_user_id is null
          or v_bundle.whole_root_reviewed_at_utc is null
          or not pg_catalog.isfinite(v_bundle.whole_root_reviewed_at_utc)) then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_TARGET_REVIEW_REQUIRED';
        end if;
      end if;
      -- One composer over each exact named snapshot's existing segment vector.
      -- It selects no current upload, movement ladder, revision history or I7.
      v_tf_components[v_i]:=private.weekly_source_entitlement_components_v1(
        v_tf.invoice_breakdown_json->'segments','[]'::jsonb);
    end if;
  end loop;

  perform 1 from public.candidates where id=v_bundle.candidate_id for key share;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_bundle.candidate_id for update;
  if not found then
    raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_CANDIDATE_OWNER_MISSING';
  end if;
  -- Unique immutable Source event, never current_revision_id/applied_revision_id.
  for v_i in 1..2 loop
    if v_before_heads[v_i] is not null or p_before_source_event_ids[v_i] is null then
      continue;
    end if;
    select * into v_revision from private.bpay_next_work_revision
      where source_event_id=p_before_source_event_ids[v_i] for share;
    if not found or v_revision.source_kind is distinct from 'SOURCE'
       or v_revision.source_head_id is not null
       or v_revision.financial_snapshot_id is distinct from p_before_financial_snapshot_ids[v_i]
       or v_revision.physical_timesheet_id is distinct from v_roots[v_i]
       or v_revision.physical_timesheet_version is distinct from
         (select t.version from public.timesheets t where t.timesheet_id=v_roots[v_i])
       or v_revision.week_ending_date is distinct from v_bundle.week_ending_date
       or v_revision.currency is distinct from 'GBP'
       or v_revision.approved_at_utc is null or v_revision.sealed_at_utc is null
       or not pg_catalog.isfinite(v_revision.approved_at_utc)
       or not pg_catalog.isfinite(v_revision.sealed_at_utc) then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_INITIAL_REVISION_NOT_EXACT';
    end if;
    select * into strict v_work from private.bpay_next_work where id=v_revision.work_id;
    if v_work.work_kind is distinct from 'SOURCE'
       or v_work.booking_id is distinct from v_bookings[v_i]
       or v_work.candidate_id is distinct from v_bundle.candidate_id
       or v_work.contract_id is distinct from v_contracts[v_i]
       or v_work.week_ending_date is distinct from v_bundle.week_ending_date
       or not exists (
         select 1 from private.bpay_next_publication p
         join private.bpay_next_command c on c.id=p.command_id
         join private.bpay_next_command_member m
           on m.command_id=c.id and m.candidate_id=v_bundle.candidate_id
         where p.work_id=v_work.id and p.revision_id=v_revision.id
           and p.candidate_id=v_bundle.candidate_id and p.revision_no=v_revision.revision_no
           and c.id=private.bpay_next_source_command_id_v1(p_before_source_event_ids[v_i])
           and c.command_kind='POSITION_APPLY' and c.status in ('SEALED','COMPLETE')
           and c.expected_member_count=1 and c.sealed_at_utc is not null
           and pg_catalog.isfinite(c.sealed_at_utc) and m.member_no=1) then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_INITIAL_PUBLICATION_NOT_EXACT';
    end if;
    v_before_revisions[v_i]:=v_revision.id;
  end loop;

  for v_component in select c.* from public.weekly_source_entitlement_head_components c
    where c.head_id=any(v_bundle.proposed_head_ids) and c.component_kind='WORKED_TIME'
    order by c.head_id,c.component_ordinal
  loop
    -- The retained canonical owner requires DEC JSON strings, not the JSON
    -- numbers produced by to_jsonb(row). Its own scale/padding/magnitude guards
    -- remain authoritative; nullable numeric::text preserves NULL, and the
    -- explicit ISO date is independent of the session's DateStyle.
    v_canonical:=private.weekly_source_publication_component_canonical_v1(
      (pg_catalog.to_jsonb(v_component)-array['id','head_id','component_sha256',
        'decision_bundle_id','bundle_revision','created_at_utc'])||pg_catalog.jsonb_build_object(
        'work_date',pg_catalog.to_char(v_component.work_date,'YYYY-MM-DD'),
        'hours_day',v_component.hours_day::text,'hours_night',v_component.hours_night::text,
        'hours_sat',v_component.hours_sat::text,'hours_sun',v_component.hours_sun::text,
        'hours_bh',v_component.hours_bh::text,'unit_count',v_component.unit_count::text,
        'unit_pay_rate',v_component.unit_pay_rate::text,'unit_charge_rate',v_component.unit_charge_rate::text,
        'pay_ex_vat',v_component.pay_ex_vat::text,'charge_ex_vat',v_component.charge_ex_vat::text),
      'bpay_next_pair_detail.successor');
    v_component_sha:=private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_component_content_v1(v_canonical));
    if v_component.decision_bundle_id is distinct from p_decision_bundle_id
       or v_component.bundle_revision is distinct from p_bundle_revision
       or v_component.component_sha256 is distinct from v_component_sha then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_COMPONENT_NOT_EXACT';
    end if;
    v_matches:=0; v_origin:=null; v_segment:=null;
    for v_i in 1..2 loop
      if v_before_heads[v_i] is not null then
        if exists(select 1 from public.weekly_source_entitlement_head_components c
          where c.head_id=v_before_heads[v_i] and c.component_id=v_component.component_id) then
          v_matches:=v_matches+1; v_origin:=v_i;
        end if;
      else
        select pg_catalog.count(*)::integer into v_seen
          from pg_catalog.jsonb_array_elements(v_tf_components[v_i]) c(value)
          where c.value->>'component_id'=v_component.component_id::text;
        if v_seen>1 then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_COMPONENT_AMBIGUOUS';
        elsif v_seen=1 then v_matches:=v_matches+1; v_origin:=v_i;
        end if;
      end if;
    end loop;
    if v_matches<>1 then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_COMPONENT_ORIGIN_NOT_EXACT';
    end if;
    v_i:=v_origin;
    if v_before_heads[v_i] is not null then
      select * into strict v_before_component from public.weekly_source_entitlement_head_components c
        where c.head_id=v_before_heads[v_i] and c.component_id=v_component.component_id;
      v_before_canonical:=private.weekly_source_publication_component_canonical_v1(
        (pg_catalog.to_jsonb(v_before_component)-array['id','head_id','component_sha256',
          'decision_bundle_id','bundle_revision','created_at_utc'])||pg_catalog.jsonb_build_object(
          'work_date',pg_catalog.to_char(v_before_component.work_date,'YYYY-MM-DD'),
          'hours_day',v_before_component.hours_day::text,'hours_night',v_before_component.hours_night::text,
          'hours_sat',v_before_component.hours_sat::text,'hours_sun',v_before_component.hours_sun::text,
          'hours_bh',v_before_component.hours_bh::text,'unit_count',v_before_component.unit_count::text,
          'unit_pay_rate',v_before_component.unit_pay_rate::text,'unit_charge_rate',v_before_component.unit_charge_rate::text,
          'pay_ex_vat',v_before_component.pay_ex_vat::text,'charge_ex_vat',v_before_component.charge_ex_vat::text),
        'bpay_next_pair_detail.before');
      select * into strict v_before_head from public.weekly_source_entitlement_heads
        where id=v_before_heads[v_i];
      select * into v_existing from private.bpay_next_source_chosen_detail d
        where d.head_id=v_before_heads[v_i] and d.component_id=v_component.component_id;
      if not found or v_before_component.component_sha256 is distinct from v_component_sha
         or private.weekly_source_publication_request_digest_v1(
           private.weekly_source_publication_component_content_v1(v_before_canonical))
              is distinct from v_component_sha
         or v_existing.decision_bundle_id is distinct from v_before_head.decision_bundle_id
         or v_existing.bundle_revision is distinct from v_before_head.bundle_revision
         or v_existing.component_sha256 is distinct from v_component_sha
         or v_existing.detail_sha256 is distinct from
           pg_catalog.sha256(pg_catalog.convert_to(v_existing.detail_json::text,'UTF8')) then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_HEAD_COPY_NOT_EXACT';
      end if;
      v_detail:=v_existing.detail_json; -- exact old JSON, no UTC/rate enrichment
    else
      select c.value into strict v_composed from pg_catalog.jsonb_array_elements(v_tf_components[v_i]) c(value)
        where c.value->>'component_id'=v_component.component_id::text;
      v_before_canonical:=private.weekly_source_publication_component_canonical_v1(
        v_composed,'bpay_next_pair_detail.snapshot');
      if private.weekly_source_publication_request_digest_v1(
           private.weekly_source_publication_component_content_v1(v_before_canonical))
             is distinct from v_component_sha then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_SNAPSHOT_COMPONENT_NOT_EXACT';
      end if;
      select * into strict v_tf from public.timesheets_financials
        where id=p_before_financial_snapshot_ids[v_i];
      if v_before_revisions[v_i] is not null then
        select * into v_line from private.bpay_next_approved_line
          where revision_id=v_before_revisions[v_i]
            and component_key='SOURCE:'||v_component.component_id::text;
        v_hours:=coalesce(v_component.hours_day,0)+coalesce(v_component.hours_night,0)
          +coalesce(v_component.hours_sat,0)+coalesce(v_component.hours_sun,0)+coalesce(v_component.hours_bh,0);
        if not found or v_line.source_component_id is distinct from v_component.component_id
           or v_line.component_kind is distinct from 'WORK'
           or v_line.evidence_ref is distinct from 'source-component:'||v_component.component_id::text
             ||'#'||pg_catalog.encode(v_component_sha,'hex')
           or v_line.source_pay_ex_vat is distinct from
             (case when v_component.exclude_from_pay then 0 else v_component.pay_ex_vat end)
           or v_line.work_date is distinct from v_component.work_date
           or v_line.approved_quantity is distinct from v_hours then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FROZEN_LINE_NOT_EXACT';
        end if;
        v_rates:='{}'::jsonb; v_expected_rates:=0;
        for v_bucket in select * from (values
          ('DAY'::text,'day'::text,coalesce(v_component.hours_day,0)),
          ('NIGHT','night',coalesce(v_component.hours_night,0)),
          ('SAT','sat',coalesce(v_component.hours_sat,0)),
          ('SUN','sun',coalesce(v_component.hours_sun,0)),
          ('BH','bh',coalesce(v_component.hours_bh,0))) b(code,key,hours)
        loop
          select rd.approved_hours,rd.source_pay_rate into v_stored_hours,v_rate
            from private.bpay_next_rate_detail rd
            where rd.approved_line_id=v_line.id and rd.bucket=v_bucket.code;
          if coalesce(v_stored_hours,0) is distinct from v_bucket.hours
             or (v_bucket.hours<>0 and not found)
             or (v_bucket.hours=0 and found) then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FROZEN_BUCKET_NOT_EXACT';
          end if;
          if v_bucket.hours<>0 then
            v_expected_rates:=v_expected_rates+1;
            v_rates:=v_rates||pg_catalog.jsonb_build_object(v_bucket.key,v_rate);
          end if;
        end loop;
        select pg_catalog.count(*)::integer into v_actual_rates from private.bpay_next_rate_detail rd
          where rd.approved_line_id=v_line.id;
        if v_actual_rates<>v_expected_rates or v_line.expected_rate_detail_count<>v_expected_rates then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FROZEN_BUCKET_NOT_EXACT';
        end if;
        select pg_catalog.count(*)::integer into v_seen from private.bpay_next_shift_detail s
          where s.approved_line_id=v_line.id;
        if v_seen<>1 then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FROZEN_SHIFT_NOT_EXACT';
        end if;
        select * into strict v_shift from private.bpay_next_shift_detail s where s.approved_line_id=v_line.id;
        if v_shift.work_date is distinct from v_component.work_date
           or v_shift.approved_hours is distinct from v_hours then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FROZEN_SHIFT_NOT_EXACT';
        end if;
        v_break_total:=0; v_break_no:=0; v_all_clocked:=true; v_breaks:='[]'::jsonb;
        for v_row in select b.* from private.bpay_next_break_detail b
          where b.shift_detail_id=v_shift.id order by b.break_no
        loop
          v_break_no:=v_break_no+1;
          if v_row.break_no<>v_break_no or v_row.break_minutes<0 then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
          end if;
          if v_row.break_start_local is not null then
            v_break_minutes:=((pg_catalog.substring(v_row.break_end_local,1,2)::integer*60
              +pg_catalog.substring(v_row.break_end_local,4,2)::integer)
              -(pg_catalog.substring(v_row.break_start_local,1,2)::integer*60
              +pg_catalog.substring(v_row.break_start_local,4,2)::integer)+1440)%1440;
            if v_row.break_end_local is null or v_break_minutes=0
               or v_break_minutes<>v_row.break_minutes then
              raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
            end if;
          else v_all_clocked:=false;
          end if;
          v_break_total:=v_break_total+v_row.break_minutes;
          v_breaks:=v_breaks||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'start',v_row.break_start_local,'end',v_row.break_end_local));
        end loop;
        select * into strict v_revision from private.bpay_next_work_revision where id=v_before_revisions[v_i];
        select * into strict v_work from private.bpay_next_work where id=v_revision.work_id;
        v_detail:=pg_catalog.jsonb_build_object(
          'from_frozen_pair',true,'prior_approved_line_id',v_line.id,
          'prior_revision_id',v_revision.id,'prior_work_id',v_work.id,
          'prior_source_event_id',v_revision.source_event_id,
          'prior_financial_snapshot_id',v_revision.financial_snapshot_id,
          'prior_root_timesheet_id',v_revision.physical_timesheet_id,
          'prior_root_version',v_revision.physical_timesheet_version,
          'prior_family_booking_id',v_work.booking_id,
          'source_component_id',v_component.component_id,
          'source_component_sha256',pg_catalog.encode(v_component_sha,'hex'),
          'work_date',pg_catalog.to_char(v_shift.work_date,'YYYY-MM-DD'),'start',v_shift.shift_start_local,
          'end',v_shift.shift_end_local,'overnight',v_shift.shift_overnight,
          'break_minutes',v_break_total,'rates',v_rates);
        if v_all_clocked then v_detail:=v_detail||pg_catalog.jsonb_build_object('breaks',v_breaks); end if;
        if (v_shift.shift_start_at is null)<>(v_shift.shift_end_at is null)
           or (v_shift.shift_start_at is not null and (
             not pg_catalog.isfinite(v_shift.shift_start_at) or not pg_catalog.isfinite(v_shift.shift_end_at)
             or v_shift.shift_end_at<=v_shift.shift_start_at)) then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_UTC_INVALID';
        end if;
        if v_shift.shift_start_at is not null then
          v_detail:=v_detail||pg_catalog.jsonb_build_object(
            'start_utc',pg_catalog.to_char(v_shift.shift_start_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'end_utc',pg_catalog.to_char(v_shift.shift_end_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
        end if;
      else
        -- Reviewed, not-yet-authorised B: ordinary segment identity is real;
        -- no Final/movement/work-event/Bank approval identity is manufactured.
        select pg_catalog.count(*)::integer,pg_catalog.jsonb_agg(s.value)->0 into v_seen,v_segment
          from pg_catalog.jsonb_array_elements(v_tf.invoice_breakdown_json->'segments') s(value)
          where s.value->>'segment_id'=v_component.segment_id;
        if v_i<>2 or p_before_source_event_ids[v_i] is not null or v_seen<>1 then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_TARGET_SEGMENT_NOT_EXACT';
        end if;
        if (v_segment->>'start_local' is not null and v_segment->>'start' is not null
              and v_segment->>'start_local' is distinct from v_segment->>'start')
           or (v_segment->>'end_local' is not null and v_segment->>'end' is not null
              and v_segment->>'end_local' is distinct from v_segment->>'end') then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_CLOCK_ALIAS_CONFLICT';
        end if;
        v_start:=nullif(pg_catalog.btrim(coalesce(v_segment->>'start_local',v_segment->>'start','')),'');
        v_end:=nullif(pg_catalog.btrim(coalesce(v_segment->>'end_local',v_segment->>'end','')),'');
        v_rates:='{}'::jsonb;
        for v_bucket in select * from (values
          ('day'::text,v_tf.pay_day),('night',v_tf.pay_night),('sat',v_tf.pay_sat),
          ('sun',v_tf.pay_sun),('bh',v_tf.pay_bh)) b(key,rate)
        loop
          if pg_catalog.jsonb_typeof(v_segment->'weekly_source')='object' then
            v_rate:=(v_segment#>>array['weekly_source','pay_vector','rates',v_bucket.key])::numeric;
          else v_rate:=v_bucket.rate;
          end if;
          v_rates:=v_rates||pg_catalog.jsonb_build_object(v_bucket.key,v_rate);
        end loop;
        v_break_total:=0; v_break_no:=0; v_breaks:='[]'::jsonb;
        if v_segment ? 'breaks' and pg_catalog.jsonb_typeof(v_segment->'breaks') not in ('array','null') then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
        end if;
        for v_break in select value from pg_catalog.jsonb_array_elements(
          case when pg_catalog.jsonb_typeof(v_segment->'breaks')='array' then v_segment->'breaks' else '[]'::jsonb end)
        loop
          if pg_catalog.jsonb_typeof(v_break) is distinct from 'object'
             or coalesce(v_break->>'start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
             or coalesce(v_break->>'end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
          end if;
          v_break_minutes:=((pg_catalog.substring(v_break->>'end',1,2)::integer*60
            +pg_catalog.substring(v_break->>'end',4,2)::integer)
            -(pg_catalog.substring(v_break->>'start',1,2)::integer*60
            +pg_catalog.substring(v_break->>'start',4,2)::integer)+1440)%1440;
          if v_break_minutes=0 then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
          end if;
          v_break_no:=v_break_no+1; v_break_total:=v_break_total+v_break_minutes;
          v_breaks:=v_breaks||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'start',v_break->>'start','end',v_break->>'end'));
        end loop;
        if v_segment ? 'break_mins' and v_segment ? 'break_minutes'
           and (v_segment->>'break_mins')::integer is distinct from (v_segment->>'break_minutes')::integer then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
        end if;
        if v_break_no=0 then
          v_break_total:=coalesce((v_segment->>'break_mins')::integer,(v_segment->>'break_minutes')::integer,0);
        elsif (v_segment ? 'break_mins' or v_segment ? 'break_minutes') and v_break_total is distinct from
          coalesce((v_segment->>'break_mins')::integer,(v_segment->>'break_minutes')::integer) then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
        end if;
        v_detail:=pg_catalog.jsonb_build_object(
          'from_reviewed_target_tsfin',true,'before_financial_snapshot_id',v_tf.id,
          'before_root_timesheet_id',v_tf.timesheet_id,'before_root_version',v_tf.timesheet_version,
          'before_segment_id',v_component.segment_id,'source_component_id',v_component.component_id,
          'source_component_sha256',pg_catalog.encode(v_component_sha,'hex'),
          'decision_bundle_id',p_decision_bundle_id,'bundle_revision',p_bundle_revision,
          'snapshot_detail_sha256',pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            pg_catalog.jsonb_build_object('segments',v_tf.invoice_breakdown_json->'segments',
              'actual_schedule',v_tf.actual_schedule_json,'rate_source_refs',v_tf.rate_source_refs_json)::text,'UTF8')),'hex'),
          'work_date',v_segment->>'date','start',v_start,'end',v_end,
          'overnight',(v_segment->>'overnight')::boolean,'break_minutes',v_break_total,'rates',v_rates);
        if v_break_no>0 then v_detail:=v_detail||pg_catalog.jsonb_build_object('breaks',v_breaks); end if;
        if (v_segment->'start_utc' is not null and v_segment->'start_utc'<>'null'::jsonb)
           or (v_segment->'end_utc' is not null and v_segment->'end_utc'<>'null'::jsonb) then
          if pg_catalog.jsonb_typeof(v_segment->'start_utc') is distinct from 'string'
             or pg_catalog.jsonb_typeof(v_segment->'end_utc') is distinct from 'string'
             or coalesce(v_segment->>'start_utc','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$'
             or coalesce(v_segment->>'end_utc','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$' then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_UTC_INVALID';
          end if;
          begin
            v_start_utc:=(v_segment->>'start_utc')::timestamptz;
            v_end_utc:=(v_segment->>'end_utc')::timestamptz;
          exception when invalid_datetime_format or datetime_field_overflow or invalid_time_zone_displacement_value then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_UTC_INVALID';
          end;
          if not pg_catalog.isfinite(v_start_utc) or not pg_catalog.isfinite(v_end_utc)
             or v_end_utc<=v_start_utc
             or pg_catalog.to_char(v_start_utc at time zone 'Europe/London','YYYY-MM-DD')<>v_segment->>'date'
             or pg_catalog.to_char(v_start_utc at time zone 'Europe/London','HH24:MI')<>v_start
             or pg_catalog.to_char(v_end_utc at time zone 'Europe/London','HH24:MI')<>v_end
             or (v_end_utc at time zone 'Europe/London')::date<>(v_segment->>'date')::date
                +(case when (v_segment->>'overnight')::boolean then 1 else 0 end) then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_UTC_INVALID';
          end if;
          v_detail:=v_detail||pg_catalog.jsonb_build_object('start_utc',v_segment->'start_utc','end_utc',v_segment->'end_utc');
        end if;
      end if;
    end if;
    -- Shared presentation qualification does not turn clocks into money.
    if pg_catalog.jsonb_typeof(v_detail) is distinct from 'object'
       or v_detail->>'work_date' is distinct from pg_catalog.to_char(v_component.work_date,'YYYY-MM-DD')
       or coalesce(v_detail->>'start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
       or coalesce(v_detail->>'end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
       or pg_catalog.jsonb_typeof(v_detail->'overnight') is distinct from 'boolean'
       or pg_catalog.jsonb_typeof(v_detail->'rates') is distinct from 'object'
       or coalesce(v_detail->>'break_minutes','') !~ '^[0-9]+$'
       or pg_catalog.octet_length(v_detail::text)>4096 then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_PRESENTATION_NOT_EXACT';
    end if;
    for v_bucket in select * from (values
      ('day'::text,coalesce(v_component.hours_day,0)),('night',coalesce(v_component.hours_night,0)),
      ('sat',coalesce(v_component.hours_sat,0)),('sun',coalesce(v_component.hours_sun,0)),
      ('bh',coalesce(v_component.hours_bh,0))) b(key,hours)
    loop
      v_rate:=(v_detail->'rates'->>v_bucket.key)::numeric;
      if (v_bucket.hours<>0 and not v_component.exclude_from_pay and v_rate is null)
         or (v_rate is not null and (v_rate::text in ('NaN','Infinity','-Infinity')
           or v_rate<>pg_catalog.round(v_rate,6) or pg_catalog.abs(v_rate)>=1000000000000)) then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_USED_RATE_NOT_EXACT';
      end if;
    end loop;
    v_detail_sha:=pg_catalog.sha256(pg_catalog.convert_to(v_detail::text,'UTF8'));
    insert into private.bpay_next_source_chosen_detail
      (head_id,component_id,decision_bundle_id,bundle_revision,component_sha256,detail_json,detail_sha256)
      values(v_component.head_id,v_component.component_id,p_decision_bundle_id,p_bundle_revision,
        v_component_sha,v_detail,v_detail_sha)
      on conflict(head_id,component_id) do nothing;
    select * into strict v_existing from private.bpay_next_source_chosen_detail d
      where d.head_id=v_component.head_id and d.component_id=v_component.component_id;
    if v_existing.decision_bundle_id is distinct from p_decision_bundle_id
       or v_existing.bundle_revision is distinct from p_bundle_revision
       or v_existing.component_sha256 is distinct from v_component_sha
       or v_existing.detail_json is distinct from v_detail
       or v_existing.detail_sha256 is distinct from v_detail_sha then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_REPLAY_CONFLICT';
    end if;
    v_count:=v_count+1;
  end loop;
  return v_count;
end
$function$;

alter function private.bpay_next_capture_source_pair_before_detail_v1(uuid,bigint,uuid[],uuid[]) owner to postgres;
revoke all on function private.bpay_next_capture_source_pair_before_detail_v1(uuid,bigint,uuid[],uuid[])
  from public,anon,authenticated,service_role;
commit;
