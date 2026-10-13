-- Root-only fixture batch. This file has never been executed by the credit lane.
-- Root supplies native, isolated TEST fixture IDs and canonical prepared DTOs
-- in transaction-local cloudtms.healthroster_cn_fixture_manifest. No values,
-- manager signatures, document assets, payment or external effects are invented.
-- Execute ONLY inside the server-owned outer agency_test rollback envelope,
-- after the full source closure and SOURCE_CATALOGUE_VERIFIER.sql pass.
-- All 18 runtime/document/app gates remain independently required.
do $proof$
declare
  m jsonb:=nullif(current_setting('cloudtms.healthroster_cn_fixture_manifest',true),'')::jsonb;
  c jsonb; q private.timesheet_correction_requests%rowtype; result jsonb; before_unit jsonb; after_unit jsonb;
  protected_before jsonb; protected_after jsonb; assets_before jsonb; assets_after jsonb;
  rejected boolean; candidate_id uuid; request_id uuid; case_count integer:=0;
begin
  if jsonb_typeof(m) is distinct from 'object' or m->>'environment'<>'TEST'
    or not pg_input_is_valid(m->>'active_pay_actor_id','uuid')
    or not pg_input_is_valid(m->>'foreign_candidate_id','uuid')
    or jsonb_typeof(m->'approval_cases') is distinct from 'array' or jsonb_array_length(m->'approval_cases')<1
    or jsonb_typeof(m->'applied_request_ids') is distinct from 'array'
    or not pg_input_is_valid(m->>'waiting_cancel_request_id','uuid') then
    raise exception 'HEALTHROSTER_NATIVE_FIXTURE_MANIFEST_REQUIRED';
  end if;
  if private.timesheet_correction_actor_allowed_v1((m->>'active_pay_actor_id')::uuid) is distinct from true then
    raise exception 'HEALTHROSTER_NATIVE_FIXTURE_PAY_ACTOR_UNPROVED';
  end if;

  -- Real prepare/result transport. Include valid A0/signed/null-Daily/D1 and
  -- distinct returned-native-failure cases in root's fixture manifest.
  for c in select value from jsonb_array_elements(m->'approval_cases') loop
    select request.* into strict q from private.timesheet_correction_requests request where request.id=(c->>'request_id')::uuid;
    if q.state<>'WAITING' then raise exception 'HEALTHROSTER_APPROVAL_FIXTURE_NOT_WAITING'; end if;
    select jsonb_build_object('request',to_jsonb(q),
      'timesheets',(select jsonb_agg(to_jsonb(t) order by t.timesheet_id) from public.timesheets t
        where t.timesheet_id in(q.reversed_timesheet_id,q.replacement_timesheet_id,(q.frozen_original_json->>'planned_reversal_timesheet_id')::uuid)),
      'financials',(select jsonb_agg(to_jsonb(tf) order by tf.id) from public.timesheets_financials tf
        where tf.timesheet_id in(q.reversed_timesheet_id,q.replacement_timesheet_id,(q.frozen_original_json->>'planned_reversal_timesheet_id')::uuid)),
      'weeks',(select jsonb_agg(to_jsonb(w) order by w.id) from public.contract_weeks w
        where w.id in(q.replacement_week_id,(q.frozen_original_json->>'planned_reversal_week_id')::uuid)),
      'invoice_lines',(select jsonb_agg(to_jsonb(l) order by l.id) from public.invoice_lines l
        where l.invoice_id in(q.original_invoice_id,q.return_invoice_id))) into before_unit;
    rejected:=false; result:=null;
    begin
      result:=public.timesheet_correction_authorise_atomic_v1(q.id,q.state_revision,c->'prepared',
        coalesce((c->>'actor_user_id')::uuid,(m->>'active_pay_actor_id')::uuid),now());
    exception when others then rejected:=true;
    end;
    if coalesce((c->>'expect_success')::boolean,false) then
      if rejected or result->>'state' is distinct from 'APPLIED' then raise exception 'HEALTHROSTER_APPROVAL_CASE_FAILED'; end if;
      perform private.timesheet_correction_applied_assert_v1(q.id);
      if result->'ordinary_notification_timesheet_ids' is distinct from jsonb_build_array(q.replacement_timesheet_id) then
        raise exception 'HEALTHROSTER_NOTIFICATION_UNIT_WRONG';
      end if;
    else
      if not rejected then raise exception 'HEALTHROSTER_EXPECTED_ATOMIC_REFUSAL_MISSING'; end if;
      select jsonb_build_object('request',(select to_jsonb(r) from private.timesheet_correction_requests r where r.id=q.id),
        'timesheets',(select jsonb_agg(to_jsonb(t) order by t.timesheet_id) from public.timesheets t
          where t.timesheet_id in(q.reversed_timesheet_id,q.replacement_timesheet_id,(q.frozen_original_json->>'planned_reversal_timesheet_id')::uuid)),
        'financials',(select jsonb_agg(to_jsonb(tf) order by tf.id) from public.timesheets_financials tf
          where tf.timesheet_id in(q.reversed_timesheet_id,q.replacement_timesheet_id,(q.frozen_original_json->>'planned_reversal_timesheet_id')::uuid)),
        'weeks',(select jsonb_agg(to_jsonb(w) order by w.id) from public.contract_weeks w
          where w.id in(q.replacement_week_id,(q.frozen_original_json->>'planned_reversal_week_id')::uuid)),
        'invoice_lines',(select jsonb_agg(to_jsonb(l) order by l.id) from public.invoice_lines l
          where l.invoice_id in(q.original_invoice_id,q.return_invoice_id))) into after_unit;
      if after_unit is distinct from before_unit then raise exception 'HEALTHROSTER_RETURNED_FAILURE_LEFT_PARTIAL_UNIT'; end if;
    end if;
    case_count:=case_count+1;
  end loop;

  -- Supplied request IDs must include at least one nonzero-mileage and
  -- additional/component fixture; exact verification never accepts a clamp.
  for c in select value from jsonb_array_elements(m->'applied_request_ids') loop
    request_id:=(c#>>'{}')::uuid;
    select request.* into strict q from private.timesheet_correction_requests request where request.id=request_id;
    if q.state<>'APPLIED' then raise exception 'HEALTHROSTER_APPLIED_FIXTURE_REQUIRED'; end if;
    perform private.timesheet_correction_applied_assert_v1(q.id);
    perform private.timesheet_correction_reversal_assert_v1(
      (select to_jsonb(tf) from public.timesheets_financials tf where tf.timesheet_id=q.reversed_timesheet_id and tf.is_current),
      (select to_jsonb(tf) from public.timesheets_financials tf where tf.timesheet_id=q.reversal_timesheet_id and tf.is_current));
    if public.timesheet_correction_create_projection_v1((m->>'foreign_candidate_id')::uuid,null,q.replacement_timesheet_id,null) is not null then
      raise exception 'HEALTHROSTER_FOREIGN_CREATE_CONTEXT_LEAK';
    end if;
    rejected:=false;
    begin
      perform public.timesheet_correction_create_projection_v1(q.candidate_id,null,q.replacement_timesheet_id,gen_random_uuid());
    exception when sqlstate '40001' then rejected:=true;
    end;
    if not rejected then raise exception 'HEALTHROSTER_CONTRADICTORY_CREATE_CONTEXT_ACCEPTED'; end if;
    rejected:=false;
    begin
      update public.timesheets_financials set mileage_units=mileage_units+1 where timesheet_id=q.reversal_timesheet_id and is_current;
      perform private.timesheet_correction_applied_assert_v1(q.id);
    exception when others then rejected:=true;
    end;
    if not rejected then raise exception 'HEALTHROSTER_APPLIED_QUANTITY_MUTATION_ACCEPTED'; end if;
    perform private.timesheet_correction_applied_assert_v1(q.id);
    case_count:=case_count+1;
  end loop;

  -- Cancel actual native WAITING P0, optionally manager-finalized, without
  -- borrowing a reset/Reject/foreign workflow. Retained keys are pointers only;
  -- root separately proves real immutable R2 contents and subsequent GC.
  select request.* into strict q from private.timesheet_correction_requests request
    where request.id=(m->>'waiting_cancel_request_id')::uuid;
  if q.state<>'WAITING' then raise exception 'HEALTHROSTER_CANCEL_FIXTURE_REQUIRED'; end if;
  select jsonb_build_object('timesheet',to_jsonb(t),
    'financials',(select jsonb_agg(to_jsonb(tf) order by tf.id) from public.timesheets_financials tf where tf.timesheet_id=t.timesheet_id))
    into protected_before from public.timesheets t where t.timesheet_id=q.reversed_timesheet_id;
  select coalesce(jsonb_agg(to_jsonb(cmp)-'timesheet_id' order by cmp.id),'[]'::jsonb) into assets_before
    from public.candidate_submission_components cmp where cmp.workflow_id=(select t.candidate_workflow_id from public.timesheets t
      where t.timesheet_id=q.replacement_timesheet_id) and cmp.state='IMMUTABLE';
  result:=public.timesheet_correction_cancel_atomic_v1('TEST',q.id,q.state_revision,(m->>'active_pay_actor_id')::uuid,now());
  if result->>'state' is distinct from 'CANCELLED' then raise exception 'HEALTHROSTER_CANCEL_INCOMPLETE'; end if;
  select jsonb_build_object('timesheet',to_jsonb(t),
    'financials',(select jsonb_agg(to_jsonb(tf) order by tf.id) from public.timesheets_financials tf where tf.timesheet_id=t.timesheet_id))
    into protected_after from public.timesheets t where t.timesheet_id=q.reversed_timesheet_id;
  if protected_after is distinct from protected_before then raise exception 'HEALTHROSTER_CANCEL_ORIGINAL_CHANGED'; end if;
  select coalesce(jsonb_agg(to_jsonb(cmp)-'timesheet_id' order by cmp.id),'[]'::jsonb) into assets_after
    from public.candidate_submission_components cmp where cmp.id in(select (entry->>'id')::uuid from jsonb_array_elements(assets_before) entry);
  if assets_after is distinct from assets_before then raise exception 'HEALTHROSTER_CANCEL_IMMUTABLE_AUDIT_CHANGED'; end if;
  if exists(select 1 from jsonb_array_elements_text(coalesce(result#>'{lawful_removal,r2_cleanup_keys}','[]'::jsonb)) cleanup_key(value)
    where exists(select 1 from jsonb_array_elements(assets_before) cmp
      where cleanup_key.value in(cmp->>'storage_key',cmp->>'review_storage_key',cmp->>'final_signed_storage_key',cmp->>'paper_return_page_key'))) then
    raise exception 'HEALTHROSTER_CANCEL_RETURNED_IMMUTABLE_ASSET_FOR_GC';
  end if;
  if public.timesheet_correction_create_projection_v1(q.candidate_id,q.replacement_week_id,q.replacement_timesheet_id,null)->>'state'
    is distinct from 'CANCELLED' then raise exception 'HEALTHROSTER_CANCELLED_CREATE_CONTEXT_MISSING'; end if;
  case_count:=case_count+1;
  set constraints all immediate;
  raise notice 'HEALTHROSTER request fixture batch completed: % cases; runtime/PDF/app gates unproved',case_count;
end;
$proof$;
