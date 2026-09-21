-- Verification: weekly_source_invoice_batch_integration_v1
-- Read-only catalogue and source-contract checks. Runtime economic and move
-- behaviour remains covered by weekly_source_invoice_admission_v1 verification.

\set ON_ERROR_STOP on

do $verify$
declare
  v_definition text;
begin
  if to_regprocedure('public.weekly_source_invoice_batch_candidates_v1(jsonb)') is null
     or to_regprocedure('public.weekly_source_invoice_batch_admit_atomic_v1(jsonb)') is null
     or to_regprocedure('public.weekly_source_invoice_edit_context_v1(jsonb)') is null then
    raise exception 'weekly source invoice batch integration RPC is missing';
  end if;

  select pg_get_functiondef(
    'public.weekly_source_invoice_batch_admit_atomic_v1(jsonb)'::regprocedure
  ) into v_definition;
  if v_definition not like '%weekly_source_invoice_admit_atomic_v1%'
     or v_definition not like '%WEEKLY_SOURCE_INVOICE_CYCLE_CONSOLIDATION_REFUSED%'
     or v_definition like '%invoice_operation_start_batch%'
     or v_definition like '%banking%'
     or v_definition like '%workbench%' then
    raise exception 'weekly source invoice batch admission boundary is invalid';
  end if;

  select pg_get_functiondef(
    'public.weekly_source_invoice_edit_context_v1(jsonb)'::regprocedure
  ) into v_definition;
  if v_definition not like '%destination.status=''DRAFT''%'
     or v_definition not like '%destination.issued_at_utc is null%'
     or v_definition not like '%destination.paid_at_utc is null%'
     or v_definition not like '%backing_report_numbers%' then
    raise exception 'weekly source invoice move projection is not fail-closed';
  end if;

  -- Gate 7 item G7-2 (24 section 12; 25 section 8 Removed).  The Office is
  -- offered one immutable presentation line per movable unit, with the expected
  -- presentation hash the move owner demands, and is never offered a work event
  -- as the unit of movement.
  if v_definition not like '%''movable_lines''%'
     or v_definition not like '%''presentation_line_id'',presentation.id%'
     or v_definition not like '%''presentation_hash'',pg_catalog.encode(presentation.presentation_hash,''hex'')%'
     or v_definition not like '%''indivisible_presentation''%'
     or v_definition not like '%''independently_movable''%'
     or v_definition not like '%''follows_companion_presentation_line_id''%'
     or v_definition like '%''shift_groups''%'
     or v_definition like '%group by presentation.work_event_id%' then
    raise exception 'weekly source invoice edit context still offers work-event movement units';
  end if;

  -- Gate 7 item G7-1: the move owner takes a presentation-line identity plus an
  -- expected presentation hash, and has no work-event selector at all.
  select pg_get_functiondef(
    'public.weekly_source_invoice_move_atomic_v1(jsonb)'::regprocedure
  ) into v_definition;
  if v_definition not like '%''presentation_line_id''%'
     or v_definition not like '%''expected_presentation_hash''%'
     or v_definition not like '%WEEKLY_SOURCE_INVOICE_MOVE_PRESENTATION_HASH_MISMATCH%'
     or v_definition not like '%WEEKLY_SOURCE_INVOICE_MOVE_COMPANION_REQUIRED%'
     or v_definition not like '%companion_presentation_line_id%'
     or v_definition like '%''source_shift_group_id'',%'
     or v_definition like '%movement.work_event_id=v_source_shift_group_id%' then
    raise exception 'weekly source invoice move is not bounded to one presentation line';
  end if;

  -- Gate 7 item G7-3: the one source-aware issue validator exists and is
  -- reached from both real issue entry points.
  if to_regprocedure('private.weekly_source_invoice_issue_validate_v1(uuid)') is null
     or to_regprocedure('private.weekly_source_invoice_issue_blockers_v1(uuid,text[])') is null
     or to_regprocedure('private.weekly_source_invoice_issue_reasons_v1(jsonb,text[])') is null then
    raise exception 'the source-aware invoice issue validator is missing';
  end if;
  select pg_get_functiondef(
    'private._invoice_issue_validate_batch(jsonb,date)'::regprocedure
  ) into v_definition;
  if v_definition not like '%private.weekly_source_invoice_issue_blockers_v1(f.invoice_id,%' then
    raise exception 'the asynchronous issue route is not source aware';
  end if;
  select pg_get_functiondef(
    'public.invoice_issue_one(uuid,uuid)'::regprocedure
  ) into v_definition;
  if v_definition not like '%private.weekly_source_invoice_issue_validate_v1(p_invoice_id)%'
     or v_definition not like '%private.weekly_source_invoice_issue_reasons_v1(%' then
    raise exception 'the direct issue route is not source aware';
  end if;

  -- WP-27 (Gate 13 hostile review F4; standing rule 3).  The exclusion must be
  -- keyed on the Timesheet FAMILY through the one installed resolver adapter.
  -- The earlier assertion required `lineage.timesheet_id=financial.timesheet_id`,
  -- which the reviewer executed as a double-invoice path: a lineage-bound root
  -- that rotates keeps its lineage on the OLD physical id, so the new current
  -- version was offered to the ordinary batch as READY.  That bare physical
  -- predicate is now refused outright.
  select pg_get_functiondef(
    'private._invoice_batch_generate_classification_v2(boolean,text[],timestamp with time zone)'::regprocedure
  ) into v_definition;
  if v_definition not like '%public.weekly_source_row_timesheet_lineages%'
     or pg_catalog.regexp_replace(v_definition,'[[:space:]]+','','g')
          not like '%lineage.timesheet_id=any(private.weekly_source_invoice_family_timesheet_ids_v1(financial.timesheet_id))%' then
    raise exception 'ordinary invoice generation still admits weekly source-owned timesheets';
  end if;
  if pg_catalog.regexp_replace(v_definition,'[[:space:]]+','','g')
       like '%lineage.timesheet_id=financial.timesheet_id%' then
    raise exception 'ordinary invoice generation is still keyed on the physical root id';
  end if;
  if to_regprocedure('private.weekly_source_invoice_family_timesheet_ids_v1(uuid)') is null then
    raise exception 'the one family resolver adapter is missing';
  end if;

  if has_function_privilege('anon',
       'public.weekly_source_invoice_batch_candidates_v1(jsonb)','EXECUTE')
     or has_function_privilege('authenticated',
       'public.weekly_source_invoice_batch_candidates_v1(jsonb)','EXECUTE')
     or has_function_privilege('anon',
       'public.weekly_source_invoice_batch_admit_atomic_v1(jsonb)','EXECUTE')
     or has_function_privilege('authenticated',
       'public.weekly_source_invoice_batch_admit_atomic_v1(jsonb)','EXECUTE')
     or has_function_privilege('anon',
       'public.weekly_source_invoice_edit_context_v1(jsonb)','EXECUTE')
     or has_function_privilege('authenticated',
       'public.weekly_source_invoice_edit_context_v1(jsonb)','EXECUTE') then
    raise exception 'weekly source invoice batch integration is browser executable';
  end if;
end;
$verify$;

-- ---------------------------------------------------------------------------
-- WP-27 (Gate 13 hostile review F4; standing rule 3): EXECUTED rotation case.
-- The checks above read installed text.  This section drives the real ordinary
-- batch classifier and the real invoice-line owner guard against a rotated
-- lineage-bound root, because a text read would not have caught the original
-- defect either: the text was exactly what the contract asked for, and it was
-- still a double-invoice path.  Every write is rolled back.
-- ---------------------------------------------------------------------------
\set weekly_source_verification_outer_transaction true
\set weekly_source_ordinary_verification_outer_transaction true
\set weekly_source_verification_correction_presentation 'FULL_REVERSAL_REPLACEMENT'
\set weekly_source_verification_expense_vat_enabled false
begin;
set local request.jwt.claim.role='service_role';
\ir 15092026_1534_weekly_source_ordinary_pay_projection_v1.sql

create function pg_temp.wp27_groups_naming(p_id uuid, p_now timestamptz)
returns bigint language sql as $wp27$
  select pg_catalog.count(*)
  from private._invoice_batch_generate_classification_v2(true,null,p_now) g
  where g.candidate_json::text like '%'||p_id::text||'%';
$wp27$;

create function pg_temp.wp27_line_probe(p_timesheet uuid, p_invoice uuid)
returns text language plpgsql as $wp27$
declare
  v_message text;
begin
  begin
    insert into public.invoice_lines(
      invoice_id,timesheet_id,description,hours_day,hours_night,hours_sat,hours_sun,
      hours_bh,total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,vat_rate_pct,
      vat_amount,total_inc_vat,meta_json,source_key
    ) values (
      p_invoice,p_timesheet,'WP-27 rotation probe',0,0,0,0,0,0,0,0,0,0,0,
      '{}'::jsonb,'WP27-ROTATION-'||p_timesheet::text
    );
    raise exception 'WP27_ACCEPTED';
  exception when others then
    get stacked diagnostics v_message=message_text;
    if v_message='WP27_ACCEPTED' then return 'ACCEPTED'; end if;
    return v_message;
  end;
end;
$wp27$;

do $wp27_rotation$
declare
  v_old uuid;
  v_new constant uuid:='fedcba98-0000-4000-8000-0000000027a1';
  v_control constant uuid:='fedcba98-0000-4000-8000-0000000027a2';
  v_now constant timestamptz:='2026-11-02 09:00+00';
  v_invoice uuid;
  v_family uuid[];
  v_seed public.timesheets%rowtype;
  v_seed_fin public.timesheets_financials%rowtype;
begin
  -- a lineage-bound member that carries a current, client-bearing financial row
  select lineage.timesheet_id into v_old
  from public.weekly_source_row_timesheet_lineages lineage
  join public.timesheets timesheet_row
    on timesheet_row.timesheet_id=lineage.timesheet_id and timesheet_row.is_current
  join public.timesheets_financials financial
    on financial.timesheet_id=lineage.timesheet_id and financial.is_current
   and financial.client_id is not null
  where pg_catalog.btrim(coalesce(timesheet_row.booking_id,''))<>''
  order by lineage.timesheet_id
  limit 1;
  if v_old is null then
    raise exception 'WP-27 rotation case: the fixture has no lineage-bound member with a current financial row';
  end if;
  select * into v_seed from public.timesheets where timesheet_id=v_old;
  select * into v_seed_fin from public.timesheets_financials
  where timesheet_id=v_old and is_current;

  insert into public.invoices(client_id,status,subtotal_ex_vat,vat_amount,total_inc_vat)
  values (v_seed_fin.client_id,'DRAFT',0,0,0) returning id into v_invoice;

  -- POSITIVE CONTROL: an ordinary Timesheet on its own booking, no lineage and
  -- no protected family.  If this stopped being offered or stopped accepting an
  -- ordinary line, the assertions below would pass for the wrong reason.
  create temp table wp27_control_ts on commit drop as
    select * from public.timesheets where timesheet_id=v_old;
  update wp27_control_ts set timesheet_id=v_control, version=1, is_current=true,
    booking_id='wp27-verifier-ordinary-control';
  insert into public.timesheets select * from wp27_control_ts;
  create temp table wp27_control_fin on commit drop as
    select * from public.timesheets_financials where id=v_seed_fin.id;
  update wp27_control_fin set id=pg_catalog.gen_random_uuid(), timesheet_id=v_control,
    is_current=true, is_stale=false, locked_by_invoice_id=null,
    processing_status='READY_FOR_INVOICE'::public.ts_fin_processing_status_enum;
  insert into public.timesheets_financials select * from wp27_control_fin;

  if pg_temp.wp27_groups_naming(v_control,v_now)<1 then
    raise exception 'WP-27 rotation case: the ordinary control Timesheet is not offered to the ordinary batch, so this case proves nothing';
  end if;
  if pg_temp.wp27_line_probe(v_control,v_invoice)<>'ACCEPTED' then
    raise exception 'WP-27 rotation case: the line owner guard refused an ordinary control Timesheet';
  end if;

  -- rotate the lineage-bound root: same booking, new current version, old
  -- demoted, financial row carried across.  ROT-001 / WP-03 G3 allow
  -- pre-authorisation rotation, and lineage rows exist before authorisation.
  create temp table wp27_rotated_ts on commit drop as
    select * from public.timesheets where timesheet_id=v_old;
  update wp27_rotated_ts set timesheet_id=v_new, version=version+1, is_current=true;
  update public.timesheets set is_current=false where timesheet_id=v_old;
  insert into public.timesheets select * from wp27_rotated_ts;

  create temp table wp27_rotated_fin on commit drop as
    select * from public.timesheets_financials where id=v_seed_fin.id;
  update wp27_rotated_fin set id=pg_catalog.gen_random_uuid(), timesheet_id=v_new,
    is_current=true, is_stale=false, locked_by_invoice_id=null,
    processing_status='READY_FOR_INVOICE'::public.ts_fin_processing_status_enum;
  update public.timesheets_financials set is_current=false
  where timesheet_id=v_old and is_current;
  insert into public.timesheets_financials select * from wp27_rotated_fin;

  -- the installed resolver knows the family
  v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(v_new);
  if not (v_old=any(v_family) and v_new=any(v_family)) then
    raise exception 'WP-27 rotation case: the resolver does not report both family members';
  end if;
  -- and the lineage row is still on the OLD physical id only
  if exists(select 1 from public.weekly_source_row_timesheet_lineages
            where timesheet_id=v_new)
     or not exists(select 1 from public.weekly_source_row_timesheet_lineages
                   where timesheet_id=v_old) then
    raise exception 'WP-27 rotation case: the lineage row did not stay on the old physical id';
  end if;

  -- the two assertions this package exists for
  if pg_temp.wp27_groups_naming(v_new,v_now)<>0 then
    raise exception 'WP-27 F4: the ordinary batch offered a rotated lineage-bound root as a candidate';
  end if;
  if pg_temp.wp27_groups_naming(v_old,v_now)<>0 then
    raise exception 'WP-27 F4: the ordinary batch offered the old lineage-bound root as a candidate';
  end if;
  if pg_temp.wp27_line_probe(v_new,v_invoice)
       <>'WEEKLY_SOURCE_INVOICE_MOVEMENT_OWNER_REQUIRED' then
    raise exception 'WP-27 F4: the line owner guard did not fire for a rotated lineage-bound root';
  end if;
  if pg_temp.wp27_line_probe(v_old,v_invoice)
       <>'WEEKLY_SOURCE_INVOICE_MOVEMENT_OWNER_REQUIRED' then
    raise exception 'WP-27 F4: the line owner guard did not fire for the old lineage-bound root';
  end if;

  -- F5: a protected family recorded against the OLD root is this family's
  -- protected state when the family is approached by the NEW physical root.
  insert into public.weekly_exceptional_pay_target_families(
    agency_id,candidate_id,contract_id,week_start_date,week_ending_date,
    root_timesheet_id,root_family_booking_id,ownership_state,
    first_signed_evidence_fingerprint,current_lifecycle_state,creation_idempotency_key
  )
  select (select source_group.agency_id from public.weekly_source_groups source_group
          order by source_group.id limit 1),
         contract_row.candidate_id,timesheet_row.contract_id,
         timesheet_row.week_ending_date-6,timesheet_row.week_ending_date,
         v_old,pg_catalog.btrim(timesheet_row.booking_id),'TARGET_MANAGED',
         pg_catalog.sha256('WP27_ROTATION_CASE'::bytea),'PROTECTED',
         'wp27-verifier-rotation-family'
  from public.timesheets timesheet_row
  join public.contracts contract_row on contract_row.id=timesheet_row.contract_id
  where timesheet_row.timesheet_id=v_new;

  if exists(select 1 from public.weekly_exceptional_pay_target_families
            where root_timesheet_id=v_new and ownership_state='TARGET_MANAGED') then
    raise exception 'WP-27 F5: the probe family was recorded against the wrong physical root';
  end if;
  if not exists(select 1 from public.weekly_exceptional_pay_target_families
                where root_timesheet_id=any(
                  private.weekly_source_invoice_family_timesheet_ids_v1(v_new))
                  and ownership_state='TARGET_MANAGED') then
    raise exception 'WP-27 F5: a protected family on a rotated sibling is invisible to the family predicate';
  end if;
end
$wp27_rotation$;

select pg_catalog.jsonb_build_object(
  'ok',true,'verification','weekly_source_invoice_batch_integration_v1',
  'rotated lineage-bound root offered to the ordinary batch',false,
  'line owner guard fires on the rotated root',true,
  'protected family visible through the resolver family',true
);
rollback;
