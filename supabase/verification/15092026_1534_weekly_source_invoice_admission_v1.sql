-- Rollback-only proof for weekly_source_invoice_admission_v1.
-- This deliberately reuses the complete finalisation fixture twice: once for
-- FULL reversal/replacement presentation and once for the narrowly supported
-- non-NHSP NET presentation.  Every write is rolled back.

\set ON_ERROR_STOP on

\set weekly_source_verification_outer_transaction true
\set weekly_source_ordinary_verification_outer_transaction true
\set weekly_source_verification_correction_presentation 'FULL_REVERSAL_REPLACEMENT'
\set weekly_source_verification_expense_vat_enabled false
begin;
\ir 15092026_1534_weekly_source_ordinary_pay_projection_v1.sql

create function pg_temp.admit_all_manifests()
returns void language plpgsql as $function$
declare
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_result jsonb;
  v_invoice_text text;
begin
  for v_manifest in
    select * from public.weekly_source_client_manifests
    where invoice_state='READY'
    order by created_at_utc,id
  loop
    v_result:=public.weekly_source_invoice_admit_atomic_v1(
      pg_catalog.jsonb_build_object(
        'actor_user_id','a0000000-0000-4000-8000-000000000001',
        'client_manifest_id',v_manifest.id,
        'expected_manifest_hash',pg_catalog.encode(v_manifest.manifest_hash,'hex')
      )
    );
    if v_manifest.movement_count=0 and v_result->>'status'<>'NO_MOVEMENTS' then
      raise exception 'ASSERTION_FAILED: zero manifest admission result %',v_result;
    elsif v_manifest.movement_count>0 and v_result->>'status'<>'ADMITTED' then
      raise exception 'ASSERTION_FAILED: movement manifest admission result %',v_result;
    end if;
    for v_invoice_text in
      select value from pg_catalog.jsonb_array_elements_text(v_result->'invoice_ids')
    loop
      perform private.weekly_source_invoice_allocation_assert_v1(v_invoice_text::uuid);
    end loop;
  end loop;
end;
$function$;

create function pg_temp.invoice_for_cycle(p_cycle_id uuid)
returns uuid language sql stable as $function$
  select binding.invoice_id
  from public.weekly_source_client_manifests manifest
  join public.weekly_source_manifest_movements manifest_movement
    on manifest_movement.client_manifest_id=manifest.id
  join public.weekly_source_invoice_line_bindings binding
    on binding.billing_movement_id=manifest_movement.billing_movement_id
   and binding.state='CURRENT'
  where manifest.source_cycle_id=p_cycle_id
  order by binding.invoice_id
  limit 1;
$function$;

select pg_temp.admit_all_manifests();

-- The ordinary pay projection owns a distinct immutable pay facet.  Real
-- invoice admission follows it and owns a separate immutable invoice facet;
-- both point to the same frozen expense authority and ordinary Weekly root.
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1
   from public.weekly_source_expense_pay_materialisations pay_facet
   join public.weekly_source_ordinary_pay_projection_receipts receipt
     on receipt.published_timesheet_financial_id=
          pay_facet.candidate_timesheet_financial_id
    and receipt.root_timesheet_id=pay_facet.root_timesheet_id
   join public.weekly_source_expense_materialisations invoice_facet
     on invoice_facet.expense_authority_generation_id=
          pay_facet.expense_authority_generation_id
    and invoice_facet.billing_movement_id=pay_facet.billing_movement_id
   join public.weekly_source_billing_movements movement
     on movement.id=pay_facet.billing_movement_id
    and movement.invoice_timesheet_id=pay_facet.root_timesheet_id
   where receipt.idempotency_key='projection-expense-only-e1'
     and invoice_facet.state='MATERIALISED')
  and (select pg_catalog.count(*)=1
       from public.weekly_source_expense_pay_materialisations pay_facet
       join public.weekly_source_ordinary_pay_projection_receipts receipt
         on receipt.published_timesheet_financial_id=
              pay_facet.candidate_timesheet_financial_id
       where receipt.idempotency_key='projection-expense-only-e1')
  and (select pg_catalog.count(*)=1
       from public.weekly_source_expense_materialisations invoice_facet
       join public.weekly_source_expense_pay_materialisations pay_facet
         on pay_facet.expense_authority_generation_id=
              invoice_facet.expense_authority_generation_id
       join public.weekly_source_ordinary_pay_projection_receipts receipt
         on receipt.published_timesheet_financial_id=
              pay_facet.candidate_timesheet_financial_id
       where receipt.idempotency_key='projection-expense-only-e1'),
  'pay-first source expense must admit one separate invoice facet on the same authority and root'
);

-- The inverse lifecycle is intentionally not a second mutation route.  Once a
-- source movement is admitted to a real Draft invoice, a later ordinary
-- projection must not mutate the ordinary pay position of that root.
--
-- ADOPTED FROM WP-06 handoff N8 (Gate 2), 18 September 2026.  Gate 2 deleted
-- the old REFUSED_LOCKED / WEEKLY_SOURCE_EXISTING_CORRECTION_PATH_REQUIRED
-- behaviour: the ordinary pay projection owner no longer refuses and no longer
-- publishes.  Asserting "outcome is not REFUSED_LOCKED" would now be vacuously
-- true and would test nothing, so the assertion is rewritten against the real
-- outcome.  EXECUTED on the full NEW build banking_modal_v2_release5_20260918:
-- this fixture root is already authorised, so the owner returns the PROPOSED
-- outcome with a null timesheet_financials_id and nothing is published.  (WP-06
-- N8 suggested PREPARED_FOR_AUTHORISATION here; that is the never-authorised
-- branch and it is NOT what this root returns.  The prologue itself already
-- takes a PROPOSED receipt for this root under the idempotency key
-- "projection-a6-paid-refusal", so the call below is its exact replay.)
--
-- The safety property the block exists for is unchanged and is still asserted:
-- the later projection publishes nothing and materialises no pay facet.
do $invoice_first_projection_proposal$
declare v_result jsonb;
begin
  v_result:=public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
    pg_temp.ordinary_projection_request(
      'a6000000-0000-4000-8000-000000000001',
      'invoice-first-a6-refusal'
    )
  );
  if v_result->>'outcome'<>'PROPOSED'
     or coalesce((v_result->>'ok')::boolean,false) is not true
     or v_result->>'timesheet_financials_id' is not null
     or v_result->>'error_code' is not null then
    raise exception 'ASSERTION_FAILED: invoice-bound source root must propose and publish nothing %',
      v_result;
  end if;
end;
$invoice_first_projection_proposal$;
select pg_temp.assert_true(
  not exists(
    select 1
    from public.weekly_source_expense_pay_materialisations pay_facet
    join public.weekly_source_billing_movements movement
      on movement.id=pay_facet.billing_movement_id
    where movement.finalisation_cycle_id='a6000000-0000-4000-8000-000000000001'
  ),
  'invoice-first source root must not materialise a later mutable ordinary pay facet'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=0
   from public.weekly_source_ordinary_pay_projection_receipts receipt
   where receipt.idempotency_key='invoice-first-a6-refusal'
     and receipt.published_timesheet_financial_id is not null),
  'invoice-first source root must not publish a Candidate financial snapshot'
);

-- Automatic source admission is one Client and one finalised cycle/report per
-- invoice.  A zero-movement manifest creates no invoice at all.
select pg_temp.assert_true(
  not exists(
    select 1
    from public.invoices invoice
    join public.weekly_source_invoice_line_bindings binding
      on binding.invoice_id=invoice.id and binding.state='CURRENT'
    join public.weekly_source_invoice_line_bindings other_binding
      on other_binding.invoice_id=invoice.id and other_binding.state='CURRENT'
    join public.weekly_source_manifest_movements first_member
      on first_member.billing_movement_id=binding.billing_movement_id
    join public.weekly_source_manifest_movements other_member
      on other_member.billing_movement_id=other_binding.billing_movement_id
    join public.weekly_source_client_manifests first_manifest
      on first_manifest.id=first_member.client_manifest_id
    join public.weekly_source_client_manifests other_manifest
      on other_manifest.id=other_member.client_manifest_id
    where first_manifest.source_cycle_id<>other_manifest.source_cycle_id
       or first_manifest.client_id<>other_manifest.client_id
  ),
  'automatic admission must never consolidate different Clients or finalised cycles'
);
select pg_temp.assert_true(
  not exists(
    select 1
    from public.weekly_source_client_manifests manifest
    where manifest.movement_count=0
      and exists(
        select 1
        from public.weekly_source_invoice_line_bindings binding
        join public.weekly_source_manifest_movements member
          on member.billing_movement_id=binding.billing_movement_id
        where member.client_manifest_id=manifest.id and binding.state='CURRENT'
      )
  ),
  'a zero-movement source root/manifest must never become an invoice line'
);

-- FULL policy retains every generated reversal and replacement as a separate
-- visible line.  NHSP physical rows always remain separate regardless of any
-- configurable roster policy.
select pg_temp.assert_true(
  (select pg_catalog.count(distinct binding.invoice_line_id)
   from public.weekly_source_client_manifests manifest
   join public.weekly_source_manifest_movements member
     on member.client_manifest_id=manifest.id
   join public.weekly_source_billing_movements movement
     on movement.id=member.billing_movement_id
   join public.weekly_source_invoice_line_bindings binding
     on binding.billing_movement_id=movement.id and binding.state='CURRENT'
   where manifest.source_cycle_id='a3000000-0000-4000-8000-000000000001'
     and movement.movement_role in ('REVERSAL','REPLACEMENT'))=2,
  'FULL roster correction must expose separate reversal and replacement lines'
);
select pg_temp.assert_true(
  (select pg_catalog.count(distinct binding.invoice_line_id)
   from public.weekly_source_client_manifests manifest
   join public.weekly_source_manifest_movements member
     on member.client_manifest_id=manifest.id
   join public.weekly_source_invoice_line_bindings binding
     on binding.billing_movement_id=member.billing_movement_id and binding.state='CURRENT'
   where manifest.source_cycle_id='b1000000-0000-4000-8000-000000000001')=2
  and (select invoice.header_snapshot_json#>>'{meta,backing_report_numbers,0}'='BR-001'
       from public.invoices invoice
       where invoice.id=pg_temp.invoice_for_cycle(
         'b1000000-0000-4000-8000-000000000001'
       )),
  'NHSP physical signed rows must remain separate and retain backing report number'
);

-- Source-fixed expenses are admitted from the immutable CURRENT authority
-- generation exactly once.  Changed, omitted/explicit-zero and VAT-off paths
-- are all represented by their source movements without candidate expense
-- evidence or Timesheet financial writes.
select pg_temp.assert_true(
  not exists(
    select 1
    from public.weekly_expense_authority_generations generation
    where exists(
      select 1 from public.weekly_source_manifest_movements member
      join public.weekly_source_billing_movements movement
        on movement.id=member.billing_movement_id
      where movement.expense_authority_generation_id=generation.id
    )
      and (select pg_catalog.count(*)
           from public.weekly_source_expense_materialisations materialisation
           where materialisation.expense_authority_generation_id=generation.id)<>1
  ),
  'each admitted source-fixed expense authority generation must materialise exactly once'
);
select pg_temp.assert_true(
  not exists(
    select 1 from public.weekly_source_invoice_presentation_lines presentation
    where presentation.line_kind='SOURCE_FIXED_EXPENSE'
      and (presentation.total_pay_ex_vat<>presentation.total_charge_ex_vat
        or presentation.vat_rate_pct<>0 or presentation.vat_amount<>0
        or presentation.amount_authority<>'VALIDATED_SOURCE_PENCE'
        or presentation.source_validation_charge_pence is distinct from
           presentation.invoice_presentation_charge_pence)
  )
  and exists(
    select 1 from public.weekly_source_invoice_presentation_lines presentation
    where presentation.line_kind='SOURCE_FIXED_EXPENSE'
      and presentation.total_charge_ex_vat=-1.25
  ),
  'source-fixed expenses must preserve source-pence authority, equal pay/charge, VAT off and zero-source reversal'
);

-- Admission retries are exact and produce no duplicate invoice, line,
-- binding, placement or expense materialisation.
do $admission_replay$
declare
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_before integer;
  v_result jsonb;
begin
  select * into strict v_manifest from public.weekly_source_client_manifests
  where source_cycle_id='a1000000-0000-4000-8000-000000000001';
  select pg_catalog.count(*) into v_before from public.invoice_lines;
  v_result:=public.weekly_source_invoice_admit_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','a0000000-0000-4000-8000-000000000001',
      'client_manifest_id',v_manifest.id,
      'expected_manifest_hash',pg_catalog.encode(v_manifest.manifest_hash,'hex')
    )
  );
  if not (v_result->>'idempotent')::boolean
     or (select pg_catalog.count(*) from public.invoice_lines)<>v_before then
    raise exception 'ASSERTION_FAILED: admission retry duplicated materialisation';
  end if;
end;
$admission_replay$;

-- Gate 7 item G7-6 (gap row XSG-017; 25 section 4 Removed, bullet 1).
-- Issue proof goes through the REAL owners only.  A direct database status
-- change to ISSUED is not proof that the real issue route accepts a source
-- invoice, so this block no longer writes public.invoices.status: it drives
-- the real asynchronous validator private._invoice_issue_validate_batch, the
-- real batch issue classifier private._invoice_batch_issue_classification_v2,
-- and the real direct owner public.invoice_issue_one, then the real unissue
-- owner public.invoice_unissue_one.
do $issue_unissue$
declare
  v_invoice_id uuid:=pg_temp.invoice_for_cycle(
    'a6000000-0000-4000-8000-000000000001'
  );
  v_validation record;
  v_verdict jsonb;
  v_issue record;
  v_classified integer;
  v_blockers text[];
begin
  -- The one source-aware validator admits this sealed self-bill (ISS-001).
  v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_invoice_id);
  if coalesce((v_verdict->>'is_source_invoice')::boolean,false) is not true
     or coalesce((v_verdict->>'ok')::boolean,false) is not true then
    raise exception 'ASSERTION_FAILED: source issue validator refused a sealed self-bill %',
      v_verdict;
  end if;

  -- The real asynchronous entry point.  ISS-002/ISS-003/ISS-005/ISS-006: the
  -- ordinary Candidate evidence, reference, receipt/mileage, HealthRoster and
  -- correction-unit blockers are the ones this route used to raise for a
  -- sealed source self-bill; none may survive here.
  select * into v_validation
  from private._invoice_issue_validate_batch(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'request_key','weekly-source-issue-proof',
      'invoice_id',v_invoice_id,
      'expected_revision',(select document_revision from public.invoices where id=v_invoice_id),
      'allow_early',true,'deliver',false
    )),
    (pg_catalog.statement_timestamp() at time zone 'Europe/London')::date
  );
  v_blockers:=array(select pg_catalog.jsonb_array_elements_text(
    coalesce(v_validation.hard_blocker_codes,'[]'::jsonb)));
  if not coalesce(v_validation.can_issue_only,false) then
    raise exception 'ASSERTION_FAILED: real async validator blocked a sealed source self-bill %',
      v_validation.hard_blocker_codes;
  end if;
  if exists(
    select 1 from pg_catalog.unnest(v_blockers) code
    where private.weekly_source_invoice_issue_skippable_code_v1(code)
  ) then
    raise exception 'ASSERTION_FAILED: skippable ordinary evidence survived %',v_blockers;
  end if;

  -- The real batch issue classifier offers the same invoice as issuable.
  select pg_catalog.count(*)::integer into v_classified
  from private._invoice_batch_issue_classification_v2(
    true,array[v_invoice_id]::uuid[],pg_catalog.statement_timestamp()
  ) classified
  where (classified.candidate_json->>'can_issue_only')::boolean;
  if v_classified<>1 then
    raise exception 'ASSERTION_FAILED: batch issue classifier did not offer the source invoice';
  end if;

  -- The real direct owner performs the transition (ISS-001, ISS-013).
  select * into v_issue
  from public.invoice_issue_one(
    v_invoice_id,'a0000000-0000-4000-8000-000000000001'
  );
  if v_issue.status<>'ISSUED' then
    raise exception 'ASSERTION_FAILED: real direct issue owner refused the source invoice: % %',
      v_issue.status,v_issue.reasons;
  end if;
  if exists(
    select 1 from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_billing_movements movement
      on movement.id=binding.billing_movement_id
    where binding.invoice_id=v_invoice_id and binding.state='CURRENT'
      and movement.placement_state<>'ISSUED'
  ) then
    raise exception 'ASSERTION_FAILED: issue did not mark every movement issued';
  end if;

  -- The real unissue owner restores the movable placement state (ISS-014).
  perform public.invoice_unissue_one(
    v_invoice_id,'a0000000-0000-4000-8000-000000000001',true
  );
  if (select status::text from public.invoices where id=v_invoice_id)<>'DRAFT' then
    raise exception 'ASSERTION_FAILED: real unissue owner did not return the invoice to DRAFT';
  end if;
  if exists(
    select 1 from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_billing_movements movement
      on movement.id=binding.billing_movement_id
    where binding.invoice_id=v_invoice_id and binding.state='CURRENT'
      and movement.placement_state<>'PLACED'
  ) then
    raise exception 'ASSERTION_FAILED: unissue did not restore movable placement state';
  end if;

  -- Reissue through the same real owner (ISS-014), then leave it DRAFT so the
  -- rest of this proof sees the fixture it expects.
  select * into v_issue
  from public.invoice_issue_one(
    v_invoice_id,'a0000000-0000-4000-8000-000000000001'
  );
  if v_issue.status<>'ISSUED' then
    raise exception 'ASSERTION_FAILED: reissue through the real owner failed % %',
      v_issue.status,v_issue.reasons;
  end if;
  perform public.invoice_unissue_one(
    v_invoice_id,'a0000000-0000-4000-8000-000000000001',true
  );
end;
$issue_unissue$;

-- Gate 7 items G7-1 and G7-2 (24 section 12; 25 section 8).
-- ONE immutable source presentation line moves between idle DRAFT invoices for
-- the same Client, including a different source group and finalised week. A
-- source-fixed expense follows its
-- declared companion and nothing else travels with it.  Selecting one line
-- moves exactly one line when that line has no companion.  Moving by whole
-- work-event id is not offered at all: the request contract has no such field.
do $move$
declare
  v_source_invoice uuid:=pg_temp.invoice_for_cycle(
    'a1000000-0000-4000-8000-000000000001'
  );
  v_destination_invoice uuid;
  v_cross_client_invoice uuid:=pg_temp.invoice_for_cycle(
    'b1000000-0000-4000-8000-000000000001'
  );
  v_source_revision bigint;
  v_destination_revision bigint;
  v_cross_client_revision bigint;
  v_shift_presentation public.weekly_source_invoice_presentation_lines%rowtype;
  v_expense_presentation public.weekly_source_invoice_presentation_lines%rowtype;
  v_shift_invoice_line uuid;
  v_context jsonb;
  v_movable jsonb;
  v_request jsonb;
  v_result jsonb;
begin
  -- PHD-003: the only business compatibility boundary is the Client. Build a
  -- second active source group for that same Client and prove that its idle,
  -- unissued Draft is both offered and accepted as a destination. The older
  -- same-source-group fixture would not prove this rule.
  insert into public.weekly_source_groups(
    id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
  ) values (
    'c9000000-0000-4000-8000-000000000005','TEST',
    'a0000000-0000-4000-8000-000000000006','FINALISER_ROSTER_SECOND',
    'Finaliser Roster Second','ROSTER',3,'15:00'
  );
  update public.weekly_source_group_clients
  set valid_to='2026-09-27'
  where id='a0000000-0000-4000-8000-000000000007';
  insert into public.weekly_source_group_clients(
    id,source_group_id,client_id,valid_from,valid_to,created_by_user_id
  ) values (
    'c9000000-0000-4000-8000-000000000007',
    'c9000000-0000-4000-8000-000000000005',
    'a0000000-0000-4000-8000-000000000002','2026-09-28','2026-10-04',
    'a0000000-0000-4000-8000-000000000001'
  );
  insert into public.weekly_source_group_clients(
    id,source_group_id,client_id,valid_from,created_by_user_id
  ) values (
    'c9000000-0000-4000-8000-000000000008',
    'a0000000-0000-4000-8000-000000000005',
    'a0000000-0000-4000-8000-000000000002','2026-10-05',
    'a0000000-0000-4000-8000-000000000001'
  );
  insert into public.weekly_source_client_policies(
    id,source_group_id,client_id,effective_from,authority_mode,document_mode,
    self_bill_enabled,self_bill_correction_presentation,
    source_fixed_expenses_enabled,source_expense_vat_enabled,
    weekly_rate_classification_method,manager_queries_enabled,manager_query_recipient,
    created_by_user_id
  ) values (
    'c9000000-0000-4000-8000-000000000012',
    'c9000000-0000-4000-8000-000000000005',
    'a0000000-0000-4000-8000-000000000002','2026-09-28',
    'SOURCE_AUTHORITY','CHECK_ONLY',true,'FULL_REVERSAL_REPLACEMENT',
    true,false,'SPLIT_RATE_WINDOWS',true,
    'manager@example.test','a0000000-0000-4000-8000-000000000001'
  );
  perform pg_temp.roster_cycle(
    'c9100000-0000-4000-8000-000000000001','c9100000-0000-4000-8000-000000000002',
    'c9100000-0000-4000-8000-000000000003','c9100000-0000-4000-8000-000000000004',
    '2026-10-04','35555555-5555-4555-8555-555555555555','LINE-SECOND-GROUP',
    'NOT_APPLICABLE','2026-09-28 09:00','2026-09-28 17:00',30,450,7500,15000,100,
    true,'2026-09-28','2026-09-28','2026-09-29',
    'c9000000-0000-4000-8000-000000000005'
  );
  perform pg_temp.finalise_cycle(
    'c9100000-0000-4000-8000-000000000001','c9100000-0000-4000-8000-000000000002',
    'c9100000-0000-4000-8000-000000000003'
  );
  perform pg_temp.admit_all_manifests();
  v_destination_invoice:=pg_temp.invoice_for_cycle(
    'c9100000-0000-4000-8000-000000000001'
  );
  if v_destination_invoice is null then
    raise exception 'ASSERTION_FAILED: second same-Client source group produced no destination invoice';
  end if;

  select presentation.* into strict v_shift_presentation
  from public.weekly_source_invoice_line_bindings binding
  join public.weekly_source_invoice_presentation_lines presentation
    on presentation.id=binding.presentation_line_id
  join public.weekly_source_billing_movements movement
    on movement.id=binding.billing_movement_id
  where binding.invoice_id=v_source_invoice and binding.state='CURRENT'
    and movement.movement_role='POSITIVE';
  select presentation.* into strict v_expense_presentation
  from public.weekly_source_invoice_presentation_lines presentation
  where presentation.companion_presentation_line_id=v_shift_presentation.id;
  select binding.invoice_line_id into strict v_shift_invoice_line
  from public.weekly_source_invoice_line_bindings binding
  where binding.presentation_line_id=v_shift_presentation.id
    and binding.state='CURRENT';

  select document_revision into strict v_source_revision
    from public.invoices where id=v_source_invoice;
  select document_revision into strict v_destination_revision
    from public.invoices where id=v_destination_invoice;
  select document_revision into strict v_cross_client_revision
    from public.invoices where id=v_cross_client_invoice;

  -- G7-2: the Office edit context offers presentation lines, never work events.
  v_context:=public.weekly_source_invoice_edit_context_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','a0000000-0000-4000-8000-000000000001',
      'invoice_id',v_source_invoice
    )
  );
  if v_context ? 'shift_groups' or not (v_context ? 'movable_lines') then
    raise exception 'ASSERTION_FAILED: edit context still offers work-event movement units';
  end if;
  select value into strict v_movable
  from pg_catalog.jsonb_array_elements(v_context->'movable_lines') value
  where (value->>'presentation_line_id')::uuid=v_shift_presentation.id;
  if v_movable->>'presentation_hash'
       is distinct from pg_catalog.encode(v_shift_presentation.presentation_hash,'hex')
     or (v_movable->>'independently_movable')::boolean is not true
     or not (v_movable->'companion_presentation_line_ids'
             @> pg_catalog.to_jsonb(v_expense_presentation.id))
     or (v_movable->>'invoice_line_id')::uuid is distinct from v_shift_invoice_line then
    raise exception 'ASSERTION_FAILED: movable line projection is incomplete %',v_movable;
  end if;
  if not exists(
    select 1 from pg_catalog.jsonb_array_elements(v_context->'movable_lines') value
    where (value->>'presentation_line_id')::uuid=v_expense_presentation.id
      and (value->>'independently_movable')::boolean is false
      and (value->>'follows_companion_presentation_line_id')::uuid=v_shift_presentation.id
  ) then
    raise exception 'ASSERTION_FAILED: companion expense is not marked as following its shift';
  end if;
  if not exists(
    select 1
    from pg_catalog.jsonb_array_elements(v_context->'compatible_destinations') value
    where (value->>'invoice_id')::uuid=v_destination_invoice
  ) then
    raise exception 'ASSERTION_FAILED: same-Client cross-source-group destination was not offered';
  end if;

  v_request:=pg_catalog.jsonb_build_object(
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'source_invoice_id',v_source_invoice,
    'destination_invoice_id',v_destination_invoice,
    'presentation_line_id',v_shift_presentation.id,
    'expected_presentation_hash',
      pg_catalog.encode(v_shift_presentation.presentation_hash,'hex'),
    'expected_source_document_revision',v_source_revision,
    'expected_destination_document_revision',v_destination_revision,
    'reason','Move one source presentation line to the correct unissued self-bill invoice.'
  );

  -- Moving by whole work-event id is prohibited; the field does not exist.
  begin
    perform public.weekly_source_invoice_move_atomic_v1(
      v_request||pg_catalog.jsonb_build_object(
        'source_shift_group_id',v_shift_presentation.work_event_id)
    );
    raise exception 'ASSERTION_FAILED: work-event move request was accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_INVOICE_MOVE_UNKNOWN_FIELD' then raise; end if;
  end;

  -- The expected presentation hash is load-bearing.
  begin
    perform public.weekly_source_invoice_move_atomic_v1(
      v_request||pg_catalog.jsonb_build_object(
        'expected_presentation_hash',
        pg_catalog.encode(v_expense_presentation.presentation_hash,'hex'))
    );
    raise exception 'ASSERTION_FAILED: wrong expected presentation hash was accepted';
  exception when sqlstate '40001' then
    if sqlerrm<>'WEEKLY_SOURCE_INVOICE_MOVE_PRESENTATION_HASH_MISMATCH' then raise; end if;
  end;

  -- A source-fixed expense follows its companion; it is never selected alone.
  begin
    perform public.weekly_source_invoice_move_atomic_v1(
      v_request||pg_catalog.jsonb_build_object(
        'presentation_line_id',v_expense_presentation.id,
        'expected_presentation_hash',
          pg_catalog.encode(v_expense_presentation.presentation_hash,'hex'))
    );
    raise exception 'ASSERTION_FAILED: companion expense was moved on its own';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_INVOICE_MOVE_COMPANION_REQUIRED' then raise; end if;
  end;

  begin
    perform public.weekly_source_invoice_move_atomic_v1(
      v_request||pg_catalog.jsonb_build_object(
        'destination_invoice_id',v_cross_client_invoice,
        'expected_destination_document_revision',v_cross_client_revision
      )
    );
    raise exception 'ASSERTION_FAILED: cross-Client source move was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_INVOICE_MOVE_SCOPE_INVALID' then raise; end if;
  end;

  v_result:=public.weekly_source_invoice_move_atomic_v1(v_request);
  -- Exactly the selected presentation and its declared companion expense:
  -- two visible lines, two movements, nothing else.
  if v_result->>'status'<>'MOVED' or (v_result->>'idempotent')::boolean
     or (v_result->>'movement_count')::integer<>2
     or (v_result->>'line_count')::integer<>2
     or pg_catalog.jsonb_array_length(v_result->'presentation_line_ids')<>2
     or not (v_result->'presentation_line_ids'
             @> pg_catalog.to_jsonb(v_expense_presentation.id)) then
    raise exception 'ASSERTION_FAILED: same-Client cross-source-group move did not carry its companion %',
      v_result;
  end if;
  perform private.weekly_source_invoice_allocation_assert_v1(v_source_invoice);
  perform private.weekly_source_invoice_allocation_assert_v1(v_destination_invoice);
  if exists(
    select 1
    from public.weekly_source_invoice_line_bindings binding
    where binding.invoice_id=v_source_invoice and binding.state='CURRENT'
      and binding.presentation_line_id
          in (v_shift_presentation.id,v_expense_presentation.id)
  ) or (select pg_catalog.count(*)
        from public.weekly_source_invoice_placements placement
        join public.weekly_source_invoice_placements prior
          on prior.id=placement.prior_placement_id
        join public.weekly_source_invoice_line_bindings binding
          on binding.billing_movement_id=placement.billing_movement_id
         and binding.state='CURRENT'
        where placement.is_current and placement.invoice_id=v_destination_invoice
          and prior.invoice_id=v_source_invoice
          and binding.presentation_line_id
              in (v_shift_presentation.id,v_expense_presentation.id))<>2 then
    raise exception 'ASSERTION_FAILED: presentation line was moved partially';
  end if;
  v_result:=public.weekly_source_invoice_move_atomic_v1(v_request);
  if not (v_result->>'idempotent')::boolean then
    raise exception 'ASSERTION_FAILED: exact move retry was not idempotent';
  end if;

  if coalesce((private.weekly_source_invoice_allocation_assert_v1(
       v_source_invoice
     )->>'empty')::boolean,false) is not true then
    raise exception 'ASSERTION_FAILED: emptied source invoice lost source identity';
  end if;
  -- An empty source Draft stays unissuable through the REAL owners, not by a
  -- direct status write (G7-4 backstop plus G7-3 validator; ISS-011).
  if coalesce((private.weekly_source_invoice_issue_validate_v1(
       v_source_invoice)->>'ok')::boolean,true) is not false then
    raise exception 'ASSERTION_FAILED: the validator admitted an empty source invoice';
  end if;
  if (select can_issue_only from private._invoice_issue_validate_batch(
        pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'request_key','weekly-source-empty-issue-proof',
          'invoice_id',v_source_invoice,
          'expected_revision',(select document_revision from public.invoices
                               where id=v_source_invoice),
          'allow_early',true,'deliver',false)),
        (pg_catalog.statement_timestamp() at time zone 'Europe/London')::date)) then
    raise exception 'ASSERTION_FAILED: the real async validator admitted an empty source invoice';
  end if;
  -- The real direct owner refuses an empty invoice outright, before any
  -- evidence branch is reached.  Either refusal shape is acceptable; issuing
  -- it is not.
  begin
    if (select status from public.invoice_issue_one(
          v_source_invoice,'a0000000-0000-4000-8000-000000000001'))='ISSUED' then
      raise exception 'ASSERTION_FAILED: the real direct owner issued an empty source invoice';
    end if;
  exception when others then
    if sqlerrm like 'ASSERTION_FAILED%' then raise; end if;
  end;
  update public.invoices set status='DRAFT',on_hold_reason=null
  where id=v_source_invoice and status='ON_HOLD';

  insert into public.invoice_lines(
    invoice_id,timesheet_id,description,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
    total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,
    meta_json,source_key
  ) values (
    v_source_invoice,null,'Forbidden non-source line in emptied source invoice',0,0,0,0,0,
    0,0,0,0,0,0,'{}'::jsonb,'FORBIDDEN-EMPTY-SOURCE-INVOICE'
  );
  begin
    perform private.weekly_source_invoice_allocation_assert_v1(v_source_invoice);
    raise exception 'ASSERTION_FAILED: emptied source invoice accepted a non-source line';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_INVOICE_EXTRA_LINE' then raise; end if;
  end;
  delete from public.invoice_lines
  where invoice_id=v_source_invoice and source_key='FORBIDDEN-EMPTY-SOURCE-INVOICE';
end;
$move$;

-- 24 section 12: "An NHSP physical row moves independently" and the Office
-- selecting one line moves exactly one line.  The NHSP self-bill fixture holds
-- two independent physical rows on one invoice; moving one must leave the other
-- exactly where it was.
do $single_presentation_move$
declare
  v_source_invoice uuid:=pg_temp.invoice_for_cycle(
    'b1000000-0000-4000-8000-000000000001'
  );
  v_destination_invoice uuid:=pg_temp.invoice_for_cycle(
    'c1000000-0000-4000-8000-000000000001'
  );
  v_selected public.weekly_source_invoice_presentation_lines%rowtype;
  v_retained uuid;
  v_retained_line uuid;
  v_before integer;
  v_result jsonb;
begin
  select pg_catalog.count(*)::integer into v_before
  from public.weekly_source_invoice_line_bindings binding
  where binding.invoice_id=v_source_invoice and binding.state='CURRENT';
  if v_before<>2 then
    raise exception 'ASSERTION_FAILED: NHSP fixture no longer holds two physical rows';
  end if;
  select presentation.* into strict v_selected
  from public.weekly_source_invoice_line_bindings binding
  join public.weekly_source_invoice_presentation_lines presentation
    on presentation.id=binding.presentation_line_id
  where binding.invoice_id=v_source_invoice and binding.state='CURRENT'
    and presentation.line_kind='SOURCE_ORDINARY';
  select binding.presentation_line_id,binding.invoice_line_id
    into strict v_retained,v_retained_line
  from public.weekly_source_invoice_line_bindings binding
  where binding.invoice_id=v_source_invoice and binding.state='CURRENT'
    and binding.presentation_line_id<>v_selected.id;

  v_result:=public.weekly_source_invoice_move_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','a0000000-0000-4000-8000-000000000001',
      'source_invoice_id',v_source_invoice,
      'destination_invoice_id',v_destination_invoice,
      'presentation_line_id',v_selected.id,
      'expected_presentation_hash',
        pg_catalog.encode(v_selected.presentation_hash,'hex'),
      'expected_source_document_revision',
        (select document_revision from public.invoices where id=v_source_invoice),
      'expected_destination_document_revision',
        (select document_revision from public.invoices where id=v_destination_invoice),
      'reason','Office selected one NHSP physical presentation line.'
    )
  );
  if v_result->>'status'<>'MOVED'
     or (v_result->>'line_count')::integer<>1
     or (v_result->>'movement_count')::integer<>1
     or pg_catalog.jsonb_array_length(v_result->'presentation_line_ids')<>1 then
    raise exception 'ASSERTION_FAILED: selecting one line moved more than one line %',v_result;
  end if;
  if not exists(
    select 1 from public.weekly_source_invoice_line_bindings binding
    where binding.invoice_id=v_source_invoice and binding.state='CURRENT'
      and binding.presentation_line_id=v_retained
      and binding.invoice_line_id=v_retained_line
  ) or (select pg_catalog.count(*) from public.weekly_source_invoice_line_bindings binding
        where binding.invoice_id=v_source_invoice and binding.state='CURRENT')<>1 then
    raise exception 'ASSERTION_FAILED: the unselected NHSP physical row did not stay put';
  end if;
  if (select invoice_id from public.invoice_lines where id=v_retained_line)
       is distinct from v_source_invoice then
    raise exception 'ASSERTION_FAILED: the unselected NHSP visible line was re-pointed';
  end if;
  perform private.weekly_source_invoice_allocation_assert_v1(v_source_invoice);
  perform private.weekly_source_invoice_allocation_assert_v1(v_destination_invoice);

  -- INV-030 / 24 section 12: both invoices remain admissible to the real issue
  -- route after the move, and the moved line's economics are unchanged.
  if coalesce((private.weekly_source_invoice_issue_validate_v1(
       v_source_invoice)->>'ok')::boolean,false) is not true
     or coalesce((private.weekly_source_invoice_issue_validate_v1(
       v_destination_invoice)->>'ok')::boolean,false) is not true then
    raise exception 'ASSERTION_FAILED: a moved-from or moved-to invoice failed source issue validation';
  end if;
end;
$single_presentation_move$;

-- A protected TARGET_MANAGED root with no source movement is valid for C1
-- preparation but cannot be inserted into any invoice by a legacy path.
do $protected_zero_root$
declare
  v_prepare jsonb;
  v_root uuid;
  v_invoice uuid:=pg_temp.invoice_for_cycle(
    'a3000000-0000-4000-8000-000000000001'
  );
begin
  -- The protected-family producer intentionally requires the existing
  -- payment-authoriser capability.  Elevate only this transaction-local
  -- fixture actor; invoice admission itself remains an Office-admin action.
  update public.tms_users
  set payment_authoriser=true
  where id='a0000000-0000-4000-8000-000000000001';

  v_prepare:=public.weekly_exceptional_pay_prepare_family_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','a0000000-0000-4000-8000-000000000001',
      'source_cycle_id','a5000000-0000-4000-8000-000000000001',
      'candidate_id','a0000000-0000-4000-8000-000000000003',
      'client_id','a0000000-0000-4000-8000-000000000002',
      'contract_id','a0000000-0000-4000-8000-000000000004',
      'week_ending_date','2026-10-11','work_date','2026-10-06',
      'start_at_local','2026-10-06 09:00','end_at_local','2026-10-06 17:00',
      'break_minutes',30,'reason','Protect a claimed shift while source is absent.',
      'idempotency_key','invoice-proof-protected-zero-0001'
    )
  );
  v_root:=(v_prepare->>'root_timesheet_id')::uuid;
  if not private.weekly_source_invoice_movement_only_integrity_v1(v_root)
     or exists(select 1 from public.weekly_source_billing_movements
               where invoice_timesheet_id=v_root)
     or exists(select 1 from public.invoice_lines where timesheet_id=v_root) then
    raise exception 'ASSERTION_FAILED: protected zero-source root integrity failed';
  end if;
  begin
    insert into public.invoice_lines(
      invoice_id,timesheet_id,description,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,
      meta_json,source_key
    ) values (
      v_invoice,v_root,'Forbidden protected source-absent line',0,0,0,0,0,
      0,0,0,0,0,0,'{}'::jsonb,'FORBIDDEN-PROTECTED-ZERO'
    );
    raise exception 'ASSERTION_FAILED: legacy invoice path admitted protected zero root';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_INVOICE_MOVEMENT_OWNER_REQUIRED' then raise; end if;
  end;
end;
$protected_zero_root$;

select pg_temp.assert_true(
  pg_catalog.strpos(pg_catalog.pg_get_functiondef(
    'public.weekly_source_invoice_admit_atomic_v1(jsonb)'::pg_catalog.regprocedure
  ),'weekly_source_manifest_movements')>0
  and pg_catalog.strpos(pg_catalog.pg_get_functiondef(
    'public.weekly_source_invoice_admit_atomic_v1(jsonb)'::pg_catalog.regprocedure
  ),'weekly_exceptional_pay')=0
  and pg_catalog.strpos(pg_catalog.pg_get_functiondef(
    'public.weekly_source_invoice_admit_atomic_v1(jsonb)'::pg_catalog.regprocedure
  ),'timesheets_financials')=0,
  'invoice admission must remain movement-only and independent of protected pay/C1'
);

do $weekly_source_invoice_report_owner$
declare
  v_invoice uuid;
  v_report jsonb;
  v_row jsonb;
begin
  select binding.invoice_id into v_invoice
  from public.weekly_source_invoice_line_bindings binding
  where binding.state='CURRENT'
  order by binding.invoice_id
  limit 1;
  perform pg_catalog.set_config('request.jwt.claim.role','service_role',true);
  v_report:=public.weekly_source_invoice_report_rows_v1(
    pg_catalog.jsonb_build_object('invoice_ids',pg_catalog.jsonb_build_array(v_invoice)));
  v_row:=v_report->'rows'->0;
  perform pg_temp.assert_true(
    v_report->>'ok'='true'
      and (v_report->>'invoice_count')::integer=1
      and (v_row->>'invoice_id')::uuid=v_invoice
      and (v_row->>'movement_count')::integer=(
        select pg_catalog.count(*)::integer
        from public.weekly_source_invoice_line_bindings
        where invoice_id=v_invoice and state='CURRENT')
      and (v_row->>'source_movement_ex_vat')::numeric=(
        select (pg_catalog.sum(m.invoice_presentation_charge_pence)::numeric/100.0)::numeric(12,2)
        from public.weekly_source_invoice_line_bindings b
        join public.weekly_source_billing_movements m on m.id=b.billing_movement_id
        where b.invoice_id=v_invoice and b.state='CURRENT'),
    'the installed invoice report owner did not return the exact immutable source movements: '
      ||coalesce(v_report::text,'<null>'));
end;
$weekly_source_invoice_report_owner$;

select pg_catalog.jsonb_build_object(
  'ok',true,'verification','weekly_source_invoice_admission_full_v1',
  'invoice_count',(select pg_catalog.count(*) from public.invoices),
  'line_count',(select pg_catalog.count(*) from public.invoice_lines),
  'movement_count',(select pg_catalog.count(*) from public.weekly_source_billing_movements),
  'protected_zero_root_excluded',true
);
rollback;

-- Repeat the complete fixture under NET presentation and VAT-on source-fixed
-- expenses. Only an exact same-correction-unit non-NHSP pair may share a line.
\set weekly_source_verification_correction_presentation 'NET_DIFFERENCE_PRESENTATION'
\set weekly_source_verification_expense_vat_enabled true
begin;
-- The production FINAL_ISSUE renderer correctly refuses a self-bill without
-- its legally required wording.  Establish that ordinary prerequisite before
-- building the NET fixture so this proof exercises an actually issuable
-- self-bill rather than weakening the renderer or relying on baseline data.
update public.settings_defaults
set invoice_document_presentation_json=pg_catalog.jsonb_set(
  coalesce(invoice_document_presentation_json,'{}'::jsonb),
  '{self_bill_legal_wording}',
  pg_catalog.to_jsonb('Self-billed invoice'::text),
  true
)
where id=1;
\ir 15092026_1534_weekly_source_ordinary_pay_projection_v1.sql

create function pg_temp.admit_all_net_manifests()
returns void language plpgsql as $function$
declare
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_result jsonb;
begin
  for v_manifest in
    select * from public.weekly_source_client_manifests
    where invoice_state='READY' order by created_at_utc,id
  loop
    v_result:=public.weekly_source_invoice_admit_atomic_v1(
      pg_catalog.jsonb_build_object(
        'actor_user_id','a0000000-0000-4000-8000-000000000001',
        'client_manifest_id',v_manifest.id,
        'expected_manifest_hash',pg_catalog.encode(v_manifest.manifest_hash,'hex')
      )
    );
  end loop;
end;
$function$;
select pg_temp.admit_all_net_manifests();

create function pg_temp.assert_illegal_rebind_rejected(
  p_source_movement_id uuid,
  p_target_invoice_line_id uuid,
  p_label text
) returns void language plpgsql as $function$
declare
  v_binding public.weekly_source_invoice_line_bindings%rowtype;
  v_target public.invoice_lines%rowtype;
begin
  select * into strict v_binding
  from public.weekly_source_invoice_line_bindings
  where billing_movement_id=p_source_movement_id and state='CURRENT';
  select * into strict v_target from public.invoice_lines
  where id=p_target_invoice_line_id;

  begin
    update public.weekly_source_invoice_line_bindings
    set state='SUPERSEDED',superseded_at_utc=pg_catalog.statement_timestamp()
    where id=v_binding.id and state='CURRENT';
    insert into public.weekly_source_invoice_line_bindings(
      billing_movement_id,presentation_line_id,invoice_line_id,invoice_id,
      original_final_revision_id,original_cycle_id,client_id,correction_root_id,
      correction_role,manifest_hash,materialised_line_hash,binding_version,
      prior_binding_id,state
    ) values (
      v_binding.billing_movement_id,v_binding.presentation_line_id,
      v_target.id,v_target.invoice_id,v_binding.original_final_revision_id,
      v_binding.original_cycle_id,v_binding.client_id,v_binding.correction_root_id,
      v_binding.correction_role,v_binding.manifest_hash,v_binding.materialised_line_hash,
      v_binding.binding_version+1,v_binding.id,'CURRENT'
    );
    perform private.weekly_source_invoice_allocation_assert_v1(v_target.invoice_id);
    raise exception 'ASSERTION_FAILED: illegal % binding was accepted',p_label;
  exception when sqlstate '55000' then
    if sqlerrm not in (
      'WEEKLY_SOURCE_INVOICE_ALLOCATION_MISMATCH',
      'WEEKLY_SOURCE_INVOICE_NET_CARDINALITY_INVALID'
    ) then
      raise;
    end if;
  end;
end;
$function$;

do $illegal_net_binding_proofs$
declare
  v_target_line uuid;
  v_same_cycle_other_unit uuid;
  v_cross_cycle uuid;
  v_cross_client uuid;
  v_binding public.weekly_source_invoice_line_bindings%rowtype;
begin
  select binding.invoice_line_id into strict v_target_line
  from public.weekly_source_client_manifests manifest
  join public.weekly_source_manifest_movements member
    on member.client_manifest_id=manifest.id
  join public.weekly_source_billing_movements movement
    on movement.id=member.billing_movement_id
  join public.weekly_source_invoice_line_bindings binding
    on binding.billing_movement_id=movement.id and binding.state='CURRENT'
  where manifest.source_cycle_id='a3000000-0000-4000-8000-000000000001'
    and movement.movement_role='REPLACEMENT';

  select movement.id into strict v_same_cycle_other_unit
  from public.weekly_source_client_manifests manifest
  join public.weekly_source_manifest_movements member
    on member.client_manifest_id=manifest.id
  join public.weekly_source_billing_movements movement
    on movement.id=member.billing_movement_id
  where manifest.source_cycle_id='a3000000-0000-4000-8000-000000000001'
    and movement.movement_role='EXPENSE_REPLACEMENT';
  select movement.id into strict v_cross_cycle
  from public.weekly_source_client_manifests manifest
  join public.weekly_source_manifest_movements member
    on member.client_manifest_id=manifest.id
  join public.weekly_source_billing_movements movement
    on movement.id=member.billing_movement_id
  where manifest.source_cycle_id='a1000000-0000-4000-8000-000000000001'
    and movement.movement_role='POSITIVE';
  select movement.id into strict v_cross_client
  from public.weekly_source_client_manifests manifest
  join public.weekly_source_manifest_movements member
    on member.client_manifest_id=manifest.id
  join public.weekly_source_billing_movements movement
    on movement.id=member.billing_movement_id
  where manifest.source_cycle_id='b1000000-0000-4000-8000-000000000001'
  order by movement.id limit 1;

  perform pg_temp.assert_illegal_rebind_rejected(
    v_same_cycle_other_unit,v_target_line,'cross-correction-unit'
  );
  perform pg_temp.assert_illegal_rebind_rejected(
    v_cross_cycle,v_target_line,'cross-cycle'
  );
  perform pg_temp.assert_illegal_rebind_rejected(
    v_cross_client,v_target_line,'cross-Client'
  );

  select * into strict v_binding
  from public.weekly_source_invoice_line_bindings
  where invoice_line_id=v_target_line and state='CURRENT'
  order by billing_movement_id limit 1;
  begin
    insert into public.weekly_source_invoice_line_bindings(
      billing_movement_id,presentation_line_id,invoice_line_id,invoice_id,
      original_final_revision_id,original_cycle_id,client_id,correction_root_id,
      correction_role,manifest_hash,materialised_line_hash,binding_version,
      prior_binding_id,state
    ) values (
      v_binding.billing_movement_id,v_binding.presentation_line_id,
      v_binding.invoice_line_id,v_binding.invoice_id,v_binding.original_final_revision_id,
      v_binding.original_cycle_id,v_binding.client_id,v_binding.correction_root_id,
      v_binding.correction_role,v_binding.manifest_hash,v_binding.materialised_line_hash,
      v_binding.binding_version+1,v_binding.id,'CURRENT'
    );
    raise exception 'ASSERTION_FAILED: duplicate current movement binding was accepted';
  exception when unique_violation then
    null;
  end;
end;
$illegal_net_binding_proofs$;

select pg_temp.assert_true(
  (select pg_catalog.count(distinct binding.invoice_line_id)
   from public.weekly_source_client_manifests manifest
   join public.weekly_source_manifest_movements member
     on member.client_manifest_id=manifest.id
   join public.weekly_source_billing_movements movement
     on movement.id=member.billing_movement_id
   join public.weekly_source_invoice_line_bindings binding
     on binding.billing_movement_id=movement.id and binding.state='CURRENT'
   where manifest.source_cycle_id='a3000000-0000-4000-8000-000000000001'
     and movement.movement_role in ('REVERSAL','REPLACEMENT'))=1
  and (select invoice_line.total_charge_ex_vat=20.00
       from public.weekly_source_client_manifests manifest
       join public.weekly_source_manifest_movements member
         on member.client_manifest_id=manifest.id
       join public.weekly_source_billing_movements movement
         on movement.id=member.billing_movement_id
       join public.weekly_source_invoice_line_bindings binding
         on binding.billing_movement_id=movement.id and binding.state='CURRENT'
       join public.invoice_lines invoice_line on invoice_line.id=binding.invoice_line_id
       where manifest.source_cycle_id='a3000000-0000-4000-8000-000000000001'
         and movement.movement_role='REPLACEMENT'),
  'NET roster correction must share one line with exact replacement minus reversal arithmetic'
);
select pg_temp.assert_true(
  (select pg_catalog.count(distinct binding.invoice_line_id)
   from public.weekly_source_client_manifests manifest
   join public.weekly_source_manifest_movements member
     on member.client_manifest_id=manifest.id
   join public.weekly_source_billing_movements movement
     on movement.id=member.billing_movement_id
   join public.weekly_source_invoice_line_bindings binding
     on binding.billing_movement_id=movement.id and binding.state='CURRENT'
   where manifest.source_cycle_id='a3000000-0000-4000-8000-000000000001'
     and movement.movement_role in ('EXPENSE_REVERSAL','EXPENSE_REPLACEMENT'))=1
  and exists(
    select 1
    from public.weekly_source_client_manifests manifest
    join public.weekly_source_manifest_movements member
      on member.client_manifest_id=manifest.id
    join public.weekly_source_billing_movements movement
      on movement.id=member.billing_movement_id
    join public.weekly_source_invoice_line_bindings binding
      on binding.billing_movement_id=movement.id and binding.state='CURRENT'
    join public.invoice_lines invoice_line on invoice_line.id=binding.invoice_line_id
    where manifest.source_cycle_id='a3000000-0000-4000-8000-000000000001'
      and movement.movement_role='EXPENSE_REPLACEMENT'
      and invoice_line.total_charge_ex_vat=.25
      and invoice_line.vat_rate_pct=20
      and invoice_line.vat_amount=.05
      and invoice_line.total_inc_vat=.30
  ),
  'NET source-fixed expense must preserve exact delta and configured VAT-on arithmetic'
);
select pg_temp.assert_true(
  not exists(
    select binding.invoice_line_id
    from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_billing_movements movement
      on movement.id=binding.billing_movement_id
    where binding.state='CURRENT'
    group by binding.invoice_line_id
    having pg_catalog.count(*)=2 and (
      pg_catalog.count(distinct movement.correction_unit_id)<>1
      or pg_catalog.count(distinct movement.work_event_id)<>1
      or pg_catalog.count(distinct movement.final_revision_id)<>1
      or pg_catalog.count(distinct movement.finalisation_cycle_id)<>1
      or pg_catalog.count(distinct movement.actual_client_id)<>1
      or pg_catalog.count(*) filter(
        where movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
      )>0
    )
  )
  and exists(
    select 1
    from public.weekly_source_client_manifests manifest
    join public.weekly_source_manifest_movements member
      on member.client_manifest_id=manifest.id
    join public.weekly_source_invoice_line_bindings binding
      on binding.billing_movement_id=member.billing_movement_id
     and binding.state='CURRENT'
    where manifest.source_cycle_id='a3000000-0000-4000-8000-000000000001'
    group by binding.invoice_id
    having pg_catalog.count(distinct binding.invoice_line_id)>=2
  ),
  'NET sharing must refuse cross-unit/event/cycle/Client and keep distinct correction units separate'
);
select pg_temp.assert_true(
  (select pg_catalog.count(distinct binding.invoice_line_id)
   from public.weekly_source_client_manifests manifest
   join public.weekly_source_manifest_movements member
     on member.client_manifest_id=manifest.id
   join public.weekly_source_invoice_line_bindings binding
     on binding.billing_movement_id=member.billing_movement_id and binding.state='CURRENT'
   where manifest.source_cycle_id='b1000000-0000-4000-8000-000000000001')=2,
  'NHSP physical rows must never use NET presentation'
);
select pg_temp.assert_true(
  not exists(
    select 1 from public.invoices invoice
    where exists(
      select 1 from public.weekly_source_invoice_line_bindings binding
      where binding.invoice_id=invoice.id and binding.state='CURRENT'
    ) and (private.weekly_source_invoice_allocation_assert_v1(invoice.id)->>'ok')::boolean is not true
  ),
  'every NET/FULL/NHSP/VAT-on invoice must satisfy exact allocation'
);

-- ---------------------------------------------------------------------------
-- WP-51, closing WP-48's combined-review finding F1: the two family resolvers
-- must not disagree about what counts as one family.
--
-- Executed first use, not a source search.  A whitespace-padded sibling of a
-- lineage-bound root is created - the RAW unique indexes
-- `timesheets_booking_id_current_uidx` (unique on booking_id where is_current)
-- and `timesheets_booking_id_version_uidx` accept it, because both are on the
-- stored text and neither can express a canonical comparison - and then the
-- PERMISSIVE side and the STRICT side are required to give the same answer:
--   * the invoice family adapter resolves the pair as ONE family, both ways;
--   * the ordinary invoice batch offers NO group naming the padded sibling;
--   * the invoice-line owner guard REFUSES a line on the padded sibling with
--     WEEKLY_SOURCE_INVOICE_MOVEMENT_OWNER_REQUIRED, as it already did for the
--     lineage-bound root;
--   * the managed-root guard still reports the pair bound and refuses.
-- Membership is an explicit set comparison; nothing here is decided by a
-- `limit`, an `order by` or by what an index accepts.
--
-- The last assertion is the no-regression half: where there is NO raw/canonical
-- split, the adapter must return exactly the rotation-scope family it returned
-- before, which is why ordinary invoicing is unchanged.
-- ---------------------------------------------------------------------------
do $weekly_source_family_resolver_reconciliation$
declare
  v_root uuid;
  v_sibling constant uuid:='fedcba98-0000-4000-8000-00000000f051';
  v_booking text;
  v_invoice uuid;
  v_family_from_sibling uuid[];
  v_family_from_root uuid[];
  v_guard jsonb;
  v_groups bigint;
  v_line_refusal text;
begin
  select lineage.timesheet_id into v_root
  from public.weekly_source_row_timesheet_lineages lineage
  join public.timesheets timesheet_row
    on timesheet_row.timesheet_id=lineage.timesheet_id
   and timesheet_row.is_current
  join public.timesheets_financials financial
    on financial.timesheet_id=lineage.timesheet_id
   and financial.is_current
   and financial.client_id is not null
  where pg_catalog.btrim(coalesce(timesheet_row.booking_id,''))<>''
  order by lineage.timesheet_id
  limit 1;
  perform pg_temp.assert_true(
    v_root is not null,
    'the fixture must provide a lineage-bound current root with a booking reference'
  );
  select timesheet_row.booking_id into v_booking
  from public.timesheets timesheet_row where timesheet_row.timesheet_id=v_root;

  insert into public.invoices(client_id,status,subtotal_ex_vat,vat_amount,total_inc_vat)
  select financial.client_id,'DRAFT',0,0,0
  from public.timesheets_financials financial
  where financial.timesheet_id=v_root and financial.is_current
  returning id into v_invoice;

  create temp table weekly_source_wp51_sibling on commit drop as
    select * from public.timesheets where timesheet_id=v_root;
  update weekly_source_wp51_sibling
    set timesheet_id=v_sibling, version=1, is_current=true,
        booking_id=' '||v_booking;
  insert into public.timesheets select * from weekly_source_wp51_sibling;
  perform pg_temp.assert_true(
    exists(select 1 from public.timesheets t where t.timesheet_id=v_sibling),
    'the raw unique index accepts a whitespace-padded sibling of a current root'
  );

  v_family_from_sibling:=private.weekly_source_invoice_family_timesheet_ids_v1(v_sibling);
  v_family_from_root:=private.weekly_source_invoice_family_timesheet_ids_v1(v_root);
  perform pg_temp.assert_true(
    v_family_from_sibling is not null
      and pg_catalog.cardinality(v_family_from_sibling)>=2
      and v_root=any(v_family_from_sibling)
      and v_sibling=any(v_family_from_sibling),
    'the invoice family adapter must resolve a trim-equivalent sibling into the same family'
  );
  perform pg_temp.assert_true(
    v_family_from_root is not null
      and v_root=any(v_family_from_root)
      and v_sibling=any(v_family_from_root),
    'the family must be the same set resolved from either member'
  );

  v_guard:=private.weekly_source_managed_root_guard_v1(v_sibling);
  perform pg_temp.assert_true(
    coalesce(
      case when pg_catalog.jsonb_typeof(v_guard->'weekly_source_bound')='boolean'
        then (v_guard->>'weekly_source_bound')::boolean end,false)
      and coalesce(
        case when pg_catalog.jsonb_typeof(v_guard->'ok')='boolean'
          then (v_guard->>'ok')::boolean end,false) is not true,
    'the managed-root guard must still report the padded sibling bound and refuse it'
  );

  select pg_catalog.count(*) into v_groups
  from private._invoice_batch_generate_classification_v2(
    true,null,'2026-11-02 09:00+00'::timestamptz
  ) candidate
  where candidate.candidate_json::text like '%'||v_sibling::text||'%';
  perform pg_temp.assert_true(
    coalesce(v_groups,1)=0,
    'the ordinary invoice batch must not offer a trim-equivalent sibling of a lineage-bound root'
  );

  begin
    insert into public.invoice_lines(
      invoice_id,timesheet_id,description,hours_day,hours_night,hours_sat,hours_sun,
      hours_bh,total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,vat_rate_pct,
      vat_amount,total_inc_vat,meta_json,source_key)
    values(v_invoice,v_sibling,'WP-51 reconciliation probe',
      0,0,0,0,0,0,0,0,0,0,0,'{}'::jsonb,'WP51-RECONCILIATION-'||v_sibling::text);
    v_line_refusal:='ACCEPTED';
  exception when sqlstate '55000' then
    v_line_refusal:=sqlerrm;
  end;
  perform pg_temp.assert_true(
    v_line_refusal='WEEKLY_SOURCE_INVOICE_MOVEMENT_OWNER_REQUIRED',
    'the invoice-line owner guard must refuse a trim-equivalent sibling exactly as it refuses the root'
  );
end;
$weekly_source_family_resolver_reconciliation$;

select pg_temp.assert_true(
  not exists(
    select 1
    from public.timesheets timesheet_row
    where not exists(
      select 1 from public.timesheets sibling
      where pg_catalog.btrim(sibling.booking_id)=pg_catalog.btrim(timesheet_row.booking_id)
        and sibling.booking_id<>timesheet_row.booking_id
    )
    and (
      select pg_catalog.cardinality(adapter.family)
      from (select private.weekly_source_invoice_family_timesheet_ids_v1(
              timesheet_row.timesheet_id) family) adapter
    ) is distinct from (
      select pg_catalog.cardinality(scope.family)
      from (select coalesce(
              (select pg_catalog.array_agg(distinct resolved.family_timesheet_id)
               from public._pay_timesheet_rotation_scope(
                 array[timesheet_row.timesheet_id]::uuid[]) resolved
               where resolved.family_timesheet_id is not null),
              array[timesheet_row.timesheet_id]::uuid[]) family) scope
    )
  ),
  'without a raw/canonical split the adapter must return exactly the rotation-scope family'
);

-- FTI-020 / XSG-030: execute the protected invoice-discounting owner against
-- the real signed Weekly Source invoices created above.  This is deliberately
-- after the issue/unissue and move proofs so the ledger must describe the
-- resulting product state, not a hand-built substitute.
do $weekly_source_async_issue_owner$
declare
  v_actor constant uuid:='a0000000-0000-4000-8000-000000000001';
  v_invoice uuid;
  v_operation uuid;
  v_chunk uuid;
  v_document uuid;
  v_start jsonb;
  v_result jsonb;
begin
  select binding.invoice_id into v_invoice
  from public.weekly_source_invoice_line_bindings binding
  join public.invoices invoice on invoice.id=binding.invoice_id
  where binding.state='CURRENT' and invoice.status='DRAFT'
    and invoice.issued_at_utc is null and invoice.paid_at_utc is null
  order by binding.invoice_id limit 1;
  perform pg_temp.assert_true(v_invoice is not null,
    'the real asynchronous issue proof found no source invoice');

  -- Enter through the real operation-start owner so the parent operation,
  -- processor policy and issue chunk are exactly what production creates.
  v_start:=private._invoice_operation_start_core_v8(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'command_type','ISSUE_INVOICES',
      'invoice_ids',pg_catalog.jsonb_build_array(v_invoice),
      'allow_early',true,'deliver',false,
      'command_token','weekly-source-real-async-issue-'||v_invoice::text,
      'delivery_intent',pg_catalog.jsonb_build_object(
        'recipient_set','[]'::jsonb,'cc','[]'::jsonb,'bcc','[]'::jsonb)
    )),v_actor,'2026-09-09T15:00:00Z'
  );
  perform pg_temp.assert_true(
    coalesce((v_start->0->>'accepted')::boolean,false),
    'the real asynchronous issue start owner refused the source invoice: '
      ||v_start::text
  );
  v_operation:=(v_start->0->>'operation_id')::uuid;
  select id into strict v_chunk
  from public.invoice_operation_chunks
  where operation_id=v_operation and chunk_type='ISSUE_INVOICE'
    and entity_id=v_invoice;

  v_result:=private._invoice_issue_advance_core_v8(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'chunk_id',v_chunk,'phase','VALIDATE')),'2026-09-09T15:00:00Z');
  perform pg_temp.assert_true(
    (select phase='FREEZE' and status='QUEUED'
     from public.invoice_operation_chunks where id=v_chunk),
    'the real asynchronous ISSUE_INVOICES owner did not validate the source invoice: '
      ||v_result::text);

  v_result:=private._invoice_issue_advance_core_v8(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'chunk_id',v_chunk,'phase','FREEZE')),'2026-09-09T15:00:01Z');
  select document_version_id into v_document
  from public.invoice_operation_chunks where id=v_chunk;
  perform pg_temp.assert_true(
    v_document is not null
      and (select phase='WAIT_DOCUMENT' and status='WAITING'
           from public.invoice_operation_chunks where id=v_chunk),
    'the real asynchronous ISSUE_INVOICES owner did not freeze and delegate its document: '
      ||v_result::text||'; chunk='||(
        select pg_catalog.jsonb_build_object(
          'phase',phase,'status',status,'error',error_json,
          'progress',progress_json
        )::text
        from public.invoice_operation_chunks where id=v_chunk
      ));

  -- The document phase is executed separately through the real Worker renderer
  -- and storage owner.  Here its verified receipt is fed back to the actual SQL
  -- issue state machine, exactly as that worker does in production.
  update public.invoice_document_versions set
    status='READY',r2_key='verification/weekly-source-real-async.pdf',
    sha256=pg_catalog.repeat('a',64),size_bytes=100,page_count=1,
    core_page_count=1,supporting_page_count=0,
    ready_at_utc='2026-09-09T15:00:02Z',verified_at_utc='2026-09-09T15:00:02Z'
  where id=v_document;
  v_result:=private._invoice_issue_advance_core_v8(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'chunk_id',v_chunk,'phase','WAIT_DOCUMENT')),'2026-09-09T15:00:03Z');
  perform pg_temp.assert_true(
    (select phase='FINALISE' and status='QUEUED'
     from public.invoice_operation_chunks where id=v_chunk),
    'the real asynchronous ISSUE_INVOICES owner did not accept the verified document: '
      ||v_result::text);
  v_result:=private._invoice_issue_advance_core_v8(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'chunk_id',v_chunk,'phase','FINALISE')),'2026-09-09T15:00:04Z');
  perform pg_temp.assert_true(
    (select status='ISSUED' and issued_document_version_id=v_document
     from public.invoices where id=v_invoice)
    and (select phase='COMPLETE' and status='COMPLETE'
         from public.invoice_operation_chunks where id=v_chunk),
    'the real asynchronous ISSUE_INVOICES owner did not issue from the verified document: '
      ||v_result::text);
end;
$weekly_source_async_issue_owner$;

do $weekly_source_invoice_discounting$
declare
  v_actor constant uuid:='a0000000-0000-4000-8000-000000000001';
  v_preview jsonb;
  v_after jsonb;
  v_draft jsonb;
  v_commit jsonb;
  v_replay jsonb;
  v_ref text;
  v_before_count integer;
  v_source uuid;
  v_destination uuid;
  v_presentation_id uuid;
  v_presentation_hash bytea;
  v_move jsonb;
  v_refusal text;
  v_positive integer;
  v_negative integer;
  v_mixed integer;
  v_reissue_invoice uuid;
  v_reissue_result record;
  v_ledger_before jsonb;
  v_failure text;
  v_frozen_source_revision bigint;
  v_frozen_destination_revision bigint;
begin
  perform pg_temp.assert_true(
    exists(
      select 1 from information_schema.columns
      where table_schema='public' and table_name='id_invoice_ledger'
        and column_name='ledger_revision' and is_nullable='NO'
    ) and exists(
      select 1 from information_schema.columns
      where table_schema='public' and table_name='id_consolidation_run_lines'
        and column_name='ledger_revision'
    ),
    'invoice-discounting ledger revision schema is missing'
  );

  -- Refresh through the existing owner.  Its invoice-line and header triggers
  -- call this same owner in production; calling it here also catches a stale
  -- trigger or a hidden zero fallback before any consolidation is started.
  perform public.id_ledger_recompute_and_sync_invoice(source_invoice.invoice_id)
  from (
    select distinct binding.invoice_id
    from public.weekly_source_invoice_line_bindings binding
    where binding.state='CURRENT'
  ) source_invoice;

  select
    pg_catalog.count(*) filter (where shape.positive_lines>0 and shape.negative_lines=0),
    pg_catalog.count(*) filter (where shape.negative_lines>0 and shape.positive_lines=0),
    pg_catalog.count(*) filter (where shape.negative_lines>0 and shape.positive_lines>0)
  into v_positive,v_negative,v_mixed
  from (
    select binding.invoice_id,
      pg_catalog.count(*) filter (where line.total_charge_ex_vat>0) positive_lines,
      pg_catalog.count(*) filter (where line.total_charge_ex_vat<0) negative_lines
    from public.weekly_source_invoice_line_bindings binding
    join public.invoice_lines line on line.id=binding.invoice_line_id
    where binding.state='CURRENT'
    group by binding.invoice_id
  ) shape;
  perform pg_temp.assert_true(v_positive>0,
    'invoice-discounting proof has no positive source invoice');
  perform pg_temp.assert_true(v_negative>0,
    'invoice-discounting proof has no negative-only source invoice');
  perform pg_temp.assert_true(v_mixed>0,
    'invoice-discounting proof has no mixed signed source invoice');

  perform pg_temp.assert_true(
    not exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.invoices invoice on invoice.id=binding.invoice_id
      join public.id_invoice_ledger ledger on ledger.invoice_id=invoice.id
      where binding.state='CURRENT'
      group by invoice.id,invoice.subtotal_ex_vat,invoice.vat_amount,
               invoice.total_inc_vat,ledger.current_ex_vat,ledger.current_vat,
               ledger.current_inc_vat
      having ledger.current_ex_vat is distinct from invoice.subtotal_ex_vat
          or ledger.current_vat is distinct from invoice.vat_amount
          or ledger.current_inc_vat is distinct from invoice.total_inc_vat
    ),
    'invoice-discounting ledger did not retain the exact signed source invoice totals'
  );

  v_preview:=public.id_consolidation_preview();
  v_before_count:=coalesce((v_preview->>'line_count')::integer,0);
  perform pg_temp.assert_true(v_before_count>0,
    'signed source invoices produced no invoice-discounting preview');
  perform pg_temp.assert_true(
    not exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.id_invoice_ledger ledger on ledger.invoice_id=binding.invoice_id
      where binding.state='CURRENT'
        and (ledger.current_ex_vat<>ledger.last_reported_ex_vat
          or ledger.current_vat<>ledger.last_reported_vat
          or ledger.current_inc_vat<>ledger.last_reported_inc_vat)
        and not exists(
          select 1 from pg_catalog.jsonb_array_elements(v_preview->'lines') preview_line
          where (preview_line->>'invoice_id')::uuid=binding.invoice_id
            and (preview_line->>'delta_ex_vat')::numeric
                =ledger.current_ex_vat-ledger.last_reported_ex_vat
            and (preview_line->>'delta_vat')::numeric
                =ledger.current_vat-ledger.last_reported_vat
            and (preview_line->>'delta_inc_vat')::numeric
                =ledger.current_inc_vat-ledger.last_reported_inc_vat
        )
    ),
    'invoice-discounting preview omitted or changed a signed source delta'
  );

  -- A cancelled Draft freezes nothing permanently and the same exact preview
  -- remains available.
  v_draft:=public.id_consolidation_run_draft_start(v_actor,'Weekly Source cancellation proof');
  v_ref:=v_draft->>'id_ref';
  perform pg_temp.assert_true((v_draft->>'line_count')::integer=v_before_count,
    'invoice-discounting Draft did not freeze the complete preview');
  begin
    perform public.id_consolidation_run_draft_start(v_actor,'Conflicting Draft must refuse');
    raise exception 'ASSERTION_FAILED: a second active invoice-discounting Draft was accepted';
  exception when others then
    if sqlerrm like 'ASSERTION_FAILED:%' then raise; end if;
    if sqlerrm<>'ID_ACTIVE_DRAFT_EXISTS' then raise; end if;
  end;
  perform public.id_consolidation_run_draft_cancel(v_ref,v_actor);
  v_after:=public.id_consolidation_preview();
  perform pg_temp.assert_true(
    (v_after->>'line_count')::integer=v_before_count
      and (v_after->>'total_delta_ex_vat')::numeric
          =(v_preview->>'total_delta_ex_vat')::numeric,
    'cancelled invoice-discounting Draft changed the signed source preview'
  );

  -- Commit advances each frozen baseline once.  Exact replay is idempotent;
  -- a different bank-upload identity and cancelling a committed run refuse.
  v_draft:=public.id_consolidation_run_draft_start(v_actor,'Weekly Source commit proof');
  v_ref:=v_draft->>'id_ref';
  v_commit:=public.id_consolidation_run_draft_commit(v_ref,'WS-ID-PROOF-1',v_actor);
  perform pg_temp.assert_true(coalesce((v_commit->>'did_commit')::boolean,false),
    'invoice-discounting Draft did not commit');
  v_replay:=public.id_consolidation_run_draft_commit(v_ref,'WS-ID-PROOF-1',v_actor);
  perform pg_temp.assert_true(not coalesce((v_replay->>'did_commit')::boolean,true),
    'exact invoice-discounting commit replay was not idempotent');
  begin
    perform public.id_consolidation_run_draft_commit(v_ref,'WS-ID-DIFFERENT',v_actor);
    raise exception 'ASSERTION_FAILED: committed Draft accepted a different bank-upload identity';
  exception when others then
    if sqlerrm like 'ASSERTION_FAILED:%' then raise; end if;
    if sqlerrm<>'ID_RUN_ALREADY_COMMITTED_DIFFERENT_CODE' then raise; end if;
  end;
  begin
    perform public.id_consolidation_run_draft_cancel(v_ref,v_actor);
    raise exception 'ASSERTION_FAILED: committed invoice-discounting Draft was cancelled';
  exception when others then
    if sqlerrm like 'ASSERTION_FAILED:%' then raise; end if;
    if sqlerrm<>'CANNOT_CANCEL_COMMITTED_RUN' then raise; end if;
  end;
  perform pg_temp.assert_true(
    not exists(
      select 1 from public.weekly_source_invoice_line_bindings binding
      join public.id_invoice_ledger ledger on ledger.invoice_id=binding.invoice_id
      where binding.state='CURRENT'
        and (ledger.current_ex_vat<>ledger.last_reported_ex_vat
          or ledger.current_vat<>ledger.last_reported_vat
          or ledger.current_inc_vat<>ledger.last_reported_inc_vat)
    ),
    'committing the invoice-discounting Draft did not advance every source baseline'
  );

  -- A Draft whose source allocation changed after the freeze is stale.  Use
  -- the real same-Client move owner for both changes: a direct invoice-line
  -- update is intentionally refused by the production owner guard and would
  -- not prove the route the Office can actually use.
  select source_binding.invoice_id,destination.id,
         presentation.id,presentation.presentation_hash
  into v_source,v_destination,v_presentation_id,v_presentation_hash
  from public.weekly_source_invoice_line_bindings source_binding
  join public.weekly_source_invoice_presentation_lines presentation
    on presentation.id=source_binding.presentation_line_id
  join public.invoices source_header on source_header.id=source_binding.invoice_id
  join lateral (
    select candidate.id
    from public.invoices candidate
    where candidate.client_id=source_header.client_id
      and candidate.id<>source_header.id
      and candidate.status='DRAFT'
      and candidate.issued_at_utc is null
      and candidate.paid_at_utc is null
      and exists(select 1 from public.weekly_source_invoice_line_bindings destination_binding
                 where destination_binding.invoice_id=candidate.id
                   and destination_binding.state='CURRENT')
    order by candidate.id limit 1
  ) destination on true
  where source_binding.state='CURRENT'
    and source_header.status='DRAFT'
    and source_header.issued_at_utc is null
    and source_header.paid_at_utc is null
    and presentation.companion_presentation_line_id is null
    and not exists(select 1 from public.weekly_source_invoice_presentation_lines companion
                   where companion.companion_presentation_line_id=presentation.id)
  order by source_binding.invoice_id,presentation.id
  limit 1;
  perform pg_temp.assert_true(v_source is not null and v_destination is not null,
    'stale Draft proof found no same-Client movable source presentation');
  v_move:=public.weekly_source_invoice_move_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id',v_actor,
      'source_invoice_id',v_source,
      'destination_invoice_id',v_destination,
      'presentation_line_id',v_presentation_id,
      'expected_presentation_hash',pg_catalog.encode(v_presentation_hash,'hex'),
      'expected_source_document_revision',(select document_revision from public.invoices where id=v_source),
      'expected_destination_document_revision',(select document_revision from public.invoices where id=v_destination),
      'reason','Create the stale Draft proof through the real move owner.'
    )
  );
  perform pg_temp.assert_true(v_move->>'status'='MOVED',
    'first real source move failed during stale Draft proof');
  v_draft:=public.id_consolidation_run_draft_start(v_actor,'Stale invoice-discounting Draft proof');
  v_ref:=v_draft->>'id_ref';
  select line.ledger_revision into v_frozen_source_revision
  from public.id_consolidation_run_lines line
  where line.id_ref=v_ref and line.invoice_id=v_source;
  select line.ledger_revision into v_frozen_destination_revision
  from public.id_consolidation_run_lines line
  where line.id_ref=v_ref and line.invoice_id=v_destination;
  perform pg_temp.assert_true(
    v_frozen_source_revision is not null and v_frozen_destination_revision is not null,
    'invoice-discounting Draft did not freeze both ledger revisions'
  );
  v_move:=public.weekly_source_invoice_move_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id',v_actor,
      'source_invoice_id',v_destination,
      'destination_invoice_id',v_source,
      'presentation_line_id',v_presentation_id,
      'expected_presentation_hash',pg_catalog.encode(v_presentation_hash,'hex'),
      'expected_source_document_revision',(select document_revision from public.invoices where id=v_destination),
      'expected_destination_document_revision',(select document_revision from public.invoices where id=v_source),
      'reason','Change the frozen position through the real move owner.'
    )
  );
  perform pg_temp.assert_true(v_move->>'status'='MOVED',
    'second real source move failed during stale Draft proof');
  perform pg_temp.assert_true(
    (select ledger_revision from public.id_invoice_ledger where invoice_id=v_source)
      > v_frozen_source_revision
    and (select ledger_revision from public.id_invoice_ledger where invoice_id=v_destination)
      > v_frozen_destination_revision,
    'returning the source presentation did not retain the intervening ledger revisions'
  );
  begin
    perform public.id_consolidation_run_draft_commit(v_ref,'WS-ID-STALE',v_actor);
    raise exception 'ASSERTION_FAILED: a stale invoice-discounting Draft committed';
  exception when others then
    if sqlerrm like 'ASSERTION_FAILED:%' then raise; end if;
    if sqlerrm<>'ID_RUN_STALE_LEDGER' then raise; end if;
  end;
  perform pg_temp.assert_true(
    exists(select 1 from public.id_consolidation_runs run
           where run.id_ref=v_ref and run.bank_uploaded_at_utc is null
             and run.bank_upload_code is null),
    'stale commit advanced the Draft header');
  perform public.id_consolidation_run_draft_cancel(v_ref,v_actor);
  perform pg_temp.assert_true((public.id_consolidation_preview()->>'line_count')::integer=0,
    'stale Draft refusal/restoration did not return the ledger to its committed baseline');

  -- Issue, unissue, reissue and unissue a source invoice after its ledger
  -- baseline has already been committed.  Status-only lifecycle transitions
  -- must not invent a second invoice-discounting delta.
  select binding.invoice_id into v_reissue_invoice
  from public.weekly_source_invoice_line_bindings binding
  join public.invoices invoice on invoice.id=binding.invoice_id
  where binding.state='CURRENT'
    and invoice.status='DRAFT'
    and invoice.issued_at_utc is null
    and invoice.paid_at_utc is null
  order by binding.invoice_id
  limit 1;
  perform pg_temp.assert_true(v_reissue_invoice is not null,
    'invoice-discounting reissue proof found no eligible source invoice');
  select * into v_reissue_result
  from public.invoice_issue_one(v_reissue_invoice,v_actor);
  perform pg_temp.assert_true(v_reissue_result.status='ISSUED',
    'invoice-discounting proof could not issue its source invoice');
  perform public.invoice_unissue_one(v_reissue_invoice,v_actor,true);
  select * into v_reissue_result
  from public.invoice_issue_one(v_reissue_invoice,v_actor);
  perform pg_temp.assert_true(v_reissue_result.status='ISSUED',
    'invoice-discounting proof could not reissue its source invoice');
  perform public.invoice_unissue_one(v_reissue_invoice,v_actor,true);
  perform pg_temp.assert_true(
    (public.id_consolidation_preview()->>'line_count')::integer=0,
    'issue/unissue/reissue invented a duplicate invoice-discounting delta'
  );

  -- Move one independently movable source presentation between two idle,
  -- unissued invoices for the same Client through the real move owner.  The
  -- next preview must contain both changed invoices with opposite signed
  -- deltas, then cancel/rebuild/commit must converge to zero again.
  select source_binding.invoice_id,destination.id,
         presentation.id,presentation.presentation_hash
  into v_source,v_destination,v_presentation_id,v_presentation_hash
  from public.weekly_source_invoice_line_bindings source_binding
  join public.weekly_source_invoice_presentation_lines presentation
    on presentation.id=source_binding.presentation_line_id
  join public.invoices source_header on source_header.id=source_binding.invoice_id
  join lateral (
    select candidate.id
    from public.invoices candidate
    where candidate.client_id=source_header.client_id
      and candidate.id<>source_header.id
      and candidate.status='DRAFT'
      and candidate.issued_at_utc is null
      and candidate.paid_at_utc is null
      and exists(select 1 from public.weekly_source_invoice_line_bindings destination_binding
                 where destination_binding.invoice_id=candidate.id
                   and destination_binding.state='CURRENT')
    order by candidate.id limit 1
  ) destination on true
  where source_binding.state='CURRENT'
    and source_header.status='DRAFT'
    and source_header.issued_at_utc is null
    and source_header.paid_at_utc is null
    and presentation.companion_presentation_line_id is null
    and not exists(select 1 from public.weekly_source_invoice_presentation_lines companion
                   where companion.companion_presentation_line_id=presentation.id)
  order by source_binding.invoice_id,presentation.id
  limit 1;
  perform pg_temp.assert_true(v_source is not null and v_destination is not null,
    'invoice-discounting proof found no same-Client movable source presentation');

  v_move:=public.weekly_source_invoice_move_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id',v_actor,
      'source_invoice_id',v_source,
      'destination_invoice_id',v_destination,
      'presentation_line_id',v_presentation_id,
      'expected_presentation_hash',pg_catalog.encode(v_presentation_hash,'hex'),
      'expected_source_document_revision',(select document_revision from public.invoices where id=v_source),
      'expected_destination_document_revision',(select document_revision from public.invoices where id=v_destination),
      'reason','Prove signed invoice-discounting movement.'
    )
  );
  perform pg_temp.assert_true(v_move->>'status'='MOVED',
    'real source presentation move failed during invoice-discounting proof');
  v_after:=public.id_consolidation_preview();
  perform pg_temp.assert_true(
    exists(select 1 from pg_catalog.jsonb_array_elements(v_after->'lines') line
           where (line->>'invoice_id')::uuid=v_source)
    and exists(select 1 from pg_catalog.jsonb_array_elements(v_after->'lines') line
               where (line->>'invoice_id')::uuid=v_destination),
    'moving one source presentation did not produce both invoice-discounting deltas'
  );
  perform pg_temp.assert_true(
    (select pg_catalog.round(pg_catalog.sum((line->>'delta_ex_vat')::numeric),2)
     from pg_catalog.jsonb_array_elements(v_after->'lines') line
     where (line->>'invoice_id')::uuid in (v_source,v_destination))=0,
    'same-Client source movement changed the combined invoice-discounting value'
  );
  v_draft:=public.id_consolidation_run_draft_start(v_actor,'Weekly Source move cancellation proof');
  perform public.id_consolidation_run_draft_cancel(v_draft->>'id_ref',v_actor);
  perform pg_temp.assert_true((public.id_consolidation_preview()->>'line_count')::integer>0,
    'cancelling a moved-line Draft lost the pending source deltas');
  v_draft:=public.id_consolidation_run_draft_start(v_actor,'Weekly Source move commit proof');
  perform public.id_consolidation_run_draft_commit(v_draft->>'id_ref','WS-ID-PROOF-2',v_actor);
  perform pg_temp.assert_true((public.id_consolidation_preview()->>'line_count')::integer=0,
    'committed moved-line invoice-discounting position did not converge');

  -- Inject a failure in the canonical invoice-total owner while attempting to
  -- move the same source presentation back.  The move, both invoice headers,
  -- both discounting-ledger positions and the immutable binding must all stay
  -- exactly where they were.  A silent zero fallback fails this proof.
  select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(ledger) order by ledger.invoice_id)
    into v_ledger_before
  from public.id_invoice_ledger ledger
  where ledger.invoice_id in (v_source,v_destination);
  perform pg_catalog.set_config('weekly_source.fail_invoice_a',v_source::text,true);
  perform pg_catalog.set_config('weekly_source.fail_invoice_b',v_destination::text,true);
  execute $ddl$
    create function pg_temp.weekly_source_fail_invoice_recompute_v1()
    returns trigger language plpgsql as $body$
    begin
      if new.id::text in (
        pg_catalog.current_setting('weekly_source.fail_invoice_a',true),
        pg_catalog.current_setting('weekly_source.fail_invoice_b',true)
      ) then
        raise exception 'WEEKLY_SOURCE_INJECTED_INVOICE_RECOMPUTE_FAILURE';
      end if;
      return new;
    end;
    $body$
  $ddl$;
  execute $ddl$
    create trigger weekly_source_fail_invoice_recompute
    before update on public.invoices
    for each row execute function pg_temp.weekly_source_fail_invoice_recompute_v1()
  $ddl$;
  v_failure:=null;
  begin
    perform public.weekly_source_invoice_move_atomic_v1(
      pg_catalog.jsonb_build_object(
        'actor_user_id',v_actor,
        'source_invoice_id',v_destination,
        'destination_invoice_id',v_source,
        'presentation_line_id',v_presentation_id,
        'expected_presentation_hash',pg_catalog.encode(v_presentation_hash,'hex'),
        'expected_source_document_revision',(select document_revision from public.invoices where id=v_destination),
        'expected_destination_document_revision',(select document_revision from public.invoices where id=v_source),
        'reason','Injected rollback proof.'
      )
    );
    raise exception 'ASSERTION_FAILED: injected invoice recompute failure did not refuse the move';
  exception when others then
    get stacked diagnostics v_failure=message_text;
    if v_failure like 'ASSERTION_FAILED:%' then raise; end if;
    if v_failure not like '%ID_LEDGER_INVOICE_RECOMPUTE_FAILED%'
       and v_failure not like '%WEEKLY_SOURCE_INJECTED_INVOICE_RECOMPUTE_FAILURE%' then
      raise exception 'ASSERTION_FAILED: unexpected injected move failure %',v_failure;
    end if;
  end;
  execute 'drop trigger weekly_source_fail_invoice_recompute on public.invoices';
  perform pg_temp.assert_true(
    (select binding.invoice_id from public.weekly_source_invoice_line_bindings binding
     where binding.presentation_line_id=v_presentation_id and binding.state='CURRENT')=v_destination,
    'injected recompute failure moved the immutable presentation binding');
  perform pg_temp.assert_true(
    (select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(ledger) order by ledger.invoice_id)
     from public.id_invoice_ledger ledger
     where ledger.invoice_id in (v_source,v_destination))=v_ledger_before,
    'injected recompute failure changed one or both invoice-discounting ledger positions');
end;
$weekly_source_invoice_discounting$;

select pg_catalog.jsonb_build_object(
  'ok',true,'verification','weekly_source_invoice_admission_net_v1',
  'net_pair_proved',true,'vat_on_proved',true,'nhsp_net_refused',true,
  'family_resolver_reconciliation_proved',true,
  'invoice_discounting_positive_negative_mixed_move_commit_cancel_proved',true,
  'invoice_discounting_conflicting_draft_and_stale_commit_refused',true,
  'invoice_discounting_injected_recompute_failure_rolls_back_move_and_both_ledgers',true
);
rollback;

\unset weekly_source_verification_outer_transaction
\unset weekly_source_ordinary_verification_outer_transaction
\unset weekly_source_verification_correction_presentation
\unset weekly_source_verification_expense_vat_enabled
