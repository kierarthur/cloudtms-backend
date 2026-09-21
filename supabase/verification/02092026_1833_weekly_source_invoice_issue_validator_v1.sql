-- Rollback-only proof for weekly_source_invoice_issue_validator_v1 (Gate 7, G7-3).
--
-- Every row of P:\annexes\invoice-issue-real-route-matrix.csv (ISS-001..ISS-014)
-- is exercised through the REAL issue entry points:
--
--   * private._invoice_issue_validate_batch - the single hard-blocker gate of
--     the asynchronous route, consumed unchanged by
--     private._invoice_issue_advance_core_v8 and by
--     private._invoice_batch_issue_classification_v2;
--   * private._invoice_batch_issue_classification_v2 - the batch issue
--     classifier the Office list is built from;
--   * public.invoice_issue_one and public.invoice_unissue_one - the real direct
--     owners that perform the transition.
--
-- 25 section 4 Removed, bullet 1: nothing here writes public.invoices.status
-- directly to prove an issue.  Every write is rolled back.

\set ON_ERROR_STOP on

\set weekly_source_verification_outer_transaction true
\set weekly_source_ordinary_verification_outer_transaction true
\set weekly_source_verification_correction_presentation 'FULL_REVERSAL_REPLACEMENT'
\set weekly_source_verification_expense_vat_enabled false
begin;
\ir 15092026_1534_weekly_source_ordinary_pay_projection_v1.sql

create function pg_temp.iss_admit_all()
returns void language plpgsql as $function$
declare v_manifest public.weekly_source_client_manifests%rowtype;
begin
  for v_manifest in
    select * from public.weekly_source_client_manifests
    where invoice_state='READY' order by created_at_utc,id
  loop
    perform public.weekly_source_invoice_admit_atomic_v1(
      pg_catalog.jsonb_build_object(
        'actor_user_id','a0000000-0000-4000-8000-000000000001',
        'client_manifest_id',v_manifest.id,
        'expected_manifest_hash',pg_catalog.encode(v_manifest.manifest_hash,'hex')
      )
    );
  end loop;
end;
$function$;

create function pg_temp.iss_invoice_for_cycle(p_cycle_id uuid)
returns uuid language sql stable as $function$
  select binding.invoice_id
  from public.weekly_source_client_manifests manifest
  join public.weekly_source_manifest_movements manifest_movement
    on manifest_movement.client_manifest_id=manifest.id
  join public.weekly_source_invoice_line_bindings binding
    on binding.billing_movement_id=manifest_movement.billing_movement_id
   and binding.state='CURRENT'
  where manifest.source_cycle_id=p_cycle_id
  order by binding.invoice_id limit 1;
$function$;

-- The real asynchronous gate, for one invoice.
create function pg_temp.iss_async_can_issue(p_invoice_id uuid)
returns boolean language sql stable as $function$
  select coalesce(v.can_issue_only,false)
  from private._invoice_issue_validate_batch(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'request_key','iss:'||p_invoice_id::text,'invoice_id',p_invoice_id,
      'expected_revision',(select document_revision from public.invoices
                           where id=p_invoice_id),
      'allow_early',true,'deliver',false)),
    date '2026-12-31'
  ) v;
$function$;

create function pg_temp.iss_async_blockers(p_invoice_id uuid)
returns jsonb language sql stable as $function$
  select coalesce(v.hard_blocker_codes,'[]'::jsonb)
  from private._invoice_issue_validate_batch(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'request_key','iss:'||p_invoice_id::text,'invoice_id',p_invoice_id,
      'expected_revision',(select document_revision from public.invoices
                           where id=p_invoice_id),
      'allow_early',true,'deliver',false)),
    date '2026-12-31'
  ) v;
$function$;

-- The real batch issue classifier.
create function pg_temp.iss_classifier_can_issue(p_invoice_id uuid)
returns boolean language sql stable as $function$
  select coalesce(bool_or((classified.candidate_json->>'can_issue_only')::boolean),false)
  from private._invoice_batch_issue_classification_v2(
    true,array[p_invoice_id]::uuid[],pg_catalog.statement_timestamp()
  ) classified;
$function$;

-- The real direct owner, reported without aborting the proof.
create function pg_temp.iss_direct_issue(p_invoice_id uuid)
returns text language plpgsql as $function$
declare v_row record;
begin
  select * into v_row from public.invoice_issue_one(
    p_invoice_id,'a0000000-0000-4000-8000-000000000001');
  return v_row.status;
exception when others then
  return 'RAISED:'||sqlerrm;
end;
$function$;

create function pg_temp.iss_assert(p_ok boolean,p_row text,p_detail text)
returns void language plpgsql as $function$
begin
  if p_ok is not true then
    raise exception 'ASSERTION_FAILED: % - %',p_row,p_detail;
  end if;
end;
$function$;

select pg_temp.iss_admit_all();

-- ISS-001  Sealed WEEKLY_FINAL_SOURCE self-bill positive, Office issue one.
-- ISS-013  The same invoice while Candidate pay is protected/frozen: the
--          validator reads no pay state at all, so pay cannot gate issue.
-- ISS-014  Issue, unissue and reissue through the real owners.
do $iss_001_013_014$
declare
  v_invoice uuid:=pg_temp.iss_invoice_for_cycle('d1000000-0000-4000-8000-000000000001');
  v_definition text;
  v_status text;
begin
  perform pg_temp.iss_assert(
    coalesce((private.weekly_source_invoice_issue_validate_v1(v_invoice)->>'ok')::boolean,false),
    'ISS-001','the source-aware validator refused a sealed positive self-bill');
  perform pg_temp.iss_assert(pg_temp.iss_async_can_issue(v_invoice),
    'ISS-001','the real asynchronous validator blocked it: '
      ||pg_temp.iss_async_blockers(v_invoice)::text);
  perform pg_temp.iss_assert(pg_temp.iss_classifier_can_issue(v_invoice),
    'ISS-001','the real batch issue classifier did not offer it');

  -- ISS-013: no Candidate pay, protected-pay, Workbench or authorisation fact
  -- may appear anywhere in the source branch.
  select pg_catalog.pg_get_functiondef(
    'private.weekly_source_invoice_issue_validate_v1(uuid)'::pg_catalog.regprocedure
  ) into v_definition;
  perform pg_temp.iss_assert(
    pg_catalog.strpos(v_definition,'pay_batch')=0
    and pg_catalog.strpos(v_definition,'timesheet_pay_state')=0
    and pg_catalog.strpos(v_definition,'workbench')=0
    and pg_catalog.strpos(v_definition,'weekly_exceptional_pay')=0
    and pg_catalog.strpos(v_definition,'authorised')=0
    and pg_catalog.strpos(v_definition,'timesheets_financials')=0,
    'ISS-013','the source issue validator reads Candidate pay or authorisation state');
  perform pg_temp.iss_assert(
    exists(select 1 from public.weekly_exceptional_pay_target_families),
    'ISS-013','the fixture no longer carries a protected pay family');

  v_status:=pg_temp.iss_direct_issue(v_invoice);
  perform pg_temp.iss_assert(v_status='ISSUED','ISS-001',
    'the real direct owner returned '||v_status);
  perform pg_temp.iss_assert(
    not exists(select 1 from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_billing_movements movement
        on movement.id=binding.billing_movement_id
      where binding.invoice_id=v_invoice and binding.state='CURRENT'
        and movement.placement_state<>'ISSUED'),
    'ISS-001','issue did not mark every movement issued');

  perform public.invoice_unissue_one(
    v_invoice,'a0000000-0000-4000-8000-000000000001',true);
  perform pg_temp.iss_assert(
    (select status::text from public.invoices where id=v_invoice)='DRAFT'
    and not exists(select 1 from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_billing_movements movement
        on movement.id=binding.billing_movement_id
      where binding.invoice_id=v_invoice and binding.state='CURRENT'
        and movement.placement_state<>'PLACED'),
    'ISS-014','unissue did not restore the movable placement state');
  v_status:=pg_temp.iss_direct_issue(v_invoice);
  perform pg_temp.iss_assert(v_status='ISSUED','ISS-014',
    'reissue through the real owner returned '||v_status);
end;
$iss_001_013_014$;

-- ISS-002  Sealed source fixed expense with no receipt or mileage evidence.
do $iss_002$
declare
  v_invoice uuid:=pg_temp.iss_invoice_for_cycle('e1000000-0000-4000-8000-000000000001');
  v_status text;
begin
  perform pg_temp.iss_assert(
    exists(select 1 from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_invoice_presentation_lines presentation
        on presentation.id=binding.presentation_line_id
      where binding.invoice_id=v_invoice and binding.state='CURRENT'
        and presentation.origin_kind='SOURCE_FIXED_EXPENSE'),
    'ISS-002','the expense-only fixture invoice is not a source expense invoice');
  perform pg_temp.iss_assert(pg_temp.iss_async_can_issue(v_invoice),
    'ISS-002','the real asynchronous validator blocked a source expense invoice: '
      ||pg_temp.iss_async_blockers(v_invoice)::text);
  v_status:=pg_temp.iss_direct_issue(v_invoice);
  perform pg_temp.iss_assert(v_status='ISSUED','ISS-002',
    'the real direct owner returned '||v_status);
end;
$iss_002$;

-- ISS-003  An original source invoice is issued; a later legal source reversal
--          on the same Timesheet root is then offered on a second invoice.  The
--          generic same-unit split rejection must not fire.
do $iss_003$
declare
  v_original uuid:=pg_temp.iss_invoice_for_cycle('d2000000-0000-4000-8000-000000000001');
  v_later uuid:=pg_temp.iss_invoice_for_cycle('d3000000-0000-4000-8000-000000000001');
  v_status text;
  v_roots integer;
begin
  select pg_catalog.count(distinct movement.invoice_timesheet_id)::integer into v_roots
  from public.weekly_source_invoice_line_bindings binding
  join public.weekly_source_billing_movements movement
    on movement.id=binding.billing_movement_id
  where binding.invoice_id in (v_original,v_later) and binding.state='CURRENT';
  perform pg_temp.iss_assert(v_roots>=1,'ISS-003',
    'the later-reversal fixture has no shared Timesheet root');
  v_status:=pg_temp.iss_direct_issue(v_original);
  perform pg_temp.iss_assert(v_status='ISSUED','ISS-003',
    'the original source invoice returned '||v_status);
  perform pg_temp.iss_assert(pg_temp.iss_async_can_issue(v_later),
    'ISS-003','the real asynchronous validator rejected a legal later reversal: '
      ||pg_temp.iss_async_blockers(v_later)::text);
  perform pg_temp.iss_assert(pg_temp.iss_classifier_can_issue(v_later),
    'ISS-003','the batch issue classifier rejected a legal later reversal');
end;
$iss_003$;

-- INV-030 (re-executed; acceptance row corrected 17 September 2026).
-- The accepted NHSP validation sum differs from the canonical CloudTMS Contract
-- calculation by exactly one penny under the named rounding-equivalent rule.
-- Presentation, visible line, invoice header and the real issue route must all
-- carry the same frozen exact signed source pence, and no source penny may
-- reach Candidate pay.
do $inv_030$
declare
  v_invoice uuid:=pg_temp.iss_invoice_for_cycle('b1000000-0000-4000-8000-000000000001');
  v_penny record;
  v_line public.invoice_lines%rowtype;
  v_header numeric;
begin
  select presentation.* into strict v_penny
  from public.weekly_source_invoice_presentation_lines presentation
  where presentation.origin_kind='NHSP_PHYSICAL_ROW'
    and presentation.price_check_result='SOURCE_ROUNDING_EQUIVALENT';
  perform pg_temp.iss_assert(
    pg_catalog.abs(v_penny.source_validation_charge_pence
                   -v_penny.calculated_comparison_charge_pence)=1,
    'INV-030','the rounding-equivalent NHSP row is not exactly one penny apart');
  perform pg_temp.iss_assert(
    v_penny.amount_authority='VALIDATED_SOURCE_PENCE'
    and v_penny.invoice_presentation_charge_pence=v_penny.source_validation_charge_pence
    and (v_penny.total_charge_ex_vat*100)::bigint=v_penny.source_validation_charge_pence,
    'INV-030','the presentation does not carry the exact signed source pence');

  select invoice_line.* into strict v_line
  from public.weekly_source_invoice_line_bindings binding
  join public.invoice_lines invoice_line on invoice_line.id=binding.invoice_line_id
  where binding.presentation_line_id=v_penny.id and binding.state='CURRENT';
  perform pg_temp.iss_assert(
    (v_line.total_charge_ex_vat*100)::bigint=v_penny.source_validation_charge_pence,
    'INV-030','the visible invoice line does not carry the exact signed source pence');
  perform pg_temp.iss_assert(
    not exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_billing_movements movement
        on movement.id=binding.billing_movement_id
      join public.timesheets_financials financial
        on financial.timesheet_id=movement.invoice_timesheet_id and financial.is_current
      where binding.presentation_line_id=v_penny.id and binding.state='CURRENT'
        and (financial.total_charge_ex_vat*100)::bigint
            =v_penny.source_validation_charge_pence
        and (financial.total_charge_ex_vat*100)::bigint
            <>v_penny.calculated_comparison_charge_pence),
    'INV-030','the source rounding penny reached Candidate pay');

  select invoice.total_inc_vat into strict v_header
  from public.invoices invoice where invoice.id=v_invoice;
  perform pg_temp.iss_assert(
    v_header=(select pg_catalog.round(pg_catalog.sum(line.total_inc_vat),2)
              from public.invoice_lines line where line.invoice_id=v_invoice),
    'INV-030','the invoice header total does not equal the sum of its source lines');
  perform pg_temp.iss_assert(
    coalesce((private.weekly_source_invoice_issue_validate_v1(v_invoice)->>'ok')::boolean,false),
    'INV-030','the source issue validator refused the rounding-equivalent invoice');
  perform pg_temp.iss_assert(pg_temp.iss_async_can_issue(v_invoice),'INV-030',
    'the real asynchronous validator refused the rounding-equivalent invoice: '
      ||pg_temp.iss_async_blockers(v_invoice)::text);
end;
$inv_030$;

-- ISS-004  NHSP physical negative and corrected positive in the same report:
--          two independent lines on one invoice, never netted.
do $iss_004$
declare
  v_invoice uuid:=pg_temp.iss_invoice_for_cycle('b1000000-0000-4000-8000-000000000001');
  v_lines integer;
  v_status text;
begin
  select pg_catalog.count(*)::integer into v_lines
  from public.invoice_lines where invoice_id=v_invoice;
  perform pg_temp.iss_assert(v_lines=2,'ISS-004',
    'the NHSP fixture invoice does not carry two physical lines');
  perform pg_temp.iss_assert(
    not exists(
      select 1 from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_invoice_presentation_lines presentation
        on presentation.id=binding.presentation_line_id
      where binding.invoice_id=v_invoice and binding.state='CURRENT'
        and presentation.correction_role='NET_DIFFERENCE'),
    'ISS-004','an NHSP physical row was netted');
  perform pg_temp.iss_assert(pg_temp.iss_async_can_issue(v_invoice),'ISS-004',
    'the real asynchronous validator blocked the NHSP pair: '
      ||pg_temp.iss_async_blockers(v_invoice)::text);
  v_status:=pg_temp.iss_direct_issue(v_invoice);
  perform pg_temp.iss_assert(v_status='ISSUED','ISS-004',
    'the real direct owner returned '||v_status);
  perform pg_temp.iss_assert(
    (select pg_catalog.count(*)::integer from public.invoice_lines
     where invoice_id=v_invoice)=2,
    'ISS-004','issue changed the physical line count');
end;
$iss_004$;

-- ISS-005  Non-NHSP configured FULL reversal and replacement.
do $iss_005$
declare
  v_invoice uuid:=pg_temp.iss_invoice_for_cycle('a3000000-0000-4000-8000-000000000001');
  v_status text;
begin
  perform pg_temp.iss_assert(
    exists(select 1 from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_invoice_presentation_lines presentation
        on presentation.id=binding.presentation_line_id
      where binding.invoice_id=v_invoice and binding.state='CURRENT'
        and presentation.correction_role in ('REVERSAL','REPLACEMENT')),
    'ISS-005','the FULL correction fixture invoice has no reversal/replacement pair');
  perform pg_temp.iss_assert(pg_temp.iss_async_can_issue(v_invoice),'ISS-005',
    'the real asynchronous validator blocked a configured full pair: '
      ||pg_temp.iss_async_blockers(v_invoice)::text);
  v_status:=pg_temp.iss_direct_issue(v_invoice);
  perform pg_temp.iss_assert(v_status='ISSUED','ISS-005',
    'the real direct owner returned '||v_status);
end;
$iss_005$;

-- ISS-007  A source invoice mixed with an unbound ordinary line fails closed.
-- ISS-010  A tampered line amount fails closed.
-- ISS-011  An empty source Draft fails closed.
-- ISS-008  A source header with no source binding fails closed.
-- ISS-009  A stale final source revision fails closed.
do $iss_negatives$
declare
  v_invoice uuid:=pg_temp.iss_invoice_for_cycle('a4000000-0000-4000-8000-000000000001');
  v_line uuid;
  v_charge numeric;
  v_empty uuid;
  v_revision uuid;
  v_verdict jsonb;
begin
  -- ISS-007
  insert into public.invoice_lines(
    invoice_id,timesheet_id,description,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
    total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,
    meta_json,source_key
  ) values (
    v_invoice,null,'Unbound ordinary line on a source invoice',0,0,0,0,0,
    0,0,0,0,0,0,'{}'::jsonb,'ISS-007-UNBOUND'
  );
  v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_invoice);
  perform pg_temp.iss_assert(
    (v_verdict->>'ok')::boolean is false
    and v_verdict->'blocker_codes' @> '["WEEKLY_SOURCE_ISSUE_UNBOUND_LINE"]'::jsonb,
    'ISS-007','a mixed source invoice was admitted: '||v_verdict::text);
  perform pg_temp.iss_assert(not pg_temp.iss_async_can_issue(v_invoice),
    'ISS-007','the real asynchronous validator admitted a mixed source invoice');
  perform pg_temp.iss_assert(not pg_temp.iss_classifier_can_issue(v_invoice),
    'ISS-007','the batch issue classifier admitted a mixed source invoice');
  perform pg_temp.iss_assert(
    pg_temp.iss_direct_issue(v_invoice)<>'ISSUED',
    'ISS-007','the real direct owner issued a mixed source invoice');
  delete from public.invoice_lines
  where invoice_id=v_invoice and source_key='ISS-007-UNBOUND';
  perform pg_temp.iss_assert(
    coalesce((private.weekly_source_invoice_issue_validate_v1(v_invoice)->>'ok')::boolean,false),
    'ISS-007','removing the unbound line did not restore admissibility');

  -- ISS-010: tamper the visible amount through the owner guard, then restore.
  select id,total_charge_ex_vat into strict v_line,v_charge
  from public.invoice_lines where invoice_id=v_invoice order by id limit 1;
  perform pg_catalog.set_config(
    'cloudtms.weekly_source_invoice_owner','MOVE_SOURCE_INVOICE',true);
  update public.invoice_lines set total_charge_ex_vat=v_charge+1 where id=v_line;
  perform pg_catalog.set_config('cloudtms.weekly_source_invoice_owner','',true);
  v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_invoice);
  perform pg_temp.iss_assert((v_verdict->>'ok')::boolean is false,
    'ISS-010','a tampered line amount was admitted: '||v_verdict::text);
  perform pg_temp.iss_assert(not pg_temp.iss_async_can_issue(v_invoice),
    'ISS-010','the real asynchronous validator admitted a tampered source invoice');
  perform pg_temp.iss_assert(pg_temp.iss_direct_issue(v_invoice)<>'ISSUED',
    'ISS-010','the real direct owner issued a tampered source invoice');
  perform pg_catalog.set_config(
    'cloudtms.weekly_source_invoice_owner','MOVE_SOURCE_INVOICE',true);
  update public.invoice_lines set total_charge_ex_vat=v_charge where id=v_line;
  perform pg_catalog.set_config('cloudtms.weekly_source_invoice_owner','',true);
  update public.invoices set status='DRAFT',on_hold_reason=null
  where id=v_invoice and status='ON_HOLD';

  -- ISS-008 and ISS-011: a source header with neither lines nor bindings.
  insert into public.invoices(
    type,client_id,status,status_date_utc,subtotal_ex_vat,vat_amount,total_inc_vat,
    header_snapshot_json,do_not_send,document_revision,document_state,issue_state
  )
  select 'INVOICE',invoice.client_id,'DRAFT',pg_catalog.transaction_timestamp(),0,0,0,
         invoice.header_snapshot_json,false,1,'STALE','NOT_STARTED'
  from public.invoices invoice where invoice.id=v_invoice
  returning id into v_empty;
  v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_empty);
  perform pg_temp.iss_assert(
    (v_verdict->>'is_source_invoice')::boolean
    and (v_verdict->>'ok')::boolean is false
    and v_verdict->'blocker_codes' @> '["WEEKLY_SOURCE_ISSUE_NO_SOURCE_BINDING"]'::jsonb
    and v_verdict->'blocker_codes' @> '["WEEKLY_SOURCE_ISSUE_EMPTY_INVOICE"]'::jsonb,
    'ISS-008/ISS-011','a source header with no binding was admitted: '||v_verdict::text);
  perform pg_temp.iss_assert(not pg_temp.iss_async_can_issue(v_empty),
    'ISS-008/ISS-011','the real asynchronous validator admitted an empty source invoice');
  perform pg_temp.iss_assert(pg_temp.iss_direct_issue(v_empty)<>'ISSUED',
    'ISS-008/ISS-011','the real direct owner issued an empty source invoice');

  -- ISS-009: a superseded final source revision.
  select manifest.final_revision_id into strict v_revision
  from public.weekly_source_invoice_line_bindings binding
  join public.weekly_source_manifest_movements manifest_movement
    on manifest_movement.billing_movement_id=binding.billing_movement_id
  join public.weekly_source_client_manifests manifest
    on manifest.id=manifest_movement.client_manifest_id
  where binding.invoice_id=v_invoice and binding.state='CURRENT' limit 1;
  update public.weekly_source_final_revisions set state='SUPERSEDED' where id=v_revision;
  v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_invoice);
  perform pg_temp.iss_assert(
    (v_verdict->>'ok')::boolean is false
    and v_verdict->'blocker_codes' @> '["WEEKLY_SOURCE_ISSUE_STALE_FINAL_REVISION"]'::jsonb,
    'ISS-009','a stale final source revision was admitted: '||v_verdict::text);
  perform pg_temp.iss_assert(not pg_temp.iss_async_can_issue(v_invoice),
    'ISS-009','the real asynchronous validator admitted a stale final source revision');
  perform pg_temp.iss_assert(pg_temp.iss_direct_issue(v_invoice)<>'ISSUED',
    'ISS-009','the real direct owner issued against a stale final source revision');
  update public.weekly_source_final_revisions set state='CURRENT' where id=v_revision;
  update public.invoices set status='DRAFT',on_hold_reason=null
  where id=v_invoice and status='ON_HOLD';
end;
$iss_negatives$;

-- ISS-012  An ordinary evidence-backed invoice never enters the source branch,
--          and every ordinary control is preserved.  The complete before/after
--          differential over an ordinary invoice population is a separate proof
--          recorded in the WP-05 report; this is the in-verifier guarantee that
--          the branch is not selected and that nothing is skipped.
do $iss_012$
declare
  v_ordinary uuid;
  v_verdict jsonb;
  v_blockers jsonb;
begin
  insert into public.invoices(
    type,client_id,status,status_date_utc,subtotal_ex_vat,vat_amount,total_inc_vat,
    header_snapshot_json,do_not_send,document_revision,document_state,issue_state
  ) values (
    'INVOICE','a0000000-0000-4000-8000-000000000002','DRAFT',
    pg_catalog.transaction_timestamp(),50.00,10.00,60.00,
    '{"schema_version":"ORDINARY_INVOICE_V1","meta":{"self_bill":false}}'::jsonb,
    false,1,'STALE','NOT_STARTED'
  ) returning id into v_ordinary;
  insert into public.invoice_lines(
    invoice_id,timesheet_id,description,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
    total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,
    meta_json,source_key
  ) values (
    v_ordinary,null,'Ordinary line',0,0,0,0,0,
    40.00,50.00,10.00,20.00,10.00,60.00,'{}'::jsonb,'ISS-012-ORDINARY'
  );
  v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_ordinary);
  perform pg_temp.iss_assert(
    v_verdict='{"is_source_invoice": false}'::jsonb,
    'ISS-012','the source branch was selected for an ordinary invoice: '||v_verdict::text);
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_blockers_v1(
      v_ordinary,array['MISSING_REFERENCE','MISSING_EXPENSE_EVIDENCE','INVALID_TOTALS']
    )=array['MISSING_REFERENCE','MISSING_EXPENSE_EVIDENCE','INVALID_TOTALS'],
    'ISS-012','the asynchronous seam changed an ordinary blocker array');
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_reasons_v1(
      v_verdict,array['TS 00000000-0000-4000-8000-000000000001: missing timesheet PDF']
    )=array['TS 00000000-0000-4000-8000-000000000001: missing timesheet PDF'],
    'ISS-012','the direct seam changed an ordinary reason array');
  v_blockers:=pg_temp.iss_async_blockers(v_ordinary);
  perform pg_temp.iss_assert(
    not exists(
      select 1 from pg_catalog.jsonb_array_elements_text(v_blockers) code
      where code like 'WEEKLY_SOURCE_%'),
    'ISS-012','an ordinary invoice received a Weekly Source blocker: '||v_blockers::text);
end;
$iss_012$;

-- WP-05c, HANDOVER 2 round 5 Part E.  The three financial-record states, each
-- EXECUTED against the real corpus and each distinguishable in what the owner
-- returns, not merely in its internal logic.
--
--   1  never applicable, proved by positive route and profile evidence -> skipped
--   2  expected but missing                                            -> blocks
--   3  stale                                                           -> blocks
do $tsfin_states$
declare
  v_proved_invoice uuid;
  v_missing_invoice uuid;
  v_present_invoice uuid;
  v_state jsonb;
begin
  -- 1.  The lineage-free first-activation NHSP physical full negative.  The
  -- installed projection owner recorded NO_OP_FIRST_NEGATIVE for its root and
  -- published no financial, which is the positive proof of no Candidate-pay
  -- effect (02 section 297; 08 SRC-T11 and NHSBR-T04; 14 NHSP-BR-014).
  select line.invoice_id into v_proved_invoice
  from public.invoice_lines line
  join public.weekly_source_invoice_line_bindings binding
    on binding.invoice_line_id=line.id and binding.state='CURRENT'
  join public.weekly_source_invoice_presentation_lines presentation
    on presentation.id=binding.presentation_line_id
  join public.weekly_source_billing_movements movement
    on movement.id=presentation.billing_movement_id
  where line.timesheet_id is not null
  group by line.invoice_id,line.timesheet_id
  having pg_catalog.bool_and(
           movement.source_line_kind='NHSP_PHYSICAL_FULL_NEGATIVE')
     and not exists(select 1 from public.timesheets_financials financial
                    where financial.timesheet_id=line.timesheet_id)
  order by line.invoice_id limit 1;
  perform pg_temp.iss_assert(v_proved_invoice is not null,
    'WP-05c R5-E','the corpus has no lineage-free first-activation negative to prove');
  v_state:=private.weekly_source_invoice_tsfin_state_v1(v_proved_invoice);
  perform pg_temp.iss_assert(
    v_state->>'state'='TSFIN_NOT_APPLICABLE_PROVED'
    and (v_state#>>'{counts,not_applicable_proved}')::integer>0
    and (v_state#>>'{counts,expected_but_missing}')::integer=0
    and (v_state#>>'{counts,stale}')::integer=0
    and (v_state#>>'{members,0,evidence,positive_evidence_complete}')='true'
    and (v_state#>>'{members,0,evidence,no_candidate_pay_effect_receipt}')='true'
    and (v_state#>>'{members,0,evidence,profile_full_reversal}')='true'
    and (v_state#>>'{members,0,evidence,family_ever_had_financial}')='false',
    'WP-05c R5-E','the never-applicable state was not proved: '||v_state::text);
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_tsfin_skippable_v1(
      v_proved_invoice,'INVOICE_CORRECTION_TSFIN_MISSING'),
    'WP-05c R5-E','a proved never-applicable record was not skippable');
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_blockers_v1(v_proved_invoice,
      array['INVOICE_CORRECTION_TSFIN_MISSING']::text[])=array[]::text[],
    'WP-05c R5-E','the real asynchronous seam did not skip a proved never-applicable record');
  -- WITHDRAWN BY R8A SECTION 1.  "stale always blocks" was round 8 Part 2.2,
  -- which R8A withdraws for a wholly sealed source-backed self-bill; this
  -- invoice is wholly sealed, so INVOICE_CORRECTION_TSFIN_STALE is skipped here
  -- for the same reason INVOICE_CORRECTION_TSFIN_MISSING is.  What replaces it
  -- is the bound that survives: the skip follows the POSITIVE sealed proof, and
  -- a code outside the closed three-member Candidate-pay enumeration is never
  -- skipped whatever the state.
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_blockers_v1(v_proved_invoice,
      array['INVOICE_CORRECTION_TSFIN_STALE']::text[])=array[]::text[]
    and private.weekly_source_invoice_issue_blockers_v1(v_proved_invoice,
      array['INVOICE_CORRECTION_MEMBER_MISSING','INVOICE_CORRECTION_UNIT_INVALID']::text[])
      =array['INVOICE_CORRECTION_MEMBER_MISSING','INVOICE_CORRECTION_UNIT_INVALID']::text[],
    'WP-05c R5-E / R8A 1','the Candidate-pay boundary was refused on a wholly '
      ||'sealed invoice, or it widened past the closed enumeration');

  -- 1b.  Remove ONE positive limb and nothing else.  The financial record is
  -- exactly as absent as before, so if the outcome changes it can only be
  -- because the skip is conditioned on the evidence rather than on absence.
  begin
    update public.weekly_source_format_profiles set active=false
    where profile_code='NHSP_FINAL_BACKING_V1';
    v_state:=private.weekly_source_invoice_tsfin_state_v1(v_proved_invoice);
    perform pg_temp.iss_assert(
      v_state->>'state'='TSFIN_EXPECTED_BUT_MISSING'
      and (v_state#>>'{members,0,evidence,profile_full_reversal}')='false',
      'WP-05c R5-E','removing the profile limb did not move the state to expected-but-missing: '
        ||v_state::text);
    -- WITHDRAWN BY R8A SECTION 1, and replaced by what the withdrawn limb still
    -- has to change.  Under R8A a missing Candidate-pay record no longer blocks
    -- a wholly sealed source self-bill, so the INVOICE outcome is deliberately
    -- the same before and after.  What must still change is the DISPOSITION and
    -- the CANDIDATE-PAY outcome: the record was proved never applicable before,
    -- and is merely absent now, so the invoice disposition stops being CLEAR and
    -- the Candidate-pay disposition starts blocking.  If those did not change,
    -- the evidence limb would be decorative.
    perform pg_temp.iss_assert(
      private.weekly_source_invoice_issue_validate_v1(v_proved_invoice)
        ->>'tsfin_invoice_disposition'
        ='WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE'
      and private.weekly_source_invoice_issue_validate_v1(v_proved_invoice)
        ->'tsfin_payment_blocker_codes'
        ='["WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING"]'::jsonb
      -- and it is NOT called awaiting-first-authorisation: R8A keeps the proven
      -- first-activation never-applicable receipt a SEPARATE state, and limb F
      -- of the proof is what keeps them apart.
      and private.weekly_source_invoice_tsfin_first_authorisation_v1(
        (select line.timesheet_id from public.invoice_lines line
         where line.invoice_id=v_proved_invoice and line.timesheet_id is not null
         order by line.timesheet_id limit 1))->'reason_codes'
        ? 'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_NEVER_APPLICABLE_RECEIPT',
      'WP-05c R5-E / R8A 1','withdrawing the profile limb did not change the '
        ||'Candidate-pay disposition, or the never-applicable case was merged '
        ||'into the awaiting-first-authorisation state');
    raise exception 'WP05C_RELEASE_THE_PROFILE_LIMB';
  exception when raise_exception then
    if sqlerrm<>'WP05C_RELEASE_THE_PROFILE_LIMB' then raise; end if;
  end;
  perform pg_temp.iss_assert(
    (select active from public.weekly_source_format_profiles
     where profile_code='NHSP_FINAL_BACKING_V1'),
    'WP-05c R5-E','the profile limb was not released again');

  -- 2.  A source-positive member with no financial record at all: expected but
  -- missing.  Nothing proves it was never applicable, so it blocks.
  select line.invoice_id into v_missing_invoice
  from public.invoice_lines line
  join public.weekly_source_invoice_line_bindings binding
    on binding.invoice_line_id=line.id and binding.state='CURRENT'
  join public.weekly_source_invoice_presentation_lines presentation
    on presentation.id=binding.presentation_line_id
  join public.weekly_source_billing_movements movement
    on movement.id=presentation.billing_movement_id
  where line.timesheet_id is not null
  group by line.invoice_id,line.timesheet_id
  having pg_catalog.bool_and(movement.source_line_kind='SOURCE_ORDINARY')
     and not exists(select 1 from public.timesheets_financials financial
                    where financial.timesheet_id=line.timesheet_id)
  order by line.invoice_id limit 1;
  perform pg_temp.iss_assert(v_missing_invoice is not null,
    'WP-05c R5-E','the corpus has no expected-but-missing financial record to prove');
  v_state:=private.weekly_source_invoice_tsfin_state_v1(v_missing_invoice);
  perform pg_temp.iss_assert(
    v_state->>'state'='TSFIN_EXPECTED_BUT_MISSING'
    and (v_state#>>'{counts,expected_but_missing}')::integer>0
    and (v_state#>>'{counts,not_applicable_proved}')::integer=0,
    'WP-05c R5-E','the expected-but-missing state was not recorded: '||v_state::text);
  -- SUPERSEDED BY HANDOVER 2 ROUND 8 PART 2.1, AND ADOPTED RATHER THAN WEAKENED.
  --
  -- WP-05c wrote these two assertions under round 5 Part E, when every
  -- expected-but-missing record blocked.  Round 8 Part 2.1 rules that the exact
  -- awaiting-first-authorisation case must NOT stop its source invoice, and the
  -- sealed pack's own acceptance row ISS-013 ("Source invoice while Candidate
  -- pay pending/protected/frozen" -> ISSUED) says the same.  This corpus member
  -- is a SOURCE_ORDINARY positive line whose week has never been authorised by
  -- either mechanism and has never had a financial record - executed, and the
  -- facts are asserted below - so it IS that case and it IS now skippable.
  --
  -- HANDOVER 2 CORRECTION ADDENDUM R8A section 1 then went further and made the
  -- WHOLE Candidate-pay class non-blocking for a wholly sealed source self-bill,
  -- so the skip no longer turns on the first-authorisation proof at all - it
  -- turns on the POSITIVE sealed proof.  The first-authorisation proof still
  -- decides which typed disposition the invoice carries and whether the
  -- never-applicable case stays separate, and both are asserted below.
  --
  -- The PURPOSE of WP-05c's assertion is preserved: nothing here rests on
  -- ABSENCE.  The skip rests on a positive sealed proof, and the block after
  -- next withdraws one source-side fact and shows the skip go away again.
  perform pg_temp.iss_assert(
    coalesce((private.weekly_source_invoice_tsfin_first_authorisation_v1(
      (select line.timesheet_id from public.invoice_lines line
       where line.invoice_id=v_missing_invoice and line.timesheet_id is not null
       order by line.timesheet_id limit 1))->>'proved')::boolean,false),
    'WP-05c R5-E','the expected-but-missing corpus member is not positively '
      ||'proved to be awaiting its first authorisation, so the skip below would '
      ||'be resting on absence after all');
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_tsfin_skippable_v1(
      v_missing_invoice,'INVOICE_CORRECTION_TSFIN_MISSING')
    and private.weekly_source_invoice_issue_tsfin_skippable_v1(
      v_missing_invoice,'INVOICE_CORRECTION_TSFIN_NOT_READY'),
    'WP-05c R5-E','a proved awaiting-first-authorisation record was refused, '
      ||'which round 8 Part 2.1 exempts');
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_blockers_v1(v_missing_invoice,
      array['INVOICE_CORRECTION_TSFIN_MISSING','INVOICE_CORRECTION_TSFIN_NOT_READY']::text[])
      =array[]::text[],
    'WP-05c R5-E','the real asynchronous seam did not apply the round 8 Part 2.1 '
      ||'exemption to a proved awaiting-first-authorisation record');
  -- WITHDRAW ONE LIMB OF THE FIRST-AUTHORISATION PROOF - the ordinary
  -- authorisation stamp - and the INVOICE outcome is deliberately unchanged
  -- under R8A, because the invoice is still wholly sealed.  What must change is
  -- the typed disposition, which stops naming the first-authorisation state.
  begin
    update public.timesheets set authorised_at_server=pg_catalog.transaction_timestamp()
    where timesheet_id in (
      select line.timesheet_id from public.invoice_lines line
      where line.invoice_id=v_missing_invoice and line.timesheet_id is not null);
    perform pg_temp.iss_assert(
      private.weekly_source_invoice_issue_validate_v1(v_missing_invoice)
        ->>'tsfin_invoice_disposition'
        ='WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE'
      and private.weekly_source_invoice_issue_validate_v1(v_missing_invoice)
        ->'tsfin_payment_blocker_codes'
        ='["WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING"]'::jsonb,
      'WP-05c R5-E / R8A 1','withdrawing a limb of the first-authorisation proof '
        ||'did not change the typed disposition, so the proof is decorative');
    raise exception 'WP05C_RELEASE_THE_AUTHORISATION_STAMP';
  exception when raise_exception then
    if sqlerrm<>'WP05C_RELEASE_THE_AUTHORISATION_STAMP' then raise; end if;
  end;
  -- AND THE SEALED PROOF IS WHAT THE SKIP RESTS ON.  Withdraw one SOURCE-SIDE
  -- fact - the final source revision - and all three Candidate-pay skips go.
  begin
    update public.weekly_source_final_revisions set state='SUPERSEDED'
    where id in (
      select manifest.final_revision_id
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_manifest_movements manifest_movement
        on manifest_movement.billing_movement_id=binding.billing_movement_id
      join public.weekly_source_client_manifests manifest
        on manifest.id=manifest_movement.client_manifest_id
      where binding.invoice_id=v_missing_invoice and binding.state='CURRENT');
    perform pg_temp.iss_assert(
      coalesce((private.weekly_source_invoice_issue_validate_v1(v_missing_invoice)
                ->>'wholly_sealed_source_self_bill')::boolean,true) is false
      and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
        v_missing_invoice,'INVOICE_CORRECTION_TSFIN_MISSING')
      and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
        v_missing_invoice,'INVOICE_CORRECTION_TSFIN_NOT_READY')
      and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
        v_missing_invoice,'INVOICE_CORRECTION_TSFIN_STALE')
      and private.weekly_source_invoice_issue_blockers_v1(v_missing_invoice,
        array['INVOICE_CORRECTION_TSFIN_MISSING','INVOICE_CORRECTION_TSFIN_NOT_READY']::text[])
        @> array['INVOICE_CORRECTION_TSFIN_MISSING','INVOICE_CORRECTION_TSFIN_NOT_READY']::text[],
      'WP-05c R5-E / R8A 1','the Candidate-pay skip survived the invoice ceasing '
        ||'to be wholly sealed, so it is inherited rather than proved');
    raise exception 'WP05C_RELEASE_THE_SOURCE_REVISION';
  exception when raise_exception then
    if sqlerrm<>'WP05C_RELEASE_THE_SOURCE_REVISION' then raise; end if;
  end;

  -- 3.  Stale always blocks.
  select line.invoice_id into v_present_invoice
  from public.invoice_lines line
  where line.timesheet_id is not null
    and exists(select 1 from public.timesheets_financials financial
               where financial.timesheet_id=line.timesheet_id and financial.is_current)
  order by line.invoice_id limit 1;
  perform pg_temp.iss_assert(v_present_invoice is not null,
    'WP-05c R5-E','the corpus has no current financial record to make stale');
  begin
    update public.timesheets_financials financial
    set is_stale=true,stale_reason='WP05C_TSFIN_STALE_PROOF'
    where financial.is_current
      and financial.timesheet_id in (
        select line.timesheet_id from public.invoice_lines line
        where line.invoice_id=v_present_invoice and line.timesheet_id is not null);
    v_state:=private.weekly_source_invoice_tsfin_state_v1(v_present_invoice);
    perform pg_temp.iss_assert(
      v_state->>'state'='TSFIN_STALE' and (v_state#>>'{counts,stale}')::integer>0,
      'WP-05c R5-E','the stale state was not recorded: '||v_state::text);
    -- WITHDRAWN BY R8A SECTION 1 for a WHOLLY SEALED source self-bill, and
    -- replaced by the bound that still holds.  A Candidate-pay financial-record
    -- code is now skippable on this invoice because the invoice is wholly
    -- sealed - every source-side limb proved - and for no other reason.  The
    -- test that remains is the one that matters: the skip must follow the
    -- POSITIVE sealed proof, so breaking one source-side fact and nothing else
    -- must take all three skips away again.
    perform pg_temp.iss_assert(
      private.weekly_source_invoice_issue_tsfin_skippable_v1(
        v_present_invoice,'INVOICE_CORRECTION_TSFIN_STALE')
      and private.weekly_source_invoice_issue_tsfin_skippable_v1(
        v_present_invoice,'INVOICE_CORRECTION_TSFIN_MISSING')
      and private.weekly_source_invoice_issue_tsfin_skippable_v1(
        v_present_invoice,'INVOICE_CORRECTION_TSFIN_NOT_READY'),
      'WP-05c R5-E / R8A 1','a wholly sealed source self-bill was refused a '
        ||'Candidate-pay skip that R8A section 1 requires');
    perform pg_temp.iss_assert(
      not private.weekly_source_invoice_issue_tsfin_skippable_v1(
        v_present_invoice,'INVOICE_CORRECTION_MEMBER_MISSING')
      and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
        v_present_invoice,'INVOICE_CORRECTION_UNIT_INVALID')
      and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
        v_present_invoice,'INVOICE_CORRECTION_CHAIN_CYCLE'),
      'WP-05c R5-E / R8A 1','the Candidate-pay boundary widened past the closed '
        ||'three-member enumeration');
    begin
      update public.weekly_source_final_revisions set state='SUPERSEDED'
      where id in (
        select manifest.final_revision_id
        from public.weekly_source_invoice_line_bindings binding
        join public.weekly_source_manifest_movements manifest_movement
          on manifest_movement.billing_movement_id=binding.billing_movement_id
        join public.weekly_source_client_manifests manifest
          on manifest.id=manifest_movement.client_manifest_id
        where binding.invoice_id=v_present_invoice and binding.state='CURRENT');
      perform pg_temp.iss_assert(
        coalesce((private.weekly_source_invoice_issue_validate_v1(v_present_invoice)
                  ->>'wholly_sealed_source_self_bill')::boolean,true) is false
        and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
          v_present_invoice,'INVOICE_CORRECTION_TSFIN_STALE')
        and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
          v_present_invoice,'INVOICE_CORRECTION_TSFIN_MISSING')
        and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
          v_present_invoice,'INVOICE_CORRECTION_TSFIN_NOT_READY'),
        'WP-05c R5-E / R8A 1','the Candidate-pay skip was inherited by an '
          ||'invoice that is no longer wholly sealed');
      raise exception 'WP05C_RELEASE_THE_SEALED_PROOF';
    exception when raise_exception then
      if sqlerrm<>'WP05C_RELEASE_THE_SEALED_PROOF' then raise; end if;
    end;
    raise exception 'WP05C_RELEASE_THE_STALE_FIXTURE';
  exception when raise_exception then
    if sqlerrm<>'WP05C_RELEASE_THE_STALE_FIXTURE' then raise; end if;
  end;

  -- The three outcomes are distinguishable in what the owner RETURNS.
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_validate_v1(v_proved_invoice)
      #>>'{detail,tsfin,state}'='TSFIN_NOT_APPLICABLE_PROVED'
    and private.weekly_source_invoice_issue_validate_v1(v_missing_invoice)
      #>>'{detail,tsfin,state}'='TSFIN_EXPECTED_BUT_MISSING',
    'WP-05c R5-E','the verdict a consumer receives does not name the financial-record state');
end;
$tsfin_states$;

-- The skip list is exactly the four categories 24 section 11 names plus the
-- generic correction-unit family 25 section 4 Removed bullet 3 supersedes, and
-- nothing else.  Everything not named stays a blocker for a source invoice too.
--
-- The list is CLOSED.  The third block below enumerates EVERY code the
-- installed private._invoice_correction_validate_batch can emit and fails if a
-- single code outside the authorised six is skippable; the fourth proves a code
-- that does not exist is not skippable either, so a prefix wildcard cannot come
-- back and a code added to the family in future is blocking until it is listed.
do $skip_list$
declare v_kept text; v_all text[]; v_skipped text[];
begin
  foreach v_kept in array array[
    'REQUEST_KEY_REQUIRED','REQUEST_KEY_DUPLICATE','EVALUATION_DATE_REQUIRED',
    'INVOICE_NOT_FOUND','EXPECTED_REVISION_INVALID','ALLOW_EARLY_INVALID',
    'DELIVERY_INTENT_INVALID','INVOICE_ON_HOLD','INVOICE_NOT_DRAFT',
    'SOURCE_REVISION_CHANGED','INVALID_TOTALS','MISSING_HIGHER_RATE_SUPPORT',
    'EARLY_ISSUE_NOT_ALLOWED','CONFLICTING_ISSUE_OPERATION',
    'TIMESHEET_DOCUMENT_NOT_READY','REQUIRED_ASSET_NOT_READY'
  ] loop
    perform pg_temp.iss_assert(
      not private.weekly_source_invoice_issue_skippable_code_v1(v_kept),
      'G7-3','the source branch would skip the ordinary control '||v_kept);
  end loop;
  -- the 24 section 11 evidence families, plus the 25 section 4 bullet 3 judgement
  foreach v_kept in array array[
    'MISSING_TIMESHEET','MANUAL_TIMESHEET_SOURCE_MISSING','QR_TIMESHEET_UNSIGNED',
    'TIMESHEET_DOCUMENT_FAILED','MISSING_REFERENCE','MISSING_MILEAGE_EVIDENCE',
    'MISSING_EXPENSE_EVIDENCE','ASSET_NOT_REGISTERED','ASSET_WORKFLOW_MISSING',
    'REQUIRED_ASSET_FAILED','MISSING_IMPORT_SOURCE_EVIDENCE',
    'CORRECTION_LINES_NOT_UNIT_SAFE',
    'INVOICE_CORRECTION_UNIT_SPLIT_ACROSS_INVOICES',
    'INVOICE_CORRECTION_PAIR_PLACEMENT_INCOMPLETE',
    'INVOICE_CORRECTION_STREAM_MISMATCH',
    'INVOICE_CORRECTION_TARGET_STREAM_MISMATCH'
  ] loop
    perform pg_temp.iss_assert(
      private.weekly_source_invoice_issue_skippable_code_v1(v_kept),
      'G7-3','the source branch does not skip the irrelevant requirement '||v_kept);
  end loop;
  -- HANDOVER 2 round 5, Part E.  The two financial-record codes were in the
  -- list above and were therefore skipped on the code TEXT alone, which is a
  -- skip conditioned on ABSENCE.  They must no longer be skippable without the
  -- invoice, and INVOICE_CORRECTION_TSFIN_STALE must not be skippable at all.
  foreach v_kept in array array[
    'INVOICE_CORRECTION_TSFIN_MISSING',
    'INVOICE_CORRECTION_TSFIN_NOT_READY',
    'INVOICE_CORRECTION_TSFIN_STALE'
  ] loop
    perform pg_temp.iss_assert(
      not private.weekly_source_invoice_issue_skippable_code_v1(v_kept),
      'WP-05c R5-E','the text-only predicate still skips '||v_kept
        ||' with no evidence that the record was never applicable');
    perform pg_temp.iss_assert(
      not private.weekly_source_invoice_issue_tsfin_skippable_v1(null,v_kept),
      'WP-05c R5-E','the evidence gate skipped '||v_kept||' for a null invoice');
  end loop;
  perform pg_temp.iss_assert(
    not private.weekly_source_invoice_issue_tsfin_skippable_v1(
      pg_catalog.gen_random_uuid(),'INVOICE_CORRECTION_TSFIN_MISSING'),
    'WP-05c R5-E','the evidence gate skipped a missing record for an invoice that does not exist');
  -- every INVOICE_CORRECTION code the installed owner can emit
  v_all:=array[
    'INVOICE_CORRECTION_ACTION_INVALID','INVOICE_CORRECTION_CHAIN_CYCLE',
    'INVOICE_CORRECTION_CHAIN_DEPTH_EXCEEDED','INVOICE_CORRECTION_CHAIN_IDENTITY_INVALID',
    'INVOICE_CORRECTION_CLASSIFICATION_INVALID','INVOICE_CORRECTION_CLIENT_MISMATCH',
    'INVOICE_CORRECTION_CONTRACT_MISMATCH','INVOICE_CORRECTION_CURRENT_POLICY_FINGERPRINT_MISMATCH',
    'INVOICE_CORRECTION_ENVELOPE_FINGERPRINT_MISMATCH','INVOICE_CORRECTION_ENVELOPE_MISSING',
    'INVOICE_CORRECTION_ENVELOPE_ROOT_MISMATCH','INVOICE_CORRECTION_ENVELOPE_SCHEMA_INVALID',
    'INVOICE_CORRECTION_FROZEN_POLICY_DRIFT','INVOICE_CORRECTION_INVOICE_POLICY_FINGERPRINT_MISMATCH',
    'INVOICE_CORRECTION_LEG_FINGERPRINT_MISMATCH','INVOICE_CORRECTION_LEG_MISSING',
    'INVOICE_CORRECTION_MEMBER_ENVELOPE_MISMATCH','INVOICE_CORRECTION_MEMBER_LIMIT_EXCEEDED',
    'INVOICE_CORRECTION_MEMBER_MISSING','INVOICE_CORRECTION_OPERATION_IDENTITY_INVALID',
    'INVOICE_CORRECTION_PAIR_PLACEMENT_INCOMPLETE','INVOICE_CORRECTION_ROUTE_FAMILY_INVALID',
    'INVOICE_CORRECTION_SEGMENT_LOCK_CONFLICT','INVOICE_CORRECTION_SOURCE_LOCK_CONFLICT',
    'INVOICE_CORRECTION_STREAM_MISMATCH','INVOICE_CORRECTION_TARGET_CLIENT_MISMATCH',
    'INVOICE_CORRECTION_TARGET_NOT_APPENDABLE','INVOICE_CORRECTION_TARGET_NOT_FOUND',
    'INVOICE_CORRECTION_TARGET_STREAM_MISMATCH','INVOICE_CORRECTION_TSFIN_MISSING',
    'INVOICE_CORRECTION_TSFIN_NOT_READY','INVOICE_CORRECTION_TSFIN_POLICY_FINGERPRINT_MISMATCH',
    'INVOICE_CORRECTION_TSFIN_STALE','INVOICE_CORRECTION_UNIT_INVALID',
    'INVOICE_CORRECTION_UNIT_SPLIT_ACROSS_INVOICES','INVOICE_CORRECTION_VAT_POLICY_MISMATCH',
    'INVOICE_CORRECTION_WEEK_MISMATCH'
  ]::text[];
  select coalesce(pg_catalog.array_agg(code order by code),array[]::text[])
    into v_skipped
  from pg_catalog.unnest(v_all) code
  where private.weekly_source_invoice_issue_skippable_code_v1(code);
  perform pg_temp.iss_assert(
    v_skipped=array[
      'INVOICE_CORRECTION_PAIR_PLACEMENT_INCOMPLETE',
      'INVOICE_CORRECTION_STREAM_MISMATCH',
      'INVOICE_CORRECTION_TARGET_STREAM_MISMATCH',
      'INVOICE_CORRECTION_UNIT_SPLIT_ACROSS_INVOICES'
    ]::text[],
    'G7-3','a correction code outside the closed 25 section 4 enumeration is skipped: '
      ||pg_catalog.array_to_string(v_skipped,','));
  -- The evidence gate can never widen that closed enumeration: for every code
  -- the installed owner can emit except the two financial-record codes it
  -- names, it returns false whatever the invoice.
  select coalesce(pg_catalog.array_agg(code order by code),array[]::text[])
    into v_skipped
  from pg_catalog.unnest(v_all) code
  where private.weekly_source_invoice_issue_tsfin_skippable_v1(
    pg_catalog.gen_random_uuid(),code);
  perform pg_temp.iss_assert(
    v_skipped=array[]::text[],
    'WP-05c R5-E','the evidence gate widened the enumeration for an unknown invoice: '
      ||pg_catalog.array_to_string(v_skipped,','));
  perform pg_temp.iss_assert(
    not private.weekly_source_invoice_issue_skippable_code_v1(
      'INVOICE_CORRECTION_CODE_THAT_DOES_NOT_EXIST_V1'),
    'G7-3','a code that does not exist is skipped, so the list is still a wildcard');
  perform pg_temp.iss_assert(
    not private.weekly_source_invoice_issue_skippable_code_v1('INVOICE_CORRECTION_'),
    'G7-3','the bare correction prefix is skipped, so the list is still a wildcard');
end;
$skip_list$;

-- ---------------------------------------------------------------------------
-- WP-28.  Gate 13 hostile review 2 findings F1, F2 and F3, asserted through the
-- REAL ROUTES rather than through the seam functions.
--
-- The blocks above this one prove the seam FUNCTIONS behave correctly when they
-- are handed a code list by hand.  The independent reviewer's F1 and F2 are
-- that this proves nothing about what happens when an invoice is issued,
-- because on the asynchronous route the ordinary correction engine surfaces
-- only blocker_codes[1] and on the direct route no financial-record state was
-- read at all.  Every assertion below is EXECUTED against
-- private._invoice_issue_validate_batch,
-- private._invoice_batch_issue_classification_v2 and public.invoice_issue_one.
-- ---------------------------------------------------------------------------

-- The real direct owner inside a raise-to-undo subtransaction, so a refusal or
-- an issue proved here leaves no state for the blocks that follow.
create function pg_temp.iss_direct_probe(p_invoice_id uuid)
returns text language plpgsql as $function$
declare v_row record; v_out text;
begin
  begin
    select * into v_row from public.invoice_issue_one(
      p_invoice_id,'a0000000-0000-4000-8000-000000000001');
    v_out:=coalesce(v_row.status,'<null>')||' || '
      ||coalesce(pg_catalog.array_to_string(v_row.reasons,' ; '),'');
    raise exception 'WP28UNDO:%',v_out;
  exception
    when raise_exception then
      if pg_catalog.left(sqlerrm,9)='WP28UNDO:' then
        return pg_catalog.substr(sqlerrm,10);
      end if;
      return 'RAISED || '||sqlerrm;
    when others then
      return 'RAISED || '||sqlerrm;
  end;
end;
$function$;

do $wp28_routes$
declare
  v_inv uuid;
  v_ts uuid;
  v_codes text[];
  v_first text;
  v_direct text;
  v_verdict jsonb;
begin
  -- -----------------------------------------------------------------------
  -- F2.  Stale TSFIN must stop issue on BOTH routes, and must do so WITHOUT
  -- depending on which ordinary correction code carries the lowest CASE
  -- ordinal.  The fixture makes the record stale AND moves the member's
  -- ordinary invoice stream to NORMAL, so the engine puts the SKIPPABLE
  -- INVOICE_CORRECTION_TARGET_STREAM_MISMATCH ahead of
  -- INVOICE_CORRECTION_TSFIN_STALE.  Executed against the pre-WP-28 text this
  -- exact shape returned can_issue=true and ISSUED.
  -- -----------------------------------------------------------------------
  select line.invoice_id into v_inv
  from public.invoice_lines line
  join public.weekly_source_invoice_line_bindings binding
    on binding.invoice_line_id=line.id and binding.state='CURRENT'
  join public.invoices invoice on invoice.id=line.invoice_id
  where line.timesheet_id is not null
    and invoice.status='DRAFT' and invoice.on_hold_reason is null
    and exists(select 1 from public.timesheets_financials financial
               where financial.timesheet_id=line.timesheet_id
                 and financial.is_current)
    and coalesce((private.weekly_source_invoice_issue_validate_v1(line.invoice_id)
                  ->>'ok')::boolean,false)
  order by line.invoice_id limit 1;
  perform pg_temp.iss_assert(v_inv is not null,
    'WP-28 F2','the corpus has no accepted source invoice with a current financial record');
  begin
    update public.timesheets_financials financial
    set is_stale=true,
        stale_reason='WP28_STALE_ROUTE_PROOF',
        basis='HR_VALIDATED'::public.timesheet_fin_basis_enum
    where financial.is_current
      and financial.timesheet_id in (
        select line.timesheet_id from public.invoice_lines line
        where line.invoice_id=v_inv and line.timesheet_id is not null);

    select c.blocker_codes,c.blocker_code into v_codes,v_first
    from private._invoice_correction_validate_batch(
      pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'request_key','wp28:'||v_inv::text,'invoice_id',v_inv,
        'validation_purpose','INVOICE_ISSUE')),
      date '2026-12-31') c
    where c.invoice_id=v_inv;

    -- The masking really is present in this shape: if it were not, the block
    -- below would prove nothing about ordering.
    perform pg_temp.iss_assert(
      'INVOICE_CORRECTION_TSFIN_STALE'
        =any(coalesce(v_codes,array[]::text[]))
      and coalesce(v_first,'')<>'INVOICE_CORRECTION_TSFIN_STALE',
      'WP-28 F2','the ordering fixture did not mask the stale code: first='
        ||coalesce(v_first,'<none>')||' all='
        ||pg_catalog.array_to_string(coalesce(v_codes,array[]::text[]),','));

    -- ===================================================================
    -- WITHDRAWN BY HANDOVER 2 CORRECTION ADDENDUM R8A SECTION 1, WHICH IS
    -- LATER AUTHORITY THAN ROUND 8 AND ACCEPTED BY THE PRODUCT OWNER.
    --
    -- WP-28 asserted here that a stale Candidate-pay TSFIN stops a conforming
    -- source self-bill on all three real routes.  R8A section 1 withdraws round
    -- 8 Part 2.2 and Part 6 item 6 for a WHOLLY SEALED source-backed self-bill:
    --   "A stale or missing Candidate-pay TSFIN likewise does not block an
    --    otherwise-valid immutable final-source invoice movement.  It blocks
    --    Candidate-pay publication, readiness or reconciliation wherever those
    --    owners require current pay evidence."
    -- restoring 02 CONTROLLING POLICY section 15 and making acceptance row
    -- ISS-013 a live gate.  WP-33 measured by execution that this predicate
    -- keys on public.timesheets_financials.is_stale alone, reads no reason, and
    -- catches no source-side staleness, so it was catching exactly the class
    -- R8A says was never an admission predicate.
    --
    -- WP-28's FIXTURE IS KEPT EXACTLY, including the masking check above, and
    -- only the outcome is inverted.  That is deliberate: the fixture's value
    -- was never the refusal, it was that the outcome must not depend on which
    -- correction code the engine surfaced, and that is now asserted in the
    -- other direction.
    -- ===================================================================
    v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_inv);
    perform pg_temp.iss_assert(
      v_verdict#>>'{detail,tsfin,state}'='TSFIN_STALE'
      and coalesce((v_verdict->>'wholly_sealed_source_self_bill')::boolean,false)
      and v_verdict->'tsfin_blocker_codes'='[]'::jsonb
      and v_verdict->>'tsfin_invoice_disposition'
          ='WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE'
      and v_verdict->'tsfin_deferred_blocker_codes'
          ='["WEEKLY_SOURCE_ISSUE_TSFIN_STALE"]'::jsonb
      -- the Candidate-pay disposition, independently correct and still blocking
      and v_verdict->'tsfin_payment_blocker_codes'
          ='["WEEKLY_SOURCE_PAYMENT_TSFIN_STALE"]'::jsonb,
      'WP-28 F2 / R8A 1','a stale Candidate-pay record is still an admission '
        ||'predicate for a wholly sealed source self-bill, or its pay '
        ||'disposition was lost: '||v_verdict::text);

    -- The REAL asynchronous gate.
    perform pg_temp.iss_assert(
      pg_temp.iss_async_can_issue(v_inv)
      and pg_temp.iss_async_blockers(v_inv)='[]'::jsonb,
      'WP-28 F2 / R8A 1','the real asynchronous route refused a wholly sealed '
        ||'source self-bill for a Candidate-pay reason: '
        ||pg_temp.iss_async_blockers(v_inv)::text);

    -- The REAL batch issue classifier the Office list is built from.
    perform pg_temp.iss_assert(
      pg_temp.iss_classifier_can_issue(v_inv),
      'WP-28 F2 / R8A 1','the real batch classifier withheld a wholly sealed '
        ||'source self-bill for a Candidate-pay reason');

    -- The REAL direct owner.  This is acceptance row ISS-013, now live.
    v_direct:=pg_temp.iss_direct_probe(v_inv);
    perform pg_temp.iss_assert(
      v_direct like 'ISSUED%',
      'WP-28 F2 / R8A 1 (ISS-013)','the real direct owner returned % for a '
        ||'wholly sealed source self-bill with a stale Candidate-pay record: '
        ||v_direct);

    -- ORDERING, in the new direction.  The masking check above proved the
    -- engine does NOT surface INVOICE_CORRECTION_TSFIN_STALE first in this
    -- shape.  The outcome must be the same when it is surfaced first, last, or
    -- not at all, so the boundary is not an artefact of ordering either.
    perform pg_temp.iss_assert(
      private.weekly_source_invoice_issue_blockers_v1(v_inv,
        array['INVOICE_CORRECTION_TSFIN_STALE']::text[])=array[]::text[]
      and private.weekly_source_invoice_issue_blockers_v1(v_inv,
        array['INVOICE_CORRECTION_TSFIN_STALE',
              'INVOICE_CORRECTION_TARGET_STREAM_MISMATCH']::text[])=array[]::text[]
      and private.weekly_source_invoice_issue_blockers_v1(v_inv,
        array['INVOICE_CORRECTION_TARGET_STREAM_MISMATCH',
              'INVOICE_CORRECTION_TSFIN_STALE']::text[])=array[]::text[]
      and private.weekly_source_invoice_issue_blockers_v1(v_inv,
        array[]::text[])=array[]::text[]
      -- and the boundary does NOT widen to a source-side or unit code
      and private.weekly_source_invoice_issue_blockers_v1(v_inv,
        array['INVOICE_CORRECTION_MEMBER_MISSING']::text[])
        =array['INVOICE_CORRECTION_MEMBER_MISSING']::text[]
      and private.weekly_source_invoice_issue_blockers_v1(v_inv,
        array['INVOICE_CORRECTION_UNIT_INVALID']::text[])
        =array['INVOICE_CORRECTION_UNIT_INVALID']::text[],
      'WP-28 F2 / R8A 1','the Candidate-pay boundary depends on correction-code '
        ||'ordering, or widened to a code R8A keeps blocking');

    raise exception 'WP28_RELEASE_STALE_ROUTE';
  exception when raise_exception then
    if sqlerrm<>'WP28_RELEASE_STALE_ROUTE' then raise; end if;
  end;
  perform pg_temp.iss_assert(
    not exists(select 1 from public.timesheets_financials financial
               where financial.stale_reason='WP28_STALE_ROUTE_PROOF'),
    'WP-28 F2','the stale fixture was not released again');

  -- -----------------------------------------------------------------------
  -- F3.  The two routes must apply the SAME skip rule to an unsigned QR
  -- Timesheet.  24 section 11 lists Candidate PDF/signature among the four
  -- ordinary evidence categories an admitted source self-bill skips, and
  -- 24 section 7 forbids Candidate evidence delaying a valid source invoice,
  -- so the asynchronous behaviour is the correct one and the direct route is
  -- corrected to match it.
  -- -----------------------------------------------------------------------
  perform pg_temp.iss_assert(
    (select count(*) from public.v_ts_invoice_precheck
     where precheck_status='BLOCK_UNSIGNED')=0,
    'WP-28 F3','a BLOCK_UNSIGNED precheck status now exists; re-check the limb');
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_skippable_code_v1('QR_TIMESHEET_UNSIGNED')
    and private.weekly_source_invoice_issue_skippable_reason_v1(
      'TS 11111111-1111-4111-8111-111111111111: precheck blocker BLOCK_QR_UNSIGNED'),
    'WP-28 F3','the two routes still disagree about an unsigned QR Timesheet');
  -- and the same two routes must still REFUSE a status neither of them may skip
  perform pg_temp.iss_assert(
    not private.weekly_source_invoice_issue_skippable_reason_v1(
      'TS 11111111-1111-4111-8111-111111111111: precheck blocker BLOCK_SOMETHING_ELSE')
    and not private.weekly_source_invoice_issue_skippable_reason_v1(
      'TS 11111111-1111-4111-8111-111111111111: precheck missing')
    and not private.weekly_source_invoice_issue_skippable_reason_v1(
      'TS 11111111-1111-4111-8111-111111111111: summary missing'),
    'WP-28 F3','the corrected direct-route limb widened past Candidate evidence');

  select s.invoice_id into v_inv
  from (select distinct binding.invoice_id
        from public.weekly_source_invoice_line_bindings binding
        where binding.state='CURRENT') s
  join public.invoices invoice on invoice.id=s.invoice_id
  where invoice.status='DRAFT' and invoice.on_hold_reason is null
    and coalesce((private.weekly_source_invoice_issue_validate_v1(s.invoice_id)
                  ->>'ok')::boolean,false)
    and private.weekly_source_invoice_tsfin_state_v1(s.invoice_id)->>'state'
        <>'TSFIN_STALE'
  order by s.invoice_id limit 1;
  perform pg_temp.iss_assert(v_inv is not null,
    'WP-28 F3','the corpus has no accepted source invoice to make QR-unsigned');
  select line.timesheet_id into v_ts from public.invoice_lines line
  where line.invoice_id=v_inv and line.timesheet_id is not null
  order by line.timesheet_id limit 1;
  begin
    update public.timesheets
    set qr_status='PENDING'::public.timesheet_qr_status_enum,
        qr_token=coalesce(qr_token,'wp28-token'),
        qr_generated_at=coalesce(qr_generated_at,now()),
        qr_scanned_at=null
    where timesheet_id=v_ts and is_current;
    perform pg_temp.iss_assert(
      (select precheck_status from public.v_ts_invoice_precheck
       where timesheet_id=v_ts)='BLOCK_QR_UNSIGNED',
      'WP-28 F3','the unsigned QR fixture did not reach BLOCK_QR_UNSIGNED');
    v_direct:=pg_temp.iss_direct_probe(v_inv);
    perform pg_temp.iss_assert(
      pg_temp.iss_async_can_issue(v_inv) and v_direct like 'ISSUED%',
      'WP-28 F3','the two real routes disagreed on an unsigned QR Timesheet: async='
        ||pg_temp.iss_async_can_issue(v_inv)::text||' direct='||v_direct);
    raise exception 'WP28_RELEASE_QR';
  exception when raise_exception then
    if sqlerrm<>'WP28_RELEASE_QR' then raise; end if;
  end;

  -- -----------------------------------------------------------------------
  -- F1, RULED.  HANDOVER 2 round 8 Part 2.1 and Part 6.6 answered the round-6
  -- escalation WP-28 left withheld: the exact awaiting-first-authorisation case
  -- does NOT block source-invoice issue, every other expected-but-missing case
  -- does, and the outcome must be an explicit typed disposition rather than a
  -- consequence of correction-code ordering.  WP-33 implements that, and these
  -- assertions replace WP-28's "asserted as withheld" pair.
  --
  -- The withheld keys are GONE from the verdict.  Asserting their absence is
  -- deliberate: a future revision that reinstates them has reinstated a
  -- fail-closed reason naming a question the approver has answered, which
  -- standing rule 12 forbids.
  -- -----------------------------------------------------------------------
  select s.invoice_id into v_inv
  from (select distinct binding.invoice_id
        from public.weekly_source_invoice_line_bindings binding
        where binding.state='CURRENT') s
  join public.invoices invoice on invoice.id=s.invoice_id
  where invoice.status='DRAFT' and invoice.on_hold_reason is null
    and private.weekly_source_invoice_tsfin_state_v1(s.invoice_id)->>'state'
        ='TSFIN_EXPECTED_BUT_MISSING'
  order by s.invoice_id limit 1;
  perform pg_temp.iss_assert(v_inv is not null,
    'WP-33 F1','the corpus has no expected-but-missing source invoice');
  v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_inv);
  perform pg_temp.iss_assert(
    v_verdict->'tsfin_blocker_codes'='[]'::jsonb
    and v_verdict->>'tsfin_invoice_disposition'
        ='WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT'
    and v_verdict->'tsfin_payment_blocker_codes'
        ='["WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING"]'::jsonb
    and v_verdict->'tsfin_withheld_blocker_codes' is null
    and v_verdict->'tsfin_withheld_reason' is null,
    'WP-33 F1','the awaiting-first-authorisation exemption is not the explicit '
      ||'typed disposition, or the retired withheld keys are back: '||v_verdict::text);
  -- The expected-but-missing FACT is RETAINED, not replaced (round 8 Part 2.1
  -- first bullet).  counts.expected_but_missing keeps its former value and is
  -- the exact sum of the two new counts.
  perform pg_temp.iss_assert(
    (private.weekly_source_invoice_tsfin_state_v1(v_inv)
       #>>'{counts,expected_but_missing}')::integer>0
    and (private.weekly_source_invoice_tsfin_state_v1(v_inv)
       #>>'{counts,expected_but_missing}')::integer
        =(private.weekly_source_invoice_tsfin_state_v1(v_inv)
       #>>'{counts,awaiting_first_authorisation}')::integer
        +(private.weekly_source_invoice_tsfin_state_v1(v_inv)
       #>>'{counts,expected_but_missing_unexplained}')::integer,
    'WP-33 F1','the expected-but-missing fact was not retained and split exactly');
  perform pg_temp.iss_assert(
    pg_temp.iss_async_can_issue(v_inv)
    and pg_temp.iss_classifier_can_issue(v_inv),
    'WP-33 F1','the asynchronous route refused a week that is genuinely awaiting '
      ||'its first authorisation, which round 8 Part 2.1 exempts');
  v_direct:=pg_temp.iss_direct_probe(v_inv);
  perform pg_temp.iss_assert(v_direct like 'ISSUED%',
    'WP-33 F1','the direct route refused a week that is genuinely awaiting its '
      ||'first authorisation, which round 8 Part 2.1 exempts: '||v_direct);
  -- ORDERING INDEPENDENCE, at the seam.  The exemption must survive the
  -- ordinary correction engine surfacing INVOICE_CORRECTION_TSFIN_MISSING
  -- FIRST, which is exactly the shape the masking hid before WP-33.
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_blockers_v1(v_inv,
      array['INVOICE_CORRECTION_TSFIN_MISSING',
            'INVOICE_CORRECTION_TARGET_STREAM_MISMATCH']::text[])=array[]::text[]
    and private.weekly_source_invoice_issue_blockers_v1(v_inv,
      array['INVOICE_CORRECTION_TARGET_STREAM_MISMATCH',
            'INVOICE_CORRECTION_TSFIN_MISSING']::text[])=array[]::text[]
    and private.weekly_source_invoice_issue_blockers_v1(v_inv,
      array['INVOICE_CORRECTION_TSFIN_MISSING']::text[])=array[]::text[]
    and private.weekly_source_invoice_issue_blockers_v1(v_inv,
      array['INVOICE_CORRECTION_TSFIN_NOT_READY']::text[])=array[]::text[]
    and private.weekly_source_invoice_issue_blockers_v1(v_inv,
      array[]::text[])=array[]::text[],
    'WP-33 F1','the exemption depends on which correction code is surfaced first');
  -- and it must NOT widen to the codes round 8 Part 2.1 keeps blocking.
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_blockers_v1(v_inv,
      array['INVOICE_CORRECTION_MEMBER_MISSING']::text[])
      =array['INVOICE_CORRECTION_MEMBER_MISSING']::text[]
    and private.weekly_source_invoice_issue_blockers_v1(v_inv,
      array['INVOICE_CORRECTION_UNIT_INVALID']::text[])
      =array['INVOICE_CORRECTION_UNIT_INVALID']::text[]
    and private.weekly_source_invoice_issue_blockers_v1(v_inv,
      array['INVOICE_CORRECTION_CHAIN_CYCLE']::text[])
      =array['INVOICE_CORRECTION_CHAIN_CYCLE']::text[],
    'WP-33 F1','the Candidate-pay boundary widened to a code that R8A section 1 '
      ||'keeps blocking');
  -- INVOICE_CORRECTION_TSFIN_STALE is deliberately NOT in the list above any
  -- more: R8A section 1 made it a Candidate-pay code like the other two, and
  -- this invoice is wholly sealed.  Its own bound is asserted in the WP-33
  -- block below, where an invoice that is no longer wholly sealed keeps it.

  -- -----------------------------------------------------------------------
  -- The refusal predicate itself, driven over EVERY branch including the
  -- three-valued JSON reads.  `counts` absent must not be read as "zero".
  -- -----------------------------------------------------------------------
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_tsfin_refusal_v1(null)->'blocker_codes'
      ='["WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED"]'::jsonb
    and private.weekly_source_invoice_tsfin_refusal_v1('"TSFIN_STALE"'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED'
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_PRESENT"}'::jsonb)->'blocker_codes'
      ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED'
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_PRESENT","counts":{"stale":null,"expected_but_missing":null}}'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED'
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_PRESENT","counts":{"stale":"0","expected_but_missing":"0"}}'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED'
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":null,"counts":{"stale":0,"expected_but_missing":0}}'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED'
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_SOMETHING_NEW","counts":{"stale":0,"expected_but_missing":0}}'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED',
    'WP-28','an unreadable financial-record state did not fail closed');
  -- WP-33 changes the document SHAPE: the three new counts and the state
  -- owner's own invoice_disposition_claim are now part of what makes a state
  -- readable at all, so the documents below carry them.  WP-28's three-valued
  -- assertions above keep their exact meaning but are now written as
  -- CONTAINMENT rather than whole-array equality, because a document that omits
  -- the new keys now also disagrees with its own (absent) disposition claim and
  -- correctly returns WEEKLY_SOURCE_ISSUE_TSFIN_DISPOSITION_DISAGREEMENT beside
  -- WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED.  Both are fail-closed; the
  -- assertion is about the state being unreadable, not about the array's
  -- cardinality.
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_PRESENT","counts":{"members":1,"stale":1,"expected_but_missing":0,"awaiting_first_authorisation":0,"expected_but_missing_unexplained":0},"invoice_disposition_claim":"WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED"}'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STALE'
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_STALE","counts":{"members":1,"stale":0,"expected_but_missing":0,"awaiting_first_authorisation":0,"expected_but_missing_unexplained":0},"invoice_disposition_claim":"WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED"}'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STALE',
    'WP-28','the stale limbs are not independent, so a reordering could unblock');
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_PRESENT","counts":{"members":1,"stale":0,"expected_but_missing":0,"awaiting_first_authorisation":0,"expected_but_missing_unexplained":0},"invoice_disposition_claim":"WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR"}'::jsonb)
      ->'blocker_codes'='[]'::jsonb
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_NOT_APPLICABLE_PROVED","counts":{"members":1,"stale":0,"expected_but_missing":0,"awaiting_first_authorisation":0,"expected_but_missing_unexplained":0},"invoice_disposition_claim":"WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR"}'::jsonb)
      ->'blocker_codes'='[]'::jsonb
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_NO_MEMBERS","counts":{"members":0,"stale":0,"expected_but_missing":0,"awaiting_first_authorisation":0,"expected_but_missing_unexplained":0},"invoice_disposition_claim":"WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR"}'::jsonb)
      ->'blocker_codes'='[]'::jsonb,
    'WP-28','a clean financial-record state acquired a refusal');

  -- An ORDINARY invoice must still receive both seam arguments unchanged.
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_reasons_v1(
      pg_catalog.jsonb_build_object('is_source_invoice',false),
      array['TS 11111111-1111-4111-8111-111111111111: missing timesheet PDF']::text[])
      =array['TS 11111111-1111-4111-8111-111111111111: missing timesheet PDF']::text[],
    'WP-28','an ordinary invoice lost a reason through the direct seam');
  -- A verdict that does not carry the array at all fails closed, and under
  -- WP-33 it fails closed TWICE: once because the code array is missing, and
  -- once because the typed disposition is missing.  Both are required: a seam
  -- that inferred "no codes therefore issue" would be reading an omission as an
  -- outcome, which is what round 8 Part 2.1 forbids.
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_reasons_v1(
      pg_catalog.jsonb_build_object('is_source_invoice',true,'ok',true),
      array[]::text[])
      =array['WEEKLY SOURCE: WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED',
             'WEEKLY SOURCE: WEEKLY_SOURCE_ISSUE_TSFIN_DISPOSITION_UNDETERMINED']::text[],
    'WP-28','a verdict with no financial-record array did not fail closed');
  -- A verdict carrying an EMPTY code array but no disposition must also fail
  -- closed on BOTH seams: this is the exact forgery the exemption would invite.
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_reasons_v1(
      pg_catalog.jsonb_build_object('is_source_invoice',true,'ok',true,
        'tsfin_blocker_codes','[]'::jsonb),
      array[]::text[])
      =array['WEEKLY SOURCE: WEEKLY_SOURCE_ISSUE_TSFIN_DISPOSITION_UNDETERMINED']::text[]
    and private.weekly_source_invoice_issue_reasons_v1(
      pg_catalog.jsonb_build_object('is_source_invoice',true,'ok',true,
        'tsfin_blocker_codes','[]'::jsonb,
        'tsfin_invoice_disposition','WEEKLY_SOURCE_INVOICE_TSFIN_SOMETHING_NEW'),
      array[]::text[])
      =array['WEEKLY SOURCE: WEEKLY_SOURCE_ISSUE_TSFIN_DISPOSITION_UNDETERMINED']::text[]
    and private.weekly_source_invoice_issue_reasons_v1(
      pg_catalog.jsonb_build_object('is_source_invoice',true,'ok',true,
        'tsfin_blocker_codes','[]'::jsonb,
        'tsfin_invoice_disposition','WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED'),
      array[]::text[])
      =array['WEEKLY SOURCE: WEEKLY_SOURCE_ISSUE_TSFIN_DISPOSITION_UNDETERMINED']::text[]
    and private.weekly_source_invoice_issue_reasons_v1(
      pg_catalog.jsonb_build_object('is_source_invoice',true,'ok',true,
        'tsfin_blocker_codes','["WEEKLY_SOURCE_ISSUE_TSFIN_STALE"]'::jsonb,
        'tsfin_invoice_disposition',
          'WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT'),
      array[]::text[])
      =array['WEEKLY SOURCE: WEEKLY_SOURCE_ISSUE_TSFIN_STALE',
             'WEEKLY SOURCE: WEEKLY_SOURCE_ISSUE_TSFIN_DISPOSITION_UNDETERMINED']::text[]
    and private.weekly_source_invoice_issue_reasons_v1(
      pg_catalog.jsonb_build_object('is_source_invoice',true,'ok',true,
        'tsfin_blocker_codes','[]'::jsonb,
        'tsfin_invoice_disposition',
          'WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT'),
      array[]::text[])=array[]::text[],
    'WP-33','the direct seam accepted a disposition it could not verify against '
      ||'the code array it came with');
end;
$wp28_routes$;

-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- WP-33.  HANDOVER 2 CORRECTION ADDENDUM R8A section 1 — the Candidate-pay
-- admission-predicate boundary, and HANDOVER 2 round 8 Part 2.1's explicit
-- typed first-authorisation disposition, executed through the REAL routes.
--
-- R8A section 1 is LATER authority than round 8, accepted by the product owner.
-- It withdraws round 8 Part 2.2 and Part 6 item 6 for a WHOLLY SEALED
-- source-backed self-bill and restores 02 CONTROLLING POLICY section 15:
--
--   "Candidate Timesheet authorisation, Candidate-pay TSFIN existence/
--    currentness/status, READY_FOR_INVOICE, pay holds, advances, settlement and
--    pay query/reconciliation state are never self-bill admission predicates."
--   "A stale or missing Candidate-pay TSFIN likewise does not block an
--    otherwise-valid immutable final-source invoice movement.  It blocks
--    Candidate-pay publication, readiness or reconciliation wherever those
--    owners require current pay evidence."
--   "A stale, missing, invalid or ambiguous final-source movement, source
--    manifest/revision, source calculation or price comparison, Client/placement
--    binding, invoice binding, correction shape, amount, VAT, total or required
--    source lineage blocks the source invoice."
--   "Mixed, unbound or tampered invoices fail closed."
--
-- R8A requires acceptance to cover NINE cases separately, and: "Each fixture
-- must prove both invoice disposition and the independently correct
-- Candidate-pay disposition."  The nine are A1..A9 below.  The Candidate-pay
-- disposition is tsfin_payment_blocker_codes, which R8A leaves fail-closed and
-- which no branch of the boundary is permitted to empty.
--
-- WHOLLY SEALED IS A POSITIVE TEST (R8A precision point 1).  Every case that
-- issues asserts wholly_sealed_source_self_bill is explicitly true, and A10
-- proves the boundary is not inherited once it is false.
--
-- Every fixture runs inside a raise-to-undo subtransaction and every
-- public.invoice_issue_one call runs inside pg_temp.iss_direct_probe.  The
-- subjects are PINNED before the first fixture: choosing them by state inside
-- each case silently re-selects a different invoice once a fixture has changed
-- that state, which was executed and observed while writing this block.
-- ---------------------------------------------------------------------------
do $wp33_routes$
declare
  v_exempt_inv uuid;
  v_exempt_ts uuid;
  v_present_inv uuid;
  v_present_ts uuid;
  v_contract uuid;
  v_verdict jsonb;
  v_direct text;
  v_proof jsonb;
  v_result text;
begin
  select s.invoice_id,
    (select pg_catalog.min(line.timesheet_id::text)::uuid
     from public.invoice_lines line
     where line.invoice_id=s.invoice_id and line.timesheet_id is not null)
    into v_exempt_inv,v_exempt_ts
  from (select distinct binding.invoice_id
        from public.weekly_source_invoice_line_bindings binding
        where binding.state='CURRENT') s
  join public.invoices invoice on invoice.id=s.invoice_id
  where invoice.status='DRAFT' and invoice.on_hold_reason is null
    and private.weekly_source_invoice_tsfin_state_v1(s.invoice_id)->>'state'
        ='TSFIN_EXPECTED_BUT_MISSING'
  order by s.invoice_id limit 1;
  select s.invoice_id,
    (select pg_catalog.min(line.timesheet_id::text)::uuid
     from public.invoice_lines line
     where line.invoice_id=s.invoice_id and line.timesheet_id is not null)
    into v_present_inv,v_present_ts
  from (select distinct binding.invoice_id
        from public.weekly_source_invoice_line_bindings binding
        where binding.state='CURRENT') s
  join public.invoices invoice on invoice.id=s.invoice_id
  where invoice.status='DRAFT' and invoice.on_hold_reason is null
    and private.weekly_source_invoice_tsfin_state_v1(s.invoice_id)->>'state'
        ='TSFIN_PRESENT'
  order by s.invoice_id limit 1;
  perform pg_temp.iss_assert(
    v_exempt_inv is not null and v_exempt_ts is not null
    and v_present_inv is not null and v_present_ts is not null,
    'WP-33','the corpus does not carry both an expected-but-missing and a '
      ||'present source invoice, so the nine cases cannot be proved');
  select timesheet_row.contract_id into v_contract
  from public.timesheets timesheet_row where timesheet_row.timesheet_id=v_exempt_ts;

  -- =======================================================================
  -- The first-authorisation proof itself.  R8A keeps AWAITING_FIRST_AUTHORISATION
  -- as its own named state ("AWAITING_FIRST_AUTHORISATION permits the
  -- otherwise-valid source invoice to proceed while Candidate payment remains
  -- blocked"), so the proof must still be exact even though the whole
  -- Candidate-pay class is now non-blocking for the invoice.
  -- =======================================================================
  v_proof:=private.weekly_source_invoice_tsfin_first_authorisation_v1(v_exempt_ts);
  perform pg_temp.iss_assert(
    coalesce((v_proof->>'proved')::boolean,false)
    and v_proof->'reason_codes'='[]'::jsonb
    and (v_proof#>>'{facts,root_authorisation_rows}')::integer=0
    and (v_proof#>>'{facts,root_authorisation_rows_by_booking_id}')::integer=0
    and (v_proof#>>'{facts,ordinary_authorisation_stamps}')::integer=0
    and (v_proof#>>'{facts,financial_rows_ever}')::integer=0
    and (v_proof#>>'{facts,withdrawal_receipts}')::integer=0
    and (v_proof#>>'{facts,entitlement_heads}')::integer=0
    and coalesce((v_proof#>>'{facts,context_ok}')::boolean,false)
    and coalesce((v_proof#>>'{facts,context_family_agrees}')::boolean,false),
    'WP-33','the corpus week is not positively proved to be awaiting its first '
      ||'authorisation: '||v_proof::text);
  perform pg_temp.iss_assert(
    coalesce((private.weekly_source_invoice_tsfin_first_authorisation_v1(null)
              ->>'proved')::boolean,true) is false
    and coalesce((private.weekly_source_invoice_tsfin_first_authorisation_v1(
              '11111111-1111-4111-8111-111111111111'::uuid)
              ->>'proved')::boolean,true) is false,
    'WP-33','a null or unknown Timesheet was proved to be awaiting its first '
      ||'authorisation');

  -- =======================================================================
  -- A1  AWAITING FIRST AUTHORISATION.
  --     invoice: ISSUES, disposition AWAITING_FIRST_AUTHORISATION_EXEMPT
  --     pay:     BLOCKED, WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING
  -- =======================================================================
  v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_exempt_inv);
  v_direct:=pg_temp.iss_direct_probe(v_exempt_inv);
  perform pg_temp.iss_assert(
    coalesce((v_verdict->>'wholly_sealed_source_self_bill')::boolean,false)
    and v_verdict->'tsfin_blocker_codes'='[]'::jsonb
    and v_verdict->>'tsfin_invoice_disposition'
        ='WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT'
    and v_verdict->'tsfin_payment_blocker_codes'
        ='["WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING"]'::jsonb
    and v_verdict->'tsfin_withheld_blocker_codes' is null
    and pg_temp.iss_async_can_issue(v_exempt_inv)
    and pg_temp.iss_classifier_can_issue(v_exempt_inv)
    and v_direct like 'ISSUED%',
    'WP-33 A1 awaiting first authorisation','invoice or pay disposition wrong: '
      ||'direct='||v_direct||' verdict='||v_verdict::text);
  -- The expected-but-missing FACT is RETAINED and split, never replaced.
  perform pg_temp.iss_assert(
    (private.weekly_source_invoice_tsfin_state_v1(v_exempt_inv)
       #>>'{counts,expected_but_missing}')::integer>0
    and (private.weekly_source_invoice_tsfin_state_v1(v_exempt_inv)
       #>>'{counts,expected_but_missing}')::integer
        =(private.weekly_source_invoice_tsfin_state_v1(v_exempt_inv)
       #>>'{counts,awaiting_first_authorisation}')::integer
        +(private.weekly_source_invoice_tsfin_state_v1(v_exempt_inv)
       #>>'{counts,expected_but_missing_unexplained}')::integer
    and private.weekly_source_invoice_tsfin_state_v1(v_exempt_inv)->'members'->0
       ->>'state'='TSFIN_EXPECTED_BUT_MISSING'
    and (private.weekly_source_invoice_tsfin_state_v1(v_exempt_inv)->'members'->0
       ->>'blocks')::boolean,
    'WP-33 A1','the exemption overwrote the member financial-record fact '
      ||'instead of sitting beside it');

  -- =======================================================================
  -- A2  ABSENT TSFIN that is NOT awaiting a first authorisation.  Four shapes,
  --     each proved separately: ordinarily authorised; previously authorised
  --     (which is also the 24 section 4.1A withdrawal shape); previously
  --     applicable financial record; and unprovable context.
  --     invoice: ISSUES (R8A: Candidate-pay is never an admission predicate),
  --              disposition CANDIDATE_PAY_NOT_ADMISSION_PREDICATE - NOT the
  --              first-authorisation name, so the two stay distinguishable
  --     pay:     BLOCKED, and the first-authorisation proof still refuses
  -- =======================================================================
  begin
    update public.timesheets set authorised_at_server=pg_catalog.transaction_timestamp()
    where timesheet_id=v_exempt_ts;
    v_proof:=private.weekly_source_invoice_tsfin_first_authorisation_v1(v_exempt_ts);
    v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_exempt_inv);
    v_direct:=pg_temp.iss_direct_probe(v_exempt_inv);
    perform pg_temp.iss_assert(
      coalesce((v_proof->>'proved')::boolean,true) is false
      and v_proof->'reason_codes'
          ? 'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_ORDINARY_AUTHORISATION_STAMP'
      and v_verdict->>'tsfin_invoice_disposition'
          ='WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE'
      and v_verdict->'tsfin_deferred_blocker_codes'
          ='["WEEKLY_SOURCE_ISSUE_TSFIN_EXPECTED_BUT_MISSING"]'::jsonb
      and v_verdict->'tsfin_payment_blocker_codes'
          ='["WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING"]'::jsonb
      and pg_temp.iss_async_can_issue(v_exempt_inv)
      and pg_temp.iss_classifier_can_issue(v_exempt_inv)
      and v_direct like 'ISSUED%',
      'WP-33 A2 absent TSFIN (ordinarily authorised)','invoice or pay '
        ||'disposition wrong: direct='||v_direct||' proof='||v_proof::text);
    raise exception 'WP33_UNDO_A2A';
  exception when raise_exception then
    if sqlerrm<>'WP33_UNDO_A2A' then raise; end if;
  end;
  begin
    insert into public.weekly_source_root_authorisations(
      root_timesheet_id,family_booking_id,timesheet_version,
      authorisation_generation,authorised_row_signature,authorised_by_user_id)
    select timesheet_row.timesheet_id,timesheet_row.booking_id,timesheet_row.version,
      1,'WP33-VERIFIER-SIGNATURE','a0000000-0000-4000-8000-000000000001'
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=v_exempt_ts;
    v_proof:=private.weekly_source_invoice_tsfin_first_authorisation_v1(v_exempt_ts);
    v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_exempt_inv);
    perform pg_temp.iss_assert(
      coalesce((v_proof->>'proved')::boolean,true) is false
      and v_proof->'reason_codes'
          ? 'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_PREVIOUSLY_AUTHORISED'
      and v_verdict->>'tsfin_invoice_disposition'
          ='WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE'
      and v_verdict->'tsfin_payment_blocker_codes'
          ='["WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING"]'::jsonb
      and pg_temp.iss_async_can_issue(v_exempt_inv),
      'WP-33 A2 absent TSFIN (previously authorised, 24 s4.1A)','wrong: '
        ||v_proof::text);
    raise exception 'WP33_UNDO_A2B';
  exception when raise_exception then
    if sqlerrm<>'WP33_UNDO_A2B' then raise; end if;
  end;
  begin
    insert into public.timesheets_financials(timesheet_id,timesheet_version,is_current)
    select timesheet_row.timesheet_id,timesheet_row.version,false
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=v_exempt_ts;
    v_proof:=private.weekly_source_invoice_tsfin_first_authorisation_v1(v_exempt_ts);
    perform pg_temp.iss_assert(
      private.weekly_source_invoice_tsfin_state_v1(v_exempt_inv)->>'state'
        ='TSFIN_EXPECTED_BUT_MISSING'
      and coalesce((v_proof->>'proved')::boolean,true) is false
      and v_proof->'reason_codes'
          ? 'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_PREVIOUSLY_APPLICABLE_FINANCIAL'
      and private.weekly_source_invoice_issue_validate_v1(v_exempt_inv)
          ->'tsfin_payment_blocker_codes'
          ='["WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING"]'::jsonb
      and pg_temp.iss_async_can_issue(v_exempt_inv),
      'WP-33 A2 absent TSFIN (previously applicable financial)','wrong: '
        ||v_proof::text);
    raise exception 'WP33_UNDO_A2C';
  exception when raise_exception then
    if sqlerrm<>'WP33_UNDO_A2C' then raise; end if;
  end;
  begin
    update public.contracts set candidate_id=null where id=v_contract;
    v_proof:=private.weekly_source_invoice_tsfin_first_authorisation_v1(v_exempt_ts);
    perform pg_temp.iss_assert(
      coalesce((v_proof->>'proved')::boolean,true) is false
      and v_proof->'reason_codes'
          ? 'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_CONTEXT_UNAVAILABLE'
      and private.weekly_source_invoice_issue_validate_v1(v_exempt_inv)
          ->'tsfin_payment_blocker_codes'
          ='["WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING"]'::jsonb
      and pg_temp.iss_async_can_issue(v_exempt_inv),
      'WP-33 A2 absent TSFIN (context unprovable)','wrong: '||v_proof::text);
    raise exception 'WP33_UNDO_A2D';
  exception when raise_exception then
    if sqlerrm<>'WP33_UNDO_A2D' then raise; end if;
  end;

  -- =======================================================================
  -- A3  STALE TSFIN.  This is acceptance row ISS-013 and the case the product
  --     owner countermanded.  WP-33 measured that the installed predicate keys
  --     on public.timesheets_financials.is_stale alone, reads no reason and
  --     catches no source-side staleness, so it was catching exactly the class
  --     R8A says was never an admission predicate.
  --     invoice: ISSUES, disposition CANDIDATE_PAY_NOT_ADMISSION_PREDICATE
  --     pay:     BLOCKED, WEEKLY_SOURCE_PAYMENT_TSFIN_STALE
  -- =======================================================================
  begin
    update public.timesheets_financials set is_stale=true,stale_reason='UNLOCKED_BY_CREDIT'
    where timesheet_id=v_present_ts and is_current;
    v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_present_inv);
    v_direct:=pg_temp.iss_direct_probe(v_present_inv);
    perform pg_temp.iss_assert(
      private.weekly_source_invoice_tsfin_state_v1(v_present_inv)->>'state'
        ='TSFIN_STALE'
      and coalesce((v_verdict->>'wholly_sealed_source_self_bill')::boolean,false)
      and v_verdict->'tsfin_blocker_codes'='[]'::jsonb
      and v_verdict->>'tsfin_invoice_disposition'
          ='WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE'
      and v_verdict->'tsfin_deferred_blocker_codes'
          ='["WEEKLY_SOURCE_ISSUE_TSFIN_STALE"]'::jsonb
      and v_verdict->'tsfin_payment_blocker_codes'
          ='["WEEKLY_SOURCE_PAYMENT_TSFIN_STALE"]'::jsonb
      and pg_temp.iss_async_can_issue(v_present_inv)
      and pg_temp.iss_async_blockers(v_present_inv)='[]'::jsonb
      and pg_temp.iss_classifier_can_issue(v_present_inv)
      and v_direct like 'ISSUED%',
      'WP-33 A3 stale TSFIN (ISS-013)','invoice or pay disposition wrong: '
        ||'direct='||v_direct||' verdict='||v_verdict::text);
    raise exception 'WP33_UNDO_A3';
  exception when raise_exception then
    if sqlerrm<>'WP33_UNDO_A3' then raise; end if;
  end;

  -- =======================================================================
  -- A4  CURRENT TSFIN.  invoice ISSUES; pay disposition is CLEAR.
  -- =======================================================================
  v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_present_inv);
  v_direct:=pg_temp.iss_direct_probe(v_present_inv);
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_tsfin_state_v1(v_present_inv)->>'state'
      ='TSFIN_PRESENT'
    and v_verdict->'tsfin_blocker_codes'='[]'::jsonb
    and v_verdict->>'tsfin_invoice_disposition'='WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR'
    and v_verdict->'tsfin_payment_blocker_codes'='[]'::jsonb
    and v_verdict->'tsfin_deferred_blocker_codes'='[]'::jsonb
    and pg_temp.iss_async_can_issue(v_present_inv)
    and v_direct like 'ISSUED%',
    'WP-33 A4 current TSFIN','invoice or pay disposition wrong: '||v_direct);

  -- =======================================================================
  -- A5  STALE SOURCE REVISION.  The source side, which R8A keeps blocking.
  --     invoice: BLOCKED, WEEKLY_SOURCE_ISSUE_STALE_FINAL_REVISION, ok=false
  --     pay:     unchanged - the Candidate-pay state is untouched, which is the
  --              proof that source-side and Candidate-pay staleness are
  --              different facts judged by different limbs
  -- =======================================================================
  begin
    update public.weekly_source_final_revisions set state='SUPERSEDED'
    where id in (
      select manifest.final_revision_id
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_manifest_movements manifest_movement
        on manifest_movement.billing_movement_id=binding.billing_movement_id
      join public.weekly_source_client_manifests manifest
        on manifest.id=manifest_movement.client_manifest_id
      where binding.invoice_id=v_present_inv and binding.state='CURRENT');
    v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_present_inv);
    v_direct:=pg_temp.iss_direct_probe(v_present_inv);
    perform pg_temp.iss_assert(
      coalesce((v_verdict->>'ok')::boolean,true) is false
      and coalesce((v_verdict->>'wholly_sealed_source_self_bill')::boolean,true)
          is false
      and v_verdict->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_STALE_FINAL_REVISION'
      and private.weekly_source_invoice_tsfin_state_v1(v_present_inv)->>'state'
          ='TSFIN_PRESENT'
      and v_verdict->'tsfin_payment_blocker_codes'='[]'::jsonb
      and not pg_temp.iss_async_can_issue(v_present_inv)
      and not pg_temp.iss_classifier_can_issue(v_present_inv)
      and v_direct like 'ON_HOLD%'
      and v_direct like '%WEEKLY_SOURCE_ISSUE_STALE_FINAL_REVISION%',
      'WP-33 A5 stale source revision','a source-side defect did not block, or '
        ||'it disturbed the Candidate-pay disposition: '||v_direct);
    raise exception 'WP33_UNDO_A5';
  exception when raise_exception then
    if sqlerrm<>'WP33_UNDO_A5' then raise; end if;
  end;

  -- =======================================================================
  -- A6  SOURCE CALCULATION / AMOUNT EVIDENCE.  invoice BLOCKED; pay untouched.
  --
  --     R8A names "source calculation or price comparison ... amount, VAT,
  --     total" in one list of source-side defects that block.  The price
  --     comparison limb itself CANNOT be driven from a fixture: both
  --     weekly_source_invoice_presentation_lines and
  --     weekly_source_billing_movements are immutable by installed guard and an
  --     update raises WEEKLY_SOURCE_IMMUTABLE_RECORD - executed, and recorded in
  --     the WP-33 report rather than glossed.  The adjacent, drivable member of
  --     the same list is used instead: the source amount evidence on the header.
  -- =======================================================================
  begin
    update public.invoices
    set subtotal_ex_vat=coalesce(subtotal_ex_vat,0)+1
    where id=v_present_inv;
    v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_present_inv);
    v_direct:=pg_temp.iss_direct_probe(v_present_inv);
    perform pg_temp.iss_assert(
      coalesce((v_verdict->>'ok')::boolean,true) is false
      and coalesce((v_verdict->>'wholly_sealed_source_self_bill')::boolean,true)
          is false
      and pg_catalog.jsonb_array_length(v_verdict->'blocker_codes')>0
      and private.weekly_source_invoice_tsfin_state_v1(v_present_inv)->>'state'
          ='TSFIN_PRESENT'
      and v_verdict->'tsfin_payment_blocker_codes'='[]'::jsonb
      and not pg_temp.iss_async_can_issue(v_present_inv)
      and v_direct like 'ON_HOLD%',
      'WP-33 A6 stale source price evidence','a source price defect did not '
        ||'block: '||v_direct||' '||v_verdict::text);
    raise exception 'WP33_UNDO_A6';
  exception when raise_exception then
    if sqlerrm<>'WP33_UNDO_A6' then raise; end if;
  end;

  -- =======================================================================
  -- A7  STALE PLACEMENT OR INVOICE BINDING.  invoice BLOCKED; pay untouched.
  -- =======================================================================
  begin
    update public.weekly_source_invoice_line_bindings binding
    set manifest_hash=pg_catalog.decode(pg_catalog.repeat('ab',32),'hex')
    where binding.invoice_id=v_present_inv and binding.state='CURRENT';
    v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_present_inv);
    v_direct:=pg_temp.iss_direct_probe(v_present_inv);
    perform pg_temp.iss_assert(
      coalesce((v_verdict->>'ok')::boolean,true) is false
      and coalesce((v_verdict->>'wholly_sealed_source_self_bill')::boolean,true)
          is false
      and v_verdict->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_BINDING_HASH_INVALID'
      and private.weekly_source_invoice_tsfin_state_v1(v_present_inv)->>'state'
          ='TSFIN_PRESENT'
      and v_verdict->'tsfin_payment_blocker_codes'='[]'::jsonb
      and not pg_temp.iss_async_can_issue(v_present_inv)
      and v_direct like 'ON_HOLD%',
      'WP-33 A7 stale placement or binding','a tampered placement did not '
        ||'block: '||v_direct||' '||v_verdict::text);
    raise exception 'WP33_UNDO_A7';
  exception when raise_exception then
    if sqlerrm<>'WP33_UNDO_A7' then raise; end if;
  end;

  -- =======================================================================
  -- A8  MIXED / UNBOUND.  An extra invoice line with no current binding.
  --     invoice: BLOCKED, WEEKLY_SOURCE_ISSUE_UNBOUND_LINE, ok=false
  --     pay:     untouched
  -- =======================================================================
  begin
    -- The fixture is built by RETIRING the current binding rather than by
    -- adding an unbound line: the installed line-owner guard refuses a new
    -- invoice line for a lineage-bound member with
    -- WEEKLY_SOURCE_INVOICE_MOVEMENT_OWNER_REQUIRED, it is a deferred
    -- constraint trigger, and a plpgsql handler around the insert cannot catch
    -- it - executed, and recorded in the WP-33 report rather than glossed.
    -- Retiring the binding leaves the SAME invoice line present and unbound,
    -- which is the shape the rule is about.
    update public.weekly_source_invoice_line_bindings binding
    set state='SUPERSEDED',
        superseded_at_utc=pg_catalog.transaction_timestamp()
    where binding.invoice_id=v_present_inv and binding.state='CURRENT';
    v_verdict:=private.weekly_source_invoice_issue_validate_v1(v_present_inv);
    v_direct:=pg_temp.iss_direct_probe(v_present_inv);
    perform pg_temp.iss_assert(
      coalesce((v_verdict->>'ok')::boolean,true) is false
      and coalesce((v_verdict->>'wholly_sealed_source_self_bill')::boolean,true)
          is false
      and v_verdict->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_UNBOUND_LINE'
      and v_verdict->'tsfin_payment_blocker_codes'='[]'::jsonb
      and not pg_temp.iss_async_can_issue(v_present_inv)
      and v_direct like 'ON_HOLD%',
      'WP-33 A8 mixed/unbound','an unbound line did not fail closed: '
        ||v_direct||' '||v_verdict::text);
    -- and the Candidate-pay boundary is NOT inherited by it
    perform pg_temp.iss_assert(
      not private.weekly_source_invoice_issue_tsfin_skippable_v1(
        v_present_inv,'INVOICE_CORRECTION_TSFIN_STALE')
      and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
        v_present_inv,'INVOICE_CORRECTION_TSFIN_MISSING'),
      'WP-33 A8 mixed/unbound','a mixed invoice inherited the Candidate-pay '
        ||'boundary, which R8A precision point 1 forbids');
    raise exception 'WP33_UNDO_A8';
  exception when raise_exception then
    if sqlerrm<>'WP33_UNDO_A8' then raise; end if;
  end;

  -- =======================================================================
  -- A9  ORDINARY NON-SELF-BILL ROUTE, UNCHANGED, with a SENTINEL that should
  --     differ shown differing.  R8A: "Ordinary non-self-bill and
  --     evidence-required invoice routes retain their existing TSFIN and
  --     Timesheet-evidence requirements unchanged."
  -- =======================================================================
  v_result:=pg_catalog.array_to_string(
    private.weekly_source_invoice_issue_blockers_v1(
      '11111111-1111-4111-8111-111111111111'::uuid,
      array['INVOICE_CORRECTION_TSFIN_MISSING','INVOICE_CORRECTION_TSFIN_STALE',
            'MISSING_TIMESHEET','INVOICE_CORRECTION_MEMBER_MISSING']::text[]),',');
  perform pg_temp.iss_assert(
    v_result='INVOICE_CORRECTION_TSFIN_MISSING,INVOICE_CORRECTION_TSFIN_STALE,'
      ||'MISSING_TIMESHEET,INVOICE_CORRECTION_MEMBER_MISSING',
    'WP-33 A9 ordinary route','an ordinary invoice lost or gained a blocker '
      ||'through the asynchronous seam: '||v_result);
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_issue_reasons_v1(
      pg_catalog.jsonb_build_object('is_source_invoice',false),
      array['TS 11111111-1111-4111-8111-111111111111: missing timesheet PDF']::text[])
      =array['TS 11111111-1111-4111-8111-111111111111: missing timesheet PDF']::text[]
    and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
      '11111111-1111-4111-8111-111111111111'::uuid,
      'INVOICE_CORRECTION_TSFIN_STALE'),
    'WP-33 A9 ordinary route','the ordinary direct route changed, or a '
      ||'non-source invoice inherited the Candidate-pay boundary');
  v_result:=pg_catalog.array_to_string(
    private.weekly_source_invoice_issue_blockers_v1(v_present_inv,
      array['INVOICE_CORRECTION_TSFIN_MISSING','INVOICE_CORRECTION_TSFIN_STALE',
            'MISSING_TIMESHEET','INVOICE_CORRECTION_MEMBER_MISSING']::text[]),',');
  perform pg_temp.iss_assert(
    v_result<>'INVOICE_CORRECTION_TSFIN_MISSING,INVOICE_CORRECTION_TSFIN_STALE,'
      ||'MISSING_TIMESHEET,INVOICE_CORRECTION_MEMBER_MISSING'
    and v_result='INVOICE_CORRECTION_MEMBER_MISSING',
    'WP-33 A9 SENTINEL','the sentinel did not differ, so the ordinary '
      ||'comparison proves nothing: '||v_result);

  -- =======================================================================
  -- A10 THE BOUND.  "Wholly sealed" is a positive test and is never inherited.
  --     A5..A8 each already assert the skip is refused once the invoice stops
  --     being wholly sealed; this adds the two degenerate inputs.
  -- =======================================================================
  perform pg_temp.iss_assert(
    not private.weekly_source_invoice_issue_tsfin_skippable_v1(
      '11111111-1111-4111-8111-111111111111'::uuid,
      'INVOICE_CORRECTION_TSFIN_MISSING')
    and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
      null,'INVOICE_CORRECTION_TSFIN_STALE')
    -- the enumeration is CLOSED at three members
    and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
      v_present_inv,'INVOICE_CORRECTION_MEMBER_MISSING')
    and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
      v_present_inv,'INVOICE_CORRECTION_UNIT_INVALID')
    and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
      v_present_inv,'INVOICE_CORRECTION_SOURCE_LOCK_CONFLICT')
    and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
      v_present_inv,'A_CODE_THAT_DOES_NOT_EXIST_V1'),
    'WP-33 A10 bound','the Candidate-pay boundary was inherited by a non-source '
      ||'invoice, a null invoice, or a code outside the closed enumeration');

  -- =======================================================================
  -- ORDERING INDEPENDENCE, on the REAL asynchronous seam, in both directions.
  -- =======================================================================
  perform pg_temp.iss_assert(
    (select pg_catalog.count(distinct result)=1
     from (
       select pg_catalog.array_to_string(
         private.weekly_source_invoice_issue_blockers_v1(v_exempt_inv,codes),',')
         result
       from (values
         (array['INVOICE_CORRECTION_TSFIN_MISSING','INVOICE_CORRECTION_TSFIN_NOT_READY','INVOICE_CORRECTION_TSFIN_STALE','INVOICE_CORRECTION_TARGET_STREAM_MISMATCH']::text[]),
         (array['INVOICE_CORRECTION_TSFIN_STALE','INVOICE_CORRECTION_TSFIN_NOT_READY','INVOICE_CORRECTION_TSFIN_MISSING','INVOICE_CORRECTION_TARGET_STREAM_MISMATCH']::text[]),
         (array['INVOICE_CORRECTION_TARGET_STREAM_MISMATCH','INVOICE_CORRECTION_TSFIN_STALE','INVOICE_CORRECTION_TSFIN_MISSING','INVOICE_CORRECTION_TSFIN_NOT_READY']::text[]),
         (array['INVOICE_CORRECTION_TSFIN_MISSING']::text[]),
         (array['INVOICE_CORRECTION_TSFIN_STALE']::text[]),
         (array['INVOICE_CORRECTION_TARGET_STREAM_MISMATCH']::text[]),
         (array[]::text[])
       ) permutation(codes)
     ) outcomes),
    'WP-33 ordering','the Candidate-pay boundary changed with the order or the '
      ||'presence of the ordinary correction codes');
  begin
    update public.weekly_source_invoice_line_bindings binding
    set manifest_hash=pg_catalog.decode(pg_catalog.repeat('ab',32),'hex')
    where binding.invoice_id=v_present_inv and binding.state='CURRENT';
    perform pg_temp.iss_assert(
      private.weekly_source_invoice_issue_blockers_v1(v_present_inv,array[]::text[])
        @> array['WEEKLY_SOURCE_ISSUE_BINDING_HASH_INVALID']::text[]
      and private.weekly_source_invoice_issue_blockers_v1(v_present_inv,
        array['INVOICE_CORRECTION_TARGET_STREAM_MISMATCH']::text[])
        @> array['WEEKLY_SOURCE_ISSUE_BINDING_HASH_INVALID']::text[],
      'WP-33 ordering','a source-side refusal disappeared when the ordinary '
        ||'correction codes were removed, so it was never the validator that '
        ||'refused');
    raise exception 'WP33_UNDO_ORDER';
  exception when raise_exception then
    if sqlerrm<>'WP33_UNDO_ORDER' then raise; end if;
  end;

  -- =======================================================================
  -- Invalid and forged state documents cannot reach any non-blocking outcome.
  -- The predicate still computes the whole Candidate-pay truth; what R8A moved
  -- is where that truth is CONSUMED, so these stay exactly as fail-closed.
  -- =======================================================================
  perform pg_temp.iss_assert(
    private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_EXPECTED_BUT_MISSING","counts":{"members":1,"stale":0,"expected_but_missing":1},"invoice_disposition_claim":"WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT"}'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED'
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_EXPECTED_BUT_MISSING","counts":{"members":1,"stale":0,"expected_but_missing":1,"awaiting_first_authorisation":null,"expected_but_missing_unexplained":0},"invoice_disposition_claim":"WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT"}'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED'
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_EXPECTED_BUT_MISSING","counts":{"members":1,"stale":0,"expected_but_missing":1,"awaiting_first_authorisation":"1","expected_but_missing_unexplained":0},"invoice_disposition_claim":"WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT"}'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED'
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_EXPECTED_BUT_MISSING","counts":{"members":2,"stale":1,"expected_but_missing":1,"awaiting_first_authorisation":1,"expected_but_missing_unexplained":0},"invoice_disposition_claim":"WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT"}'::jsonb)
      ->>'invoice_disposition'='WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED'
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_PRESENT","counts":{"members":1,"stale":0,"expected_but_missing":0,"awaiting_first_authorisation":0,"expected_but_missing_unexplained":0},"invoice_disposition_claim":"WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT"}'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_DISPOSITION_DISAGREEMENT'
    and private.weekly_source_invoice_tsfin_refusal_v1(
      '{"state":"TSFIN_PRESENT","counts":{"members":1,"stale":0,"expected_but_missing":0,"awaiting_first_authorisation":0,"expected_but_missing_unexplained":0},"invoice_disposition_claim":"WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE"}'::jsonb)
      ->'blocker_codes' ? 'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED',
    'WP-33 forged','an invalid or forged financial-record state reached a '
      ||'non-blocking outcome');
end;
$wp33_routes$;



-- ACL: the validator and both seams are private and not browser executable.
do $acl$
declare v_signature text;
begin
  foreach v_signature in array array[
    'private.weekly_source_invoice_issue_validate_v1(uuid)',
    'private.weekly_source_invoice_tsfin_state_v1(uuid)',
    'private.weekly_source_invoice_tsfin_refusal_v1(jsonb)',
    'private.weekly_source_invoice_tsfin_first_authorisation_v1(uuid)',
    'private.weekly_source_invoice_issue_tsfin_skippable_v1(uuid,text)',
    'private.weekly_source_invoice_issue_skippable_code_v1(text)',
    'private.weekly_source_invoice_issue_skippable_reason_v1(text)',
    'private.weekly_source_invoice_issue_blockers_v1(uuid,text[])',
    'private.weekly_source_invoice_issue_reasons_v1(jsonb,text[])'
  ] loop
    if pg_catalog.to_regprocedure(v_signature) is null then
      raise exception 'ASSERTION_FAILED: % is missing',v_signature;
    end if;
    if pg_catalog.has_function_privilege('anon',v_signature,'EXECUTE')
       or pg_catalog.has_function_privilege('authenticated',v_signature,'EXECUTE')
       or pg_catalog.has_function_privilege('service_role',v_signature,'EXECUTE') then
      raise exception 'ASSERTION_FAILED: % is callable outside the owner',v_signature;
    end if;
  end loop;
end;
$acl$;

select pg_catalog.jsonb_build_object(
  'ok',true,'verification','weekly_source_invoice_issue_validator_full_v1',
  'rows_proved',pg_catalog.jsonb_build_array(
    'ISS-001','ISS-002','ISS-003','ISS-004','ISS-005','ISS-007','ISS-008',
    'ISS-009','ISS-010','ISS-011','ISS-012','ISS-013','ISS-014','INV-030',
    'WP-28 F3 unsigned QR skip agreement, both real routes',
    'WP-28 F2 / R8A 1 the masked-ordering stale fixture, outcome inverted by R8A section 1',
    'WP-33 R8A 1 A1 awaiting first authorisation: invoice ISSUES with the explicit typed disposition, Candidate pay BLOCKED',
    'WP-33 R8A 1 A2 absent TSFIN in four shapes: invoice ISSUES, Candidate pay BLOCKED, first-authorisation proof still refuses',
    'WP-33 R8A 1 A3 stale TSFIN (ISS-013 live): invoice ISSUES on all three real routes, Candidate pay BLOCKED',
    'WP-33 R8A 1 A4 current TSFIN: invoice ISSUES, Candidate-pay disposition CLEAR',
    'WP-33 R8A 1 A5 stale source revision: invoice BLOCKED, Candidate-pay disposition untouched',
    'WP-33 R8A 1 A6 source calculation/amount evidence: invoice BLOCKED, Candidate-pay disposition untouched (the price-comparison limb itself is not fixture-drivable: presentation lines and billing movements raise WEEKLY_SOURCE_IMMUTABLE_RECORD)',
    'WP-33 R8A 1 A7 invoice binding hash: invoice BLOCKED with WEEKLY_SOURCE_ISSUE_BINDING_HASH_INVALID, Candidate-pay disposition untouched (the placement-hash limb is not fixture-drivable: placements raise WEEKLY_SOURCE_INVOICE_MOVEMENT_OWNER_REQUIRED)',
    'WP-33 R8A 1 A8 mixed/unbound invoice: fails closed and does not inherit the Candidate-pay boundary',
    'WP-33 R8A 1 A9 ordinary non-self-bill route unchanged, with a differing sentinel',
    'WP-33 R8A 1 A10 wholly-sealed is a positive test: not inherited by a non-source, null or partly sealed invoice, closed three-member enumeration',
    'WP-33 ordering independence over correction-code permutations, both directions',
    'WP-33 invalid and forged state documents reach no non-blocking outcome')
);
rollback;

-- ISS-006  The narrowly supported non-NHSP NET presentation, through the same
--          real entry points.
\set weekly_source_verification_correction_presentation 'NET_DIFFERENCE_PRESENTATION'
\set weekly_source_verification_expense_vat_enabled true
begin;
\ir 15092026_1534_weekly_source_ordinary_pay_projection_v1.sql

create function pg_temp.iss_admit_all()
returns void language plpgsql as $function$
declare v_manifest public.weekly_source_client_manifests%rowtype;
begin
  for v_manifest in
    select * from public.weekly_source_client_manifests
    where invoice_state='READY' order by created_at_utc,id
  loop
    perform public.weekly_source_invoice_admit_atomic_v1(
      pg_catalog.jsonb_build_object(
        'actor_user_id','a0000000-0000-4000-8000-000000000001',
        'client_manifest_id',v_manifest.id,
        'expected_manifest_hash',pg_catalog.encode(v_manifest.manifest_hash,'hex')
      )
    );
  end loop;
end;
$function$;

create function pg_temp.iss_async_can_issue(p_invoice_id uuid)
returns boolean language sql stable as $function$
  select coalesce(v.can_issue_only,false)
  from private._invoice_issue_validate_batch(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'request_key','iss:'||p_invoice_id::text,'invoice_id',p_invoice_id,
      'expected_revision',(select document_revision from public.invoices
                           where id=p_invoice_id),
      'allow_early',true,'deliver',false)),
    date '2026-12-31'
  ) v;
$function$;

create function pg_temp.iss_async_blockers(p_invoice_id uuid)
returns jsonb language sql stable as $function$
  select coalesce(v.hard_blocker_codes,'[]'::jsonb)
  from private._invoice_issue_validate_batch(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'request_key','iss:'||p_invoice_id::text,'invoice_id',p_invoice_id,
      'expected_revision',(select document_revision from public.invoices
                           where id=p_invoice_id),
      'allow_early',true,'deliver',false)),
    date '2026-12-31'
  ) v;
$function$;

create function pg_temp.iss_direct_issue(p_invoice_id uuid)
returns text language plpgsql as $function$
declare v_row record;
begin
  select * into v_row from public.invoice_issue_one(
    p_invoice_id,'a0000000-0000-4000-8000-000000000001');
  return v_row.status;
exception when others then
  return 'RAISED:'||sqlerrm;
end;
$function$;

select pg_temp.iss_admit_all();

do $iss_006$
declare
  v_invoice uuid;
  v_status text;
begin
  select binding.invoice_id into strict v_invoice
  from public.weekly_source_invoice_line_bindings binding
  join public.weekly_source_invoice_presentation_lines presentation
    on presentation.id=binding.presentation_line_id
  where binding.state='CURRENT' and presentation.correction_role='NET_DIFFERENCE'
    and presentation.origin_kind<>'SOURCE_FIXED_EXPENSE'
  order by binding.invoice_id limit 1;
  if coalesce((private.weekly_source_invoice_issue_validate_v1(v_invoice)->>'ok')::boolean,false)
       is not true then
    raise exception 'ASSERTION_FAILED: ISS-006 - the source validator refused a NET presentation %',
      private.weekly_source_invoice_issue_validate_v1(v_invoice);
  end if;
  if not pg_temp.iss_async_can_issue(v_invoice) then
    raise exception 'ASSERTION_FAILED: ISS-006 - the real asynchronous validator blocked a NET presentation %',
      pg_temp.iss_async_blockers(v_invoice);
  end if;
  v_status:=pg_temp.iss_direct_issue(v_invoice);
  if v_status<>'ISSUED' then
    raise exception 'ASSERTION_FAILED: ISS-006 - the real direct owner returned %',v_status;
  end if;
end;
$iss_006$;

select pg_catalog.jsonb_build_object(
  'ok',true,'verification','weekly_source_invoice_issue_validator_net_v1',
  'rows_proved',pg_catalog.jsonb_build_array('ISS-006')
);
rollback;

\unset weekly_source_verification_outer_transaction
\unset weekly_source_ordinary_verification_outer_transaction
\unset weekly_source_verification_correction_presentation
\unset weekly_source_verification_expense_vat_enabled
