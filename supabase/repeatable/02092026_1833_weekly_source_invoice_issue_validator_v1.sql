-- Repeatable CloudTMS function authority: weekly_source_invoice_issue_validator_v1
--
-- Gate 7 item G7-3.  ONE source-aware invoice issue validator, reached from
-- BOTH real issue entry points:
--
--   * the asynchronous route, through private._invoice_issue_validate_batch
--     (supabase/repeatable/02092026_1834_candidate_expense_separation_delivery_v1.sql),
--     whose hard blocker array is the single gate consumed by
--     private._invoice_issue_advance_core_v8 and by
--     private._invoice_batch_issue_classification_v2;
--   * the direct route, through public.invoice_issue_one
--     (supabase/repeatable/21072026_1235_59_invoice_issue_one.sql).
--
-- The rule body is the one already proved by
-- private.weekly_source_invoice_allocation_assert_v1
-- (15092026_1534_weekly_source_invoice_admission_v1.sql:135-288); this file
-- reuses it and adds only what 24 section 11 requires on top of it:
-- manifest/final-source-revision currentness, exact recomputable binding,
-- placement and presentation-source hashes, complete movement allocation,
-- NHSP physical independence, the configured non-NHSP correction shape, the
-- source price comparison result and the non-empty rule.
--
-- 25 section 4 Removed, bullet 1: a direct database status change to ISSUED is
-- NOT proof that the real issue route accepts a source invoice.  The status
-- trigger weekly_source_invoice_issue_guard stays as a fail-closed backstop
-- (G7-4); it is not the admission decision.
--
-- FILE NAME / RELEASE ORDER.  The release runner applies repeatables in parsed
-- DDMMYYYY_HHMM order, and a "language sql" body resolves its function
-- references at CREATE time.  The asynchronous seam lives inside the
-- language-sql private._invoice_issue_validate_batch, which is installed by
-- 02092026_1834_candidate_expense_separation_delivery_v1.sql, so this file MUST
-- sort before it; a 17092026_ name would abort the whole release with
-- "function private.weekly_source_invoice_issue_blockers_v1(uuid, text[]) does
-- not exist".  Nothing in this file resolves anything at CREATE time - every
-- cross-file call is made from plpgsql and is late-bound, and the two
-- language-sql helpers reference no table and no function - so the file is
-- free to sort this early.  Deliberate deviation from the WP naming rule,
-- recorded in the WP-05 report.
--
-- 24 section 11 last paragraph: "Any mixed or incomplete invoice fails closed.
-- Ordinary invoices continue through the existing issue path unchanged."  Every
-- function here answers {"is_source_invoice": false} for an ordinary invoice and
-- the caller's behaviour is then bit-for-bit what it was before.

\set ON_ERROR_STOP on

begin;

-- ---------------------------------------------------------------------------
-- The single source-aware issue validator.
--
--   {"is_source_invoice": false}                      ordinary: continue unchanged
--   {"is_source_invoice": true, "ok": true,  ...}     exact source branch passes
--   {"is_source_invoice": true, "ok": false, ...}     fails closed with codes
--
-- An invoice enters the source branch when its immutable header declares
-- WEEKLY_FINAL_SOURCE self-bill origin, OR when it carries at least one CURRENT
-- source binding.  The second limb is what makes a mixed or mislabelled invoice
-- fail closed instead of quietly using the ordinary path.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_invoice_issue_validate_v1(
  p_invoice_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_invoice public.invoices%rowtype;
  v_has_source_header boolean;
  v_binding_count integer;
  v_line_count integer;
  v_codes text[]:=array[]::text[];
  v_detail jsonb:='{}'::jsonb;
  v_assert jsonb;
  v_assert_code text;
  -- The immutable header's source group, read ONCE and only when it really is a
  -- uuid.  A malformed value must fail this invoice closed, never abort the
  -- batch that this invoice happens to share with other invoices.
  v_header_source_group_id uuid;
  v_header_source_group_valid boolean;
  v_validator_error text;
  -- HANDOVER 2 round 5, Part E: the three financial-record states, recorded
  -- separately on the verdict this owner returns.
  v_tsfin jsonb;
  -- WP-28: the enforced and the withheld halves of that ruling, computed by
  -- private.weekly_source_invoice_tsfin_refusal_v1 from the state above.
  v_tsfin_refusal jsonb;
  -- WP-33: the R8A section 1 admission-predicate boundary, applied once, here.
  v_wholly_sealed boolean;
  v_tsfin_codes jsonb;
  v_tsfin_deferred jsonb;
  v_tsfin_disposition text;
begin
  if p_invoice_id is null then
    return pg_catalog.jsonb_build_object('is_source_invoice',false);
  end if;
  select * into v_invoice from public.invoices where id=p_invoice_id;
  if not found then
    -- The ordinary owner already has an exact "not found" behaviour; never
    -- pre-empt it and never claim the source branch for a missing invoice.
    return pg_catalog.jsonb_build_object('is_source_invoice',false);
  end if;

  v_has_source_header:=coalesce(
    v_invoice.header_snapshot_json#>>'{meta,source}'='WEEKLY_FINAL_SOURCE'
    and v_invoice.header_snapshot_json#>>'{meta,self_bill}'='true',false
  );
  select pg_catalog.count(*)::integer into v_binding_count
  from public.weekly_source_invoice_line_bindings binding
  where binding.invoice_id=p_invoice_id and binding.state='CURRENT';

  if not v_has_source_header and v_binding_count=0 then
    return pg_catalog.jsonb_build_object('is_source_invoice',false);
  end if;

  select pg_catalog.count(*)::integer into v_line_count
  from public.invoice_lines invoice_line where invoice_line.invoice_id=p_invoice_id;

  -- Everything from here to the aggregation below is wrapped in ONE handler.
  -- This function is called from inside private._invoice_issue_validate_batch,
  -- which evaluates EVERY invoice of a batch in one statement, so an
  -- unexpected error raised here would abort the verdict of every other
  -- invoice in that batch, including ordinary invoices that have nothing to do
  -- with this one.  An unexpected error is therefore converted into a blocking
  -- code on THIS invoice and nothing else.  It cannot mask an ordinary
  -- invoice: the two "is_source_invoice: false" returns above are outside the
  -- handler, so an ordinary invoice never enters it.
  begin

  -- 24 section 11 qualification list -------------------------------------
  -- "its immutable header says source = WEEKLY_FINAL_SOURCE and self_bill = true"
  if not v_has_source_header then
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_HEADER_INVALID'::text;
  end if;
  v_header_source_group_valid:=pg_catalog.pg_input_is_valid(
    coalesce(v_invoice.header_snapshot_json#>>'{meta,source_group_id}',''),'uuid');
  if v_header_source_group_valid then
    v_header_source_group_id:=(v_invoice.header_snapshot_json#>>'{meta,source_group_id}')::uuid;
  else
    -- 24 section 11: the source group is part of the immutable header the
    -- source branch is entered on.  A missing or malformed value is this
    -- invoice's own explicit failure.
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_HEADER_INVALID'::text;
    v_detail:=v_detail||pg_catalog.jsonb_build_object(
      'header_source_group_id_invalid',true
    );
  end if;
  -- "it contains at least one source binding"
  if v_binding_count=0 then
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_NO_SOURCE_BINDING'::text;
  end if;
  -- "no empty invoice"
  if v_line_count=0 then
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_EMPTY_INVOICE'::text;
  end if;
  -- "no extra ordinary or unbound line is present"
  if exists(
    select 1 from public.invoice_lines invoice_line
    where invoice_line.invoice_id=p_invoice_id
      and not exists(
        select 1 from public.weekly_source_invoice_line_bindings binding
        where binding.invoice_line_id=invoice_line.id
          and binding.invoice_id=p_invoice_id and binding.state='CURRENT'
      )
  ) then
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_UNBOUND_LINE'::text;
  end if;

  -- The established rule body.  weekly_source_invoice_allocation_assert_v1
  -- proves the header shape, extra/unbound lines, manifest hash, movement,
  -- presentation and placement binding, Contract and Client allocation, exact
  -- signed line amounts, VAT, header totals and the net-presentation
  -- cardinality.  It raises; here that becomes a blocker so the real entry
  -- points can report rather than abort.
  begin
    v_assert:=private.weekly_source_invoice_allocation_assert_v1(p_invoice_id);
    if coalesce((v_assert->>'empty')::boolean,false)
       and not (v_codes @> array['WEEKLY_SOURCE_ISSUE_EMPTY_INVOICE']) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_EMPTY_INVOICE'::text;
    end if;
  exception when others then
    get stacked diagnostics v_assert_code=message_text;
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_ALLOCATION_INVALID'::text;
    v_detail:=v_detail||pg_catalog.jsonb_build_object(
      'allocation_assert_error',pg_catalog.left(coalesce(v_assert_code,''),200)
    );
  end;

  if v_binding_count>0 then
    -- "current manifest and final source revision" -------------------------
    if exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_manifest_movements manifest_movement
        on manifest_movement.billing_movement_id=binding.billing_movement_id
      join public.weekly_source_client_manifests manifest
        on manifest.id=manifest_movement.client_manifest_id
      join public.weekly_source_final_revisions revision
        on revision.id=manifest.final_revision_id
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and revision.state<>'CURRENT'
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_STALE_FINAL_REVISION'::text;
    end if;
    if exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_manifest_movements manifest_movement
        on manifest_movement.billing_movement_id=binding.billing_movement_id
      join public.weekly_source_client_manifests manifest
        on manifest.id=manifest_movement.client_manifest_id
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and (
          binding.manifest_hash is distinct from manifest.manifest_hash
          or binding.original_final_revision_id is distinct from manifest.final_revision_id
          or binding.original_cycle_id is distinct from manifest.source_cycle_id
          or manifest.invoice_state not in ('PARTLY_ADMITTED','ADMITTED')
        )
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_STALE_MANIFEST'::text;
    end if;

    -- "exact presentation, binding and placement hashes" --------------------
    -- Every hash below is rebuilt from immutable identity text only, so the
    -- comparison is exact.  The presentation source hash is the admission
    -- owner's WEEKLY_SOURCE_INVOICE_PRESENTATION_SOURCE_V1 digest over the
    -- movement set the presentation actually binds, which is also the proof
    -- that the allocation carries no missing or duplicate movement.
    if exists(
      select 1
      from (
        select presentation.id,presentation.source_hash,
          private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_INVOICE_PRESENTATION_SOURCE_V1',
            pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
              'movement_id',movement.id,
              'movement_economic_hash',
                pg_catalog.encode(movement.movement_economic_hash,'hex')
            ) order by movement.id)
          ) rebuilt
        from public.weekly_source_invoice_line_bindings binding
        join public.weekly_source_invoice_presentation_lines presentation
          on presentation.id=binding.presentation_line_id
        join public.weekly_source_billing_movements movement
          on movement.id=binding.billing_movement_id
        where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        group by presentation.id,presentation.source_hash
      ) rebuilt_presentation
      where rebuilt_presentation.rebuilt is distinct from rebuilt_presentation.source_hash
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_PRESENTATION_HASH_INVALID'::text;
    end if;
    if exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and binding.materialised_line_hash is distinct from
          private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_INVOICE_BINDING_V1',
            case when binding.binding_version=1 then
              pg_catalog.jsonb_build_object(
                'movement_id',binding.billing_movement_id,
                'presentation_line_id',binding.presentation_line_id,
                'invoice_line_id',binding.invoice_line_id,
                'invoice_id',binding.invoice_id,
                'manifest_hash',pg_catalog.encode(binding.manifest_hash,'hex'),
                'binding_version',1
              )
            else
              pg_catalog.jsonb_build_object(
                'movement_id',binding.billing_movement_id,
                'presentation_line_id',binding.presentation_line_id,
                'invoice_line_id',binding.invoice_line_id,
                'invoice_id',binding.invoice_id,
                'manifest_hash',pg_catalog.encode(binding.manifest_hash,'hex'),
                'binding_version',binding.binding_version,
                'prior_binding_id',binding.prior_binding_id
              )
            end
          )
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_BINDING_HASH_INVALID'::text;
    end if;
    if exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_invoice_placements placement
        on placement.billing_movement_id=binding.billing_movement_id
       and placement.is_current
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and placement.placement_hash is distinct from
          private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_INVOICE_PLACEMENT_V1',
            case when placement.placement_revision=1 then
              pg_catalog.jsonb_build_object(
                'movement_id',placement.billing_movement_id,
                'invoice_id',placement.invoice_id,
                'invoice_line_id',placement.invoice_line_id,
                'placement_revision',1,
                'placement_reason',placement.placement_reason
              )
            else
              pg_catalog.jsonb_build_object(
                'movement_id',placement.billing_movement_id,
                'invoice_id',placement.invoice_id,
                'invoice_line_id',placement.invoice_line_id,
                'placement_revision',placement.placement_revision,
                'placement_reason',placement.placement_reason,
                'prior_placement_id',placement.prior_placement_id,
                'source_invoice_version_fingerprint',
                  pg_catalog.encode(placement.source_invoice_version_fingerprint,'hex'),
                'destination_invoice_version_fingerprint',
                  pg_catalog.encode(placement.destination_invoice_version_fingerprint,'hex')
              )
            end
          )
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_PLACEMENT_HASH_INVALID'::text;
    end if;

    -- "complete allocation with no missing or duplicate movement" ----------
    -- Every movement of every manifest this invoice draws on is either bound
    -- here or bound CURRENT somewhere else; none is bound twice; and the
    -- invoice never carries a movement its manifest does not contain.
    if exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
      group by binding.billing_movement_id
      having pg_catalog.count(*)>1
    ) or exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and not exists(
          select 1 from public.weekly_source_manifest_movements manifest_movement
          where manifest_movement.billing_movement_id=binding.billing_movement_id
        )
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_DUPLICATE_MOVEMENT'::text;
    end if;
    if exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_invoice_presentation_lines presentation
        on presentation.id=binding.presentation_line_id
      join public.weekly_source_invoice_line_bindings sibling
        on sibling.presentation_line_id=presentation.id
       and sibling.state='CURRENT'
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and sibling.invoice_id is distinct from p_invoice_id
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_MISSING_MOVEMENT'::text;
    end if;

    -- Every bound movement is still placed on this invoice and has not been
    -- voided by a later Correct Final Source.
    if exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_billing_movements movement
        on movement.id=binding.billing_movement_id
      left join public.weekly_source_invoice_placements placement
        on placement.billing_movement_id=movement.id and placement.is_current
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and (
          movement.placement_state not in ('PLACED','ISSUED')
          or placement.id is null
          or placement.placement_state<>'PLACED'
          or placement.invoice_id is distinct from p_invoice_id
          or placement.invoice_line_id is distinct from binding.invoice_line_id
        )
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_MOVEMENT_NOT_PLACED'::text;
    end if;

    -- "Client, source group, cycle, currency and VAT consistency" -----------
    -- CloudTMS invoices carry no per-invoice currency column, so currency
    -- consistency is structural: one Client, one source group.  Cycle
    -- consistency is deliberately NOT one cycle per invoice: 24 section 12
    -- allows a confirmed cross-week move, and the header records every
    -- contained cycle.  What must hold is that the header's contained sets
    -- describe exactly the manifests the bindings actually reference.
    if exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_manifest_movements manifest_movement
        on manifest_movement.billing_movement_id=binding.billing_movement_id
      join public.weekly_source_client_manifests manifest
        on manifest.id=manifest_movement.client_manifest_id
      join public.weekly_source_billing_movements movement
        on movement.id=binding.billing_movement_id
      join public.weekly_source_invoice_presentation_lines presentation
        on presentation.id=binding.presentation_line_id
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and (
          manifest.client_id is distinct from v_invoice.client_id
          or movement.actual_client_id is distinct from v_invoice.client_id
          or presentation.client_id is distinct from v_invoice.client_id
          or binding.client_id is distinct from v_invoice.client_id
          or manifest.source_group_id is distinct from v_header_source_group_id
        )
    ) or exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_manifest_movements manifest_movement
        on manifest_movement.billing_movement_id=binding.billing_movement_id
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and not (
          coalesce(
            v_invoice.header_snapshot_json#>'{meta,contained_client_manifest_ids}',
            '[]'::jsonb
          ) @> pg_catalog.to_jsonb(manifest_movement.client_manifest_id)
        )
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_SCOPE_INCONSISTENT'::text;
    end if;
    if exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_invoice_presentation_lines presentation
        on presentation.id=binding.presentation_line_id
      join public.invoice_lines invoice_line on invoice_line.id=binding.invoice_line_id
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and invoice_line.vat_rate_pct is distinct from presentation.vat_rate_pct
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_SCOPE_INCONSISTENT'::text;
    end if;

    -- "NHSP physical negative and positive independence" (24 section 10) -----
    -- An NHSP physical row is one movement, one presentation, one visible line;
    -- it is never netted with another physical row.
    if exists(
      select 1
      from public.weekly_source_invoice_presentation_lines presentation
      join public.weekly_source_invoice_line_bindings binding
        on binding.presentation_line_id=presentation.id
       and binding.state='CURRENT'
      where binding.invoice_id=p_invoice_id
        and presentation.origin_kind='NHSP_PHYSICAL_ROW'
      group by presentation.id,binding.invoice_line_id
      having pg_catalog.count(*)<>1
    ) or exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_invoice_presentation_lines presentation
        on presentation.id=binding.presentation_line_id
      join public.weekly_source_billing_movements movement
        on movement.id=binding.billing_movement_id
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and (
          (presentation.origin_kind='NHSP_PHYSICAL_ROW')
            <>(movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT')
          or (presentation.origin_kind='NHSP_PHYSICAL_ROW'
              and presentation.correction_role='NET_DIFFERENCE')
        )
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_NHSP_NETTING_DETECTED'::text;
    end if;

    -- "the configured non-NHSP correction shape" (24 section 10) ------------
    -- FULL pair: one movement per presentation, correction_role REVERSAL or
    -- REPLACEMENT.  NET: exactly the reversal/replacement pair of one
    -- correction unit on one visible line, correction_role NET_DIFFERENCE.
    if exists(
      select 1
      from (
        select presentation.id,presentation.correction_role,
          pg_catalog.count(*)::integer member_count,
          pg_catalog.count(distinct movement.correction_unit_id) correction_count,
          pg_catalog.array_agg(movement.movement_role order by movement.movement_role) roles
        from public.weekly_source_invoice_line_bindings binding
        join public.weekly_source_invoice_presentation_lines presentation
          on presentation.id=binding.presentation_line_id
        join public.weekly_source_billing_movements movement
          on movement.id=binding.billing_movement_id
        where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        group by presentation.id,presentation.correction_role
      ) shaped
      where shaped.member_count not in (1,2)
        or (shaped.member_count=2 and (
          shaped.correction_role is distinct from 'NET_DIFFERENCE'
          or shaped.correction_count<>1
          or shaped.roles not in (
            array['REPLACEMENT','REVERSAL']::text[],
            array['EXPENSE_REPLACEMENT','EXPENSE_REVERSAL']::text[]
          )
        ))
        or (shaped.member_count=1 and shaped.correction_role='NET_DIFFERENCE')
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_CORRECTION_SHAPE_INVALID'::text;
    end if;

    -- "source price comparison success" (24 section 13) ---------------------
    if exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_invoice_presentation_lines presentation
        on presentation.id=binding.presentation_line_id
      join public.weekly_source_billing_movements movement
        on movement.id=binding.billing_movement_id
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and (
          (presentation.origin_kind='NHSP_PHYSICAL_ROW'
            and (presentation.price_check_result not in (
                'EXACT','SOURCE_ROUNDING_EQUIVALENT','ACCEPTED_DISPARITY','ACCEPTED_ZERO'
              )
              or presentation.amount_authority<>'VALIDATED_SOURCE_PENCE'
              or presentation.source_validation_charge_pence is null
              or presentation.invoice_presentation_charge_pence
                 is distinct from presentation.source_validation_charge_pence
              or presentation.charge_acceptance_id is distinct from movement.charge_acceptance_id
              or (presentation.price_check_result in ('ACCEPTED_DISPARITY','ACCEPTED_ZERO')
                and not exists(
                  select 1 from public.weekly_source_charge_acceptances acceptance
                  where acceptance.id=presentation.charge_acceptance_id
                    and acceptance.acceptance_kind=presentation.price_check_result
                    and acceptance.upload_row_id=movement.nhsp_upload_row_id
                    and acceptance.contract_id=movement.contract_id
                    and acceptance.charge_calculation_fingerprint=movement.price_check_fingerprint
                    and acceptance.acceptance_policy_fingerprint=
                      private.weekly_source_charge_acceptance_policy_fingerprint_v1()
                ))))
          or (presentation.origin_kind='SOURCE_FIXED_EXPENSE'
            and (presentation.price_check_result<>'NOT_APPLICABLE'
              or presentation.amount_authority<>'VALIDATED_SOURCE_PENCE'))
          or (presentation.origin_kind not in ('NHSP_PHYSICAL_ROW','SOURCE_FIXED_EXPENSE')
            and presentation.amount_authority<>'CLOUDTMS_CALCULATION')
          or (movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
            and movement.price_check_result
              not in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','ACCEPTED_DISPARITY','ACCEPTED_ZERO'))
        )
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_PRICE_CHECK_FAILED'::text;
    end if;

    -- "exact signed line amounts, VAT and header total" ---------------------
    -- The allocation assert already compares the visible line with its
    -- presentation and the header with the sum of its lines.  What it does not
    -- do is tie the presentation back to the immutable movements it binds.
    if exists(
      select 1
      from (
        select presentation.id,
          presentation.total_pay_ex_vat,presentation.total_charge_ex_vat,
          presentation.vat_amount,presentation.total_inc_vat,
          presentation.invoice_presentation_charge_pence,
          presentation.calculated_comparison_charge_pence,
          pg_catalog.sum(movement.total_pay_ex_vat) movement_pay,
          pg_catalog.sum(movement.vat_amount) movement_vat,
          pg_catalog.sum(movement.total_inc_vat) movement_total,
          pg_catalog.sum(movement.invoice_presentation_charge_pence) movement_pence,
          pg_catalog.sum(movement.calculated_comparison_charge_pence) movement_calc_pence
        from public.weekly_source_invoice_line_bindings binding
        join public.weekly_source_invoice_presentation_lines presentation
          on presentation.id=binding.presentation_line_id
        join public.weekly_source_billing_movements movement
          on movement.id=binding.billing_movement_id
        where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        group by presentation.id,presentation.total_pay_ex_vat,
          presentation.total_charge_ex_vat,presentation.vat_amount,
          presentation.total_inc_vat,presentation.invoice_presentation_charge_pence,
          presentation.calculated_comparison_charge_pence
      ) amounts
      where amounts.total_pay_ex_vat is distinct from amounts.movement_pay
        or amounts.vat_amount is distinct from amounts.movement_vat
        or amounts.total_inc_vat is distinct from amounts.movement_total
        or amounts.invoice_presentation_charge_pence is distinct from amounts.movement_pence
        or amounts.calculated_comparison_charge_pence is distinct from amounts.movement_calc_pence
        or amounts.invoice_presentation_charge_pence
           is distinct from (amounts.total_charge_ex_vat*100)::bigint
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_AMOUNT_MISMATCH'::text;
    end if;
    if v_invoice.subtotal_ex_vat is distinct from (
         select pg_catalog.round(coalesce(pg_catalog.sum(line.total_charge_ex_vat),0),2)
         from public.invoice_lines line where line.invoice_id=p_invoice_id
       )
       or v_invoice.vat_amount is distinct from (
         select pg_catalog.round(coalesce(pg_catalog.sum(line.vat_amount),0),2)
         from public.invoice_lines line where line.invoice_id=p_invoice_id
       )
       or v_invoice.total_inc_vat is distinct from (
         select pg_catalog.round(coalesce(pg_catalog.sum(line.total_inc_vat),0),2)
         from public.invoice_lines line where line.invoice_id=p_invoice_id
       ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_TOTAL_MISMATCH'::text;
    end if;

    -- "Timesheet relationship where required for navigation, without treating
    -- Candidate evidence as invoice authority" (24 section 11; proof/34 section 9).
    -- The visible line keeps the immutable Timesheet identity the movement was
    -- written against.  Nothing here reads authorisation, signature, PDF or
    -- any other Candidate evidence, and nothing re-points a line.
    if exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_billing_movements movement
        on movement.id=binding.billing_movement_id
      join public.invoice_lines invoice_line on invoice_line.id=binding.invoice_line_id
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
        and (
          invoice_line.timesheet_id is null
          or invoice_line.timesheet_id is distinct from movement.invoice_timesheet_id
          or invoice_line.invoice_id is distinct from p_invoice_id
        )
    ) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_TIMESHEET_LINK_INVALID'::text;
    end if;
  end if;

    -- HANDOVER 2 round 5, Part E "Invoice and TSFIN outcomes": the three
    -- financial-record states are RECORDED SEPARATELY on the verdict so a
    -- consumer of this owner can tell them apart without reading this body.
    v_tsfin:=private.weekly_source_invoice_tsfin_state_v1(p_invoice_id);
    v_detail:=v_detail||pg_catalog.jsonb_build_object('tsfin',v_tsfin);

  exception when others then
    get stacked diagnostics v_validator_error=message_text;
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_VALIDATOR_ERROR'::text;
    v_detail:=v_detail||pg_catalog.jsonb_build_object(
      'validator_error',pg_catalog.left(coalesce(v_validator_error,''),200)
    );
  end;

  select pg_catalog.array_agg(distinct code order by code)
    into v_codes from pg_catalog.unnest(v_codes) code;
  v_codes:=coalesce(v_codes,array[]::text[]);

  -- WP-28.  The financial-record refusal is computed OUTSIDE the handler above,
  -- so a validator that blew up leaves v_tsfin null and the predicate returns
  -- WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED rather than nothing.
  --
  -- WP-33, HANDOVER 2 round 8 Part 2.1.  The predicate now answers several
  -- separate questions from the one state document, and this verdict carries
  -- each of them on its own key because the ruling keeps them apart:
  --   tsfin_blocker_codes          what ACTUALLY stops the INVOICE today;
  --   tsfin_invoice_disposition    the explicit typed invoice outcome, which is
  --                                what both seams consume - an omission is not
  --                                an outcome, so a seam that cannot read this
  --                                key fails closed;
  --   tsfin_payment_blocker_codes  the RETAINED expected-but-missing fact, which
  --                                is non-empty for the exempt case too so that
  --                                payment and financial publication stay
  --                                fail-closed while the invoice issues;
  --   tsfin_deferred_blocker_codes the expected-but-missing INVOICE refusal that
  --                                round 8 Part 2.1 requires and sealed pack 02
  --                                section 15 forbids.  Computed, named, NOT
  --                                enforced, with a reason that is true today.
  --
  -- The WP-28 tsfin_withheld_* keys are RETIRED and deliberately not renamed
  -- back into use: their reason named the round-6 question, and the approver has
  -- answered that question.  Standing rule 12 forbids shipping a fail-closed
  -- reason that is no longer true, so the deferred keys above carry the reason
  -- that IS true - an outstanding admission-predicate boundary correction.
  --
  -- It is carried on its OWN key and is deliberately NOT folded into
  -- blocker_codes, so `ok` keeps its exact 24 section 11 meaning - "the exact
  -- source branch passes" - and an admitted source invoice still skips only the
  -- four irrelevant ordinary evidence categories.  Folding a Candidate
  -- financial-record refusal into `ok` would additionally (a) stop the direct
  -- owner skipping public._ctms_assert_invoice_correction_lines_v1, turning an
  -- ON_HOLD into a raised exception, and (b) re-impose every ordinary evidence
  -- requirement the pack says is irrelevant to source self-billing.  Both seams
  -- append this array unconditionally, in the ok and the not-ok branch alike.
  v_tsfin_refusal:=private.weekly_source_invoice_tsfin_refusal_v1(v_tsfin);

  -- =========================================================================
  -- WP-33, HANDOVER 2 CORRECTION ADDENDUM R8A section 1 - the ADMISSION
  -- PREDICATE BOUNDARY.  This is the one place the boundary is applied, and it
  -- is applied to a verdict whose source-side truth has ALREADY been decided
  -- immediately above.
  --
  -- R8A section 1 is LATER authority than round 8 and is accepted by the
  -- product owner.  It withdraws round 8 Part 2.2 and Part 6 item 6 for a
  -- wholly sealed source-backed self-bill, word for word:
  --
  --   "Candidate Timesheet authorisation, Candidate-pay TSFIN existence/
  --    currentness/status, READY_FOR_INVOICE, pay holds, advances, settlement
  --    and pay query/reconciliation state are never self-bill admission
  --    predicates."
  --   "A stale or missing Candidate-pay TSFIN likewise does not block an
  --    otherwise-valid immutable final-source invoice movement.  It blocks
  --    Candidate-pay publication, readiness or reconciliation wherever those
  --    owners require current pay evidence."
  --
  -- and restores 02 CONTROLLING POLICY section 15 and acceptance row ISS-013
  -- ("Source invoice while Candidate pay pending/protected/frozen" -> ISSUED).
  --
  -- WHOLLY SEALED IS A POSITIVE TEST, and R8A's first precision point says so:
  -- "This must be implemented as a positive test of sealed status, never
  -- inherited by anything partially sealed or merely source-adjacent."  The
  -- test is cardinality(v_codes)=0 - the exact 24 section 11 branch, every
  -- source-side limb proved: current manifest and final source revision, exact
  -- presentation/binding/placement hashes, complete allocation with no missing
  -- or duplicate movement, every line bound, no extra ordinary line, exact
  -- signed amounts, VAT and header total, Client/group/cycle/currency/VAT
  -- consistency, NHSP physical independence, the configured correction shape,
  -- source price comparison success and a non-empty invoice.  It is the SAME
  -- value returned as `ok`, computed here and not re-read, so the two can never
  -- drift apart.
  --
  -- IF IT IS NOT PROVED, THE OLD BEHAVIOUR STANDS: every Candidate-pay TSFIN
  -- code remains an invoice blocker exactly as WP-28 installed it.  A mixed,
  -- unbound, tampered or partly sealed invoice is already failing closed on its
  -- own source codes and gains nothing here.
  --
  -- WHAT MOVES, AND WHAT DOES NOT.  For a wholly sealed self-bill every
  -- Candidate-pay TSFIN code moves from tsfin_blocker_codes to
  -- tsfin_deferred_blocker_codes - stale, expected-but-missing and the
  -- undetermined/disagreement codes alike, because each of them is a judgement
  -- about the Candidate financial record and R8A names that whole class.
  -- tsfin_payment_blocker_codes is NOT touched by any of this: it already
  -- carries the same facts and it is what keeps Candidate-pay publication,
  -- readiness and reconciliation fail-closed.  The invoice moves; the payment
  -- does not.  Nothing in v_codes - the source-side truth - is touched either.
  -- =========================================================================
  v_wholly_sealed:=cardinality(v_codes)=0;
  v_tsfin_codes:=coalesce(v_tsfin_refusal->'blocker_codes','[]'::jsonb);
  v_tsfin_deferred:=coalesce(v_tsfin_refusal->'deferred_blocker_codes','[]'::jsonb);
  v_tsfin_disposition:=coalesce(v_tsfin_refusal->>'invoice_disposition','');
  if v_wholly_sealed and pg_catalog.jsonb_typeof(v_tsfin_codes)='array'
     and pg_catalog.jsonb_typeof(v_tsfin_deferred)='array' then
    if pg_catalog.jsonb_array_length(v_tsfin_codes)>0 then
      -- The sorted union of the two arrays, so the deferred record stays a set
      -- and its order can never carry meaning.
      select coalesce(pg_catalog.jsonb_agg(code order by code),'[]'::jsonb)
        into v_tsfin_deferred
      from (
        select distinct element.value code
        from pg_catalog.jsonb_array_elements_text(
          v_tsfin_codes||v_tsfin_deferred) as element(value)
      ) merged;
      v_tsfin_codes:='[]'::jsonb;
      -- The typed outcome.  The proved first-authorisation state keeps its own
      -- name, because R8A names it separately and round 8 Part 2.1 requires an
      -- explicit typed disposition for it; every other Candidate-pay condition
      -- on a wholly sealed self-bill gets the boundary's own name.
      if v_tsfin_disposition
           <>'WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT' then
        v_tsfin_disposition:=
          'WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE';
      end if;
    elsif v_tsfin_disposition
            ='WEEKLY_SOURCE_INVOICE_TSFIN_EXPECTED_BUT_MISSING_NOT_ENFORCED' then
      v_tsfin_disposition:=
        'WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE';
    end if;
  end if;

  return pg_catalog.jsonb_build_object(
    'is_source_invoice',true,
    'ok',cardinality(v_codes)=0,
    'invoice_id',p_invoice_id,
    'line_count',v_line_count,
    'binding_count',v_binding_count,
    'wholly_sealed_source_self_bill',v_wholly_sealed,
    'blocker_codes',pg_catalog.to_jsonb(v_codes),
    'tsfin_blocker_codes',v_tsfin_codes,
    'tsfin_invoice_disposition',v_tsfin_disposition,
    'tsfin_payment_blocker_codes',
      coalesce(v_tsfin_refusal->'payment_blocker_codes','[]'::jsonb),
    'tsfin_deferred_blocker_codes',v_tsfin_deferred,
    'tsfin_deferred_reason',
      case when pg_catalog.jsonb_array_length(v_tsfin_deferred)>0
        then case when v_wholly_sealed
          then 'WEEKLY_SOURCE_TSFIN_CANDIDATE_PAY_NOT_A_SELF_BILL_ADMISSION_PREDICATE'
          else v_tsfin_refusal->>'deferred_reason' end end,
    'tsfin_deferred_authority',
      case when pg_catalog.jsonb_array_length(v_tsfin_deferred)>0
        then case when v_wholly_sealed
          then 'HANDOVER 2 CORRECTION ADDENDUM R8A section 1, later authority than '
            ||'round 8 and accepted by the product owner; sealed pack '
            ||'02 CONTROLLING POLICY section 15, annex FTI-013 and acceptance row '
            ||'ISS-013. The fact is retained for Candidate-pay publication, '
            ||'readiness and reconciliation in tsfin_payment_blocker_codes'
          else v_tsfin_refusal->>'deferred_authority' end end,
    'detail',v_detail
  );
end;
$function$;

-- ---------------------------------------------------------------------------
-- HANDOVER 2 round 5, Part E, "Invoice and TSFIN outcomes", word for word:
--
--   "Missing TSFIN may be skipped only when positive route/profile evidence
--    proves TSFIN is not applicable and was never required.  Expected-but-
--    missing TSFIN blocks.  Stale TSFIN always blocks.  Record these three
--    states separately."
--
-- Before this owner existed, private.weekly_source_invoice_issue_skippable_code_v1
-- skipped INVOICE_CORRECTION_TSFIN_MISSING on the strength of the CODE TEXT
-- alone.  That is a skip conditioned on ABSENCE: the predicate never looked at
-- the invoice, so it could not distinguish a record that was never applicable
-- from one that is expected and has not arrived.  ABSENCE IS NOT EVIDENCE.
--
-- The three states this owner names:
--
--   TSFIN_PRESENT                a current public.timesheets_financials row
--                                exists for the member and is not stale.
--   TSFIN_STALE                  a current row exists and is_stale.  ALWAYS
--                                blocks; it is never skippable, on any
--                                evidence, for any route or profile.
--   TSFIN_NOT_APPLICABLE_PROVED  no current row, and every positive route and
--                                profile fact below holds.  Skippable.
--   TSFIN_EXPECTED_BUT_MISSING   no current row and the positive evidence does
--                                NOT hold.  Blocks.  This is the default for
--                                every absent record, which is what makes the
--                                skip conditional on proof rather than on
--                                absence.
--
-- The positive evidence, and the pack authority for each limb.  Every limb is
-- a fact that must be POSITIVELY PRESENT; none of them is an absence.  All of
-- them must hold for EVERY line of this invoice that carries the member
-- Timesheet, so a member that also appears on an ordinary or positive line is
-- never proved:
--
--   R1 route     every such line is bound by a CURRENT
--                weekly_source_invoice_line_bindings row to a presentation line
--                whose billing movement names this same Timesheet in
--                invoice_timesheet_id.  24 section 11: the Timesheet
--                relationship is carried "where required for navigation,
--                without treating Candidate evidence as invoice authority" -
--                the line's economic authority is the source movement.
--   R2 route     every such movement is an NHSP physical backing-report row
--                (nhsp_upload_row_id present), source_profile_kind
--                NHSP_TRUST_BACKING_REPORT, source_line_kind
--                NHSP_PHYSICAL_FULL_NEGATIVE, movement_role REVERSAL.
--                24 section 10: "NHSP physical negative and positive rows are
--                never netted"; 24 section 9: they "remain separate invoice
--                movements".
--   P1 profile   the upload that produced that physical row used a source
--                format profile that is ACTIVE in
--                public.weekly_source_format_profiles with
--                final_authority_kind NHSP_TRUST_BACKING_REPORT and
--                physical_negative_meaning NHSP_FULL_REVERSAL.  The profile is
--                positively identified, never assumed.  14 section 7 and the
--                release-controlled profile seed.
--   A1 amount    every such presentation line has amount_authority
--                VALIDATED_SOURCE_PENCE and a passing source price check
--                (EXACT or SOURCE_ROUNDING_EQUIVALENT).  24 section 11:
--                "source price comparison success".  The invoiced value came
--                from the source, so it never depended on a Candidate
--                financial record.
--   N1 no pay    the installed ordinary pay projection owner has itself
--                recorded, for a member of this Timesheet's family, a
--                weekly_source_ordinary_pay_projection_receipts row whose
--                outcome is NO_OP_FIRST_NEGATIVE and which published no
--                financial.  That receipt is the INSTALLED SYSTEM'S OWN
--                POSITIVE PROOF that the movement had no Candidate-pay effect:
--                02 CONTROLLING POLICY section 297, "without unique
--                post-activation lineage it has no candidate-pay effect";
--                08 STATE TRANSITIONS SRC-T11 and NHSBR-T04, "a lineage-free
--                first-activation negative has invoice effect but no
--                candidate-pay effect"; 14 NHSP-BR-014.  A persisted receipt
--                is a fact, not an absence.
--
-- and two fail-closed guards, which are NOT the evidence and cannot stand in
-- for it.  They exist only to stop a record that once existed being called
-- "never required":
--
--   G1 no financial row has EVER existed for any member of the Timesheet
--      family (resolved through the installed family helper, so nothing is
--      keyed on the physical root id alone after schema change S8);
--   G2 no projection receipt for the family published a financial.
--
-- The family is resolved through private.weekly_source_invoice_family_timesheet_ids_v1,
-- which delegates to the installed rotation resolver
-- public._pay_timesheet_rotation_scope (call-only under contract section 2).
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_invoice_tsfin_state_v1(
  p_invoice_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_row record;
  v_members jsonb:='[]'::jsonb;
  v_present integer:=0;
  v_stale integer:=0;
  v_missing integer:=0;
  v_proved integer:=0;
  v_member_count integer:=0;
  v_state text;
  v_member_state text;
  v_family uuid[];
  v_has_current boolean;
  v_is_stale boolean;
  v_ever_had_financial boolean;
  v_published_financial boolean;
  v_no_pay_effect_receipt boolean;
  v_evidence_ok boolean;
  v_error text;
  -- WP-33, HANDOVER 2 round 8 Part 2.1.  The expected-but-missing count is
  -- RETAINED exactly as it was - the ruling requires that fact kept "for
  -- payment/readiness purposes" - and is SPLIT, alongside it and without
  -- replacing it, into the one state the ruling exempts and everything else.
  v_first_auth jsonb;
  v_member_proved boolean;
  v_member_disposition text;
  v_awaiting integer:=0;
  v_unexplained integer:=0;
  v_invoice_disposition_claim text;
begin
  if p_invoice_id is null then
    return pg_catalog.jsonb_build_object(
      'invoice_id',null,
      'state','TSFIN_NOT_EVALUATED',
      'counts',pg_catalog.jsonb_build_object(
        'members',0,'present',0,'stale',0,
        'expected_but_missing',0,'not_applicable_proved',0,
        'awaiting_first_authorisation',0,
        'expected_but_missing_unexplained',0),
      'invoice_disposition_claim','WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED',
      'members','[]'::jsonb
    );
  end if;

  begin
    for v_row in
      select line.timesheet_id,
        pg_catalog.count(*)::integer line_count,
        pg_catalog.count(*) filter (where binding.id is not null)::integer
          bound_line_count,
        coalesce(pg_catalog.bool_and(
          movement.invoice_timesheet_id=line.timesheet_id),false) route_line_bound,
        coalesce(pg_catalog.bool_and(
          movement.nhsp_upload_row_id is not null),false) route_physical_row,
        coalesce(pg_catalog.bool_and(
          movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT'),false)
          route_backing_report,
        coalesce(pg_catalog.bool_and(
          movement.source_line_kind='NHSP_PHYSICAL_FULL_NEGATIVE'),false)
          route_physical_full_negative,
        coalesce(pg_catalog.bool_and(movement.movement_role='REVERSAL'),false)
          route_reversal,
        coalesce(pg_catalog.bool_and(
          profile.id is not null and profile.active
          and profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT'
          and profile.physical_negative_meaning='NHSP_FULL_REVERSAL'),false)
          profile_full_reversal,
        coalesce(pg_catalog.bool_and(
          presentation.amount_authority='VALIDATED_SOURCE_PENCE'),false)
          amount_source_authoritative,
        coalesce(pg_catalog.bool_and(
          presentation.price_check_result
            in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','ACCEPTED_DISPARITY','ACCEPTED_ZERO')),false) amount_price_passed
      from public.invoice_lines line
      left join public.weekly_source_invoice_line_bindings binding
        on binding.invoice_line_id=line.id
       and binding.invoice_id=p_invoice_id
       and binding.state='CURRENT'
      left join public.weekly_source_invoice_presentation_lines presentation
        on presentation.id=binding.presentation_line_id
      left join public.weekly_source_billing_movements movement
        on movement.id=presentation.billing_movement_id
      left join public.weekly_source_upload_rows upload_row
        on upload_row.id=movement.nhsp_upload_row_id
      left join public.weekly_source_uploads upload
        on upload.id=upload_row.upload_id
      left join public.weekly_source_format_profiles profile
        on profile.id=upload.source_format_profile_id
      where line.invoice_id=p_invoice_id
        and line.timesheet_id is not null
      group by line.timesheet_id
      order by line.timesheet_id
    loop
      v_member_count:=v_member_count+1;
      select coalesce(pg_catalog.bool_or(true),false),
             coalesce(pg_catalog.bool_or(coalesce(financial.is_stale,false)),false)
        into v_has_current,v_is_stale
      from public.timesheets_financials financial
      where financial.timesheet_id=v_row.timesheet_id and financial.is_current;
      v_has_current:=coalesce(v_has_current,false);
      v_is_stale:=coalesce(v_is_stale,false);

      v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(
        v_row.timesheet_id);
      v_ever_had_financial:=exists(
        select 1 from public.timesheets_financials financial
        where financial.timesheet_id=any(v_family));
      v_published_financial:=exists(
        select 1 from public.weekly_source_ordinary_pay_projection_receipts receipt
        where receipt.root_timesheet_id=any(v_family)
          and receipt.published_timesheet_financial_id is not null);
      v_no_pay_effect_receipt:=exists(
        select 1 from public.weekly_source_ordinary_pay_projection_receipts receipt
        where receipt.root_timesheet_id=any(v_family)
          and receipt.outcome='NO_OP_FIRST_NEGATIVE'
          and receipt.published_timesheet_financial_id is null);

      v_evidence_ok:=
        v_row.line_count=v_row.bound_line_count
        and v_row.route_line_bound
        and v_row.route_physical_row
        and v_row.route_backing_report
        and v_row.route_physical_full_negative
        and v_row.route_reversal
        and v_row.profile_full_reversal
        and v_row.amount_source_authoritative
        and v_row.amount_price_passed
        and v_no_pay_effect_receipt
        and not v_ever_had_financial
        and not v_published_financial;

      -- The five member states are UNCHANGED by WP-33.  Nothing below moves a
      -- member out of TSFIN_EXPECTED_BUT_MISSING, because HANDOVER 2 round 8
      -- Part 2.1 requires the expected-but-missing FACT to be retained for
      -- payment and readiness.  What WP-33 adds is a second, orthogonal
      -- classification of the SAME member: its invoice disposition.
      v_first_auth:=null;
      v_member_proved:=false;
      if v_is_stale then
        v_member_state:='TSFIN_STALE';
        v_stale:=v_stale+1;
      elsif v_has_current then
        v_member_state:='TSFIN_PRESENT';
        v_present:=v_present+1;
      elsif v_evidence_ok then
        v_member_state:='TSFIN_NOT_APPLICABLE_PROVED';
        v_proved:=v_proved+1;
      else
        v_member_state:='TSFIN_EXPECTED_BUT_MISSING';
        v_missing:=v_missing+1;
        -- WP-33.  The ONE exempt state, proved POSITIVELY and only here.  The
        -- proof is computed for no other member state: a stale, present or
        -- never-applicable member is decided by the evidence above and cannot
        -- reach the exemption at all.
        v_first_auth:=private.weekly_source_invoice_tsfin_first_authorisation_v1(
          v_row.timesheet_id);
        v_member_proved:=coalesce(
          case when pg_catalog.jsonb_typeof(v_first_auth->'proved')='boolean'
            then (v_first_auth->>'proved')::boolean end,false);
        if v_member_proved then
          v_awaiting:=v_awaiting+1;
        else
          v_unexplained:=v_unexplained+1;
        end if;
      end if;

      -- The typed per-member invoice disposition.  CLEAR and EXEMPT are the
      -- only two non-blocking values and each one names exactly why.
      v_member_disposition:=case
        when v_member_state in ('TSFIN_PRESENT','TSFIN_NOT_APPLICABLE_PROVED')
          then 'WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR'
        when v_member_state='TSFIN_EXPECTED_BUT_MISSING' and v_member_proved
          then 'WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT'
        else 'WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED' end;

      v_members:=v_members||pg_catalog.jsonb_build_object(
        'timesheet_id',v_row.timesheet_id,
        'state',v_member_state,
        'invoice_disposition',v_member_disposition,
        'first_authorisation',coalesce(v_first_auth,'null'::jsonb),
        'blocks',v_member_state in ('TSFIN_STALE','TSFIN_EXPECTED_BUT_MISSING'),
        'evidence',pg_catalog.jsonb_build_object(
          'line_count',v_row.line_count,
          'bound_line_count',v_row.bound_line_count,
          'route_line_bound',v_row.route_line_bound,
          'route_physical_row',v_row.route_physical_row,
          'route_backing_report',v_row.route_backing_report,
          'route_physical_full_negative',v_row.route_physical_full_negative,
          'route_reversal',v_row.route_reversal,
          'profile_full_reversal',v_row.profile_full_reversal,
          'amount_source_authoritative',v_row.amount_source_authoritative,
          'amount_price_passed',v_row.amount_price_passed,
          'no_candidate_pay_effect_receipt',v_no_pay_effect_receipt,
          'family_size',cardinality(coalesce(v_family,array[]::uuid[])),
          'family_ever_had_financial',v_ever_had_financial,
          'family_published_financial',v_published_financial,
          'positive_evidence_complete',v_evidence_ok
        )
      );
    end loop;
  exception when others then
    -- Fail closed.  An owner that cannot establish the state must never let a
    -- missing record be skipped.
    get stacked diagnostics v_error=message_text;
    return pg_catalog.jsonb_build_object(
      'invoice_id',p_invoice_id,
      'state','TSFIN_STATE_UNDETERMINED',
      'counts',pg_catalog.jsonb_build_object(
        'members',0,'present',0,'stale',0,
        'expected_but_missing',0,'not_applicable_proved',0,
        'awaiting_first_authorisation',0,
        'expected_but_missing_unexplained',0),
      'invoice_disposition_claim','WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED',
      'members','[]'::jsonb,
      'error',pg_catalog.left(coalesce(v_error,''),200)
    );
  end;

  -- Fail-closed precedence.  Stale outranks everything; an expected-but-missing
  -- record outranks a proved one, so one unproved member is enough to block.
  if v_member_count=0 then
    v_state:='TSFIN_NO_MEMBERS';
  elsif v_stale>0 then
    v_state:='TSFIN_STALE';
  elsif v_missing>0 then
    v_state:='TSFIN_EXPECTED_BUT_MISSING';
  elsif v_proved>0 then
    v_state:='TSFIN_NOT_APPLICABLE_PROVED';
  else
    v_state:='TSFIN_PRESENT';
  end if;

  -- WP-33.  This owner's own claim about the invoice disposition.  It is a
  -- CLAIM and not the decision: private.weekly_source_invoice_tsfin_refusal_v1
  -- derives the same value independently from the counts and refuses when the
  -- two disagree, so neither the claim alone nor the counts alone can widen.
  --
  -- ONE unexplained expected-but-missing member is enough to block the whole
  -- invoice, so the exemption is never a majority verdict over the members.
  v_invoice_disposition_claim:=case
    when v_member_count=0 then 'WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR'
    when v_stale>0 then 'WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED'
    when v_missing>0 and v_unexplained=0 and v_awaiting=v_missing
      then 'WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT'
    -- Computed, not enforced: see the sealed-pack block in
    -- private.weekly_source_invoice_tsfin_refusal_v1.
    when v_missing>0
      then 'WEEKLY_SOURCE_INVOICE_TSFIN_EXPECTED_BUT_MISSING_NOT_ENFORCED'
    else 'WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR' end;

  return pg_catalog.jsonb_build_object(
    'invoice_id',p_invoice_id,
    'state',v_state,
    'counts',pg_catalog.jsonb_build_object(
      'members',v_member_count,
      'present',v_present,
      'stale',v_stale,
      'expected_but_missing',v_missing,
      'not_applicable_proved',v_proved,
      -- Retained AND split: expected_but_missing keeps its exact former value
      -- and is the sum of these two.
      'awaiting_first_authorisation',v_awaiting,
      'expected_but_missing_unexplained',v_unexplained),
    'invoice_disposition_claim',v_invoice_disposition_claim,
    'members',v_members
  );
end;
$function$;

-- ---------------------------------------------------------------------------
-- WP-33.  The POSITIVE proof that one member Timesheet's week is genuinely
-- AWAITING ITS FIRST AUTHORISATION, and nothing else.
--
-- HANDOVER 2 round 8, Part 2.1, word for word:
--
--   "RULING: the pack rule preserving source-invoice issue wins for the exact
--    awaiting-first-authorisation case.  A week that is genuinely awaiting its
--    first authorisation must not stop its source invoice merely because the
--    financial record does not yet exist.  Do not rely on the current
--    lowest-ordinal-code masking.  Make the outcome explicit:
--      - retain the computed expected-but-missing fact for payment/readiness
--        purposes;
--      - keep payment and financial publication fail-closed;
--      - return an explicit, typed, non-blocking invoice disposition for
--        AWAITING_FIRST_AUTHORISATION;
--      - permit invoice issue only for that exact proved state;
--      - do not generalise it to a missing, stale, invalid, previously
--        applicable or unexplained financial record."
--
-- and pack 24 section 4.1, which is the state this owner has to recognise:
--
--   "Office sees `Hours to authorise` and uses the existing ordinary Authorise
--    action exactly once.  The existing ordinary authorisation owner remains
--    the only first-authorisation mechanism.  The first authorisation may occur
--    before or after the source self-bill invoice is created or issued."
--
-- THIS FUNCTION IS A WIDENING, so every limb is a fact that must be POSITIVELY
-- PRESENT or a count that must be POSITIVELY ZERO.  None of them is "the
-- absence of evidence to the contrary", and none of them is decided by a
-- `limit`, an `order by` or the position of a CASE arm.  Every failing limb is
-- collected and returned; the verdict is the CONJUNCTION of all of them, so the
-- outcome cannot change if the reasons are reordered or if one is removed.
--
-- The three INDEPENDENT families of evidence, any one of which alone refuses:
--
--   A  Weekly Source authorisation history.  public.weekly_source_root_authorisations
--      is append-only; re-authorisation appends generation N+1 and a withdrawn
--      generation keeps its row (15092026_1534_weekly_source_plan6_schema.sql:2945).
--      So ZERO rows for the whole family - live and historical, keyed on the
--      family member ids AND independently on the family booking id - is the
--      exact proof that no first authorisation has ever been taken.  A week
--      whose first authorisation was WITHDRAWN under 24 section 4.1A therefore
--      does NOT qualify: it is a previously applicable record, which round 8
--      Part 2.1 names as excluded.
--   B  Ordinary authorisation.  public.timesheet_authorise_generic_atomic is the
--      only first-authorisation mechanism (24 section 4.1) and it stamps
--      public.timesheets.authorised_at_server.  Every member of the family must
--      carry a NULL stamp, so an ordinary authorisation taken outside Weekly
--      Source cannot be missed.
--   C  Financial record history.  No public.timesheets_financials row may have
--      EVER existed for any member of the family, and no ordinary pay projection
--      receipt for the family may have published one.  That is "previously
--      applicable" in the ruling's own words.
--
-- plus three INTEGRITY limbs, which are not evidence and cannot stand in for it:
--
--   I1 the member Timesheet row exists;
--   I2 the invoice-side family resolver returned a non-empty family containing
--      the member;
--   I3 private.weekly_source_first_authorisation_context_v1 answers ok=true for
--      this member - that owner is the installed root-identity, Candidate and
--      cardinality authority (17092026_0600_weekly_source_first_authorisation_v1.sql:384)
--      - AND its member set is EXACTLY the invoice-side family.  Two resolvers
--      that disagree about which Timesheets are this week is a doubt, and doubt
--      refuses.
--
-- and two further append-only negatives:
--
--   D  no private.weekly_source_first_authorisation_withdrawal_receipts row for
--      the family (18092026_0900_weekly_source_withdrawal_supersession.sql:60);
--   E  no public.weekly_source_entitlement_heads row for the family, in any
--      state.  A head is written only by the publication coordinator from an
--      accepted Office decision, so a head means an entitlement was published.
--      A certified-zero PROPOSAL writes a decision bundle, NOT a head
--      (15092026_1534_weekly_source_ordinary_pay_projection_v1.sql:1799), so an
--      unauthorised week that has a proposal still qualifies.
--   F  no weekly_source_ordinary_pay_projection_receipts row for the family
--      whose outcome is NO_OP_FIRST_NEGATIVE.
--
--      F IS THE LINE BETWEEN THE TWO NON-BLOCKING OUTCOMES, and round 8
--      Part 2.1 draws it explicitly: "The one proven first-activation
--      never-applicable receipt remains a SEPARATE valid skip."  That receipt is
--      the installed system's own positive statement that the movement has NO
--      candidate-pay effect (02 CONTROLLING POLICY section 297; 08 STATE
--      TRANSITIONS SRC-T11 and NHSBR-T04; 14 NHSP-BR-014).  A week that will
--      never have a financial record is NOT a week AWAITING one, so it must not
--      reach this exemption even though it looks identical on limbs A, B and C -
--      no authorisation, no financial row, no head.
--
--      Executed consequence, and the reason this limb exists rather than being
--      argued: WP-05c's verifier withdraws ONE positive limb from a proved
--      never-applicable invoice (it deactivates the NHSP format profile) and
--      asserts the invoice then blocks, because the skip must rest on evidence
--      and not on absence.  Without F, that invoice fell straight through into
--      this exemption and was skipped again - the never-applicable case would
--      have been MERGED into the new disposition by the back door, which the
--      ruling forbids in terms.  With F it blocks, and WP-05c's assertion stands
--      unchanged.
--
-- LOCKS.  This function is STABLE and takes none, for the same reason
-- private.weekly_source_invoice_tsfin_state_v1 takes none: it is called from
-- inside private._invoice_issue_validate_batch, which evaluates a whole batch in
-- ONE statement, and the issue lock belongs to the volatile callers.  The
-- widening is safe under that read because every relation it reads is
-- APPEND-ONLY in the dangerous direction: a concurrent transaction can only ADD
-- an authorisation, a financial row, a head or a receipt, and any of those
-- additions moves the member OUT of TSFIN_EXPECTED_BUT_MISSING (to
-- TSFIN_PRESENT) rather than into a wrongly proved exemption.  That is an
-- argument, not a measurement, and it is labelled as such in the WP-33 report.
--
--   returns {"timesheet_id": uuid,
--            "proved": boolean,
--            "reason_codes": [...]   empty exactly when proved,
--            "facts": {...}}
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_invoice_tsfin_first_authorisation_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_reasons text[]:=array[]::text[];
  v_family uuid[];
  v_family_size integer;
  v_member_present boolean:=false;
  v_context jsonb;
  v_context_ok boolean:=false;
  v_context_members uuid[];
  v_context_family_agrees boolean:=false;
  v_context_family_auth_known boolean:=false;
  v_context_live_auth_known boolean:=false;
  v_context_max_generation_known boolean:=false;
  v_context_family_auth numeric;
  v_context_live_auth numeric;
  v_context_max_generation numeric;
  v_booking_ids text[];
  v_authorisation_rows integer;
  v_authorisation_rows_by_booking integer;
  v_withdrawal_receipts integer;
  v_entitlement_heads integer;
  v_financial_rows integer;
  v_published_projection integer;
  v_never_applicable_receipts integer;
  v_authorised_stamps integer;
  v_proved boolean;
  v_error text;
begin
  if p_timesheet_id is null then
    return pg_catalog.jsonb_build_object(
      'timesheet_id',null,'proved',false,
      'reason_codes',pg_catalog.to_jsonb(
        array['WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_NO_TIMESHEET']::text[]),
      'facts','{}'::jsonb);
  end if;

  begin
    -- I1.  The member Timesheet row itself.
    select true into v_member_present
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=p_timesheet_id;
    v_member_present:=coalesce(v_member_present,false);
    if not v_member_present then
      v_reasons:=v_reasons
        ||'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_NO_TIMESHEET'::text;
    end if;

    -- I2.  The invoice-side family, resolved by the SAME helper every other
    -- limb of this owner uses, so nothing is keyed on the physical root id
    -- alone after schema change S8 (standing rule 3).
    v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(p_timesheet_id);
    v_family:=coalesce(v_family,array[]::uuid[]);
    v_family_size:=cardinality(v_family);
    if v_family_size<1 or not (p_timesheet_id=any(v_family)) then
      v_reasons:=v_reasons
        ||'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_FAMILY_UNRESOLVED'::text;
      -- Without a family there is nothing to count over.  Every remaining limb
      -- is left unproved rather than evaluated against a guessed scope.
      v_family:=array[]::uuid[];
      v_family_size:=0;
    end if;

    -- I3.  The installed root-identity / Candidate / cardinality authority.
    v_context:=private.weekly_source_first_authorisation_context_v1(p_timesheet_id);
    -- Three-valued JSON read forced to two values (standing rule 4): absent,
    -- JSON null and a non-boolean are all the unsafe value.
    v_context_ok:=coalesce(
      case when pg_catalog.jsonb_typeof(v_context->'ok')='boolean'
        then (v_context->>'ok')::boolean end,false);
    if not v_context_ok then
      v_reasons:=v_reasons
        ||'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_CONTEXT_UNAVAILABLE'::text;
    end if;

    if v_context_ok and pg_catalog.jsonb_typeof(v_context->'member_timesheet_ids')='array' then
      select pg_catalog.array_agg(distinct member_element.value::uuid)
        into v_context_members
      from pg_catalog.jsonb_array_elements_text(v_context->'member_timesheet_ids')
        as member_element(value);
    end if;
    v_context_members:=coalesce(v_context_members,array[]::uuid[]);
    -- Exact set equality in both directions, by explicit cardinality and
    -- containment.  Two resolvers that disagree about the week are a doubt.
    v_context_family_agrees:=
      v_family_size>0
      and cardinality(v_context_members)=v_family_size
      and not exists(
        select 1 from pg_catalog.unnest(v_family) as invoice_side(member_id)
        where not (invoice_side.member_id=any(v_context_members)))
      and not exists(
        select 1 from pg_catalog.unnest(v_context_members) as context_side(member_id)
        where not (context_side.member_id=any(v_family)));
    if not v_context_family_agrees then
      v_reasons:=v_reasons
        ||'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_FAMILY_DISAGREEMENT'::text;
    end if;

    -- A.  Weekly Source authorisation history, read FIVE independent ways: the
    -- context owner's three counts, and this owner's own two direct counts.
    -- A count that is not a readable JSON number is not read as zero.
    v_context_family_auth_known:=coalesce(
      pg_catalog.jsonb_typeof(v_context->'family_authorisation_count')='number',false);
    v_context_live_auth_known:=coalesce(
      pg_catalog.jsonb_typeof(v_context->'live_authorisation_count')='number',false);
    v_context_max_generation_known:=coalesce(
      pg_catalog.jsonb_typeof(v_context->'max_generation')='number',false);
    if v_context_family_auth_known then
      v_context_family_auth:=(v_context->>'family_authorisation_count')::numeric;
    end if;
    if v_context_live_auth_known then
      v_context_live_auth:=(v_context->>'live_authorisation_count')::numeric;
    end if;
    if v_context_max_generation_known then
      v_context_max_generation:=(v_context->>'max_generation')::numeric;
    end if;
    if not v_context_family_auth_known or not v_context_live_auth_known
       or not v_context_max_generation_known
       or coalesce(v_context_family_auth,1)<>0
       or coalesce(v_context_live_auth,1)<>0
       or coalesce(v_context_max_generation,1)<>0 then
      v_reasons:=v_reasons
        ||'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_PREVIOUSLY_AUTHORISED'::text;
    end if;

    select pg_catalog.count(*)::integer into v_authorisation_rows
    from public.weekly_source_root_authorisations authorisation_row
    where authorisation_row.root_timesheet_id=any(v_family);
    v_authorisation_rows:=coalesce(v_authorisation_rows,1);

    select pg_catalog.array_agg(distinct pg_catalog.btrim(timesheet_row.booking_id))
      into v_booking_ids
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=any(v_family)
      and pg_catalog.btrim(coalesce(timesheet_row.booking_id,''))<>'';
    v_booking_ids:=coalesce(v_booking_ids,array[]::text[]);
    select pg_catalog.count(*)::integer into v_authorisation_rows_by_booking
    from public.weekly_source_root_authorisations authorisation_row
    where pg_catalog.btrim(authorisation_row.family_booking_id)=any(v_booking_ids);
    v_authorisation_rows_by_booking:=coalesce(v_authorisation_rows_by_booking,1);

    if v_family_size<1 or cardinality(v_booking_ids)<1
       or v_authorisation_rows<>0 or v_authorisation_rows_by_booking<>0 then
      v_reasons:=v_reasons
        ||'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_PREVIOUSLY_AUTHORISED'::text;
    end if;

    -- B.  The ordinary authorisation stamp, on EVERY member of the family.
    select pg_catalog.count(*)::integer into v_authorised_stamps
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=any(v_family)
      and timesheet_row.authorised_at_server is not null;
    v_authorised_stamps:=coalesce(v_authorised_stamps,1);
    if v_family_size<1 or v_authorised_stamps<>0 then
      v_reasons:=v_reasons
        ||'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_ORDINARY_AUTHORISATION_STAMP'::text;
    end if;

    -- C.  Financial record history: "previously applicable", in the ruling's
    -- own words.  ANY row that ever existed for ANY member disqualifies.
    select pg_catalog.count(*)::integer into v_financial_rows
    from public.timesheets_financials financial_row
    where financial_row.timesheet_id=any(v_family);
    v_financial_rows:=coalesce(v_financial_rows,1);
    select pg_catalog.count(*)::integer into v_published_projection
    from public.weekly_source_ordinary_pay_projection_receipts receipt
    where receipt.root_timesheet_id=any(v_family)
      and receipt.published_timesheet_financial_id is not null;
    v_published_projection:=coalesce(v_published_projection,1);
    if v_family_size<1 or v_financial_rows<>0 or v_published_projection<>0 then
      v_reasons:=v_reasons
        ||'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_PREVIOUSLY_APPLICABLE_FINANCIAL'::text;
    end if;

    -- D.  A recorded first-authorisation withdrawal (24 section 4.1A).  The
    -- week is back at `Awaiting authorisation`, but it is NOT awaiting its
    -- FIRST authorisation: a generation was taken and retired, which is a
    -- previously applicable record.
    select pg_catalog.count(*)::integer into v_withdrawal_receipts
    from private.weekly_source_first_authorisation_withdrawal_receipts receipt
    where receipt.root_timesheet_id=any(v_family)
       or receipt.requested_timesheet_id=any(v_family);
    v_withdrawal_receipts:=coalesce(v_withdrawal_receipts,1);
    if v_family_size<1 or v_withdrawal_receipts<>0 then
      v_reasons:=v_reasons
        ||'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_WITHDRAWAL_RECORDED'::text;
    end if;

    -- E.  A published entitlement head, in any state.
    select pg_catalog.count(*)::integer into v_entitlement_heads
    from public.weekly_source_entitlement_heads head
    where head.root_timesheet_id=any(v_family);
    v_entitlement_heads:=coalesce(v_entitlement_heads,1);
    if v_family_size<1 or v_entitlement_heads<>0 then
      v_reasons:=v_reasons
        ||'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_ENTITLEMENT_HEAD_EXISTS'::text;
    end if;

    -- F.  The proven first-activation never-applicable receipt.  Its presence
    -- says this movement will NEVER have a candidate-pay effect, so the week is
    -- not AWAITING a first authorisation and belongs to the separate skip round
    -- 8 Part 2.1 preserves, never to this exemption.
    select pg_catalog.count(*)::integer into v_never_applicable_receipts
    from public.weekly_source_ordinary_pay_projection_receipts receipt
    where receipt.root_timesheet_id=any(v_family)
      and receipt.outcome='NO_OP_FIRST_NEGATIVE';
    v_never_applicable_receipts:=coalesce(v_never_applicable_receipts,1);
    if v_family_size<1 or v_never_applicable_receipts<>0 then
      v_reasons:=v_reasons
        ||'WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_NEVER_APPLICABLE_RECEIPT'::text;
    end if;
  exception when others then
    -- An owner that cannot establish the state must never widen.
    get stacked diagnostics v_error=message_text;
    return pg_catalog.jsonb_build_object(
      'timesheet_id',p_timesheet_id,'proved',false,
      'reason_codes',pg_catalog.to_jsonb(
        array['WEEKLY_SOURCE_FIRST_AUTHORISATION_NOT_PROVED_ERROR']::text[]),
      'facts',pg_catalog.jsonb_build_object(
        'error',pg_catalog.left(coalesce(v_error,''),200)));
  end;

  select coalesce(pg_catalog.array_agg(distinct reason order by reason),
                  array[]::text[])
    into v_reasons from pg_catalog.unnest(v_reasons) reason;

  -- The verdict is the CONJUNCTION of every positive limb, stated again in
  -- full.  It is deliberately NOT `cardinality(v_reasons)=0` alone: if a future
  -- revision adds a limb and forgets its reason code, this expression still
  -- refuses, and the two independent formulations are cross-checked in the
  -- verifier.
  v_proved:=
    v_member_present
    and v_family_size>0
    and p_timesheet_id=any(v_family)
    and v_context_ok
    and v_context_family_agrees
    and v_context_family_auth_known and coalesce(v_context_family_auth,1)=0
    and v_context_live_auth_known and coalesce(v_context_live_auth,1)=0
    and v_context_max_generation_known and coalesce(v_context_max_generation,1)=0
    and cardinality(coalesce(v_booking_ids,array[]::text[]))>0
    and coalesce(v_authorisation_rows,1)=0
    and coalesce(v_authorisation_rows_by_booking,1)=0
    and coalesce(v_authorised_stamps,1)=0
    and coalesce(v_financial_rows,1)=0
    and coalesce(v_published_projection,1)=0
    and coalesce(v_withdrawal_receipts,1)=0
    and coalesce(v_entitlement_heads,1)=0
    and cardinality(v_reasons)=0;

  return pg_catalog.jsonb_build_object(
    'timesheet_id',p_timesheet_id,
    'proved',v_proved,
    'reason_codes',pg_catalog.to_jsonb(v_reasons),
    'facts',pg_catalog.jsonb_build_object(
      'member_row_present',v_member_present,
      'family_size',v_family_size,
      'family_booking_id_count',cardinality(coalesce(v_booking_ids,array[]::text[])),
      'context_ok',v_context_ok,
      'context_family_agrees',v_context_family_agrees,
      'context_family_authorisation_count',
        case when v_context_family_auth_known then v_context_family_auth end,
      'context_live_authorisation_count',
        case when v_context_live_auth_known then v_context_live_auth end,
      'context_max_generation',
        case when v_context_max_generation_known then v_context_max_generation end,
      'root_authorisation_rows',v_authorisation_rows,
      'root_authorisation_rows_by_booking_id',v_authorisation_rows_by_booking,
      'ordinary_authorisation_stamps',v_authorised_stamps,
      'financial_rows_ever',v_financial_rows,
      'projection_published_financials',v_published_projection,
      'withdrawal_receipts',v_withdrawal_receipts,
      'entitlement_heads',v_entitlement_heads)
  );
end;
$function$;

-- ---------------------------------------------------------------------------
-- WP-28.  The BLOCKING half of the HANDOVER 2 round 5 Part E ruling, as ONE
-- predicate over the recorded state, so that both real issue routes refuse from
-- the same fact rather than from whatever the ordinary correction engine
-- happens to surface first.
--
-- Gate 13 hostile review 2, findings F1 and F2, executed on the real routes:
--
--   F2  "Stale TSFIN always blocks" held on the asynchronous route only because
--       private._invoice_correction_validate_batch puts
--       INVOICE_CORRECTION_TSFIN_STALE at a lower CASE ordinal than the
--       skippable stream codes, and its caller surfaces blocker_codes[1].  Any
--       earlier-ordinal skippable code masks it.  On the direct route no TSFIN
--       state was read at all and a stale source self-bill ISSUED, three times
--       out of three.  A rule that holds by CASE order is not implemented, so
--       the refusal is raised HERE and appended by both seams.
--
--   F1  "Expected-but-missing TSFIN blocks" was NOT enforced by this predicate
--       when WP-28 wrote it, because it was a live authority conflict the
--       approver had not answered.  WP-33 replaces that withheld branch: see
--       the round 8 Part 2.1 block further down.
--
-- Fail-closed reading.  The state is a jsonb document produced by
-- private.weekly_source_invoice_tsfin_state_v1.  A value that is absent, JSON
-- null, of the wrong JSON type, or an enumeration member this predicate does
-- not know is UNDETERMINED, and undetermined blocks.  Nothing here is decided
-- by a `limit`, an `order by` or the position of a CASE arm, and every count is
-- accepted only when jsonb_typeof proves it is a JSON number - "not readable"
-- is never read as "zero".
--
-- The state limb and the count limb are DELIBERATELY independent: a stale
-- member blocks whether or not the owner's own precedence still puts
-- TSFIN_STALE at the top, so a future reordering inside that owner cannot
-- silently unblock a stale record.
--
--   returns {"blocker_codes":         [...],  INVOICE refusals, both routes
--            "invoice_disposition":   text,   the typed invoice outcome
--            "payment_blocker_codes": [...],  PAYMENT and financial publication
--                                             refusals, retained separately}
--
-- Locks.  This predicate reads no relation.  The state document it judges was
-- read by private.weekly_source_invoice_tsfin_state_v1, which is STABLE by
-- contract because the asynchronous gate evaluates an entire batch in one
-- statement; the issue lock belongs to the volatile callers.  WP-28 introduces
-- no new unlocked read of guarded state - it judges a read that already
-- happened, and WP-33 adds no read here at all.
--
-- ===========================================================================
-- WP-33.  HANDOVER 2 round 8, Part 2.1 and Part 6.6, decide the conflict WP-28
-- escalated and left withheld.  Part 6.6, word for word:
--
--   "Resolved by Part 2.1: the exact awaiting-first-authorisation case does not
--    block source-invoice issue, but remains non-admissible for payment/
--    financial publication.  Implement an explicit typed invoice exception; do
--    not depend on masking or lowest-ordinal error selection.  All other
--    expected-but-missing and stale cases remain fail-closed."
--
-- So this predicate now answers TWO questions from the one document, and keeps
-- them apart:
--
--   blocker_codes          what stops the INVOICE.  The exempt state emits
--                          nothing here.
--   payment_blocker_codes  what stops PAYMENT and financial publication.  The
--                          exempt state STILL emits
--                          WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING
--                          here, which is the ruling's "retain the computed
--                          expected-but-missing fact ... keep payment and
--                          financial publication fail-closed".
--
-- THE EXEMPTION IS EXPLICIT, NOT A CONSEQUENCE OF ORDERING.  It fires only when
-- every one of these holds, and each is an independent, positively-read fact:
--
--   X1 the state is exactly TSFIN_EXPECTED_BUT_MISSING;
--   X2 counts.stale is a readable JSON number and is 0;
--   X3 counts.expected_but_missing is a readable JSON number and is > 0;
--   X4 counts.awaiting_first_authorisation is a readable JSON number and equals
--      counts.expected_but_missing - EVERY absent member is proved, not a
--      majority of them;
--   X5 counts.expected_but_missing_unexplained is a readable JSON number and is
--      0 - the same statement made the other way round, deliberately, so a
--      miscount in either direction refuses;
--   X6 counts.members is a readable JSON number and is > 0;
--   X7 the state owner's own invoice_disposition_claim is exactly
--      WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT.
--
-- X7 makes the two authorities CONJUNCTIVE: this predicate derives the
-- disposition itself from X1-X6, the state owner derives it independently from
-- the members it walked, and they must agree.  Neither alone can widen, and a
-- disagreement is WEEKLY_SOURCE_ISSUE_TSFIN_DISPOSITION_DISAGREEMENT, which
-- blocks.
--
-- STALE IS UNTOUCHED BY WP-33.  The two stale limbs below are WP-28's, byte for
-- byte, and the exemption cannot reach them: X2 requires counts.stale = 0 and
-- X1 requires the state not to be TSFIN_STALE.  Round 8 Part 2.2 ("a stale
-- record remains blocking ... must not be downgraded to the invoice exception
-- above") is therefore satisfied structurally, not by ordering.  The product
-- owner's 18 September countermand on stale blocking is recorded in the WP-33
-- report; this package neither extends nor reverts that block.
-- ===========================================================================
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_invoice_tsfin_refusal_v1(
  p_tsfin jsonb
) returns jsonb
language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_state text;
  v_codes text[]:=array[]::text[];
  v_payment text[]:=array[]::text[];
  v_deferred text[]:=array[]::text[];
  v_stale_known boolean;
  v_missing_known boolean;
  v_stale numeric;
  v_missing numeric;
  -- WP-33.
  v_awaiting_known boolean;
  v_unexplained_known boolean;
  v_members_known boolean;
  v_awaiting numeric;
  v_unexplained numeric;
  v_members numeric;
  v_claim text;
  v_exempt boolean:=false;
  v_disposition text;
begin
  -- No document, JSON null, or not an object: the financial-record state could
  -- not be established at all.  An owner that cannot establish the state must
  -- never let the invoice through.
  if p_tsfin is null or pg_catalog.jsonb_typeof(p_tsfin)<>'object' then
    return pg_catalog.jsonb_build_object(
      'blocker_codes',pg_catalog.to_jsonb(
        array['WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED']::text[]),
      'invoice_disposition','WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED',
      'payment_blocker_codes',pg_catalog.to_jsonb(
        array['WEEKLY_SOURCE_PAYMENT_TSFIN_STATE_UNDETERMINED']::text[])
    );
  end if;

  v_state:=coalesce(p_tsfin->>'state','');

  -- Three-valued read, forced to two values.  `#>` returns SQL NULL when the
  -- key is ABSENT and jsonb 'null' when it is present and null, so
  -- jsonb_typeof(...)='number' is itself NULL in the absent case and
  -- `if not v_stale_known` would silently not fire.  That is standing rule 4 of
  -- this programme and it was executed against this very predicate before the
  -- coalesce was added: a document with no `counts` object returned NO blocker
  -- at all.  Anything not provably a JSON number is now false.
  v_stale_known:=coalesce(
    pg_catalog.jsonb_typeof(p_tsfin#>'{counts,stale}')='number',false);
  v_missing_known:=coalesce(
    pg_catalog.jsonb_typeof(p_tsfin#>'{counts,expected_but_missing}')='number',
    false);
  if v_stale_known then
    v_stale:=(p_tsfin#>>'{counts,stale}')::numeric;
  end if;
  if v_missing_known then
    v_missing:=(p_tsfin#>>'{counts,expected_but_missing}')::numeric;
  end if;

  -- "Stale TSFIN always blocks."  Two independent limbs, neither of which is an
  -- ordering assumption.
  if v_state='TSFIN_STALE' then
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_TSFIN_STALE'::text;
  end if;
  if v_stale_known and v_stale>0 then
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_TSFIN_STALE'::text;
  end if;

  -- WP-33.  The same three-valued treatment for the three counts and the claim
  -- the round 8 exemption is derived from.  Absent, JSON null and a non-number
  -- are all "not readable", and not readable never becomes zero.  A document
  -- produced by an owner that does not carry these keys is therefore
  -- UNDETERMINED and blocks, which is what makes the exemption impossible to
  -- obtain from a partial or hand-made state document.
  v_awaiting_known:=coalesce(
    pg_catalog.jsonb_typeof(p_tsfin#>'{counts,awaiting_first_authorisation}')
      ='number',false);
  v_unexplained_known:=coalesce(
    pg_catalog.jsonb_typeof(p_tsfin#>'{counts,expected_but_missing_unexplained}')
      ='number',false);
  v_members_known:=coalesce(
    pg_catalog.jsonb_typeof(p_tsfin#>'{counts,members}')='number',false);
  if v_awaiting_known then
    v_awaiting:=(p_tsfin#>>'{counts,awaiting_first_authorisation}')::numeric;
  end if;
  if v_unexplained_known then
    v_unexplained:=(p_tsfin#>>'{counts,expected_but_missing_unexplained}')::numeric;
  end if;
  if v_members_known then
    v_members:=(p_tsfin#>>'{counts,members}')::numeric;
  end if;
  v_claim:=coalesce(p_tsfin->>'invoice_disposition_claim','');

  -- A count that is not a readable JSON number leaves the state undetermined,
  -- and undetermined blocks.
  if not v_stale_known or not v_missing_known
     or not v_awaiting_known or not v_unexplained_known or not v_members_known then
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED'::text;
  end if;
  -- The state owner's claim has THREE legal values and deliberately not four.
  -- WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE is set by
  -- the verdict assembly AFTER this predicate has run, from the wholly-sealed
  -- proof this predicate cannot see, so a state document claiming it is a
  -- document claiming an outcome it has no standing to reach.
  if v_claim not in (
       'WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR',
       'WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT',
       'WEEKLY_SOURCE_INVOICE_TSFIN_EXPECTED_BUT_MISSING_NOT_ENFORCED',
       'WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED') then
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED'::text;
  end if;

  -- Any state outside the closed enumeration this predicate was written
  -- against - including TSFIN_STATE_UNDETERMINED, TSFIN_NOT_EVALUATED and any
  -- member a future revision of the state owner adds - blocks until it is
  -- named here with its authority.  TSFIN_NO_MEMBERS is the one state with
  -- nothing to judge: an admitted source invoice always carries at least one
  -- member Timesheet, because a CURRENT binding whose invoice line has no
  -- timesheet_id already raises WEEKLY_SOURCE_ISSUE_TIMESHEET_LINK_INVALID.
  if v_state not in (
       'TSFIN_PRESENT','TSFIN_NOT_APPLICABLE_PROVED','TSFIN_STALE',
       'TSFIN_EXPECTED_BUT_MISSING','TSFIN_NO_MEMBERS') then
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED'::text;
  end if;

  -- -------------------------------------------------------------------------
  -- WP-33.  The round 8 Part 2.1 exemption, computed BEFORE anything consumes
  -- it and from nothing but positively-read facts.  X1..X7 are the seven limbs
  -- named in the header block; all seven, or no exemption.
  -- -------------------------------------------------------------------------
  v_exempt:=
    v_state='TSFIN_EXPECTED_BUT_MISSING'                             -- X1
    and v_stale_known and coalesce(v_stale,1)=0                      -- X2
    and v_missing_known and coalesce(v_missing,0)>0                  -- X3
    and v_awaiting_known and coalesce(v_awaiting,-1)=coalesce(v_missing,-2)  -- X4
    and v_unexplained_known and coalesce(v_unexplained,1)=0          -- X5
    and v_members_known and coalesce(v_members,0)>0                  -- X6
    and v_claim='WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT';

  -- -------------------------------------------------------------------------
  -- "Expected-but-missing TSFIN blocks" (HANDOVER 2 round 5 Part E, restated by
  -- round 8 Part 2.1 as "permit invoice issue only for that exact proved
  -- state") is COMPUTED HERE and DELIBERATELY NOT ENFORCED as an invoice
  -- refusal.  It is returned on its own key, typed, with a reason that is true
  -- on 18 September 2026.  Read the reason before changing this.
  --
  -- WHY IT IS NOT ENFORCED.  Enforcing it would make a Candidate financial-
  -- record state an admission predicate for a source-backed self-bill, and the
  -- SEALED pack forbids exactly that, in terms, in three places:
  --
  --   02 CONTROLLING POLICY section 15 "Self-bill", word for word:
  --     "Candidate Timesheet authorisation, TSFIN status, `READY_FOR_INVOICE`,
  --      candidate pay hold/advance/settlement and query/reconciliation state
  --      are never self-bill admission predicates.  This remains true even
  --      though the existing self-bill authorisation operation may set
  --      `READY_FOR_INVOICE`."
  --   annexes/financial-touchpoint-impact-matrix.csv FTI-013:
  --     "Candidate pay and Banking Pay state are irrelevant to source issue
  --      eligibility."
  --   annexes/invoice-issue-real-route-matrix.csv ISS-013, the acceptance row
  --   for this exact shape: "Source invoice while Candidate pay
  --   pending/protected/frozen" - validator expected "Ignore pay state;
  --   validate source only", generic rules skipped "Candidate pay checks
  --   irrelevant", expected result ISSUED.
  --
  -- Part 1 rule 1 of this programme: the pack outranks the contract, and the
  -- contract outranks this package's judgement.  A correction addendum
  -- restoring that boundary was outstanding when WP-33 was written, and the
  -- product owner had already countermanded the sibling stale rule on the same
  -- authority.  So WP-33 does not switch this on, and does not switch it off
  -- either: it computes it, names it, and leaves today's behaviour for these
  -- states byte-identical on both routes.
  --
  -- WHAT WP-33 DOES CHANGE is the OTHER half of round 8 Part 2.1, which every
  -- authority agrees on: the awaiting-first-authorisation invoice must issue
  -- EXPLICITLY, by a typed disposition and a positively proved state, instead
  -- of issuing only because INVOICE_CORRECTION_TARGET_STREAM_MISMATCH happens
  -- to carry a lower CASE ordinal than INVOICE_CORRECTION_TSFIN_MISSING.
  --
  -- TO ENFORCE IT, if the correction addendum rules that way: move the two
  -- appends below from v_deferred to v_codes.  That edit was EXECUTED against
  -- the installed text inside a rolled-back transaction and both routes were
  -- captured for all five states; the results are in the WP-33 report, so this
  -- is not a described-but-untried future edit (standing rule 11).
  -- -------------------------------------------------------------------------
  if v_state='TSFIN_EXPECTED_BUT_MISSING' and not v_exempt then
    v_deferred:=v_deferred||'WEEKLY_SOURCE_ISSUE_TSFIN_EXPECTED_BUT_MISSING'::text;
  end if;
  if v_missing_known and v_missing>0 and not v_exempt then
    v_deferred:=v_deferred||'WEEKLY_SOURCE_ISSUE_TSFIN_EXPECTED_BUT_MISSING'::text;
  end if;

  -- -------------------------------------------------------------------------
  -- PAYMENT AND FINANCIAL PUBLICATION.  "retain the computed expected-but-
  -- missing fact for payment/readiness purposes; keep payment and financial
  -- publication fail-closed" (round 8 Part 2.1).  These codes are emitted for
  -- the EXEMPT invoice too - that is the whole point of keeping them on a
  -- separate key.  The invoice issues; the payment does not.
  -- -------------------------------------------------------------------------
  if v_state='TSFIN_EXPECTED_BUT_MISSING' or (v_missing_known and v_missing>0) then
    v_payment:=v_payment||'WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING'::text;
  end if;
  if v_state='TSFIN_STALE' or (v_stale_known and v_stale>0) then
    v_payment:=v_payment||'WEEKLY_SOURCE_PAYMENT_TSFIN_STALE'::text;
  end if;
  if not v_stale_known or not v_missing_known or not v_awaiting_known
     or not v_unexplained_known or not v_members_known
     or v_state not in (
          'TSFIN_PRESENT','TSFIN_NOT_APPLICABLE_PROVED','TSFIN_STALE',
          'TSFIN_EXPECTED_BUT_MISSING','TSFIN_NO_MEMBERS') then
    v_payment:=v_payment||'WEEKLY_SOURCE_PAYMENT_TSFIN_STATE_UNDETERMINED'::text;
  end if;

  -- -------------------------------------------------------------------------
  -- The typed invoice disposition, DERIVED here and then required to agree with
  -- the state owner's independent claim.  The derivation is a closed CASE whose
  -- arms are mutually exclusive on their own conditions, so its result does not
  -- depend on arm order; the verifier proves that by driving every arm.
  -- -------------------------------------------------------------------------
  v_disposition:=case
    when cardinality(coalesce(v_codes,array[]::text[]))>0
      then 'WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED'
    when v_exempt
      then 'WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT'
    when cardinality(coalesce(v_deferred,array[]::text[]))>0
      then 'WEEKLY_SOURCE_INVOICE_TSFIN_EXPECTED_BUT_MISSING_NOT_ENFORCED'
    else 'WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR' end;

  -- The two authorities must agree.  A state owner that claims CLEAR while this
  -- predicate derives EXEMPT, or claims BLOCKED while this predicate derives
  -- CLEAR, is a contradiction and refuses.
  if v_claim is distinct from v_disposition then
    v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_TSFIN_DISPOSITION_DISAGREEMENT'::text;
    v_payment:=v_payment||'WEEKLY_SOURCE_PAYMENT_TSFIN_DISPOSITION_DISAGREEMENT'::text;
    v_disposition:='WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED';
  end if;

  select coalesce(pg_catalog.array_agg(distinct code order by code),
                  array[]::text[])
    into v_codes from pg_catalog.unnest(v_codes) code;
  select coalesce(pg_catalog.array_agg(distinct code order by code),
                  array[]::text[])
    into v_payment from pg_catalog.unnest(v_payment) code;
  select coalesce(pg_catalog.array_agg(distinct code order by code),
                  array[]::text[])
    into v_deferred from pg_catalog.unnest(v_deferred) code;

  return pg_catalog.jsonb_build_object(
    'blocker_codes',pg_catalog.to_jsonb(coalesce(v_codes,array[]::text[])),
    'invoice_disposition',v_disposition,
    'payment_blocker_codes',
      pg_catalog.to_jsonb(coalesce(v_payment,array[]::text[])),
    -- COMPUTED, NOT ENFORCED.  See the block above for the three sealed-pack
    -- statements that forbid enforcing it and the one-line edit that would.
    'deferred_blocker_codes',
      pg_catalog.to_jsonb(coalesce(v_deferred,array[]::text[])),
    'deferred_reason',
      case when cardinality(coalesce(v_deferred,array[]::text[]))>0
        then 'WEEKLY_SOURCE_TSFIN_ADMISSION_PREDICATE_BOUNDARY_CORRECTION_OUTSTANDING' end,
    'deferred_authority',
      case when cardinality(coalesce(v_deferred,array[]::text[]))>0
        then 'HANDOVER 2 round 8 Part 2.1 and Part 6.6 require this refusal; '
          ||'sealed pack 02 CONTROLLING POLICY section 15, annex FTI-013 and '
          ||'annex row ISS-013 forbid a Candidate TSFIN state being a self-bill '
          ||'admission predicate at all.  The product owner countermanded the '
          ||'sibling stale rule on 18 September 2026 and a correction addendum '
          ||'is outstanding, so WP-33 computes this refusal and does not '
          ||'enforce it' end
  );
end;
$function$;

-- ---------------------------------------------------------------------------
-- The evidence gate for the two financial-record codes the ordinary correction
-- engine emits for an ABSENT or NOT-YET-READY record.  Nothing here is keyed on
-- the code text alone.
--
--   INVOICE_CORRECTION_TSFIN_MISSING    skippable ONLY when this invoice's
--     state is TSFIN_NOT_APPLICABLE_PROVED: at least one member proved never
--     applicable, and NO member is stale or expected-but-missing.
--   INVOICE_CORRECTION_TSFIN_NOT_READY  the record exists but is not
--     READY_FOR_INVOICE, or ready_count<>member_count.  25 section 5,
--     "self-bill finalisation and issue never wait for a protected-pay decision
--     or a frozen payment state", still authorises skipping a PAYMENT STATE -
--     but the installed predicate also fires when a record is ABSENT
--     (ready_count<>member_count), so it is now gated on the same proof: no
--     member may be stale or expected-but-missing.
--   INVOICE_CORRECTION_TSFIN_STALE      never skippable.  "Stale TSFIN always
--     blocks."  It is not named below and the final `return false` covers it.
--
-- Anything else returns false, so this function can never widen the closed
-- enumeration of weekly_source_invoice_issue_skippable_code_v1.
--
-- ===========================================================================
-- WP-33.  THIS FUNCTION IS WHERE THE ORDERING DEPENDENCE ACTUALLY LIVED, and
-- it is the second half of the round 8 Part 2.1 instruction "do not rely on the
-- current lowest-ordinal-code masking".
--
-- Before WP-33, an awaiting-first-authorisation invoice issued for one reason
-- only: private._invoice_correction_validate_batch derives
-- expected_invoice_stream = NORMAL when there is no financial row, so the
-- SKIPPABLE INVOICE_CORRECTION_TARGET_STREAM_MISMATCH carried a lower CASE
-- ordinal than INVOICE_CORRECTION_TSFIN_MISSING and its caller surfaces
-- blocker_codes[1].  This function returned FALSE for that state, so the moment
-- TSFIN_MISSING reached the seam - a different corpus, a reordered CASE, a
-- removed stream code - the same conforming invoice would have been refused.
-- The outcome the pack requires was a by-product of which code sorted first.
--
-- It is now decided by the SAME typed disposition both seams consume, computed
-- by private.weekly_source_invoice_tsfin_refusal_v1 from the recorded state.
-- The exemption authorises a skip of exactly the two financial-record codes and
-- of nothing else: INVOICE_CORRECTION_TSFIN_STALE, INVOICE_CORRECTION_MEMBER_MISSING,
-- INVOICE_CORRECTION_UNIT_INVALID and every other correction code are rejected
-- by the first guard below, which is a CLOSED two-member enumeration reached
-- before any state is read.
-- ===========================================================================
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_invoice_issue_tsfin_skippable_v1(
  p_invoice_id uuid,
  p_code text
) returns boolean
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_state jsonb;
  v_refusal jsonb;
  v_verdict jsonb;
  v_disposition text;
  v_stale integer;
  v_missing integer;
  v_proved integer;
begin
  -- The CLOSED enumeration.  Three members, and INVOICE_CORRECTION_TSFIN_STALE
  -- is the third only because HANDOVER 2 CORRECTION ADDENDUM R8A section 1 made
  -- it one; see the wholly-sealed branch below, which is the ONLY branch that
  -- can ever return true for it.  Every other correction code - MEMBER_MISSING,
  -- UNIT_INVALID, the lock, chain, envelope, Client, Contract, week and VAT
  -- codes, and every code a future revision adds - is rejected HERE, before any
  -- state is read, so nothing can be widened through this function by accident.
  if p_invoice_id is null or coalesce(p_code,'') not in (
       'INVOICE_CORRECTION_TSFIN_MISSING',
       'INVOICE_CORRECTION_TSFIN_NOT_READY',
       'INVOICE_CORRECTION_TSFIN_STALE') then
    return false;
  end if;

  -- =======================================================================
  -- WP-33, R8A section 1.  The asynchronous route reaches the Candidate-pay
  -- financial record TWICE: through this owner's own refusal, which the verdict
  -- assembly has already moved to tsfin_deferred_blocker_codes for a wholly
  -- sealed self-bill, and through the ordinary correction engine, whose
  -- INVOICE_CORRECTION_TSFIN_* codes arrive in the seam's ordinary blocker
  -- array.  Moving only the first would leave the second still blocking, and
  -- the boundary would be restored on paper and not in the product - EXECUTED:
  -- before this branch the stale fixture returned
  -- ["INVOICE_CORRECTION_TSFIN_STALE","WEEKLY_SOURCE_ISSUE_TSFIN_STALE"] from
  -- the real asynchronous gate, and removing only the second still refused.
  --
  -- WHOLLY SEALED, PROVED POSITIVELY and from the SAME value the verdict used:
  -- is_source_invoice and ok must both be explicitly boolean true, read
  -- three-valued so absent, JSON null and non-boolean are all the unsafe value.
  -- If wholly-sealed cannot be positively proved this branch does nothing and
  -- the pre-R8A behaviour stands untouched below.
  -- =======================================================================
  v_verdict:=private.weekly_source_invoice_issue_validate_v1(p_invoice_id);
  if coalesce(
       case when pg_catalog.jsonb_typeof(v_verdict->'is_source_invoice')='boolean'
         then (v_verdict->>'is_source_invoice')::boolean end,false)
     and coalesce(
       case when pg_catalog.jsonb_typeof(v_verdict->'ok')='boolean'
         then (v_verdict->>'ok')::boolean end,false)
     and coalesce(
       case when pg_catalog.jsonb_typeof(
              v_verdict->'wholly_sealed_source_self_bill')='boolean'
         then (v_verdict->>'wholly_sealed_source_self_bill')::boolean end,false)
  then
    return true;
  end if;
  if p_code='INVOICE_CORRECTION_TSFIN_STALE' then
    -- Not wholly sealed: stale is exactly as unskippable as it was before R8A.
    return false;
  end if;
  v_state:=private.weekly_source_invoice_tsfin_state_v1(p_invoice_id);

  -- WP-33.  ONE authority.  The typed disposition is derived by the same
  -- predicate both issue seams consume, so the skip decision and the refusal
  -- decision can never diverge, and neither depends on what the ordinary
  -- correction engine surfaced.
  v_refusal:=private.weekly_source_invoice_tsfin_refusal_v1(v_state);
  v_disposition:=coalesce(v_refusal->>'invoice_disposition','');
  if v_disposition<>'WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR'
     or pg_catalog.jsonb_typeof(v_refusal->'blocker_codes')<>'array'
     or pg_catalog.jsonb_array_length(
          coalesce(v_refusal->'blocker_codes','[]'::jsonb))<>0 then
    -- Blocked, or a disposition this function cannot read: never skip.
    return false;
  end if;
  -- There is deliberately NO awaiting-first-authorisation branch here any more.
  -- Every path that could reach one requires a wholly sealed self-bill, and the
  -- wholly-sealed branch above has already returned true for that; a source
  -- invoice that is NOT wholly sealed must not obtain a Candidate-pay skip on
  -- any evidence, first-authorisation proof included.  The typed exemption
  -- still exists and is still returned on the verdict - round 8 Part 2.1
  -- requires it and R8A section 1 names it - it simply is no longer the thing
  -- that unblocks the invoice, because R8A made the whole class non-blocking.

  -- Only two states can ever authorise a skip.  Everything else - stale,
  -- expected-but-missing, undetermined, not evaluated, and an invoice with no
  -- member at all - blocks.  An invoice whose state cannot be established is
  -- not an invoice whose record was proved never applicable.
  if v_state->>'state' not in ('TSFIN_PRESENT','TSFIN_NOT_APPLICABLE_PROVED') then
    return false;
  end if;
  -- A count that cannot be read counts as blocking, never as proved.
  v_stale:=coalesce((v_state#>>'{counts,stale}')::integer,1);
  v_missing:=coalesce((v_state#>>'{counts,expected_but_missing}')::integer,1);
  v_proved:=coalesce((v_state#>>'{counts,not_applicable_proved}')::integer,0);
  if v_stale>0 or v_missing>0
     or coalesce((v_state#>>'{counts,members}')::integer,0)<1 then
    return false;
  end if;
  if p_code='INVOICE_CORRECTION_TSFIN_MISSING' then
    return v_proved>0;
  end if;
  return true;
exception when others then
  return false;
end;
$function$;

-- ---------------------------------------------------------------------------
-- The ordinary evidence requirements 24 section 11 permits the issue owner to
-- skip, and only those.
--
--   "For an invoice that passes that exact source branch, the issue owner skips
--    only ordinary evidence requirements that are irrelevant to source
--    self-billing: Candidate PDF/signature, ordinary Timesheet reference,
--    ordinary expense receipt/mileage evidence and ordinary HealthRoster
--    validation."
--
-- plus 25 section 4 Removed, bullet 3: "Treating the same Timesheet root on an
-- earlier and later source invoice as an illegal split without considering
-- immutable source movements".
--
-- That is ONE rule, so the list below is CLOSED and ENUMERATED.  An earlier
-- revision matched the whole correction family with a LIKE prefix wildcard over
-- the INVOICE_CORRECTION code space.  That skipped all 37 codes the installed
-- private._invoice_correction_validate_batch can emit, and every future code in
-- the family, including controls 24 section 11 never authorised removing.
--
-- The DIRECT entry point is the calibration.  public.invoice_issue_one skips
-- exactly one ordinary correction control for an admitted source self-bill:
-- public._ctms_assert_invoice_correction_lines_v1, whose only refusal is
-- INVOICE_CORRECTION_LINES_NOT_UNIT_SAFE and whose only test is that the
-- correction unit pair placement is COMPLETE_SAME_INVOICE or
-- COMPLETE_SPLIT_INVOICES
-- (21072026_1235_00b_import_correction_runtime_guards.sql:3370-3401).  It
-- applies no stream, TSFIN, lock, chain, envelope, Client, Contract, week or
-- VAT rule at all.  24 section 11 requires both entry points to use the same
-- source-aware rule, so the asynchronous list is the asynchronous spelling of
-- that same judgement, and nothing more.
--
-- The six correction codes below are exactly the codes whose installed
-- predicate is the ordinary correction-unit engine judging an IMMUTABLE SOURCE
-- MOVEMENT by ordinary correction-unit truth.  Each is named with the predicate
-- that produces it, in
-- 23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_correction_validate_batch.sql:
--
--   UNIT_SPLIT_ACROSS_INVOICES  :868  cardinality(conflicting_ids)>0 - the same
--     Timesheet root carried by an earlier and a later source invoice.  This is
--     25 section 4 Removed bullet 3 word for word.
--   PAIR_PLACEMENT_INCOMPLETE   :861  pair scope placement_state='INCOMPLETE_MOVE'
--     - the same judgement while a 24 section 12 one-line move is in flight.
--     The direct owner tolerates COMPLETE_SPLIT_INVOICES for the same reason.
--   STREAM_MISMATCH             :790, :826, :856  the unit ordinary invoice
--     stream, derived from timesheets_financials and the ordinary Client
--     self-bill flag, is not the manifest-led self-bill stream the header
--     declares.  24 section 10 makes the manifest the routing authority for a
--     source self-bill, and the source branch proves header origin, Client and
--     source group itself.
--   TARGET_STREAM_MISMATCH      :796  the same fact, target side.
--   TSFIN_MISSING               :842  missing_financial_count>0 - the Candidate
--     financial snapshot does not exist.  24 section 10: "Candidate pay and
--     payment state do not control invoice eligibility."
--   TSFIN_NOT_READY             :846  ready_count<>member_count for the same
--     reason.  25 section 5: self-bill finalisation and issue never wait for a
--     protected-pay decision or a frozen payment state.
--
-- Everything else stays, including every control the independent review named
-- as otherwise uncovered: INVOICE_CORRECTION_TSFIN_STALE,
-- FROZEN_POLICY_DRIFT, SOURCE_LOCK_CONFLICT, SEGMENT_LOCK_CONFLICT,
-- CHAIN_CYCLE, CHAIN_DEPTH_EXCEEDED, CHAIN_IDENTITY_INVALID, CLIENT_MISMATCH,
-- TARGET_CLIENT_MISMATCH, CONTRACT_MISMATCH, WEEK_MISMATCH,
-- VAT_POLICY_MISMATCH, MEMBER_MISSING, MEMBER_LIMIT_EXCEEDED, UNIT_INVALID,
-- ROUTE_FAMILY_INVALID, CLASSIFICATION_INVALID, ACTION_INVALID,
-- OPERATION_IDENTITY_INVALID, TARGET_NOT_FOUND, TARGET_NOT_APPENDABLE and
-- every ENVELOPE, LEG and POLICY_FINGERPRINT_MISMATCH code - 31 of the 37 -
-- together with MISSING_HIGHER_RATE_SUPPORT, INVALID_TOTALS,
-- INVOICE_NOT_DRAFT, INVOICE_ON_HOLD, SOURCE_REVISION_CHANGED,
-- EARLY_ISSUE_NOT_ALLOWED, CONFLICTING_ISSUE_OPERATION and every malformed
-- request code, which are untouched on both routes.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_invoice_issue_skippable_code_v1(
  p_code text
) returns boolean
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce(p_code,'') in (
    -- Candidate PDF / signature
    'MISSING_TIMESHEET',
    'MANUAL_TIMESHEET_SOURCE_MISSING',
    'QR_TIMESHEET_UNSIGNED',
    'TIMESHEET_DOCUMENT_FAILED',
    -- ordinary Timesheet reference
    'MISSING_REFERENCE',
    -- ordinary expense receipt / mileage evidence, and the asset lifecycle of
    -- exactly those evidence rows
    'MISSING_MILEAGE_EVIDENCE',
    'MISSING_EXPENSE_EVIDENCE',
    'ASSET_NOT_REGISTERED',
    'ASSET_WORKFLOW_MISSING',
    'REQUIRED_ASSET_FAILED',
    -- ordinary HealthRoster / NHSP import-source validation
    'MISSING_IMPORT_SOURCE_EVIDENCE',
    -- 25 section 4 Removed, bullet 3 - the generic same-unit split judgement.
    -- CLOSED enumeration; there is deliberately no wildcard, so a code added
    -- to the correction family in future is BLOCKING until it is listed here
    -- with its authority.
    'CORRECTION_LINES_NOT_UNIT_SAFE',
    'INVOICE_CORRECTION_UNIT_SPLIT_ACROSS_INVOICES',
    'INVOICE_CORRECTION_PAIR_PLACEMENT_INCOMPLETE',
    'INVOICE_CORRECTION_STREAM_MISMATCH',
    'INVOICE_CORRECTION_TARGET_STREAM_MISMATCH'
    -- INVOICE_CORRECTION_TSFIN_MISSING and INVOICE_CORRECTION_TSFIN_NOT_READY
    -- were listed here and were therefore skipped on the strength of the CODE
    -- TEXT alone, with nothing in this predicate able to look at the invoice.
    -- HANDOVER 2 round 5, Part E forbids that: a missing financial record may
    -- be skipped only where positive route or profile evidence proves it was
    -- never applicable.  They are now decided by
    -- private.weekly_source_invoice_issue_tsfin_skippable_v1, which takes the
    -- invoice id and requires that proof.  INVOICE_CORRECTION_TSFIN_STALE is
    -- named by neither predicate and always blocks.
  );
$function$;

-- ---------------------------------------------------------------------------
-- Adapter for the ASYNCHRONOUS route.  One call, wrapping the blocker array the
-- owner already builds.  For an ordinary invoice the argument is returned
-- unchanged, which is why ordinary issue is byte-identical before and after.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_invoice_issue_blockers_v1(
  p_invoice_id uuid,
  p_ordinary_blockers text[]
) returns text[]
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_verdict jsonb;
  v_result text[];
  v_tsfin text[];
  v_disposition text;
begin
  v_verdict:=private.weekly_source_invoice_issue_validate_v1(p_invoice_id);
  if not coalesce((v_verdict->>'is_source_invoice')::boolean,false) then
    return p_ordinary_blockers;
  end if;
  -- WP-28.  The financial-record refusal of HANDOVER 2 round 5 Part E, raised
  -- by private.weekly_source_invoice_tsfin_refusal_v1 and appended in BOTH
  -- branches below, so it can never be masked by the ordinary correction
  -- engine surfacing a lower-ordinal skippable code first (Gate 13 finding F2).
  if pg_catalog.jsonb_typeof(v_verdict->'tsfin_blocker_codes')='array' then
    select coalesce(pg_catalog.array_agg(code order by ordinality),array[]::text[])
      into v_tsfin
    from pg_catalog.jsonb_array_elements_text(v_verdict->'tsfin_blocker_codes')
      with ordinality tsfin_row(code,ordinality);
  else
    -- A verdict that does not carry the array cannot be judged; fail closed.
    v_tsfin:=array['WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED']::text[];
  end if;
  v_tsfin:=coalesce(v_tsfin,array[]::text[]);

  -- WP-33, HANDOVER 2 round 8 Part 2.1: "Update both authorities so the
  -- exception is explicit rather than hidden by correction-code ordering."
  -- This is the ASYNCHRONOUS authority's half.  The exemption is a value this
  -- seam READS and NAMES, not the absence of a code it happened not to receive:
  -- an unreadable or unknown disposition fails closed, and so does any
  -- disagreement between the disposition and the code array it came with.
  v_disposition:=coalesce(v_verdict->>'tsfin_invoice_disposition','');
  if v_disposition not in (
       'WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR',
       'WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT',
       'WEEKLY_SOURCE_INVOICE_TSFIN_EXPECTED_BUT_MISSING_NOT_ENFORCED',
       'WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE',
       'WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED')
     or (v_disposition='WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED'
         and cardinality(v_tsfin)=0)
     or (v_disposition<>'WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED'
         and cardinality(v_tsfin)<>0) then
    v_tsfin:=v_tsfin
      ||'WEEKLY_SOURCE_ISSUE_TSFIN_DISPOSITION_UNDETERMINED'::text;
  end if;
  if coalesce((v_verdict->>'ok')::boolean,false) then
    -- The exact source branch passed: drop only the irrelevant ordinary
    -- evidence requirements and keep every other ordinary control.
    select coalesce(pg_catalog.array_agg(code order by ordinality),array[]::text[])
      into v_result
    from pg_catalog.unnest(coalesce(p_ordinary_blockers,array[]::text[]))
      with ordinality code_row(code,ordinality)
    where not private.weekly_source_invoice_issue_skippable_code_v1(code_row.code)
      -- The two financial-record codes are decided on evidence, not on text.
      and not private.weekly_source_invoice_issue_tsfin_skippable_v1(
            p_invoice_id,code_row.code);
    return coalesce(v_result,array[]::text[])||v_tsfin;
  end if;
  -- Mixed, stale, tampered or incomplete: fail closed.  Nothing is skipped and
  -- the source refusals are added to whatever the ordinary path already found.
  select coalesce(pg_catalog.array_agg(code order by ordinality),array[]::text[])
    into v_result
  from (
    select code,ordinality from pg_catalog.unnest(
      coalesce(p_ordinary_blockers,array[]::text[])) with ordinality t(code,ordinality)
    union all
    select code,1000000+ordinality from pg_catalog.jsonb_array_elements_text(
      coalesce(v_verdict->'blocker_codes','[]'::jsonb)) with ordinality s(code,ordinality)
  ) merged(code,ordinality);
  return coalesce(v_result,array[]::text[])||v_tsfin;
end;
$function$;

-- ---------------------------------------------------------------------------
-- Adapter for the DIRECT route (public.invoice_issue_one), whose blockers are
-- human-readable reasons rather than codes.  It takes the verdict the owner has
-- already fetched, so the validator is still called exactly once per issue.
-- For an ordinary invoice the argument is returned unchanged.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_invoice_issue_reasons_v1(
  p_verdict jsonb,
  p_ordinary_reasons text[]
) returns text[]
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_result text[];
  v_tsfin text[];
  v_disposition text;
begin
  if not coalesce((p_verdict->>'is_source_invoice')::boolean,false) then
    return p_ordinary_reasons;
  end if;
  -- WP-28.  The same financial-record refusal the asynchronous seam appends,
  -- in this owner's own human-readable spelling, appended in BOTH branches.
  -- Gate 13 finding F2: before this, nothing on the direct route read a
  -- financial-record state at all and a stale source self-bill ISSUED.
  if pg_catalog.jsonb_typeof(p_verdict->'tsfin_blocker_codes')='array' then
    select coalesce(pg_catalog.array_agg(
             'WEEKLY SOURCE: '||code order by ordinality),array[]::text[])
      into v_tsfin
    from pg_catalog.jsonb_array_elements_text(p_verdict->'tsfin_blocker_codes')
      with ordinality tsfin_row(code,ordinality);
  else
    v_tsfin:=array[
      'WEEKLY SOURCE: WEEKLY_SOURCE_ISSUE_TSFIN_STATE_UNDETERMINED']::text[];
  end if;
  v_tsfin:=coalesce(v_tsfin,array[]::text[]);

  -- WP-33, HANDOVER 2 round 8 Part 2.1: "Update both authorities so the
  -- exception is explicit rather than hidden by correction-code ordering."
  -- This is the DIRECT authority's half, and it is deliberately the same test,
  -- written out in full rather than delegated, so neither seam can drift.
  --
  -- The exemption is NOT expressed by adding a non-blocking element to this
  -- array: every element of it is a refusal to public.invoice_issue_one and an
  -- extra element would put the invoice ON_HOLD, which is the opposite of the
  -- ruling.  It is expressed by a named branch on a typed value, and the
  -- returned verdict carries tsfin_invoice_disposition for any surface that
  -- needs to show it.
  v_disposition:=coalesce(p_verdict->>'tsfin_invoice_disposition','');
  if v_disposition not in (
       'WEEKLY_SOURCE_INVOICE_TSFIN_CLEAR',
       'WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT',
       'WEEKLY_SOURCE_INVOICE_TSFIN_EXPECTED_BUT_MISSING_NOT_ENFORCED',
       'WEEKLY_SOURCE_INVOICE_TSFIN_CANDIDATE_PAY_NOT_ADMISSION_PREDICATE',
       'WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED')
     or (v_disposition='WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED'
         and cardinality(v_tsfin)=0)
     or (v_disposition<>'WEEKLY_SOURCE_INVOICE_TSFIN_BLOCKED'
         and cardinality(v_tsfin)<>0) then
    v_tsfin:=v_tsfin
      ||'WEEKLY SOURCE: WEEKLY_SOURCE_ISSUE_TSFIN_DISPOSITION_UNDETERMINED'::text;
  end if;
  if coalesce((p_verdict->>'ok')::boolean,false) then
    select coalesce(pg_catalog.array_agg(reason order by ordinality),array[]::text[])
      into v_result
    from pg_catalog.unnest(coalesce(p_ordinary_reasons,array[]::text[]))
      with ordinality reason_row(reason,ordinality)
    where not private.weekly_source_invoice_issue_skippable_reason_v1(reason_row.reason);
    return coalesce(v_result,array[]::text[])||v_tsfin;
  end if;
  select coalesce(pg_catalog.array_agg(reason order by ordinality),array[]::text[])
    into v_result
  from (
    select reason,ordinality from pg_catalog.unnest(
      coalesce(p_ordinary_reasons,array[]::text[])) with ordinality t(reason,ordinality)
    union all
    select 'WEEKLY SOURCE: '||code,1000000+ordinality
    from pg_catalog.jsonb_array_elements_text(
      coalesce(p_verdict->'blocker_codes','[]'::jsonb)) with ordinality s(code,ordinality)
  ) merged(reason,ordinality);
  return coalesce(v_result,array[]::text[])||v_tsfin;
end;
$function$;

-- The direct owner's reasons are built from public.v_ts_invoice_precheck
-- statuses and the summary HR validation flag
-- (21072026_1235_59_invoice_issue_one.sql:216-360).  These are the same four
-- skip categories as the code list above, expressed in that owner's own text.
--
-- WP-28, Gate 13 finding F3.  The limb below read `BLOCK_UNSIGNED.*`, and no
-- such precheck status exists: public.v_ts_invoice_precheck emits exactly
-- BLOCK_QR_UNSIGNED, BLOCK_NO_PDF, BLOCK_NO_REFERENCE,
-- BLOCK_NO_MILEAGE_EVIDENCE, BLOCK_NO_EXPENSES_EVIDENCE and OK
-- (08012026_v_ts_invoice_precheck.sql:65-155), and the direct owner's CASE maps
-- every one of those except BLOCK_QR_UNSIGNED to its own sentence, sending only
-- BLOCK_QR_UNSIGNED down the generic `precheck blocker <STATUS>` limb.  The
-- pattern therefore matched nothing that can occur, so the asynchronous route
-- skipped QR_TIMESHEET_UNSIGNED while the Office button put the same conforming
-- source invoice ON_HOLD - executed, and confirmed by
-- `select count(*) from public.v_ts_invoice_precheck where
--  precheck_status='BLOCK_UNSIGNED'` returning 0.
--
-- The ASYNCHRONOUS side is the correct one here and the direct side is
-- corrected to match it, not the other way round: pack 24 section 11 lists
-- "Candidate PDF/signature" among the four ordinary evidence requirements an
-- admitted source self-bill skips, and 24 section 7 forbids Candidate pay or
-- Candidate evidence delaying a valid source invoice.  The remaining
-- `precheck blocker ...` limbs are unreachable while that CASE stands and are
-- retained only so a future change to it cannot silently reopen the gap; each
-- one is a status 24 section 11 names.
--
-- Two direct-route reasons are deliberately NOT skippable and have no
-- asynchronous counterpart: "precheck missing" (no public.v_ts_invoice_precheck
-- row for a member Timesheet) and "summary missing" (no
-- public.v_timesheets_summary_base row).  Neither is Candidate evidence; both
-- mean the ordinary evidence could not be evaluated at all, which fails closed.
create or replace function private.weekly_source_invoice_issue_skippable_reason_v1(
  p_reason text
) returns boolean
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce(p_reason,'') ~
    ('^TS [0-9a-fA-F-]{36}: ('
      -- Candidate PDF / signature
      ||'missing timesheet PDF'
      ||'|precheck blocker BLOCK_NO_PDF'
      ||'|precheck blocker BLOCK_QR_UNSIGNED'
      -- ordinary Timesheet reference
      ||'|missing reference/PO'
      ||'|missing reference/PO for [0-9]+ shift\(s\) \(required to issue\)'
      ||'|precheck blocker BLOCK_NO_REFERENCE'
      -- ordinary expense receipt / mileage evidence
      ||'|missing mileage evidence'
      ||'|missing expenses evidence'
      ||'|precheck blocker BLOCK_NO_MILEAGE_EVIDENCE'
      ||'|precheck blocker BLOCK_NO_EXPENSES_EVIDENCE'
      -- ordinary HealthRoster validation
      ||'|HR validation not passed'
      ||')$');
$function$;

alter function private.weekly_source_invoice_issue_validate_v1(uuid) owner to postgres;
alter function private.weekly_source_invoice_tsfin_state_v1(uuid) owner to postgres;
alter function private.weekly_source_invoice_tsfin_refusal_v1(jsonb) owner to postgres;
alter function private.weekly_source_invoice_tsfin_first_authorisation_v1(uuid)
  owner to postgres;
alter function private.weekly_source_invoice_issue_tsfin_skippable_v1(uuid,text)
  owner to postgres;
alter function private.weekly_source_invoice_issue_skippable_code_v1(text) owner to postgres;
alter function private.weekly_source_invoice_issue_skippable_reason_v1(text) owner to postgres;
alter function private.weekly_source_invoice_issue_blockers_v1(uuid,text[]) owner to postgres;
alter function private.weekly_source_invoice_issue_reasons_v1(jsonb,text[]) owner to postgres;

revoke all on function private.weekly_source_invoice_issue_validate_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_tsfin_state_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_tsfin_refusal_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_tsfin_first_authorisation_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_issue_tsfin_skippable_v1(uuid,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_issue_skippable_code_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_issue_skippable_reason_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_issue_blockers_v1(uuid,text[])
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_issue_reasons_v1(jsonb,text[])
  from public,anon,authenticated,service_role;

comment on function private.weekly_source_invoice_issue_validate_v1(uuid) is
  'Gate 7 G7-3. The single source-aware invoice issue validator, reached from both real issue entry points. Returns is_source_invoice=false for every ordinary invoice so the existing issue path is unchanged; a Weekly final-source self-bill is admitted only when the exact 24 section 11 predicate holds, and any mixed, stale, tampered or incomplete invoice fails closed.';

comment on function private.weekly_source_invoice_issue_blockers_v1(uuid,text[]) is
  'Asynchronous issue seam. Returns the caller''s blocker array unchanged for an ordinary invoice; for an admitted source self-bill it removes only the ordinary evidence codes 24 section 11 lists, and for a failing source invoice it appends the source refusals without removing anything.';

comment on function private.weekly_source_invoice_issue_reasons_v1(jsonb,text[]) is
  'Direct issue seam for public.invoice_issue_one. Same rule as the asynchronous seam, expressed against that owner''s human-readable precheck reasons.';

comment on function private.weekly_source_invoice_tsfin_state_v1(uuid) is
  'HANDOVER 2 round 5 Part E. Records the three Candidate financial-record states of a source invoice separately and returns them: TSFIN_STALE, TSFIN_EXPECTED_BUT_MISSING (the default for every absent record) and TSFIN_NOT_APPLICABLE_PROVED (skippable, and only when positive route and profile evidence proves the record was never applicable and never required). Absence is never evidence, and an owner that cannot establish the state returns TSFIN_STATE_UNDETERMINED, which blocks. WP-33 adds a SECOND, ORTHOGONAL classification of the same members without altering any of the above: counts.expected_but_missing keeps its exact former value and is split into counts.awaiting_first_authorisation and counts.expected_but_missing_unexplained, each member carries its typed invoice_disposition and the first_authorisation proof document behind it, and the owner returns its own invoice_disposition_claim, which private.weekly_source_invoice_tsfin_refusal_v1 re-derives independently and refuses on disagreement.';

comment on function private.weekly_source_invoice_issue_tsfin_skippable_v1(uuid,text) is
  'HANDOVER 2 round 5 Part E and round 8 Part 2.1. The evidence gate for INVOICE_CORRECTION_TSFIN_MISSING and INVOICE_CORRECTION_TSFIN_NOT_READY, and the ONE place WP-33 widens behaviour. It returns true in exactly two cases: the pre-existing clean case (no stale and no expected-but-missing member, and for TSFIN_MISSING at least one member proved never applicable), and the round 8 Part 2.1 exemption, when the typed disposition from private.weekly_source_invoice_tsfin_refusal_v1 is WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT, the refusal carries no blocker, and the invoice positively passes the exact 24 section 11 wholly-sealed source branch. Before WP-33 the exempt case issued only because INVOICE_CORRECTION_TARGET_STREAM_MISMATCH carried a lower CASE ordinal than INVOICE_CORRECTION_TSFIN_MISSING and the caller surfaces blocker_codes[1]; it now issues because the state was proved. INVOICE_CORRECTION_TSFIN_STALE, INVOICE_CORRECTION_MEMBER_MISSING, INVOICE_CORRECTION_UNIT_INVALID and every other code are rejected by the closed two-member enumeration before any state is read.';

comment on function private.weekly_source_invoice_tsfin_refusal_v1(jsonb) is
  'WP-28 and WP-33. The single predicate that turns the recorded financial-record state into a refusal, consumed unchanged by BOTH issue seams so nothing depends on which ordinary correction code happens to carry the lowest CASE ordinal. Returns three things and keeps them apart: blocker_codes (what stops the INVOICE), invoice_disposition (the explicit typed invoice outcome) and payment_blocker_codes (what stops PAYMENT and financial publication, retained even when the invoice is exempt). ENFORCED as invoice refusals: TSFIN_STALE (WP-28, HANDOVER 2 round 5 Part E, untouched by WP-33), TSFIN_EXPECTED_BUT_MISSING, and any state, count or disposition claim this predicate cannot read. The ONE exemption, HANDOVER 2 round 8 Part 2.1: an invoice whose every expected-but-missing member is POSITIVELY proved to be awaiting its first authorisation returns invoice_disposition WEEKLY_SOURCE_INVOICE_TSFIN_AWAITING_FIRST_AUTHORISATION_EXEMPT and no invoice blocker, while payment_blocker_codes still carries WEEKLY_SOURCE_PAYMENT_TSFIN_EXPECTED_BUT_MISSING. The exemption requires all seven of X1-X7 including agreement with the state owner''s independent claim, so neither authority alone can widen it, and it cannot reach a stale, missing, invalid, previously applicable or unexplained record.';

comment on function private.weekly_source_invoice_tsfin_first_authorisation_v1(uuid) is
  'WP-33, HANDOVER 2 round 8 Part 2.1. The POSITIVE proof that one member Timesheet''s week is genuinely awaiting its FIRST authorisation, and the only thing that can grant the source-invoice exemption. Three independent families of evidence must all hold - no Weekly Source root-authorisation row has ever existed for the family (live or withdrawn, keyed on member ids and independently on the family booking id), no member carries an ordinary authorised_at_server stamp, and no timesheets_financials row or published projection financial has ever existed for the family - together with no withdrawal receipt, no entitlement head, and three integrity limbs including exact agreement between the invoice-side family resolver and the installed first-authorisation context owner. Every failing limb is returned; the verdict is the conjunction of all of them, so it cannot change if the reasons are reordered or one is removed. A week whose first authorisation was withdrawn under pack 24 section 4.1A is deliberately NOT proved: it is a previously applicable record, which round 8 Part 2.1 excludes by name.';

commit;
