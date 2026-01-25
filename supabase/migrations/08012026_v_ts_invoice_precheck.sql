-- ============================================================
-- CloudTMS: v_ts_invoice_precheck (VIEW)
-- Client-led attach flags + reference gating + evidence gating
-- SAFE TO RE-RUN: CREATE OR REPLACE VIEW (idempotent)
--
-- IMPORTANT:
--  - Keeps existing column order for the first 7 columns
--  - Appends the 3 "effective_*" columns and 1 debug column at the end
--  - Uses London anchor date for selecting the latest effective client_settings row
--  - Derives client_id + claim fields from current TSFIN (timesheets has no client_id)
--
-- UPDATED:
--  - PDF gating (BLOCK_NO_PDF) is satisfied if a timesheet PDF exists via either:
--      * timesheets.manual_pdf_r2_key (MANUAL)
--      * timesheets.generated_pdf_at_utc (non-MANUAL)
--      * timesheet_evidence(kind='TIMESHEET') (any)
--  - Existing expense-only PDF exception retained.
--  - Appends has_timesheet_evidence_pdf for UI/debug.
-- ============================================================

-- CloudTMS: v_ts_invoice_precheck (VIEW)
-- SAFE TO RE-RUN: CREATE OR REPLACE VIEW
-- C6.1: Refs required to ISSUE (not INVOICE)
-- New columns appended at end: reference_number_required_to_issue_invoice, issue_missing_reference, issue_missing_reference_count

-- ============================================================
-- CloudTMS: v_ts_invoice_precheck (VIEW)
-- Client-led attach flags + reference gating + evidence gating
-- SAFE TO RE-RUN: CREATE OR REPLACE VIEW (idempotent)
--
-- IMPORTANT:
--  - Keeps existing column order for the first 7 columns
--  - Appends the 3 "effective_*" columns and 1 debug column at the end
--  - Uses London anchor date for selecting the latest effective client_settings row
--  - Derives client_id + claim fields from current TSFIN (timesheets has no client_id)
--
-- UPDATED:
--  - PDF gating (BLOCK_NO_PDF) is satisfied if a timesheet PDF exists via either:
--      * timesheets.manual_pdf_r2_key (MANUAL)
--      * timesheets.generated_pdf_at_utc (non-MANUAL)
--      * timesheet_evidence(kind='TIMESHEET') (any)
--  - Existing expense-only PDF exception retained.
--  - Appends has_timesheet_evidence_pdf for UI/debug.
-- ============================================================

-- CloudTMS: v_ts_invoice_precheck (VIEW)
-- SAFE TO RE-RUN: CREATE OR REPLACE VIEW
-- C6.1: Refs required to ISSUE (not INVOICE)
-- New columns appended at end: reference_number_required_to_issue_invoice, issue_missing_reference, issue_missing_reference_count

create or replace view public.v_ts_invoice_precheck as
with anchor as (
  select (now() at time zone 'Europe/London')::date as anchor_ymd
)
select
  ts.timesheet_id,
  ts.week_ending_date,
  ts.submission_mode,
  ts.manual_pdf_r2_key,
  ts.reference_number,

  -- ✅ overrideclientsettings applied: contract policy only when overrideclientsettings=true
  coalesce(
    case when c.overrideclientsettings then c.require_reference_to_invoice end,
    cs.invoice_reference_required,
    false
  ) as require_reference_to_invoice,

  case
    -- PDF gating (client-led) — ✅ overrideclientsettings applied for ts_attach_to_invoice
    when coalesce(
           case when c.overrideclientsettings then c.ts_attach_to_invoice end,
           cs.ts_attach_to_invoice,
           sd.ts_attach_to_invoice,
           true
         ) = true
     and (
       (
         ts.submission_mode = 'MANUAL'::submission_mode_enum
         and ts.manual_pdf_r2_key is null
         and coalesce(tepdf.has_timesheet_evidence_pdf, false) = false
       )
       or
       (
         ts.submission_mode is distinct from 'MANUAL'::submission_mode_enum
         and ts.generated_pdf_at_utc is null
         and coalesce(tepdf.has_timesheet_evidence_pdf, false) = false
       )
     )
     and not (
       coalesce(tf.total_hours, 0) = 0
       and (
         coalesce(tf.travel_charge_ex_vat, 0)
         + coalesce(tf.accommodation_charge_ex_vat, 0)
         + coalesce(tf.other_charge_ex_vat, 0)
         + coalesce(tf.mileage_charge_ex_vat, 0)
       ) > 0
     )
      then 'BLOCK_NO_PDF'::text

    -- Reference/PO gating (invoicing):
    -- ✅ Uses effective require_reference_to_invoice (overrideclientsettings-aware)
    -- ✅ Only HOURS require refs; additional/expenses/mileage-only never require refs
    -- ✅ Net non-positive hours (<= 0) never block
    when coalesce(
           case when c.overrideclientsettings then c.require_reference_to_invoice end,
           cs.invoice_reference_required,
           false
         ) = true
     and coalesce(tf.total_hours, 0) > 0
     and coalesce(refchk.missing_raw, false) = true
      then 'BLOCK_NO_REFERENCE'::text

    -- Mileage evidence gating (kind = MILEAGE)
    when (
      (coalesce(tf.mileage_units, 0) > 0)
      or (coalesce(tf.mileage_charge_ex_vat, 0) > 0)
      or (coalesce(tf.mileage_pay_ex_vat, 0) > 0)
    )
    and not exists (
      select 1
      from public.timesheet_evidence te
      where te.timesheet_id = ts.timesheet_id
        and upper(te.kind) = 'MILEAGE'
    )
      then 'BLOCK_NO_MILEAGE_EVIDENCE'::text

    -- Expense category evidence gating (TRAVEL / ACCOMMODATION / OTHER)
    when (
      (
        (coalesce(tf.travel_pay_ex_vat, 0) > 0 or coalesce(tf.travel_charge_ex_vat, 0) > 0)
        and not exists (
          select 1
          from public.timesheet_evidence te
          where te.timesheet_id = ts.timesheet_id
            and upper(te.kind) = 'TRAVEL'
        )
      )
      or
      (
        (coalesce(tf.accommodation_pay_ex_vat, 0) > 0 or coalesce(tf.accommodation_charge_ex_vat, 0) > 0)
        and not exists (
          select 1
          from public.timesheet_evidence te
          where te.timesheet_id = ts.timesheet_id
            and upper(te.kind) = 'ACCOMMODATION'
        )
      )
      or
      (
        (coalesce(tf.other_pay_ex_vat, 0) > 0 or coalesce(tf.other_charge_ex_vat, 0) > 0)
        and not exists (
          select 1
          from public.timesheet_evidence te
          where te.timesheet_id = ts.timesheet_id
            and upper(te.kind) = 'OTHER'
        )
      )
    )
      then 'BLOCK_NO_EXPENSES_EVIDENCE'::text

    else 'OK'::text
  end as precheck_status,

  -- existing appended columns (unchanged order) — ✅ overrideclientsettings applied for attach flags
  coalesce(
    case when c.overrideclientsettings then c.ts_attach_to_invoice end,
    cs.ts_attach_to_invoice,
    sd.ts_attach_to_invoice,
    true
  ) as effective_ts_attach_to_invoice,

  coalesce(
    case when c.overrideclientsettings then c.hr_attach_to_invoice end,
    cs.hr_attach_to_invoice,
    sd.hr_attach_to_invoice,
    true
  ) as effective_hr_attach_to_invoice,

  coalesce(cs.auto_invoice_default, false) as effective_auto_invoice_default,
  coalesce(tepdf.has_timesheet_evidence_pdf, false) as has_timesheet_evidence_pdf,

  -- C6.1 appended columns (must be at the end)
  -- ✅ overrideclientsettings applied: contract ref-to-issue only when overrideclientsettings=true, else client_settings
  coalesce(
    case when c.overrideclientsettings then c.reference_number_required_to_issue_invoice end,
    cs.reference_number_required_to_issue_invoice,
    false
  ) as reference_number_required_to_issue_invoice,

  (
    coalesce(
      case when c.overrideclientsettings then c.reference_number_required_to_issue_invoice end,
      cs.reference_number_required_to_issue_invoice,
      false
    ) = true
    and coalesce(tf.total_hours, 0) > 0
    and coalesce(refchk.missing_raw, false) = true
  ) as issue_missing_reference,

  (
    case
      when coalesce(tf.total_hours, 0) <= 0 then 0
      when coalesce(
             case when c.overrideclientsettings then c.reference_number_required_to_issue_invoice end,
             cs.reference_number_required_to_issue_invoice,
             false
           ) = true
        then coalesce(refchk.missing_count, 0)
      else 0
    end
  )::int as issue_missing_reference_count

from public.timesheets ts
left join public.contract_weeks cw
  on cw.timesheet_id = ts.timesheet_id
left join public.contracts c
  on c.id = coalesce(ts.contract_id, cw.contract_id)

left join public.settings_defaults sd
  on sd.id = 1

-- derive client_id + claim fields from the current TSFIN snapshot
left join lateral (
  select
    tf0.client_id,
    tf0.total_hours,

    -- invoice breakdown (needed for SEGMENTS ref checks)
    tf0.invoice_breakdown_json,

    -- additional (retained for other gating uses; refs do NOT depend on these)
    tf0.additional_units_json,
    tf0.additional_pay_ex_vat,
    tf0.additional_charge_ex_vat,

    -- mileage
    tf0.mileage_units,
    tf0.mileage_pay_ex_vat,
    tf0.mileage_charge_ex_vat,

    -- category expenses
    tf0.travel_pay_ex_vat,
    tf0.travel_charge_ex_vat,
    tf0.accommodation_pay_ex_vat,
    tf0.accommodation_charge_ex_vat,
    tf0.other_pay_ex_vat,
    tf0.other_charge_ex_vat
  from public.timesheets_financials tf0
  where tf0.timesheet_id = ts.timesheet_id
    and tf0.is_current = true
  order by tf0.updated_at desc nulls last
  limit 1
) tf on true

-- timesheet evidence PDF presence (kind = TIMESHEET)
left join lateral (
  select exists(
    select 1
    from public.timesheet_evidence te
    where te.timesheet_id = ts.timesheet_id
      and upper(te.kind) = 'TIMESHEET'
  ) as has_timesheet_evidence_pdf
) tepdf on true

-- choose client_settings row by London "anchor" date
left join lateral (
  select
    cs0.client_id,
    cs0.auto_invoice_default,
    cs0.hr_attach_to_invoice,
    cs0.ts_attach_to_invoice,
    cs0.reference_number_required_to_issue_invoice,
    cs0.invoice_reference_required
  from public.client_settings cs0
  cross join anchor a
  where cs0.client_id = tf.client_id
    and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
  order by cs0.effective_from desc nulls last
  limit 1
) cs on true

-- compute missing reference boolean + count (independent of require-to-invoice / require-to-issue gates)
left join lateral (
  with
  -- SEGMENTS mode: count missing refs for segments that are (a) not already invoiced and (b) net positive
  segs as (
    select
      seg,
      nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') as locked_by,
      nullif(btrim(coalesce(seg->>'ref_num','')), '') as ref_num,
      (
        coalesce(nullif(seg->>'hours_day','')::numeric,0)
        + coalesce(nullif(seg->>'hours_night','')::numeric,0)
        + coalesce(nullif(seg->>'hours_sat','')::numeric,0)
        + coalesce(nullif(seg->>'hours_sun','')::numeric,0)
        + coalesce(nullif(seg->>'hours_bh','')::numeric,0)
      ) as hours_sum,
      coalesce(nullif(seg->>'charge_amount','')::numeric,0) as charge_ex
    from (
      select value as seg
      from jsonb_array_elements(
        case
          when tf.invoice_breakdown_json is not null
           and jsonb_typeof(tf.invoice_breakdown_json) = 'object'
           and upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
           and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
          then tf.invoice_breakdown_json->'segments'
          else '[]'::jsonb
        end
      ) value
    ) s
    where jsonb_typeof(seg) = 'object'
  ),
  segs_pos as (
    select *
    from segs
    where locked_by is null
      and (coalesce(hours_sum,0) > 0 or coalesce(charge_ex,0) > 0)
  ),
  segs_missing as (
    select count(*)::int as cnt
    from segs_pos
    where ref_num is null
  ),

  -- WEEKLY MANUAL: count missing refs per schedule entry with start/end present
  weekly_manual_missing as (
    select
      case
        when ts.actual_schedule_json is null
          or jsonb_typeof(ts.actual_schedule_json) <> 'array'
        then 1
        when jsonb_array_length(ts.actual_schedule_json) = 0
        then 1
        else (
          select count(*)::int
          from jsonb_array_elements(ts.actual_schedule_json) seg(value)
          where coalesce(btrim(seg.value->>'start'), '') <> ''
            and coalesce(btrim(seg.value->>'end'), '') <> ''
            and coalesce(btrim(seg.value->>'ref_num'), '') = ''
        )
      end as cnt
  ),

  -- WEEKLY non-MANUAL aggregate: require at least one ref (freeform array OR any non-empty day ref value)
  weekly_nonmanual_has_any as (
    select
      (
        exists (
          select 1
          from jsonb_array_elements_text(
            case
              when ts.day_references_json is not null
               and jsonb_typeof(ts.day_references_json) = 'object'
               and jsonb_typeof(ts.day_references_json->'__freeform_refs') = 'array'
              then ts.day_references_json->'__freeform_refs'
              when ts.day_references_json is not null
               and jsonb_typeof(ts.day_references_json) = 'object'
               and jsonb_typeof(ts.day_references_json->'__freeform') = 'array'
              then ts.day_references_json->'__freeform'
              when ts.day_references_json is not null
               and jsonb_typeof(ts.day_references_json) = 'object'
               and jsonb_typeof(ts.day_references_json->'__freeform_lines') = 'array'
              then ts.day_references_json->'__freeform_lines'
              when ts.day_references_json is not null
               and jsonb_typeof(ts.day_references_json) = 'array'
              then ts.day_references_json
              else '[]'::jsonb
            end
          ) t(x)
          where nullif(btrim(coalesce(t.x,'')), '') is not null
        )
        or
        exists (
          select 1
          from jsonb_each_text(
            case
              when ts.day_references_json is not null and jsonb_typeof(ts.day_references_json) = 'object'
              then ts.day_references_json
              else '{}'::jsonb
            end
          ) j(k,v)
          where nullif(btrim(coalesce(j.v,'')), '') is not null
            and left(coalesce(j.k,''), 2) <> '__'
        )
      ) as has_any
  )

  select
    (
      case
        when tf.invoice_breakdown_json is not null
         and jsonb_typeof(tf.invoice_breakdown_json) = 'object'
         and upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
         and coalesce((select cnt from segs_missing),0) > 0
        then true

        when tf.invoice_breakdown_json is not null
         and jsonb_typeof(tf.invoice_breakdown_json) = 'object'
         and upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
         and coalesce((select cnt from segs_missing),0) = 0
        then false

        when ts.sheet_scope = 'DAILY'::timesheet_scope_enum then
          (ts.reference_number is null or length(btrim(ts.reference_number)) = 0)

        when ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum
         and ts.submission_mode = 'MANUAL'::submission_mode_enum then
          (coalesce((select cnt from weekly_manual_missing),0) > 0)

        when ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum
         and ts.submission_mode <> 'MANUAL'::submission_mode_enum then
          (coalesce((select has_any from weekly_nonmanual_has_any), false) = false)

        else false
      end
    ) as missing_raw,

    (
      case
        when tf.invoice_breakdown_json is not null
         and jsonb_typeof(tf.invoice_breakdown_json) = 'object'
         and upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
        then coalesce((select cnt from segs_missing),0)

        when ts.sheet_scope = 'DAILY'::timesheet_scope_enum then
          case when (ts.reference_number is null or length(btrim(ts.reference_number)) = 0) then 1 else 0 end

        when ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum
         and ts.submission_mode = 'MANUAL'::submission_mode_enum then
          coalesce((select cnt from weekly_manual_missing),0)

        when ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum
         and ts.submission_mode <> 'MANUAL'::submission_mode_enum then
          case when coalesce((select has_any from weekly_nonmanual_has_any), false) = false then 1 else 0 end

        else 0
      end
    ) as missing_count
) refchk on true;

