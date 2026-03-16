begin;

-- ============================================================================
-- Approved mailshot source views
-- ----------------------------------------------------------------------------
-- Convention:
--   1) Any column starting with "_" is a helper column, not a merge field.
--   2) Future catalogue-sync logic should ignore underscore-prefixed columns.
--   3) These views are safe to rerun via CREATE OR REPLACE VIEW.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Candidate
-- ----------------------------------------------------------------------------
create or replace view public.v_mailshot_src_candidate as
select
  c.id                                            as _context_id,
  c.tms_ref,
  c.title,
  c.first_name,
  c.last_name,
  c.display_name,
  c.email,
  c.phone,
  c.pay_method,
  c.active,
  c.band,
  u.name                                          as umbrella_name
from public.candidates c
left join public.umbrellas u
  on u.id = c.umbrella_id;

-- ----------------------------------------------------------------------------
-- Client
-- ----------------------------------------------------------------------------
create or replace view public.v_mailshot_src_client as
select
  c.id                                            as _context_id,
  c.cli_ref,
  c.name,
  c.invoice_address,
  c.primary_invoice_email,
  c.ap_phone,
  c.ts_queries_email,
  c.contact_title,
  c.contact_known_as,
  c.contact_forename,
  c.contact_surname,
  c.contact_job_title,
  c.contact_tel,
  c.contact_mobile,
  c.contact_email,
  c.website,
  c.vat_chargeable,
  c.payment_terms_days,
  cs.default_submission_mode,
  cs.week_ending_weekday,
  cs.pay_reference_required,
  cs.invoice_reference_required,
  cs.auto_invoice_default
from public.clients c
left join lateral (
  select cs.*
  from public.client_settings cs
  where cs.client_id = c.id
  order by
    cs.effective_from desc nulls last,
    cs.updated_at desc nulls last,
    cs.created_at desc nulls last,
    cs.id desc
  limit 1
) cs on true;

-- ----------------------------------------------------------------------------
-- Contract
-- ----------------------------------------------------------------------------
create or replace view public.v_mailshot_src_contract as
select
  c.id                                            as _context_id,
  c.role,
  c.band,
  c.display_site,
  c.ward_hint,
  c.start_date,
  c.end_date,
  c.pay_method_snapshot,
  c.default_submission_mode,
  c.week_ending_weekday_snapshot,
  c.auto_invoice,
  c.require_reference_to_pay,
  c.require_reference_to_invoice,
  c.self_bill,
  c.weekly_timesheet_source,
  c.no_timesheet_required,
  c.daily_calc_of_invoices,
  c.group_nightsat_sunbh,
  c.is_nhsp,
  c.autoprocess_hr,
  c.requires_hr,
  c.hr_attach_to_invoice,
  c.ts_attach_to_invoice,
  c.reference_number_required_to_issue_invoice,
  c.send_manual_invoices_to_different_email,
  c.manual_invoices_alt_email_address,
  c.is_ad_hoc
from public.contracts c;

-- ----------------------------------------------------------------------------
-- Timesheet
-- Single visible family: operational + approved current TSFIN fields
-- ----------------------------------------------------------------------------
create or replace view public.v_mailshot_src_timesheet as
select
  t.timesheet_id                                  as _context_id,

  -- operational fields
  t.booking_id,
  t.week_ending_date,
  t.sheet_scope,
  t.submission_mode,
  t.status,
  t.reference_number,
  t.reference_set_at,
  t.auth_name,
  t.auth_job_title,
  t.authorised_at_server,
  t.scheduled_start_iso,
  t.scheduled_end_iso,
  t.worked_start_iso,
  t.worked_end_iso,
  t.break_start_iso,
  t.break_end_iso,
  t.break_minutes,
  t.worked_minutes,
  t.hospital_norm,
  t.ward_norm,
  t.job_title_norm,
  t.shift_label_norm,
  t.line_type,
  t.generated_pdf_at_utc,

  -- approved finance/state fields from current TSFIN
  tf.basis,
  tf.processing_status,
  tf.pay_method,
  tf.role,
  tf.band,
  tf.candidate_assignment,
  tf.hours_day,
  tf.hours_night,
  tf.hours_sat,
  tf.hours_sun,
  tf.hours_bh,
  tf.total_hours,

  tf.pay_day,
  tf.pay_night,
  tf.pay_sat,
  tf.pay_sun,
  tf.pay_bh,

  tf.charge_day,
  tf.charge_night,
  tf.charge_sat,
  tf.charge_sun,
  tf.charge_bh,

  tf.total_pay_ex_vat,
  tf.total_charge_ex_vat,
  tf.margin_ex_vat,

  tf.expenses_pay_ex_vat,
  tf.expenses_charge_ex_vat,

  tf.mileage_units,
  tf.mileage_pay_rate,
  tf.mileage_charge_rate,
  tf.mileage_pay_ex_vat,
  tf.mileage_charge_ex_vat,

  tf.travel_pay_ex_vat,
  tf.travel_charge_ex_vat,
  tf.accommodation_pay_ex_vat,
  tf.accommodation_charge_ex_vat,
  tf.other_pay_ex_vat,
  tf.other_charge_ex_vat,

  tf.additional_pay_ex_vat,
  tf.additional_charge_ex_vat,
  tf.additional_margin_ex_vat,

  tf.pay_on_hold,
  tf.pay_on_hold_reason,
  tf.paid_at_utc,
  tf.payment_reference,

  (tf.locked_by_invoice_id is not null)          as invoice_locked,
  tf.locked_by_invoice_id

from public.timesheets t
left join lateral (
  select tf.*
  from public.timesheets_financials tf
  where tf.timesheet_id = t.timesheet_id
    and tf.is_current = true
  order by
    tf.updated_at desc nulls last,
    tf.created_at desc nulls last,
    tf.id desc
  limit 1
) tf on true;

-- ----------------------------------------------------------------------------
-- Invoice
-- Single visible family: operational/header + historically safe snapshot totals
-- ----------------------------------------------------------------------------
create or replace view public.v_mailshot_src_invoice as
with line_agg as (
  select
    il.invoice_id,
    count(*)                                         as line_count,
    count(distinct il.timesheet_id)                  as timesheet_count,

    coalesce(sum(il.hours_day), 0)                   as hours_day,
    coalesce(sum(il.hours_night), 0)                 as hours_night,
    coalesce(sum(il.hours_sat), 0)                   as hours_sat,
    coalesce(sum(il.hours_sun), 0)                   as hours_sun,
    coalesce(sum(il.hours_bh), 0)                    as hours_bh,

    coalesce(sum(il.hours_day), 0)
      + coalesce(sum(il.hours_night), 0)
      + coalesce(sum(il.hours_sat), 0)
      + coalesce(sum(il.hours_sun), 0)
      + coalesce(sum(il.hours_bh), 0)                as total_hours,

    coalesce(sum(il.total_pay_ex_vat), 0)            as total_pay_ex_vat_lines,
    coalesce(sum(il.total_charge_ex_vat), 0)         as total_charge_ex_vat_lines,
    coalesce(sum(il.margin_ex_vat), 0)               as margin_ex_vat_lines,
    coalesce(sum(il.vat_amount), 0)                  as vat_amount_lines,
    coalesce(sum(il.total_inc_vat), 0)               as total_inc_vat_lines
  from public.invoice_lines il
  group by il.invoice_id
)
select
  i.id                                              as _context_id,

  -- operational/header fields
  i.invoice_no,
  i.type,
  i.status,
  i.status_date_utc,
  i.issued_at_utc,
  i.due_at_utc,
  i.paid_at_utc,
  i.notes,
  i.invoice_pdf_generated_at_utc,

  -- header snapshot totals
  i.subtotal_ex_vat,
  i.vat_amount,
  i.total_inc_vat,

  -- verified line-level historical aggregates
  coalesce(la.line_count, 0)                        as line_count,
  coalesce(la.timesheet_count, 0)                   as timesheet_count,
  coalesce(la.hours_day, 0)                         as hours_day,
  coalesce(la.hours_night, 0)                       as hours_night,
  coalesce(la.hours_sat, 0)                         as hours_sat,
  coalesce(la.hours_sun, 0)                         as hours_sun,
  coalesce(la.hours_bh, 0)                          as hours_bh,
  coalesce(la.total_hours, 0)                       as total_hours,
  coalesce(la.total_pay_ex_vat_lines, 0)            as total_pay_ex_vat_lines,
  coalesce(la.total_charge_ex_vat_lines, 0)         as total_charge_ex_vat_lines,
  coalesce(la.margin_ex_vat_lines, 0)               as margin_ex_vat_lines,
  coalesce(la.vat_amount_lines, 0)                  as vat_amount_lines,
  coalesce(la.total_inc_vat_lines, 0)               as total_inc_vat_lines

from public.invoices i
left join line_agg la
  on la.invoice_id = i.id;

-- ----------------------------------------------------------------------------
-- Umbrella
-- ----------------------------------------------------------------------------
create or replace view public.v_mailshot_src_umbrella as
select
  u.id                                            as _context_id,
  u.name,
  u.remittance_email,
  u.enabled,
  u.vat_chargeable,
  u.company_number,
  u.address_line1,
  u.address_line2,
  u.address_line3,
  u.town_city,
  u.county,
  u.postcode,
  u.country
from public.umbrellas u;

-- ----------------------------------------------------------------------------
-- System
-- ----------------------------------------------------------------------------
create or replace view public.v_mailshot_src_system as
select
  'system'::text                                  as _context_key,
  (now() at time zone 'Europe/London')::date      as today_uk,
  to_char((now() at time zone 'Europe/London')::date, 'YYYY-MM-DD')
                                                 as today_ymd,
  to_char((now() at time zone 'Europe/London')::date, 'DD/MM/YYYY')
                                                 as today_ddmmyyyy,
  now()                                           as now_utc,
  (now() at time zone 'Europe/London')            as now_uk;

-- ----------------------------------------------------------------------------
-- Resolution graph
-- ----------------------------------------------------------------------------
create or replace view public.v_mailshot_resolution_graph as
select *
from (
  values
    ('candidate'::text, 'candidate'::text, 'v_mailshot_src_candidate'::text),
    ('candidate'::text, 'umbrella'::text,  'v_mailshot_src_umbrella'::text),
    ('candidate'::text, 'system'::text,    'v_mailshot_src_system'::text),

    ('client'::text,    'client'::text,    'v_mailshot_src_client'::text),
    ('client'::text,    'system'::text,    'v_mailshot_src_system'::text),

    ('contract'::text,  'contract'::text,  'v_mailshot_src_contract'::text),
    ('contract'::text,  'candidate'::text, 'v_mailshot_src_candidate'::text),
    ('contract'::text,  'client'::text,    'v_mailshot_src_client'::text),
    ('contract'::text,  'umbrella'::text,  'v_mailshot_src_umbrella'::text),
    ('contract'::text,  'system'::text,    'v_mailshot_src_system'::text),

    ('timesheet'::text, 'timesheet'::text, 'v_mailshot_src_timesheet'::text),
    ('timesheet'::text, 'contract'::text,  'v_mailshot_src_contract'::text),
    ('timesheet'::text, 'candidate'::text, 'v_mailshot_src_candidate'::text),
    ('timesheet'::text, 'client'::text,    'v_mailshot_src_client'::text),
    ('timesheet'::text, 'umbrella'::text,  'v_mailshot_src_umbrella'::text),
    ('timesheet'::text, 'system'::text,    'v_mailshot_src_system'::text),

    ('invoice'::text,   'invoice'::text,   'v_mailshot_src_invoice'::text),
    ('invoice'::text,   'client'::text,    'v_mailshot_src_client'::text),
    ('invoice'::text,   'system'::text,    'v_mailshot_src_system'::text),

    ('umbrella'::text,  'umbrella'::text,  'v_mailshot_src_umbrella'::text),
    ('umbrella'::text,  'system'::text,    'v_mailshot_src_system'::text)
) as g(root_entity_type, source_family, source_view_name);

commit;
