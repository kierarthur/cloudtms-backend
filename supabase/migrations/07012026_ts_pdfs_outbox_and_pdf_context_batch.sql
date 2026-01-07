create or replace function public.timesheet_pdf_load_context_batch(p_timesheet_ids uuid[])
returns table (
  timesheet_id uuid,
  out_ts jsonb,
  out_summary jsonb,
  out_contract jsonb,
  out_client jsonb,
  out_candidate jsonb,
  out_fin jsonb,
  out_def jsonb
)
language sql
stable
as $$
with wanted as (
  select distinct unnest(p_timesheet_ids) as timesheet_id
  where p_timesheet_ids is not null
),
t as (
  select ts.*
  from wanted w
  join public.timesheets ts
    on ts.timesheet_id = w.timesheet_id
   and ts.is_current = true
),
s as (
  -- ONLY fields that actually exist on v_timesheets_summary and are needed for identity resolution
  select
    t.timesheet_id,
    vs.candidate_id,
    vs.client_id,
    vs.candidate_name,
    vs.client_name,
    vs.contract_id
  from t
  left join public.v_timesheets_summary vs
    on vs.timesheet_id = t.timesheet_id
),
c as (
  select t.timesheet_id, ct.*
  from t
  left join s on s.timesheet_id = t.timesheet_id
  left join public.contracts ct
    on ct.id = coalesce(t.contract_id, s.contract_id)
),
ids as (
  select
    t.timesheet_id,
    coalesce(c.candidate_id, s.candidate_id) as eff_candidate_id,
    coalesce(c.client_id,    s.client_id)    as eff_client_id
  from t
  left join s on s.timesheet_id = t.timesheet_id
  left join c on c.timesheet_id = t.timesheet_id
)
select
  t.timesheet_id,
  to_jsonb(t) as out_ts,
  to_jsonb(s) as out_summary,
  to_jsonb(c) as out_contract,
  to_jsonb(cl) as out_client,
  to_jsonb(ca) as out_candidate,
  to_jsonb(tf) as out_fin,
  jsonb_build_object(
    'agency_name', sd.agency_name,
    'agency_logo', sd.agency_logo,
    'timesheet_header_json', sd.timesheet_header_json,
    'timesheet_footer_json', sd.timesheet_footer_json,

    -- TEXT declaration columns do NOT exist in settings_defaults (keep keys for renderer compatibility):
    'temporary_worker_declaration', null::text,
    'client_declaration', null::text,

    -- JSON declarations DO exist:
    'temporary_worker_declaration_json', sd.temporary_worker_declaration_json,
    'client_declaration_json', sd.client_declaration_json
  ) as out_def
from t
left join s on s.timesheet_id = t.timesheet_id
left join c on c.timesheet_id = t.timesheet_id
left join ids on ids.timesheet_id = t.timesheet_id
left join public.clients    cl on cl.id = ids.eff_client_id
left join public.candidates ca on ca.id = ids.eff_candidate_id
left join public.timesheets_financials tf
  on tf.timesheet_id = t.timesheet_id
 and tf.is_current = true
left join public.settings_defaults sd on sd.id = 1;
$$;
